const std = @import("std");
const os = std.os;
const linux = os.linux;
const system = std.os.system;
const errno = std.os.system.getErrno;
const iovec = os.iovec;
const iovec_const = os.iovec_const;
const time = std.time;
const Address = std.net.Address;
const print = std.debug.print;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const CircularQueue = @import("circular_queue.zig").CircularQueue;
const lib_reactor = @import("reactor.zig");
const util = @import("util.zig");
const Reactor = lib_reactor.Reactor;
const ReactorTask = lib_reactor.ReactorTask;
const CompletionHandler = lib_reactor.CompletionHandler;
//const E = std.os.linux.errno.generic.E;
const IO_Uring = linux.IO_Uring;
const RecvBuffer = IO_Uring.RecvBuffer;

const c = @cImport({
    @cInclude("sys/eventfd.h");
    @cInclude("fcntl.h");
    @cInclude("unistd.h");
    @cInclude("sys/socket.h");
    @cInclude("netinet/tcp.h");
    @cInclude("fcntl.h");
    @cInclude("arpa/inet.h");
    @cInclude("netinet/in.h");
    @cInclude("netinet/ip.h");
});

const WriteEntry = struct { buf: []u8, len: usize, handler: *WriteCompleteHandler };

const WriteQueue = CircularQueue(WriteEntry);

pub const ConnectHandler = struct {
     onComplete: *const fn (iface: *ConnectHandler, res: i32) anyerror!void,
 };

pub const WriteCompleteHandler = struct {
     onComplete: *const fn (iface: *WriteCompleteHandler, buf:[]u8) anyerror!void,
};

pub const ReadCompleteHandler = struct {
     onComplete: *const fn (iface: *ReadCompleteHandler, buf:[]u8, res:i32) anyerror!void,
};

pub const SocketConfig = struct {
    ipv4: bool = true,
    allocator: Allocator,
    fd: ?i32 = null,
    reactor: *Reactor,
};

pub const Socket = struct {
    fd: i32,
    reactor: *Reactor,
    pending_queue: WriteQueue,
    write_queue: WriteQueue,
    write_completed_iface: CompletionHandler,
    read_completed_iface: CompletionHandler,
    connect_completed_iface: CompletionHandler,
    write_schedule_iface: ReactorTask,
    write_scheduled: bool = false,
    io_vec: [100]iovec_const = undefined,
    bytes_written: u64,
    bytes_read: u64,
    connect_handler: ?*ConnectHandler = undefined,
    read_handler: ?*ReadCompleteHandler,
    read_buffer: ?[]u8,

    pub fn init(config: SocketConfig) !*Socket {
        var self = try config.allocator.create(Socket);
        
        if (config.fd) |fd| {
            self.fd = fd;
        } else {
            const domain: c_int = if (config.ipv4) c.AF_INET else c.AF_INET6;
            const res = c.socket(domain, c.SOCK_STREAM, 0);
            if (res < 0) {
                return os.unexpectedErrno(util.errno(res));
            }
            self.fd = res;
        }
        self.reactor = config.reactor;
        self.read_completed_iface = CompletionHandler{ .handle = handleReadComplete };
        self.read_handler = null;
        self.read_buffer = null;
        self.connect_handler = null;
        self.connect_completed_iface = CompletionHandler{ .handle = handleConnectComplete };
        self.write_completed_iface = CompletionHandler{ .handle = handleWriteComplete };
        self.write_schedule_iface = ReactorTask{ .exec = handleWriteStart };
        self.write_queue = try WriteQueue.init(1024, config.allocator);
        self.write_scheduled = false;
        self.pending_queue = try WriteQueue.init(1024, config.allocator);
        self.bytes_written = 0;
        self.bytes_read = 0;
        return self;
    }

    // todo: Should be non blocking
    pub fn connect(self: *Socket, addr: *Address, handler:*ConnectHandler) !void {
        if (self.reactor.debug) {
            std.debug.print("connect to {}\n", .{ addr });
        }

        // todo: check if already trying to connect.
        assert(self.connect_handler==null);

        self.connect_handler = handler;
        const user_data: u64 = @ptrToInt(&self.connect_completed_iface);
        _ = try self.reactor.uring.connect(user_data, self.fd, &addr.any, addr.getOsSockLen());
    }

    fn handleConnectComplete(iface: *CompletionHandler, res: i32, flags: u32) !void {
        var self = @fieldParentPtr(Socket, "connect_completed_iface", iface);

        if (self.reactor.debug) {
            std.debug.print("handleConnectComplete res={} flags={}\n", .{ res, flags });
        }

        if (res < 0) {
            const e =  util.errno(res);
            std.debug.print("Failed tp cpmmect socket. errno {} ", .{e});
            return os.unexpectedErrno(e);
        }

        var connect_handler = self.connect_handler orelse unreachable;  

        try connect_handler.onComplete(connect_handler, res);
    }

    pub fn bind(self: *Socket, addr: Address) !void {
        try os.bind(self.fd, &addr.any, addr.getOsSockLen());
    }

    pub fn accept(self: *Socket, handler: *CompletionHandler, addr: ?*os.sockaddr, addrlen: ?*os.socklen_t) !void {
        // Submit 1 accept
        // var accept_addr: os.sockaddr = undefined;
        // var accept_addr_len: os.socklen_t = @sizeOf(@TypeOf(accept_addr));
        // _ = try ring.accept(0xaaaaaaaa, listener_socket, &accept_addr, &accept_addr_len, 0);


        _ = try self.reactor.uring.accept(@ptrToInt(handler), self.fd, addr, addrlen, 0);
    }

    pub fn listen(self: *Socket, backlog: u16) !void {
        const res = c.listen(self.fd, backlog);

        if (res < 0) {
            return os.unexpectedErrno(util.errno(res));
        }
    }

    pub fn read(self: *Socket, buf: []u8, handler: *ReadCompleteHandler) !void {
        if (self.reactor.debug) {
            std.debug.print("{s} starting socket read\n", .{self.reactor.name});
        }

        if (self.read_handler != null or self.read_buffer != null) {
            std.debug.print("{s} unwanted read handler on socket\n", .{self.reactor.name});
            //std.debug.printStackTrace();
            // No concurrent reads.
            return error.oops;
        }

        self.read_handler = handler;
        self.read_buffer = buf;

        var read_buffer = RecvBuffer{ .buffer = buf };
        const user_data: u64 = @ptrToInt(&self.read_completed_iface);
        // recv is faster than read.
        _ = try self.reactor.uring.recv(user_data, self.fd, read_buffer, 0);

        //std.debug.print("{s} starting socket read issued\n", .{self.reactor.name});
    }

    fn handleReadComplete(iface: *CompletionHandler, res: i32, flags: u32) !void {
        
        var self = @fieldParentPtr(Socket, "read_completed_iface", iface);

        if (self.reactor.debug) {
            std.debug.print("{s} socket read completed res {}\n", .{self.reactor.name, res});
        }

        if (res >= 0) {
            if (res == 0){
                std.debug.print("{s} close socket due to EOF\n", .{self.reactor.name});
                self.close();
            }

            const read_handler = self.read_handler orelse unreachable;  
            const read_buffer = self.read_buffer orelse unreachable;  
            self.read_handler = null;
            self.read_buffer = null;
            try read_handler.onComplete(read_handler, read_buffer, res);
        } else {
            std.debug.print("Failed to complete read res={} flags={}\n", .{util.errno(res), flags});
            return error.oops;
        }
    }

    pub fn write(self: *Socket, buf: []u8, len: usize, handler: *WriteCompleteHandler) !void {
        if (self.reactor.debug) {
            std.debug.print("{s} write len {}\n", .{self.reactor.name, len});
        }

        const write_entry = WriteEntry{ .buf = buf, .len = len, .handler = handler };
        
        if (!self.write_queue.offer(write_entry)) {
            return error.oops;
        }

        if (!self.write_scheduled) {
            self.write_scheduled = true;

            if (!self.reactor.offer(&self.write_schedule_iface)) {
                return error.oops;
            }
        }
    }

    fn handleWriteStart(iface: *ReactorTask) !void {
        var self = @fieldParentPtr(Socket, "write_schedule_iface", iface);

        if (self.reactor.debug) {
            std.debug.print("{s} socket handleWriteStart writeQueue.len={}, pendingQueue.len={}\n", .{ self.reactor.name, self.write_queue.len, self.pending_queue.len });
        }

        var drain_count = self.write_queue.drainTo(&self.pending_queue);
        if (self.reactor.debug) {
            std.debug.print("{s} socket drain_count {}\n", .{ self.reactor.name, drain_count });
        }

        const len = self.pending_queue.len;
        if (len == 0) {
            return;
        } else if (len == 1) {
            var entry = self.pending_queue.items[self.pending_queue.head & self.pending_queue.mask];
            const user_data: u64 = @ptrToInt(&self.write_completed_iface);

            // send is faster than write.
            _ = try self.reactor.uring.send(user_data, self.fd, entry.buf.ptr[0..entry.len], 0);
        } else {
            // https://nmichaels.org/zig/pointers.html
            var i: u16 = 0;
            while (i < len and i < self.io_vec.len) {
                var write_entry = self.pending_queue.items[(self.pending_queue.head + i) & self.pending_queue.mask];
                self.io_vec[i] = .{ .iov_base = write_entry.buf.ptr, .iov_len = write_entry.len };
                i += 1;
            }
            const user_data: u64 = @ptrToInt(&self.write_completed_iface);

            const v = self.io_vec[0..len];
            _ = try self.reactor.uring.writev(user_data, self.fd, v, 0);
        }
    }

    fn handleWriteComplete(iface: *CompletionHandler, res: i32, flags: u32) !void {
        var self = @fieldParentPtr(Socket, "write_completed_iface", iface);

        if (self.reactor.debug) {
            std.debug.print("{s} handleWriteComplete res={} flags={}\n", .{ self.reactor.name, res, flags });
        }

        if (res < 0) {
            std.debug.print("{s} Failed to write to socket res: {}", .{self.reactor.name, util.errno(res)});
            return;
        }

        self.bytes_written += @intCast(u64, res);
        var remaining = @intCast(usize, res);
        //std.debug.print("pending_queue.len {}\n", .{pending_queue.len});

        assert(self.pending_queue.len > 0);

        var completed: u16 = 0;
        while (self.pending_queue.len > 0 and remaining > 0) {
            const index = self.pending_queue.head & self.pending_queue.mask;
            var write_entry = &self.pending_queue.items[index];

            if (write_entry.len <= remaining) {
                //std.debug.print("Packet fully written\n", .{});
                // packet has been fully written
                // todo: no need to pass res/flags
                try write_entry.handler.onComplete(write_entry.handler, write_entry.buf);
                // ditch the item
                _ = self.pending_queue.poll();
                remaining -= write_entry.len;
                completed += 1;
            } else {
                assert(false);

                std.debug.print("Packet not fully written\n", .{});
                // item hasn't been fully written
                // todo: we need to update this buffer; offset + length is no good
                break;
            }
        }

        //std.debug.print("Completed writes in 1 go {}\n", .{completed});

        if (self.write_queue.len > 0 or remaining > 0) {
            // The write queue isn't empty, so we need to do more writing
            try handleWriteStart(&self.write_schedule_iface);
        } else {
            // Everything has been written, we are done
            self.write_scheduled = false;
        }
    }

    pub fn close(self: *Socket) void {
        _ = c.close(self.fd);
        // if (res < 0) {
        //     return os.unexpectedErrno(errno(res));
        // }
    }

    pub fn getLocalAddress(self: *Socket) !linux.sockaddr {
        var addr: linux.sockaddr = undefined;
        var addr_len = @intCast(linux.socklen_t, @sizeOf(*linux.socklen_t));

        var res = linux.getsockname(self.fd, &addr, &addr_len);
        if (res != 0) {
            return os.unexpectedErrno(util.errno(res));
        }

        return addr;
    }

    //pub fn getLocalPort(self: *Socket) !i32 {
    //    var ip: [8]u8 = undefined;
    //    var port: c_int = undefined;
    //    const res = c.socket_local4(self.fd, &ip, &port);
    //    if (res != 0) {
    //        return os.unexpectedErrno(errno(res));
    //    }
    //
    //    return port;
    //}

    pub fn getRemoteAddress(self: *Socket) !linux.sockaddr {
        var addr: linux.sockaddr = undefined;
        var addr_len = @intCast(linux.socklen_t, @sizeOf(*linux.socklen_t));

        var res = linux.getpeername(self.fd, &addr, &addr_len);
        if (res != 0) {
            return os.unexpectedErrno(util.errno(res));
        }

        return addr;
    }

    pub fn setTcpNoDelay(self: *Socket, tcpNoDelay: bool) !void {
        const value: c_int = if (tcpNoDelay) 1 else 0;

        const res = c.setsockopt(self.fd, c.SOL_TCP, c.TCP_NODELAY, &value, @sizeOf(c_int));
        if (res == -1) {
            return os.unexpectedErrno(util.errno(res));
        }
    }

    pub fn setBlocking(self: *Socket, block: bool) !void {
        // todo: fix name
        const foo: u8 = 0;
        const flags = c.fcntl(self.fd, c.F_GETFL, foo);
        if (flags == -1) {
            return os.unexpectedErrno(util.errno(flags));
        }

        const new_flags = if (block) flags & ~c.O_NONBLOCK else flags | c.O_NONBLOCK;
        const res = c.fcntl(self.fd, c.F_SETFL, new_flags);
        if (res == -1) {
            return os.unexpectedErrno(errno(res));
        }
    }
};
