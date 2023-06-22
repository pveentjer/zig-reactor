const std = @import("std");
const os = std.os;
const system = std.os.system;
const errno = std.os.system.getErrno;
const time = std.time;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Address = std.net.Address;
const reactor = @import("reactor.zig");
const Reactor = reactor.Reactor;
const ReactorTask = reactor.ReactorTask;
const ReactorConfig = reactor.ReactorConfig;
const CompletionHandler = reactor.CompletionHandler;
const lib_socket = @import("socket.zig");
const Socket = lib_socket.Socket;
const SocketConfig = lib_socket.SocketConfig;
const ConnectHandler = lib_socket.ConnectHandler;
const WriteCompleteHandler = lib_socket.WriteCompleteHandler;
const ReadCompleteHandler = lib_socket.ReadCompleteHandler;
const global_allocator = std.heap.page_allocator;
const CircularQueue = @import("circular_queue.zig").CircularQueue;
const util = @import("util.zig");

const BufferPool = CircularQueue([]u8);

pub const StartAcceptTask = struct {
    reactor_task_iface: ReactorTask,
    accept_handler_iface: CompletionHandler,
    server_socket: *Socket,
    addr: os.sockaddr,
    addrlen: os.socklen_t,
    allocator: Allocator,

    fn init(allocator: Allocator, server_socket: *Socket) !*StartAcceptTask {
        var self = try allocator.create(StartAcceptTask);
        self.reactor_task_iface = ReactorTask{ .exec = run };
        self.accept_handler_iface = CompletionHandler{ .handle = onAccept };
        self.server_socket = server_socket;
        self.allocator = allocator;
        return self;
    }

    fn run(iface: *ReactorTask) !void {
        var self = @fieldParentPtr(StartAcceptTask, "reactor_task_iface", iface);

        try self.server_socket.accept(&self.accept_handler_iface, null, null);
    }

    fn onAccept(iface: *CompletionHandler, res: i32, _: u32) !void {
        var self = @fieldParentPtr(StartAcceptTask, "accept_handler_iface", iface);

        if (res < 0) {
            const e =  util.errno(res);
            std.debug.print("Failed to accept, failed with errno {} ", .{e});
            return os.unexpectedErrno(e);
        }

        std.debug.print("Accepted socket with fd {}\n", .{res});

        var socket = try Socket.init(SocketConfig{
            .allocator=self.allocator,
            .reactor=self.server_socket.reactor,
            .ipv4=true,
            .fd=res,
        });
    
        try socket.setTcpNoDelay(true);
        try socket.setBlocking(true);

        var socket_handler = try ServerSocketHandler.init(self.allocator, socket);
        try socket_handler.start();

        // and rearm for the next accept.
        try self.server_socket.accept(&self.accept_handler_iface, null, null);
    }
};

pub const ServerSocketHandler = struct {
    write_complete_iface: WriteCompleteHandler,
    read_complete_iface: ReadCompleteHandler,
    socket: *Socket,
    iteration: u64,
    allocator: Allocator,
    buffer_pool: BufferPool,
   
    fn init(allocator: Allocator, socket: *Socket) !*ServerSocketHandler {
        var self = try allocator.create(ServerSocketHandler);
        self.write_complete_iface = WriteCompleteHandler{ .onComplete = write_complete };
        self.read_complete_iface = ReadCompleteHandler{ .onComplete = read_complete };
        self.buffer_pool = try BufferPool.init(100,allocator);
        self.socket = socket;
        self.iteration = 0;
        self.allocator = allocator;
        return self;
    }

    fn get_buffer(self: *ServerSocketHandler) ![]u8 {
        if (self.buffer_pool.poll()) |buffer| {
            return buffer;
        } else {
            return self.allocator.alloc(u8, 64*1024);
        }
    }

    fn start(self: *ServerSocketHandler)! void{
        var buf = try self.get_buffer();
        try self.socket.read(buf, &self.read_complete_iface);
        
        std.debug.print("ServerSocketHandler started\n",.{});
    }

    fn write_complete(iface: *WriteCompleteHandler, buf:[]u8) !void {
        var self = @fieldParentPtr(ServerSocketHandler, "write_complete_iface", iface);

        //std.debug.print("Server completed write\n",.{});

        assert(self.buffer_pool.offer(buf));
    }

    fn read_complete(iface: *ReadCompleteHandler, buf:[]u8, res:i32) !void {
        var self = @fieldParentPtr(ServerSocketHandler, "read_complete_iface", iface);

        self.iteration += 1;
        if (@mod(self.iteration,100_000) == 0) {
            std.debug.print("server at {}\n",.{self.iteration});
        }

        if (res >= 0) {
            var next_read_buf = try self.get_buffer();
            // rearm the read.
            try self.socket.read(next_read_buf, &self.read_complete_iface);
            
            // and write back what has been read.
            try self.socket.write(buf, @intCast(usize, res), &self.write_complete_iface);
        } else {
             const e =  util.errno(res);
            std.debug.print("server socket.read failed with errno {} ", .{e});
            return os.unexpectedErrno(e);
        }
    }
};

pub const StartClientTask = struct {
    task_iface: ReactorTask,
    connect_handler_iface: ConnectHandler,
    socket: *Socket,
    addr: Address,

    fn init(allocator: Allocator, socket: *Socket, addr: Address) !*StartClientTask {
        var self = try allocator.create(StartClientTask);
        self.task_iface = ReactorTask{ .exec = run };
        self.socket = socket;
        self.addr = addr;
        self.connect_handler_iface = ConnectHandler{.onComplete=onConnect};
        return self;
    }

    fn run(iface: *ReactorTask) !void {
        var self = @fieldParentPtr(StartClientTask, "task_iface", iface);
       
        try self.socket.connect(&self.addr, &self.connect_handler_iface);
    }

    fn onConnect(iface: *ConnectHandler, res:i32) !void {
        var self = @fieldParentPtr(StartClientTask, "connect_handler_iface", iface);

        if (res >= 0) {
            std.debug.print("Socket.connect completed res={} \n", .{ res });

            var handler = try ClientSocketHandler.init(global_allocator, self.socket) ; 
            try handler.start();
        } else {
            const e =  util.errno(res);
            std.debug.print("server socket connect failed with errno {} ", .{e});
            return os.unexpectedErrno(e);
        }
    }
};

pub const ClientSocketHandler = struct {
    write_complete_iface: WriteCompleteHandler,
    read_complete_iface: ReadCompleteHandler,
    socket: *Socket,
    buffer_pool: BufferPool,
    iteration: u64,
    allocator: Allocator,

    fn init(allocator: Allocator, socket: *Socket) !*ClientSocketHandler {
        var self = try allocator.create(ClientSocketHandler);
        self.write_complete_iface = WriteCompleteHandler{ .onComplete = write_complete };
        self.read_complete_iface = ReadCompleteHandler{ .onComplete = read_complete };
        self.socket = socket;
        self.iteration = 0;
        self.buffer_pool = try BufferPool.init(100,allocator);
        self.allocator = allocator;
        return self;
    }

    fn get_buffer(self: *ClientSocketHandler) ![]u8 {
        if (self.buffer_pool.poll()) |buffer|{
            return buffer;
        } else {
            return self.allocator.alloc(u8, 64*1024);
        }
    }

    fn start(self: *ClientSocketHandler)! void{
        var i: u16 = 0;
        
        while (i < 1) {
            var buf = try self.get_buffer();
            try self.socket.write(buf, 8, &self.write_complete_iface);
            i += 1;
        }

        var next_read_buf = try self.get_buffer();
        try self.socket.read(next_read_buf, &self.read_complete_iface);

        std.debug.print("Client SocketHandler started\n",.{});
    }

    fn write_complete(iface: *WriteCompleteHandler, buf:[]u8) !void {
        var self = @fieldParentPtr(ClientSocketHandler, "write_complete_iface", iface);
    
        //std.debug.print("Client completed write\n",.{});

        assert(self.buffer_pool.offer(buf));
    }

    fn read_complete(iface: *ReadCompleteHandler, buf:[]u8, res:i32) !void {
        var self = @fieldParentPtr(ClientSocketHandler, "read_complete_iface", iface);

        self.iteration += 1;
        if (@mod(self.iteration,100_000) == 0) {
            std.debug.print("client at {}\n",.{self.iteration});
        }

        //std.debug.print("client socket.read completed res={} flags={} \n", .{ res, flags });

        if (res >= 0) {
            var next_read_buf = try self.get_buffer();
            // re-arm the read.
            try self.socket.read(next_read_buf, &self.read_complete_iface);
            
            // send back the buffer we have read
            try self.socket.write(buf, @intCast(usize, res), &self.write_complete_iface);
        } else{
            const e =  util.errno(res);
            std.debug.print("client socket.read failed with errno {} ", .{e});
            return os.unexpectedErrno(e);
        }
    }
};


// pub fn main() void {
    
//     std.debug.print("{}\n",.{util.errno(-80)});
    
//     // var s: usize = @bitCast(usize, @intCast(i64,-79));
//     // var x = os.linux.getErrno(s);
//     // std.debug.print("errno {!}\n",.{x});
    
//     //var e = os.unexpectedErrno(x);
//     //std.debug.print("error {!}\n",.{e});
// }

const duration_s: u64 = 10;

pub fn main() !void {
    var prng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.os.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const rand = prng.random();
    const port: u16 = 5000 + @mod(rand.int(u16), 200);
    
    const addr = try Address.parseIp4("127.0.0.1",  port);
 
    std.debug.print("Using port {}\n", .{port});

    var server_reactor = try Reactor.init(ReactorConfig{
        .name = "server-reactor",
        .allocator = global_allocator,
        .debug = false,
        .spin = false,
        // .delay_ns = 1000,
    });
    try server_reactor.start();
    defer server_reactor.shutdown();

    var server_sock = try Socket.init( SocketConfig{
        .allocator=global_allocator,
        .reactor=server_reactor,
        .ipv4=true,
    });
    defer server_sock.close();

    std.debug.print("server_sock fd {}\n", .{server_sock.fd});

    try server_sock.bind(addr);
    std.debug.print("server socket bind\n", .{});

    try server_sock.listen(10);
    std.debug.print("server socket listen\n", .{});

    try server_sock.setBlocking(false);

    var start_accept_task = try StartAcceptTask.init(global_allocator, server_sock);
    if (!server_reactor.offer(&start_accept_task.reactor_task_iface)) {
        return error.oops;
    }
  
    std.time.sleep(std.time.ns_per_s);

    var client_reactor = try Reactor.init(ReactorConfig{
        .name = "client-reactor",
        .allocator = global_allocator,
        .debug = false,
        .spin = false,
        //.delay_ns = 1000,
    });
    try client_reactor.start();
    defer client_reactor.shutdown();

    var client_sock = try Socket.init(SocketConfig{
        .allocator = global_allocator,
        .reactor = client_reactor,
        .ipv4 = true,
    });
    
    defer client_sock.close();
    try client_sock.setTcpNoDelay(true);
    try client_sock.setBlocking(false);

    var task = try StartClientTask.init(global_allocator, client_sock, addr);
    if (!client_reactor.offer(&task.task_iface)) {
        return error.oops;
    }

    std.debug.print("Running benchmark for {} seconds.\n",.{duration_s});
    std.time.sleep(std.time.ns_per_s * duration_s);
    server_reactor.shutdown();
    client_reactor.shutdown();

    try server_reactor.join();
    try client_reactor.join();
}
