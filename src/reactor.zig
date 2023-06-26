const std = @import("std");
const Allocator = std.mem.Allocator;
const Ordering = std.atomic.Ordering;
const os = std.os;
const linux = std.os.linux;
const io_uring_params = linux.io_uring_params;
const IO_Uring = linux.IO_Uring;
const ReadBuffer = IO_Uring.ReadBuffer;
const AtomicCircularQueue = @import("atomic_circular_queue.zig").AtomicCircularQueue;
const CircularQueue = @import("circular_queue.zig").CircularQueue;
const Socket = @import("socket.zig").Socket;
const util = @import("util.zig");
const RunQueue = AtomicCircularQueue(*ReactorTask);
const SocketQueue = CircularQueue(*Socket);
const Atomic_bool = std.atomic.Atomic(bool);
const c = @cImport({
    @cInclude("sys/eventfd.h");
    @cDefine("_GNU_SOURCE", {});
    @cInclude("sched.h");
});



// Check the following for inheritance example
// https://zig.news/david_vanderson/interfaces-in-zig-o1c
pub const ReactorTask = struct {
    //id: ?[]u8 = null,
    exec: *const fn (self: *ReactorTask) anyerror!void,
};

pub const CompletionHandler = struct {
   //id: ?[]u8 = null,
    handle: *const fn (self: *CompletionHandler, res: i32, flags: u32) anyerror!void,
};

pub const ReactorConfig = struct {
    name: []const u8 = "reactor",
    debug: bool = false,
    allocator: Allocator,
    run_queue_cap: u32 = 1024,
    io_uring_entries: u13 = 1024,
    delay_ns: u64 = 0,
    io_uring_params: ?io_uring_params = null,
    cpu_set: ?c.cpu_set_t = null,
    spin: bool = false,
};

pub const Reactor = struct {
    stop: std.atomic.Atomic(bool),
    thread: std.Thread = undefined,
    name: []const u8 = undefined,
    run_queue: RunQueue,
    uring: IO_Uring,
    eventfd: i32,
    eventfd_buf: [8]u8 = [_]u8{0} ** 8,
    eventfd_handler: CompletionHandler,
    debug: bool,
    delay_ns: u64,
    cpu_set: ?c.cpu_set_t,
    spin: bool,

    pub fn init(config: *ReactorConfig) !*Reactor {
        const res: c_int = c.eventfd(0, 0);
        if (res < 0) {
            return os.unexpectedErrno(util.errno(res));
        }

        var self = try config.allocator.create(Reactor);
        self.run_queue = try RunQueue.init(config.run_queue_cap, config.allocator);
        self.name = config.name;
        if (config.io_uring_params) |*params| {
            self.uring = try IO_Uring.init_params(config.io_uring_entries, params);
        } else { 
            self.uring = try IO_Uring.init(config.io_uring_entries, 0);
        }
        self.stop = Atomic_bool.init(false);
        self.eventfd = res;
        self.eventfd_handler = CompletionHandler{ .handle = eventfd_handle };
        self.debug = config.debug;
        self.delay_ns = config.delay_ns;
        self.cpu_set = config.cpu_set;
        self.spin = config.spin;
        return self;
    }

    fn readEventFd(self: *Reactor) !void {
        if (self.debug) {
            std.debug.print("{s} async_readEventfd {}\n", .{ self.name, self.eventfd });
        }

        const userdata = @ptrToInt(&self.eventfd_handler);
        var read_buffer: ReadBuffer = ReadBuffer{ .buffer = &self.eventfd_buf };
        _ = try self.uring.read(userdata, self.eventfd, read_buffer, 0);
    }

    fn eventfd_handle(iface: *CompletionHandler, res: i32, flags: u32) !void {
        var self = @fieldParentPtr(Reactor, "eventfd_handler", iface);

        if (self.debug) {
            std.debug.print("EventFd completed res={} flags={}\n", .{ res, flags });
        }

        if (res >= 0) {
            // re-arm.
            try self.readEventFd();
        } else {
            std.debug.print("{s} FAILED to rearm the eventfd errno={}\n", .{self.name, util.errno(res) });
        }
    }

    fn wakeup(self: *Reactor) !void {
        if (self.debug) {
            std.debug.print("{s} wakeup call eventfd={}\n", .{ self.name, self.eventfd });
        }

        const res = c.eventfd_write(self.eventfd, 1);
        if (res < 0) {
            return os.unexpectedErrno(util.errno(res));
        }
    }

    pub fn deinit(self: *Reactor) void {
        self.run_queue.deinit();
        self.uring.deinit();
    }

    pub fn offer(self: *Reactor, task: *ReactorTask) bool {
        self.wakeup() catch unreachable;
        return self.run_queue.offer(task);
    }

    // https://github.com/ziglang/zig/blob/master/lib/std/os/linux/io_uring.zig
    // http://ratfactor.com/zig/stdlib-browseable/os/linux/io_uring.zig.html
    fn loop(self: *Reactor) !void {
        var uring = &self.uring;
        var run_queue = &self.run_queue;
        var stop = &self.stop;

        // todo: somehow the sched_setaffinity function isn't found.
        // if (self.cpu_set) |cpu_set| {
        //     if (c.sched_setaffinity(c.getpid(), @sizeOf(cpu_set), &cpu_set) != 0) {
        //         std.debug.println("Failed sched_setaffinity \n",.{});
        //     }
        // }

        try self.readEventFd();

        while (!stop.load(Ordering.Monotonic)) {
            if (self.delay_ns > 0) {
                std.time.sleep(self.delay_ns);
            }

            var submit_res = if (uring.cq_ready() > 0 or self.spin)
                try uring.submit()
            else
                try uring.submit_and_wait(1);

            if (self.debug) {
                std.debug.print("{s} submited res {!}\n", .{ self.name, submit_res });
            }

            var cqe_ready = uring.cq_ready();
            if (cqe_ready > 0) {
                if (self.debug) {
                    std.debug.print("{s} number of ready cqe's= {}\n", .{ self.name, cqe_ready });
                }

                var i: u16 = 0;
                while (i < cqe_ready) {
                    const cqe = try uring.copy_cqe();
                    var handler = @intToPtr(*CompletionHandler, cqe.user_data);
                    
                    handler.handle(handler, cqe.res, cqe.flags) catch |err| {
                        std.debug.print("CompletionHandler {} ran into an error {!}\n", .{ handler, err });
                    };
                    i += 1;
                }
            }

            while (run_queue.poll()) |task| {
                task.exec(task) catch |err| {
                    std.debug.print("task {} ran into an error {!}\n", .{ task, err });
                };
            }
        }
    }

    pub fn start(self: *Reactor) !void {
        self.stop.store(false, Ordering.Monotonic);
        self.thread = try std.Thread.spawn(.{}, Reactor.loop, .{self});
    }

    pub fn shutdown(self: *Reactor) void {
        self.wakeup() catch unreachable;
        self.stop.store(true, Ordering.Monotonic);
    }    

    pub fn join(self: *Reactor) !void {
        self.thread.join();
    }
};
