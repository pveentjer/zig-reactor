const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn AtomicCircularQueue(comptime T: type) type {
    return struct {
        const Self = @This();
        items: []T,
        // the number of items in the queue
        len: usize,
        // the capacity of the queue
        cap: usize,
        head: u64,
        tail: u64,
        mask: u64,
        allocator: Allocator,
        mutex: std.Thread.Mutex,

        pub fn init(cap: usize, allocator: Allocator) !Self {
            // todo: cap should be power of 2
            return Self{
                .items = try allocator.alloc(T, cap),
                .cap = cap,
                .mask = cap - 1,
                .len = 0,
                .head = 1,
                .tail = 0,
                .allocator = allocator,
                .mutex = std.Thread.Mutex{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
        }

        pub fn offer(self: *Self, item: T) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.len == self.cap) {
                return false;
            }

            self.tail += 1;
            self.items[self.tail & self.mask] = item;
            self.len += 1;
            return true;
        }

        pub fn poll(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.len == 0) {
                return null;
            }

            const item = self.items[self.head & self.mask];
            self.head += 1;
            self.len -= 1;
            return item;
        }
    };
}
