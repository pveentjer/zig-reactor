const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn CircularQueue(comptime T: type) type {
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
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
        }

        pub fn drainTo(src: *Self, dst: *Self) u64 {
            const drain_count = @min(src.len, dst.cap - dst.len);

            if (drain_count == 0) {
                return 0;
            }

            var i: u64 = 0;
            // could be done using a mem copy
            while (i < drain_count) {
                dst.tail += 1;
                dst.items[dst.tail & dst.mask] = src.items[src.head & src.mask];
                src.head += 1;
                i += 1;
            }
            src.len -= drain_count;
            dst.len += drain_count;
            return drain_count;
        }

        pub fn offer(self: *Self, item: T) bool {
            if (self.len == self.cap) {
                return false;
            }

            self.tail += 1;
            self.items[self.tail & self.mask] = item;
            self.len += 1;
            return true;
        }

        pub fn poll(self: *Self) ?T {
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
