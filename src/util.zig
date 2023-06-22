const std = @import("std");
const os = std.os;

pub fn errno(res: c_int) os.linux.E {
    var s: usize = @bitCast(usize, @intCast(i64,res));
    return os.linux.getErrno(s);
}