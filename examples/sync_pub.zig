const std = @import("std");
const nats = @import("nats-sync");
const log = std.log.scoped(.app);
pub const log_level: std.log.Level = .info;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn = try nats.connect(allocator);
    defer conn.close();

    var scratch: [1024]u8 = undefined;

    for (scratch) |_, j| {
        scratch[j] = @intCast(u8, (j % 10)) + 48;
    }

    var i: usize = 1;
    while (i < 1024) : (i += 1) {
        //const payload = try std.fmt.bufPrint(&scratch, "msg {d}", .{i});
        const buf = scratch[0..i];
        try conn.publish("foo", buf);
        log.info("{d}", .{buf.len});
    }
}
