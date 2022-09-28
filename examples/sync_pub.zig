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

    var i: usize = 0;
    var scratch: [128]u8 = undefined;

    while (i < 10) : (i += 1) {
        const payload = try std.fmt.bufPrint(&scratch, "msg {d}", .{i});
        try conn.publish("foo", payload);
        log.info("{s}", .{payload});
    }
}
