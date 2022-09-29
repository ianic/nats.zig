const std = @import("std");
const nats = @import("nats-sync");
const log = std.log.scoped(.app);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn = try nats.connect(allocator);
    defer conn.close();

    const sid = try conn.subscribe("foo");
    log.debug("subscribe sid: {d}", .{sid});

    if (try conn.read()) |msg| {
        log.debug("got msg {}", .{msg});
    }
}