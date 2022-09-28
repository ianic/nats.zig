const std = @import("std");
const nats = @import("nats-sync");

pub fn main() !void {
    var conn = try nats.connect();
    defer conn.close();

    if (try conn.read()) |msg| {
        std.debug.print("got msg {}", .{msg});
    }
}
