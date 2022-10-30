const std = @import("std");
const nats = @import("nats-threaded");

const log = std.log.scoped(.app);
pub const log_level: std.log.Level = .info;

pub fn main() !void {
    std.log.info("os: {}", .{@import("builtin").os.tag});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn = try nats.connect(allocator, .{});
    defer conn.deinit();
    conn.subscribe("foo");

    while (conn.recv()) |event| {
        switch (event) {
            .msg => |msg| {
                std.log.info("got msg {d}", .{msg.payload.?.len});
            },
            .status_changed => |s| {
                std.log.info("{d} status changed to {}", .{ s.connection_no, s.status });
            },
        }
    }
    std.log.info("loop done", .{});
}

