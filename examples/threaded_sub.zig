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
    try handleSigInt(conn);

    conn.subscribe("foo");

    while (conn.in()) |event| {
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

// TODO: logging

// TODO: how to to this without global var
fn handleSigInt(conn: *nats.Conn) !void {
    sig_conn = conn;
    var act = std.os.Sigaction{
        .handler = .{ .handler = sigIntHandler },
        .mask = std.os.empty_sigset,
        .flags = 0,
    };
    try std.os.sigaction(std.os.SIG.INT, &act, null);
}

var sig_conn: ?*nats.Conn = null;

fn sigIntHandler(_: c_int) callconv(.C) void {
    if (sig_conn) |c| {
        std.log.info("closing", .{});
        c.close();
    }
}
