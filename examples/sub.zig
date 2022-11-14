const std = @import("std");
const nats = @import("nats");

pub const scope_levels = [_]std.log.ScopeLevel{
    .{ .scope = .nats, .level = .info }, // set to debug to view nats lib logs
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn = try nats.connect(allocator, .{});
    defer conn.close();

    var sid = try conn.subscribe("foo");
    std.log.debug("subscribe sid: {d}", .{sid});
    try conn.unsubscribeBySid(sid);
    sid = try conn.subscribe("foo");
    std.log.debug("subscribe sid: {d}", .{sid});

    while (true) {
        if (try conn.read()) |msg| {
            std.log.debug("got msg with payload len {d}", .{msg.payload.?.len});
        }
    }
}
