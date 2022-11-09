const std = @import("std");
const nats = @import("nats-sync");

pub const scope_levels = [_]std.log.ScopeLevel{
    .{ .scope = .nats, .level = .info }, // set to debug to view nats lib logs
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn = try nats.connect(allocator, .{});
    defer conn.close();

    var scratch: [1024]u8 = undefined;

    for (scratch) |_, j| {
        scratch[j] = @intCast(u8, (j % 10)) + 48;
    }

    var i: usize = 1;
    while (i < 1024) : (i += 1) {
        const buf = scratch[0..i];
        try conn.publish("foo", buf);
        std.log.debug("{d}", .{buf.len});
        //std.time.sleep(std.time.ns_per_s);
    }
}
