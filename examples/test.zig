const std = @import("std");
const log = std.log;
const nats = @import("nats");
const Allocator = std.mem.Allocator;

pub const log_level: log.Level = .info;
const no_msgs: usize = 1024;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var nc = try nats.connect(alloc);
    defer nc.deinit();
    var sid = try nc.subscribe("test1");

    var sub_trd = try std.Thread.spawn(.{}, subscriber, .{nc, alloc});
    try publish(nc);
    std.Thread.join(sub_trd);
    try nc.unsubscribe(sid);

}

fn publish(nc: *nats.Conn) !void {
    var scratch: [no_msgs]u8 = undefined;

    var i: usize = 0;
    while (i < no_msgs) : (i += 1) {
        scratch[i] = @intCast(u8, i%255);
        try nc.publish("test1", scratch[0..i]);
    }
}

fn subscriber(nc: *nats.Conn, alloc: Allocator) void {
    var msgs_count: u64 = 0;
    while (nc.read()) |msg| {
        log.info("{d} subject: '{s}', data len: {d}", .{ msgs_count, msg.subject, msg.data().len });
        msg.deinit(alloc);
        msgs_count += 1;
        if (msgs_count == no_msgs) {
            return;
        }
    }
}
