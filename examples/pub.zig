const std = @import("std");
const event = std.event;
const time = std.time;
const nats = @import("nats");

pub const io_mode = .evented;
pub const event_loop_mode = .single_threaded;
pub const log_level: std.log.Level = .debug;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var loop = event.Loop.instance.?;
    defer loop.deinit();

    var nc = try nats.connect(alloc, loop);
    defer nc.deinit();

    try loop.runDetached(alloc, publish, .{nc});

    loop.run();
}

fn publish(nc: *nats.Conn) void {
    var scratch: [128]u8 = undefined;
    var i: usize = 0;
    while (true) : (i += 1) {
        var buf = std.fmt.bufPrint(scratch[0..], "msg no {d}", .{i}) catch unreachable;
        nc.publish("foo", buf) catch unreachable;
        time.sleep(1000 * time.ns_per_ms);
    }
}
