const std = @import("std");
const event = std.event;
const print = std.debug.print;
const Allocator = std.mem.Allocator;
const nats = @import("nats");

pub const io_mode = .evented;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var loop = event.Loop.instance.?;
    defer loop.deinit();

    var nc = try nats.connect(alloc);
    defer nc.deinit();

    try loop.runDetached(alloc, nats.run, .{&nc});
    try loop.runDetached(alloc, publish, .{ &nc, loop });

    loop.run();
}

const millisecond = 1000 * 1000;

fn publish(nc: *nats.Conn, loop: *event.Loop) void {
    var scratch: [128]u8 = undefined;
    var i: usize = 0;
    while (true) : (i += 1) {
        var buf = std.fmt.bufPrint(scratch[0..], "msg no {d}", .{i}) catch unreachable;
        nc.publish("foo", buf) catch unreachable;
        loop.sleep(1000 * millisecond);
    }
}
