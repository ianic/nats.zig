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

    var nc = try nats.connect(alloc);
    defer nc.deinit();
    var nc_frame = async nc.run();

    try publish(nc);

    try await nc_frame;
}

fn publish(nc: *nats.Conn) !void {
    var scratch: [128]u8 = undefined;
    var i: usize = 0;
    while (true) : (i += 1) {
        var buf = std.fmt.bufPrint(scratch[0..], "msg no {d}", .{i}) catch unreachable;
        try nc.publish("foo", buf);
        time.sleep(100 * time.ns_per_ms);
    }
}
