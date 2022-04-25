const std = @import("std");
const time = std.time;
const nats = @import("nats");

//pub const log_level: std.log.Level = .debug;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var nc = try nats.connect(alloc);
    defer nc.deinit();

    var scratch: [128]u8 = undefined;
    var i: u64 = 0;
    while (i<10) : (i += 1) {
        var buf = std.fmt.bufPrint(scratch[0..], "msg no {d}", .{i}) catch unreachable;
        try nc.publish("foo1", buf);
        time.sleep(100 * time.ns_per_ms);
    }
}

