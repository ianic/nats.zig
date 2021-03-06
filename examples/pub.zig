const std = @import("std");
const time = std.time;
const nats = @import("nats");

// if in debug mode (commented) will show exchange of protocol messages with the nats server
//pub const log_level: std.log.Level = .info;

pub fn main() !void {
    // allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // nats connection
    var nc = try nats.connect(.{.alloc = alloc});
    defer nc.deinit();

    // publish 10 messages (reusing the scratch buffer, and sleeping a little bit after each)
    var scratch: [128]u8 = undefined;
    var i: u64 = 0;
    while (i < 2) : (i += 1) {
        var buf = std.fmt.bufPrint(scratch[0..], "msg no {d}", .{i}) catch unreachable;
        // publish nats message to the foo subject
        nc.publish("foo", buf) catch |err| {
            std.log.err("publish err: {}", .{err});
        };
        time.sleep(1000 * time.ns_per_ms);
    }
}
