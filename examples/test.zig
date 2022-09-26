const std = @import("std");
const log = std.log;
const nats = @import("nats");
const Allocator = std.mem.Allocator;

pub const log_level: log.Level = .info;
const no_msgs: usize = 1024;
const no_rounds: usize = 1024*1024;
const subject = "test";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var nc = try nats.connect(.{ .alloc = alloc });
    defer nc.deinit();
    var sid = try nc.subscribe(subject);

    var sub_trd = try std.Thread.spawn(.{}, subscribe, .{ nc, alloc });
    try publish(nc);
    std.Thread.join(sub_trd);
    try nc.unsubscribe(sid);
}

fn publish(nc: *nats.Conn) !void {
    var scratch: [no_msgs]u8 = undefined;
    var msg_no_buf: [32]u8 = undefined;

    var no: usize = 0;
    var j: usize = 0;
    while (j < no_rounds) : (j += 1) {
        var i: usize = 0;
        while (i < no_msgs) : (i += 1) {
            no += 1;
            scratch[i] = @intCast(u8, (i % 25) + 97);
            var buf = scratch[0..i];
            var offset = std.fmt.formatIntBuf(msg_no_buf[0..], no, 10, .lower, .{});
            if (offset < buf.len) {
                std.mem.copy(u8, buf[0..], msg_no_buf[0..offset]);
            }
            try nc.publish(subject, buf);
            std.time.sleep(std.time.ns_per_ms);
        }
    }
    //try nc.flush();
}

fn subscribe(nc: *nats.Conn, alloc: Allocator) void {
    var msgs_count: u64 = 0;
    var no_expected = no_msgs * no_rounds;
    while (nc.read()) |msg| {

        //log.info("{d} subject: '{s}', data len: {d}", .{ msgs_count, msg.subject, msg.data().len });
        //std.debug.print("M{d}", .{msgs_count});
        //std.debug.print("M", .{});

        //std.debug.print("M", .{});
        msg.deinit(alloc);
        msgs_count += 1;
        if (msgs_count == no_expected) {
            return;
        }

        // if (no_expected - msgs_count < no_msgs) {
        //     std.debug.print(" {d}", .{no_expected-msgs_count});
        // }

    }
}
