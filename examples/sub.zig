const std = @import("std");
const nats = @import("nats");
const info = std.log.info;
const log = std.log;
const time = std.time;
const print = std.debug.print;

//pub const io_mode = .evented;
//pub const event_loop_mode = .single_threaded;
//pub const log_level: std.log.Level = .info;

pub fn main() !void {
    defer {
        log.debug("done", .{});
    }
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var nc = try nats.connect(alloc);
    defer nc.deinit();
    //log.debug("connected", .{});


    var sid = try nc.subscribe("foo1");
    log.debug("subscribed sid: {d}", .{sid});

    while(nc.read()) |msg| {
        log.info("msg received: {s}", .{msg.data()});
        msg.deinit(alloc);
    }
    //time.sleep(120*time.ns_per_s);
    //try await nc_frame;
}
