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

    var msg = try nc.request("foo.svc", "iso medo u ducan nije reko dobar dan");
    std.log.debug("response received {s}", .{msg.data()});
        //msg.deinit(alloc);

    // _ = nc.request("foo.svc1", "iso medo u ducan nije reko dobar dan") catch |err| {
    //     std.log.warn("error {}", .{err});
    // };

    std.time.sleep(1000 * time.ns_per_ms);

    std.log.debug("close 1", .{});
    try nc.close();
    std.log.debug("close 2", .{});
    try await nc_frame;
    std.log.debug("close 3", .{});
}

