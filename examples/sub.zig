const std = @import("std");
const nats = @import("nats");
const log = std.log;
const os = std.os;

pub const log_level: std.log.Level = .info;

pub fn main() !void {
    // allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // nats connection
    var nc = try nats.connect(alloc);
    defer nc.deinit();
    try nats.closeOnTerm(nc);

    // subscribe to the foo subject
    _ = try nc.subscribe("foo");

    // read and handle messages
    while (nc.read()) |msg| {
        log.info("received: {s}", .{msg.data()});
        msg.deinit(alloc);
    }
}
