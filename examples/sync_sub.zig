const std = @import("std");
const nats = @import("nats-sync");
//const log = std.log.scoped(.app);
//pub const log_level: std.log.Level = .info;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn = try nats.connect(allocator, .{});
    defer conn.close();

    var sid = try conn.subscribe("foo");
    std.log.debug("subscribe sid: {d}", .{sid});
    try conn.unsubscribe(sid);
    sid = try conn.subscribe("foo");
    std.log.debug("subscribe sid: {d}", .{sid});

    while (true) {
        if (try conn.read()) |msg| {
            std.log.debug("got msg {d}", .{msg.payload.?.len});
        }
    }
}

// what log level from libs are allowed
const libsLogLevel = std.log.Level.warn;

pub fn log(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    // Ignore all non-error logging from sources other than .default
    const scope_prefix = switch (scope) {
        .default => "",
        else => if (@enumToInt(level) <= @enumToInt(libsLogLevel))
            "(" ++ @tagName(scope) ++ ") "
        else
            return,
    };

    const prefix = comptime switch (level) {
        .debug => "[DBG] ",
        .info => "[INF] ",
        .warn => "[WRN] ",
        .err => "[ERR] ",
    } ++ scope_prefix;

    // Print the message to stderr, silently ignoring any errors
    std.debug.getStderrMutex().lock();
    defer std.debug.getStderrMutex().unlock();
    const stderr = std.io.getStdErr().writer();

    stderr.print("{d} ", .{std.time.milliTimestamp()}) catch return;
    stderr.print(prefix ++ format ++ "\n", args) catch return;
}
