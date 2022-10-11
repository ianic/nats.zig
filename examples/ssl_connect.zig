const std = @import("std");
const ssl = @import("libressl");

// copied from: https://github.com/haze/zig-libressl
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tls_configuration = (ssl.TlsConfigurationParams{}).build() catch unreachable;

    var connection = try std.net.tcpConnectToHost(allocator, "haz.ee", 443);
    var ssl_connection = try ssl.SslStream.wrapClientStream(tls_configuration, connection, "haz.ee");
    defer ssl_connection.deinit();

    var writer = ssl_connection.writer();
    var reader = ssl_connection.reader();

    try writer.writeAll("GET / HTTP/1.1\n\n");

    while (try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', std.math.maxInt(usize))) |line| {
        std.debug.print("{s}\n", .{line});
        defer allocator.free(line);
        if (std.mem.eql(u8, line, "</html>")) break;
    }
}
