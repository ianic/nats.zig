const std = @import("std");
const Ed25519 = std.crypto.sign.Ed25519;
const base64 = std.base64.standard.Encoder;
const base32 = @import("base32.zig").Decoder;
const Allocator = std.mem.Allocator;

const data_header_prefix = "-----BEGIN";
const Self = @This();

allocator: Allocator,
jwt: []u8 = undefined,
seed: []u8 = undefined,
sign_buf: [128]u8 = undefined,

fn readCredsFile(self: *Self, path: []const u8) !void {
    var file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();
    var reader = file.reader();
    var header_no: u8 = 0;

    while (true) {
        if (try lineHasPreffix(reader, data_header_prefix)) {
            if (header_no == 0) { // first data line is jwt
                self.jwt = try reader.readUntilDelimiterAlloc(self.allocator, '\n', 8192);
            } else { // second data line is seed
                self.seed = try reader.readUntilDelimiterAlloc(self.allocator, '\n', 128);
                return;
            }
            header_no += 1;
        }
    }
}

fn lineHasPreffix(reader: std.fs.File.Reader, comptime prefix: []const u8) !bool {
    var buf: [prefix.len]u8 = undefined;
    var ret = reader.readUntilDelimiterOrEof(&buf, '\n') catch |err| {
        switch (err) {
            error.StreamTooLong => {
                //std.debug.print("buf: {s}\n", .{buf});
                const match = std.mem.eql(u8, &buf, prefix);
                try reader.skipUntilDelimiterOrEof('\n'); // consume to start of next line
                return match;
            },
            else => {
                return err;
            },
        }
    };
    if (ret == null) {
        return error.EOF;
    }
    return false;
}

fn binarySeed(self: *Self) ![32]u8 {
    var buf: [64]u8 = undefined;
    _ = try base32.decode(&buf, self.seed); // base32 decode seed into buf
    return buf[2..34].*; // remove first two bytes (type) and checksum
}

pub fn sign(self: *Self, nonce: []const u8) ![]const u8 {
    const kp = try Ed25519.KeyPair.create(try self.binarySeed());
    const signature = try kp.sign(nonce, null);
    return base64.encode(&self.sign_buf, &signature.toBytes()); // base64 encode signature into buf
}

fn wipe(self: *Self) void {
    var i: usize = 0;
    while (i < self.seed.len) : (i += 1) {
        self.seed[i] = 'x'; //0xa;
    }
}

pub fn deinit(self: *Self) void {
    self.wipe();
    self.allocator.free(self.jwt);
    self.allocator.free(self.seed);
}

pub fn init(allocator: Allocator, creds_file_path: []const u8) !Self {
    var nk = Self{
        .allocator = allocator,
    };
    try nk.readCredsFile(creds_file_path);
    return nk;
}

const test_creds_file = root() ++ "nkey.txt";

test "readCredsFile" {
    const test_seed = "SUAOTBNEUHZDFJT3EUMELT7MQTP24JF3XVCXQNDSCU74G5IU6VAJBKH5LI";
    const test_jwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJHVDROVU5NRUY3Wk1XQ1JCWFZWVURLUVQ2WllQWjc3VzRKUlFYRDNMMjRIS1VKRUNRSDdRIiwiaWF0IjoxNTkwNzgxNTkzLCJpc3MiOiJBQURXTFRISUNWNFNVQUdGNkVLTlZFVzVCQlA3WVJESUJHV0dHSFo1SkJET1FZQTdHVUZNNkFRVSIsIm5hbWUiOiJPUEVSQVRPUiIsInN1YiI6IlVERTZXVEdMVFRQQ1JKUkpDS0JKUkdWTlpUTElWUjdMRUVFTFI0Q1lXV1dCS0pTN1hZSUtYRFVVIiwibmF0cyI6eyJwdWIiOnt9LCJzdWIiOnt9LCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.c_XQT04wEoVVNDRjPHeKwe17BOrSpQTcftwIbB7KoNEIz6peZCJDc4-J3emVepHofUOWy7IAo9TlLwYhuGHWAQ";

    var nk = Self{
        .allocator = std.testing.allocator,
    };
    defer nk.deinit();
    try nk.readCredsFile(test_creds_file);

    try std.testing.expectEqualStrings(nk.seed, test_seed);
    try std.testing.expectEqualStrings(nk.jwt, test_jwt);
}

test "sign" {
    var nk = try init(std.testing.allocator, test_creds_file);
    defer nk.deinit();

    try std.testing.expectEqualStrings(
        try nk.sign("iso medo u ducan"),
        "kguBfkTzHEyPiNVz4lXf4WuRcO3qO4h2DNLJrD7CpMZBmWdUs4ex/A++b97SicvWv7umC1FJ/Ipt+w794sglDg==",
    );
    try std.testing.expectEqualStrings(
        try nk.sign("nije reko dobar dan"),
        "eeIYJ8fw60oAmKFjAoPrZOUoK9aqjNvr79anI+/Z5+tATtVhIUo7W7AvukNueOQUYElvsVkU7DpKGT9KYKIOCQ==",
    );
}

test "invalid file" {
    const invalid_file = comptime root() ++ "NKeys.zig";
    var err = init(std.testing.allocator, invalid_file);
    try std.testing.expectError(error.EOF, err);
}

fn root() []const u8 {
    return comptime (std.fs.path.dirname(@src().file) orelse unreachable) ++ "/";
}
