const std = @import("std");
const Ed25519 = std.crypto.sign.Ed25519;
const base64 = std.base64.standard.Encoder;
const base32 = @import("base32.zig").Decoder;

const data_header_prefix = "-----BEGIN";
line_buf: [data_header_prefix.len]u8 = undefined,

jwt_buf: [4096]u8 = undefined,
jwt: []const u8 = undefined,

seed_buf: [64]u8 = undefined,
seed_base32: []u8 = undefined,
seed: [32]u8 = .{0} ** 32,

sign_buf: [128]u8 = undefined,

const Self = @This();

pub fn readCredsFile(self: *Self, path: []const u8) !void {
    var file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();
    var reader = file.reader();

    while (true) {
        // wait for line which is at least header_prefix len
        const line = try self.readLinePrefix(reader);
        if (line.len < data_header_prefix.len) {
            continue;
        }
        try reader.skipUntilDelimiterOrEof('\n'); // consume to start of next line

        if (std.mem.eql(u8, line, data_header_prefix)) { // if previous line was data header
            if (self.jwt.len == 0) { // first data line is jwt
                self.jwt = try reader.readUntilDelimiter(&self.jwt_buf, '\n');
                //std.debug.print("jwt: {s}\n", .{self.jwt});
            } else { // second data line is seed
                self.seed_base32 = try reader.readUntilDelimiter(&self.seed_buf, '\n');
                try self.decodeSeed();
                return;
            }
        }
    }
}

// returns zero size buf if line is not at least len long
// or first len bytes from line
fn readLinePrefix(self: *Self, reader: std.fs.File.Reader) ![]u8 {
    _ = reader.readUntilDelimiterOrEof(&self.line_buf, '\n') catch |err| {
        switch (err) {
            error.StreamTooLong => {
                return &self.line_buf;
            },
            else => {
                return err;
            },
        }
    };
    return self.line_buf[0..0];
}

fn decodeSeed(self: *Self) !void {
    var scratch: [64]u8 = undefined;
    const buf = try base32.decode(&scratch, self.seed_base32); // base32 decode seed into buf
    std.mem.copy(u8, &self.seed, buf[2..34]); // remove first two bytes (type) and checksum
}

pub fn sign(self: *Self, nonce: []const u8) ![]const u8 {
    //std.debug.print("jwt1 {d}\n", .{self.jwt});
    var ss: [32]u8 = undefined;
    std.mem.copy(u8, &ss, &self.seed);
    const kp = try Ed25519.KeyPair.create(ss);
    //std.debug.print("jwt2 {d}\n", .{self.jwt});
    const signature = try Ed25519.sign(nonce, kp, null); // sign nonce with key pair
    //std.debug.print("jwt3 {d}\n", .{self.jwt});
    return base64.encode(&self.sign_buf, &signature); // base64 encode signature into buf
}

pub fn wipe(self: *Self) void {
    var i: usize = 0;
    while (i < self.seed_base32.len) : (i += 1) {
        self.seed_base32[i] = 'x'; //0xa;
    }
    i = 0;
    while (i < self.seed.len) : (i += 1) {
        self.seed[i] = 'x';
    }
}

fn init(creds_file_path: []const u8) !Self {
    var nk = Self{};
    try nk.readCredsFile(creds_file_path);
    return nk;
}

const test_creds_file = root() ++ "nkey.txt";

test "readCredsFile" {
    const test_seed = "SUAOTBNEUHZDFJT3EUMELT7MQTP24JF3XVCXQNDSCU74G5IU6VAJBKH5LI";
    const test_jwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJHVDROVU5NRUY3Wk1XQ1JCWFZWVURLUVQ2WllQWjc3VzRKUlFYRDNMMjRIS1VKRUNRSDdRIiwiaWF0IjoxNTkwNzgxNTkzLCJpc3MiOiJBQURXTFRISUNWNFNVQUdGNkVLTlZFVzVCQlA3WVJESUJHV0dHSFo1SkJET1FZQTdHVUZNNkFRVSIsIm5hbWUiOiJPUEVSQVRPUiIsInN1YiI6IlVERTZXVEdMVFRQQ1JKUkpDS0JKUkdWTlpUTElWUjdMRUVFTFI0Q1lXV1dCS0pTN1hZSUtYRFVVIiwibmF0cyI6eyJwdWIiOnt9LCJzdWIiOnt9LCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.c_XQT04wEoVVNDRjPHeKwe17BOrSpQTcftwIbB7KoNEIz6peZCJDc4-J3emVepHofUOWy7IAo9TlLwYhuGHWAQ";

    var nk = Self{};
    try nk.readCredsFile(test_creds_file);
    //try nk.decodeSeed();

    //var nk = try init(test_creds_file);
    const sig = try nk.sign("pero");
    _ = sig;

    try std.testing.expectEqualStrings(nk.seed_base32, test_seed);
    try std.testing.expectEqualStrings(nk.jwt, test_jwt);
}

test "sign" {
    var nk = Self{};
    try nk.readCredsFile(test_creds_file);

    try std.testing.expectEqualStrings(
        try nk.sign("iso medo u ducan"),
        "kguBfkTzHEyPiNVz4lXf4WuRcO3qO4h2DNLJrD7CpMZBmWdUs4ex/A++b97SicvWv7umC1FJ/Ipt+w794sglDg==",
    );
    try std.testing.expectEqualStrings(
        try nk.sign("nije reko dobar dan"),
        "eeIYJ8fw60oAmKFjAoPrZOUoK9aqjNvr79anI+/Z5+tATtVhIUo7W7AvukNueOQUYElvsVkU7DpKGT9KYKIOCQ==",
    );
}

test "pero" {
    // var nk = Self{};
    // try nk.readCredsFile(test_creds_file);
    var nk = try init(test_creds_file);
    defer nk.wipe();
    const sig = try nk.sign("pero");
    std.debug.print("sig: {s}\n", .{sig});
    std.debug.print("ovaj je bitan jwt: \n{s}\n", .{nk.jwt});
}

fn root() []const u8 {
    return comptime (std.fs.path.dirname(@src().file) orelse unreachable) ++ "/";
}
