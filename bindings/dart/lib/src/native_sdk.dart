import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:archive/archive.dart";
import "package:crypto/crypto.dart";

const nativeSdkVersion = "0.1.5";
const nativeSdkChecksums = {
  "aarch64-apple-darwin": "34c355dc8369375d267a9853fd558e57bcc9ad4b0f9fa057dbb4a3b232f931bf",
  "aarch64-linux-android": "256da26cbeddf2ea826367fdf4d2b5264cdba1f52b2620e6d8078da69c7224ab",
  "aarch64-unknown-linux-gnu": "648368099442c72c22b0b62f0b0f2b50641ab86179aa68017de240993502d683",
  "x86_64-unknown-linux-gnu": "76e7aa7ab807621f8484955d70222db1fc534768d0e74c7b02cc57aa6f4197ab",
};

/// Downloads an exact SDK release and atomically caches its verified library.
class NativeSdk {
  NativeSdk({required Directory cacheDirectory, required this.target, required this.archiveSha256, Uri? releaseBase})
    : _directory = Directory.fromUri(cacheDirectory.uri.resolve("$nativeSdkVersion/$target/")),
      _releaseBase = releaseBase ?? Uri.parse("https://github.com/xconnio/xconn/releases/download/v$nativeSdkVersion/");

  final String target;
  final String archiveSha256;
  final Directory _directory;
  final Uri _releaseBase;

  String get libraryName => target.endsWith("apple-darwin") ? "libxconn_ffi.dylib" : "libxconn_ffi.so";

  Future<File> download() async {
    if (_directory.existsSync()) {
      return _validate(_directory);
    }
    await _directory.parent.create(recursive: true);
    final temporary = await _directory.parent.createTemp(".download-");
    final client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 30)
      ..findProxy = HttpClient.findProxyFromEnvironment;
    try {
      final name = "xconn-native-$nativeSdkVersion-$target";
      final uri = _releaseBase.resolve("$name.tar.gz");
      final request = await client.getUrl(uri);
      final response = await request.close();
      if (response.statusCode != HttpStatus.ok) {
        throw HttpException("Native SDK download failed: HTTP ${response.statusCode}", uri: uri);
      }
      final bytes = BytesBuilder(copy: false);
      await response.forEach(bytes.add).timeout(const Duration(minutes: 2));
      final compressed = bytes.takeBytes();
      if (sha256.convert(compressed).toString() != archiveSha256) {
        throw const FormatException("Native SDK archive checksum mismatch");
      }
      final archive = TarDecoder().decodeBytes(const GZipDecoder().decodeBytes(compressed));
      final entry = archive.findFile("$name/lib/$libraryName");
      if (entry == null || !entry.isFile || entry.isSymbolicLink) {
        throw FormatException("Native SDK is missing a regular library: $libraryName");
      }
      await File.fromUri(temporary.uri.resolve(libraryName)).writeAsBytes(entry.content);
      await File.fromUri(temporary.uri.resolve("sdk.tar.gz")).writeAsBytes(compressed);
      await _validate(temporary);
      try {
        await temporary.rename(_directory.path);
      } on FileSystemException {
        // Another hook may have installed this same SDK while we downloaded it.
        if (!_directory.existsSync()) {
          rethrow;
        }
      }
      return await _validate(_directory);
    } finally {
      client.close(force: true);
      if (temporary.existsSync()) {
        await temporary.delete(recursive: true);
      }
    }
  }

  Future<File> _validate(Directory directory) async {
    final compressed = await File.fromUri(directory.uri.resolve("sdk.tar.gz")).readAsBytes();
    if (sha256.convert(compressed).toString() != archiveSha256) {
      throw const FormatException("Cached native SDK archive checksum mismatch; remove the SDK cache and retry");
    }
    final archive = TarDecoder().decodeBytes(const GZipDecoder().decodeBytes(compressed));
    final manifest = archive.findFile("xconn-native-$nativeSdkVersion-$target/manifest.json");
    if (manifest == null || !manifest.isFile || manifest.isSymbolicLink) {
      throw const FormatException("Native SDK manifest is missing");
    }
    final decoded = jsonDecode(utf8.decode(manifest.content));
    if (decoded is! Map<String, dynamic> ||
        decoded["version"] != nativeSdkVersion ||
        decoded["target"] != target ||
        decoded["abi_version"] != 1 ||
        decoded["files"] is! Map<String, dynamic>) {
      throw const FormatException("Native SDK manifest does not match the requested version, target, or ABI");
    }
    if (target.endsWith("linux-android") && decoded["android_api_level"] != 24) {
      throw const FormatException("Native SDK Android API level must be 24");
    }
    final library = File.fromUri(directory.uri.resolve(libraryName));
    final expected = (decoded["files"] as Map<String, dynamic>)["lib/$libraryName"];
    if ((await sha256.bind(library.openRead()).first).toString() != expected) {
      throw const FormatException("Native SDK library checksum mismatch; remove the SDK cache and retry");
    }
    return library;
  }
}
