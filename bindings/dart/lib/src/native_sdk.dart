import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:archive/archive.dart";
import "package:crypto/crypto.dart";

const nativeSdkVersion = "0.1.1";
const nativeSdkChecksums = {
  "aarch64-apple-darwin": "ab0e3a84bb3ca13c2e078f7798c38390322b594d123f5099d1af1ed09f2bc24f",
  "aarch64-linux-android": "7e7e92a6817333bc73d883e712ea598b6bfc2a7dabc703d1ddbe5b23bce4b327",
  "aarch64-unknown-linux-gnu": "0939e739afccd5343eeb2a0f4ef3c090f40c39c82b92c1e2f1026bfd0946be3d",
  "x86_64-unknown-linux-gnu": "7af03bfd01e0b8ae9353cfa0dd336f875d26639c3839e798894e2eda2f65ef14",
};

/// Downloads an exact SDK release and atomically caches its verified library.
class NativeSdk {
  NativeSdk({required Directory cacheDirectory, required this.target, required this.archiveSha256, Uri? releaseBase})
    : _directory = Directory.fromUri(cacheDirectory.uri.resolve("$nativeSdkVersion/$target/")),
      _releaseBase =
          releaseBase ?? Uri.parse("https://github.com/xconnio/xconn-core/releases/download/v$nativeSdkVersion/");

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
