import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:archive/archive.dart";
import "package:crypto/crypto.dart";
import "package:test/test.dart";
import "package:xconn_core/src/native_sdk.dart";

const _target = "x86_64-unknown-linux-gnu";
const _libraryName = "libxconn_ffi.so";
final _libraryBytes = Uint8List.fromList(utf8.encode("fixture native library"));

Uint8List _archive({String target = _target, int abi = 1, bool includeLibrary = true}) {
  const prefix = "xconn-native-$nativeSdkVersion-$_target";
  final manifest = {
    "version": nativeSdkVersion,
    "target": target,
    "abi_version": abi,
    "files": {"lib/$_libraryName": sha256.convert(_libraryBytes).toString()},
  };
  final archive = Archive()
    ..addFile(ArchiveFile.string("$prefix/manifest.json", jsonEncode(manifest)))
    ..addFile(ArchiveFile.string("../escaped-file", "must not be extracted"));
  if (includeLibrary) {
    archive.addFile(ArchiveFile("$prefix/lib/$_libraryName", _libraryBytes.length, _libraryBytes));
  }
  return Uint8List.fromList(const GZipEncoder().encode(TarEncoder().encode(archive)));
}

void main() {
  late Directory temporary;
  late HttpServer server;
  late Uint8List archive;
  late int requests;

  setUp(() async {
    temporary = await Directory.systemTemp.createTemp("xconn-sdk-test-");
    archive = _archive();
    requests = 0;
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    unawaited(
      server.forEach((request) async {
        requests++;
        request.response.add(archive);
        await request.response.close();
      }),
    );
  });

  tearDown(() async {
    await server.close(force: true);
    await temporary.delete(recursive: true);
  });

  NativeSdk sdk({String? checksum}) => NativeSdk(
    cacheDirectory: temporary,
    target: _target,
    archiveSha256: checksum ?? sha256.convert(archive).toString(),
    releaseBase: Uri.parse("http://127.0.0.1:${server.port}/"),
  );

  test("verified downloads are reusable offline without extracting other files", () async {
    final downloader = sdk();
    final library = await downloader.download();
    expect(await library.readAsBytes(), _libraryBytes);
    expect(requests, 1);
    expect(File.fromUri(temporary.uri.resolve("escaped-file")).existsSync(), isFalse);
    await server.close(force: true);
    expect((await downloader.download()).path, library.path);
    expect(requests, 1);
  });

  test("an incorrect archive hash is rejected without caching a library", () async {
    await expectLater(sdk(checksum: "0" * 64).download(), throwsFormatException);
    expect(temporary.listSync(recursive: true).whereType<File>(), isEmpty);
  });

  test("a manifest for a different target or ABI is rejected", () async {
    for (final bytes in [_archive(target: "wrong-target"), _archive(abi: 99)]) {
      archive = bytes;
      await expectLater(sdk().download(), throwsFormatException);
    }
  });

  test("missing libraries are rejected", () async {
    archive = _archive(includeLibrary: false);
    await expectLater(sdk().download(), throwsFormatException);
  });

  test("corrupt cached libraries and archives are rejected", () async {
    final downloader = sdk();
    final library = await downloader.download();
    await library.writeAsString("corrupt");
    await expectLater(downloader.download(), throwsFormatException);
    await library.writeAsBytes(_libraryBytes);
    await File.fromUri(library.parent.uri.resolve("sdk.tar.gz")).writeAsString("corrupt");
    await expectLater(downloader.download(), throwsFormatException);
    expect(requests, 1);
  });

  test("concurrent downloads install one complete usable SDK", () async {
    final downloads = await Future.wait([sdk().download(), sdk().download()]);
    expect(downloads[0].path, downloads[1].path);
    expect(await downloads[0].readAsBytes(), _libraryBytes);
    expect(
      temporary.listSync(recursive: true).whereType<Directory>().any((entry) => entry.path.contains(".download-")),
      isFalse,
    );
  });
}
