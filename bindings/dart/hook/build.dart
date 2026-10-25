import "dart:io";

import "package:code_assets/code_assets.dart";
import "package:hooks/hooks.dart";
import "package:xconn_core/src/native_sdk.dart";

/// Bundle a verified SDK release without requiring Rust or a source checkout.
Future<void> main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) {
      return;
    }
    final code = input.config.code;
    final cpu = switch (code.targetArchitecture) {
      Architecture.arm64 => "aarch64",
      Architecture.x64 => "x86_64",
      _ => throw UnsupportedError("Unsupported XConn architecture: ${code.targetArchitecture}"),
    };
    final target = switch (code.targetOS) {
      OS.linux => "$cpu-unknown-linux-gnu",
      OS.macOS => "$cpu-apple-darwin",
      OS.android => "$cpu-linux-android",
      OS.windows => "$cpu-pc-windows-msvc",
      OS.iOS => switch ((code.iOS.targetSdk == IOSSdk.iPhoneSimulator, cpu)) {
        (true, "aarch64") => "aarch64-apple-ios-sim",
        (true, _) => "x86_64-apple-ios",
        (false, _) => "aarch64-apple-ios",
      },
      _ => throw UnsupportedError("No native SDK release for ${code.targetOS}; use a supported platform"),
    };
    final prebuiltValue = input.userDefines["native_library"];
    if (prebuiltValue != null && prebuiltValue is! String) {
      throw const FormatException("hooks.user_defines.xconn_core.native_library must be a path");
    }
    final prebuilt = input.userDefines.path("native_library");
    late final File library;
    if (prebuilt != null) {
      library = File.fromUri(prebuilt);
      output.dependencies.add(prebuilt);
    } else {
      final checksum = nativeSdkChecksums[target];
      if (checksum == null) {
        throw UnsupportedError("No native SDK release for $target; provide native_library or use a supported platform");
      }
      if (code.targetOS == OS.android && code.android.targetNdkApi < 24) {
        throw UnsupportedError("XConn requires Android API level 24 or newer");
      }
      final sdk = NativeSdk(
        cacheDirectory: Directory.fromUri(input.outputDirectoryShared),
        target: target,
        archiveSha256: checksum,
      );
      library = await sdk.download();
    }
    final name = switch (code.targetOS) {
      OS.windows => "xconn_ffi.dll",
      OS.macOS || OS.iOS => "libxconn_ffi.dylib",
      _ => "libxconn_ffi.so",
    };
    final bundled = input.outputDirectory.resolve(name);
    await library.copy(bundled.toFilePath());
    output.assets.code.add(
      CodeAsset(package: input.packageName, name: "src/native.dart", linkMode: DynamicLoadingBundled(), file: bundled),
    );
  });
}
