import "dart:io";

import "package:code_assets/code_assets.dart";
import "package:hooks/hooks.dart";

/// Build the same C ABI used by Python and Go and let Flutter bundle it.
Future<void> main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) {
      return;
    }
    final code = input.config.code;
    final os = code.targetOS;
    final arch = code.targetArchitecture;
    final cpu = switch (arch) {
      Architecture.arm64 => "aarch64",
      Architecture.x64 => "x86_64",
      _ => throw UnsupportedError("Unsupported XConn architecture: $arch"),
    };
    final target = switch (os) {
      OS.linux => "$cpu-unknown-linux-gnu",
      OS.macOS => "$cpu-apple-darwin",
      OS.windows => "$cpu-pc-windows-msvc",
      OS.android => "$cpu-linux-android",
      OS.iOS => switch ((code.iOS.targetSdk == IOSSdk.iPhoneSimulator, cpu)) {
        (true, "aarch64") => "aarch64-apple-ios-sim",
        (true, _) => "x86_64-apple-ios",
        (false, _) => "aarch64-apple-ios",
      },
      _ => throw UnsupportedError("Unsupported XConn platform: $os"),
    };
    final root = input.packageRoot.resolve("../../");
    final prebuiltValue = input.userDefines["native_library"];
    if (prebuiltValue != null && prebuiltValue is! String) {
      throw const FormatException("hooks.user_defines.xconn_core.native_library must be a path");
    }

    final prebuilt = input.userDefines.path("native_library");
    final env = <String, String>{};
    if (os == OS.android) {
      final compiler = code.cCompiler?.compiler;
      if (compiler == null) {
        throw StateError("Android NDK compiler is required to build XConn");
      }
      final clang = compiler.resolve("$target${code.android.targetNdkApi}-clang${Platform.isWindows ? ".cmd" : ""}");
      final key = target.replaceAll("-", "_");
      env["CARGO_TARGET_${key.toUpperCase()}_LINKER"] = clang.toFilePath();
      env["CC_$key"] = clang.toFilePath();
      env["AR_$key"] = compiler.resolve("llvm-ar${Platform.isWindows ? ".exe" : ""}").toFilePath();
    }
    if (os == OS.iOS) {
      env["IPHONEOS_DEPLOYMENT_TARGET"] = "${code.iOS.targetVersion}.0";
    }
    final rust = await Process.run("rustc", ["-vV"]);
    final nativeHost = (rust.stdout as String).split("\n").contains("host: $target");
    if (prebuilt != null && !nativeHost) {
      throw StateError("A prebuilt native library must match the host target $target");
    }

    if (prebuilt == null) {
      final child = await Process.start(
        "cargo",
        [
          "build",
          "--locked",
          "--release",
          "-p",
          "xconn-ffi",
          if (!nativeHost) ...["--target", target],
        ],
        workingDirectory: root.toFilePath(),
        environment: env,
      );
      await Future.wait([stdout.addStream(child.stdout), stderr.addStream(child.stderr)]);
      if (await child.exitCode != 0) {
        throw StateError("XConn native build failed. Install Rust target $target with rustup target add $target.");
      }
    }

    final name = switch (os) {
      OS.windows => "xconn_ffi.dll",
      OS.macOS || OS.iOS => "libxconn_ffi.dylib",
      _ => "libxconn_ffi.so",
    };
    final library = prebuilt ?? root.resolve("target/${nativeHost ? "" : "$target/"}release/$name");
    if (prebuilt != null) {
      output.dependencies.add(prebuilt);
    }

    // Copy into the hook output so Flutter can relocate/re-sign it on Apple platforms.
    final bundled = input.outputDirectory.resolve(name);
    await File.fromUri(library).copy(bundled.toFilePath());
    output.assets.code.add(
      CodeAsset(package: input.packageName, name: "src/native.dart", linkMode: DynamicLoadingBundled(), file: bundled),
    );
    output.dependencies.addAll([
      root.resolve("Cargo.toml"),
      root.resolve("Cargo.lock"),
      for (final base in ["crates", "bindings/runtime", "bindings/ffi"])
        await for (final entry in Directory.fromUri(root.resolve(base)).list(recursive: true))
          if (entry is File && (entry.path.endsWith(".rs") || entry.path.endsWith("Cargo.toml"))) entry.uri,
    ]);
  });
}
