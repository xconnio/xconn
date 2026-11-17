#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
archives=$(cd "${1:-target/native-dist}" && pwd)
root=$PWD
temporary=$(mktemp -d)
device=
cleanup() {
  if [[ -n "$device" ]]; then
    xcrun simctl shutdown "$device" || true
    xcrun simctl delete "$device" || true
  fi
  rm -rf "$temporary"
}
trap cleanup EXIT

for target in aarch64-apple-ios aarch64-apple-ios-sim; do
  archive=("$archives"/xconn-native-*-"$target".tar.gz)
  tar -xzf "${archive[0]}" -C "$temporary"
  library=("$temporary"/xconn-native-*-"$target"/lib/libxconn_ffi.dylib)
  lipo "${library[0]}" -verify_arch arm64
  codesign --verify --strict "${library[0]}"
  platform=IOS
  if [[ "$target" == *-sim ]]; then
    platform=IOSSIMULATOR
  fi
  xcrun vtool -show-build "${library[0]}" | grep -E "platform +$platform$"
done

flutter create --platforms=ios --project-name xconn_ios_smoke --no-pub "$temporary/app"
cp ci/fixtures/ios_native_main.dart "$temporary/app/lib/main.dart"
mkdir "$temporary/app/integration_test" "$temporary/app/assets"
cp ci/fixtures/ios_native_test.dart "$temporary/app/integration_test/native_test.dart"

# The simulator trusts this test CA; production apps receive no extra trust roots.
cat > "$temporary/ca.conf" <<'CONF'
[req]
distinguished_name=subject
x509_extensions=ca
[subject]
[ca]
basicConstraints=critical,CA:TRUE
keyUsage=critical,keyCertSign,cRLSign
CONF
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=XConn iOS test CA' \
  -config "$temporary/ca.conf" \
  -keyout "$temporary/ca.key" -out "$temporary/ca.pem"
openssl req -newkey rsa:2048 -nodes -subj '/CN=localhost' \
  -keyout "$temporary/app/assets/server.key" -out "$temporary/server.csr"
cat > "$temporary/server.ext" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
EXT
openssl x509 -req -days 1 -in "$temporary/server.csr" \
  -CA "$temporary/ca.pem" -CAkey "$temporary/ca.key" -CAcreateserial \
  -extfile "$temporary/server.ext" -out "$temporary/app/assets/server.pem"

write_pubspec() {
  cat > "$temporary/app/pubspec.yaml" <<YAML
name: xconn_ios_smoke
environment:
  sdk: '>=3.10.0 <4.0.0'
dependencies:
  flutter:
    sdk: flutter
  xconn:
    path: "$root/bindings/dart"
dev_dependencies:
  flutter_test:
    sdk: flutter
  integration_test:
    sdk: flutter
flutter:
  assets:
    - assets/
hooks:
  user_defines:
    xconn:
      native_library: "$1"
YAML
}

library=("$temporary"/xconn-native-*-aarch64-apple-ios-sim/lib/libxconn_ffi.dylib)
write_pubspec "${library[0]}"
# Use a dedicated simulator so this script never resets another developer's device.
device=$(xcrun simctl create 'XConn SDK smoke' 'iPhone 16')
xcrun simctl bootstatus "$device" -b
xcrun simctl keychain "$device" add-root-cert "$temporary/ca.pem"
cd "$temporary/app"
flutter pub get
flutter test --device-id "$device" integration_test/native_test.dart

library=("$temporary"/xconn-native-*-aarch64-apple-ios/lib/libxconn_ffi.dylib)
write_pubspec "${library[0]}"
# Check the device build without requiring Apple signing credentials.
flutter build ios --release --no-codesign
