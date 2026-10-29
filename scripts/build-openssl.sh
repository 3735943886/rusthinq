#!/usr/bin/env bash
# Build a static OpenSSL for a release target from a verified source tarball.
#
#   scripts/build-openssl.sh TARGET TARBALL PREFIX
#
# Release builds link this instead of openssl-src (OPENSSL_DIR, OPENSSL_STATIC,
# OPENSSL_NO_VENDOR), so the OpenSSL line and patch level are ours to choose.
# The options mirror what openssl-sys's vendored build passes to openssl-src.
# CC selects the compiler (a cross compiler for cross targets); MSVC is built
# by the release workflow itself.
set -euo pipefail

target=$1 tarball=$2 prefix=$3

case "$target" in
    x86_64-unknown-linux-musl) os=linux-x86_64 cc_default=musl-gcc arch_flags=(-m64) ;;
    aarch64-unknown-linux-musl) os=linux-aarch64 cc_default=aarch64-linux-musl-gcc arch_flags=() ;;
    armv7-unknown-linux-musleabihf)
        os=linux-armv4 cc_default=arm-linux-musleabihf-gcc
        arch_flags=(-march=armv7-a -mfpu=vfpv3-d16) ;;
    x86_64-apple-darwin) os=darwin64-x86_64-cc cc_default=cc arch_flags=() ;;
    aarch64-apple-darwin) os=darwin64-arm64-cc cc_default=cc arch_flags=() ;;
    *) echo "build-openssl.sh: unsupported target $target" >&2; exit 1 ;;
esac

options=(
    no-shared no-module no-tests no-comp no-zlib no-zlib-dynamic --libdir=lib
    no-ssl3 no-md2 no-rc5 no-weak-ssl-ciphers no-camellia no-idea no-seed
)
case "$target" in
    *musl*) options+=(no-engine no-async -DOPENSSL_NO_SECURE_MEMORY) ;;
esac

cc=${CC:-$cc_default}
# A cross gcc comes with its binutils; the host ar may not index foreign objects.
if [[ $cc == *-gcc ]] && command -v "${cc%gcc}ar" >/dev/null; then
    export AR="${cc%gcc}ar" RANLIB="${cc%gcc}ranlib"
fi
case "$target" in
    # Rust's own minimum macOS versions, so the objects link without newer-version warnings.
    x86_64-apple-darwin) export MACOSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-10.12} ;;
    aarch64-apple-darwin) export MACOSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-11.0} ;;
esac

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
tar -xzf "$tarball" -C "$work" --strip-components=1
cd "$work"
# As openssl-src: configuration and default root certificates load from /usr/local/ssl.
CC="$cc" ./Configure "$os" "${options[@]}" --prefix="$prefix" --openssldir=/usr/local/ssl \
    -O2 -ffunction-sections -fdata-sections -fPIC ${arch_flags[@]+"${arch_flags[@]}"}
jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)
make -j"$jobs" build_libs
make install_dev
