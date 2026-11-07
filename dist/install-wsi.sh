#!/usr/bin/env bash
# Build and install ferriscope's Vulkan WSI layer.
#
#   dist/install-wsi.sh [--prefix PREFIX]
#
# Builds the layer from the ferriscope revision in Cargo.lock, so the layer
# matches the compositor linked into moonshine, for x86_64 and i686 (32-bit
# Vulkan games, 32-bit DXVK/vkd3d-proton), and installs, under PREFIX
# (default /usr):
#   PREFIX/lib/ferriscope/libferriscope_wsi.so
#   PREFIX/lib32/ferriscope/libferriscope_wsi.so
#   PREFIX/share/vulkan/implicit_layer.d/VkLayer_FERRISCOPE_wsi.x86_64.json
#   PREFIX/share/vulkan/implicit_layer.d/VkLayer_FERRISCOPE_wsi.i686.json
# Each manifest names its library by its final PREFIX path.
#
# Run it as your own user: it builds as you and uses sudo only to install into
# a directory you cannot write. Packaging sets DESTDIR to install into a
# staging directory instead of the system.
#
# FERRISCOPE_SRC overrides the ferriscope checkout (for a local ferriscope
# that is not pushed yet). Needs cargo, jq, the i686-unknown-linux-gnu Rust
# target and a 32-bit glibc for cc -m32 (Arch: lib32-rust-libs, lib32-glibc);
# without the i686 toolchain it fails before installing anything.
set -euo pipefail

prefix=/usr
while [ $# -gt 0 ]; do
	case $1 in
	--prefix) prefix=$2; shift 2 ;;
	-h | --help) sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; /^set -euo/d'; exit 0 ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done
root=$(cd "$(dirname "$0")/.." && pwd)

# ferriscope's installer resolves DESTDIR against the current directory; make
# it absolute so a relative staging directory works from anywhere.
if [ -n "${DESTDIR:-}" ]; then
	DESTDIR=$(realpath -m "$DESTDIR")
	export DESTDIR
fi

ferriscope=${FERRISCOPE_SRC:-}
if [ -z "$ferriscope" ]; then
	manifest=$(cd "$root" && cargo metadata --format-version 1 --locked |
		jq -r '.packages[] | select(.name == "ferriscope") | .manifest_path')
	if [ -z "$manifest" ]; then
		echo "ferriscope is not in Cargo.lock" >&2
		exit 1
	fi
	# .../Cargo.toml -> the ferriscope checkout.
	ferriscope=$(dirname "$manifest")
fi

# One build directory per ferriscope checkout. cargo treats a checkout under
# ~/.cargo/git as a non-local package and never compares its sources' times, so
# a shared directory kept answering "Fresh" with the layer of the first
# revision it built, and every later revision installed that old layer.
checkout=$(realpath "$ferriscope")
CARGO_TARGET_DIR=$root/target/ferriscope-wsi/$(printf '%s' "$checkout" | sha256sum | cut -c1-16) \
	"$ferriscope/tools/install-wsi.sh" --prefix "$prefix"
