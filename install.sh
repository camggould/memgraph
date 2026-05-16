#!/bin/sh
set -eu

# memgraph install script.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/camggould/memgraph/main/install.sh | sh
#
# Override defaults via env vars:
#   MEMGRAPH_VERSION       version tag to install (default: latest release, e.g. v0.1.0)
#   MEMGRAPH_INSTALL_DIR   destination directory (default: /usr/local/bin, falling back to $HOME/.local/bin)

REPO="camggould/memgraph"
BIN="memgraph"

err()  { printf "error: %s\n" "$*" >&2; exit 1; }
info() { printf "%s\n" "$*"; }

fetch() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  else
    err "neither curl nor wget available"
  fi
}

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    err "no sha256 utility available (need sha256sum or shasum)"
  fi
}

# --- detect platform ---

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  darwin|linux) ;;
  *) err "unsupported OS: $os (Windows: download manually from https://github.com/$REPO/releases)" ;;
esac

arch_raw=$(uname -m)
case "$arch_raw" in
  x86_64|amd64)  arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) err "unsupported arch: $arch_raw" ;;
esac

# --- resolve version ---

version="${MEMGRAPH_VERSION:-}"
if [ -z "$version" ]; then
  meta=$(mktemp)
  fetch "https://api.github.com/repos/$REPO/releases/latest" "$meta" \
    || err "could not query GitHub for latest release"
  version=$(sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' "$meta" | head -1)
  rm -f "$meta"
  [ -n "$version" ] || err "could not parse latest version"
fi
ver_strip="${version#v}"

# --- pick install dir ---

install_dir="${MEMGRAPH_INSTALL_DIR:-}"
use_sudo=0
if [ -z "$install_dir" ]; then
  if [ -w "/usr/local/bin" ]; then
    install_dir="/usr/local/bin"
  elif [ -d "/usr/local/bin" ] && command -v sudo >/dev/null 2>&1; then
    install_dir="/usr/local/bin"
    use_sudo=1
  else
    install_dir="$HOME/.local/bin"
    mkdir -p "$install_dir"
  fi
fi

# --- download + verify + install ---

asset="memgraph_${ver_strip}_${os}_${arch}.tar.gz"
url="https://github.com/$REPO/releases/download/$version/$asset"
sum_url="https://github.com/$REPO/releases/download/$version/checksums.txt"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

info "Downloading memgraph $version ($os/$arch)"
fetch "$url" "$tmp/$asset" || err "download failed: $url"

info "Verifying checksum"
fetch "$sum_url" "$tmp/checksums.txt" || err "checksums download failed"
expected=$(grep "  $asset\$" "$tmp/checksums.txt" | awk '{print $1}')
[ -n "$expected" ] || err "asset $asset not in checksums.txt"
actual=$(sha256 "$tmp/$asset")
[ "$expected" = "$actual" ] || err "checksum mismatch (expected $expected, got $actual)"

info "Extracting"
tar -xzf "$tmp/$asset" -C "$tmp"
[ -f "$tmp/$BIN" ] || err "binary $BIN not found in archive"

info "Installing $BIN to $install_dir"
if [ "$use_sudo" = "1" ]; then
  sudo install -m 755 "$tmp/$BIN" "$install_dir/$BIN"
else
  install -m 755 "$tmp/$BIN" "$install_dir/$BIN" 2>/dev/null \
    || { cp "$tmp/$BIN" "$install_dir/$BIN" && chmod 755 "$install_dir/$BIN"; }
fi

info ""
info "Installed memgraph $version to $install_dir/$BIN"
case ":$PATH:" in
  *":$install_dir:"*) ;;
  *)
    info ""
    info "$install_dir is not in your PATH. Add it via your shell profile:"
    info "  export PATH=\"$install_dir:\$PATH\""
    ;;
esac
info ""
info "Run: memgraph --help"
