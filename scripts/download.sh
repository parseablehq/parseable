#!/bin/sh

DOWNLOAD_BASE_URL="https://github.com/parseablehq/parseable/releases/download"
INSTALL_DIR="${HOME}/.parseable"
BIN_DIR="${INSTALL_DIR}/bin"
BIN_NAME="${BIN_DIR}/parseable"

# Get the system's CPU architecture and operating system.
CPU_ARCH=$(uname -m)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

printf '\n=========================\n'
printf 'Detected CPU architecture: %s\n' "$CPU_ARCH"
printf 'Detected operating system: %s\n' "$OS"

# Select one downloader and use it for both the release lookup and the binary.
if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
else
    printf 'Error: Neither curl nor wget was found. Please install either curl or wget.\n' >&2
    exit 1
fi

# Match the platform names used by the release assets. Linux commonly reports
# ARM64 as "aarch64", while macOS reports it as "arm64".
case "${OS}_${CPU_ARCH}" in
    darwin_arm64)
        PARSEABLE_PREFIX="Parseable_OSS_aarch64-apple-darwin"
        ;;
    linux_arm64|linux_aarch64)
        PARSEABLE_PREFIX="Parseable_OSS_aarch64-unknown-linux-gnu"
        ;;
    linux_x86_64|linux_amd64)
        PARSEABLE_PREFIX="Parseable_OSS_x86_64-unknown-linux-gnu"
        ;;
    *)
        printf 'Error: unsupported platform %s/%s.\n' "$OS" "$CPU_ARCH" >&2
        exit 1
        ;;
esac

# Parseable's Linux release binaries require glibc.
if [ "$OS" = "linux" ]; then
    MUSL_FOUND="false"
    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
        MUSL_FOUND="true"
    else
        for MUSL_LOADER in /lib/ld-musl-*.so.1; do
            if [ -e "$MUSL_LOADER" ]; then
                MUSL_FOUND="true"
                break
            fi
        done
    fi
    if [ "$MUSL_FOUND" = "true" ]; then
        printf 'Error: Alpine Linux and other musl-based systems are not supported by the available release binaries.\n' >&2
        exit 1
    fi
fi

# Get the latest release tag from the GitHub API.
fetch_release() {
    case "$DOWNLOADER" in
        curl)
            curl -fsSL "https://api.github.com/repos/parseablehq/parseable/releases/latest"
            ;;
        wget)
            wget -q -O - "https://api.github.com/repos/parseablehq/parseable/releases/latest"
            ;;
    esac
}
if ! release=$(fetch_release); then
    printf 'Error: Could not fetch the latest release information.\n' >&2
    exit 1
fi
release_tag=$(printf '%s\n' "$release" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
if [ -z "$release_tag" ]; then
    printf 'Error: Could not determine the latest release version.\n' >&2
    exit 1
fi

printf 'Latest Parseable version: %s\n' "$release_tag"

download_url="${DOWNLOAD_BASE_URL}/${release_tag}/${PARSEABLE_PREFIX}"

if [ -d "$INSTALL_DIR" ]; then
    printf "A previous version of Parseable already exists. Run 'parseable --version' to check the version,\n"
    printf 'or remove %s before installing a new version.\n' "$INSTALL_DIR"
    exit 1
fi
mkdir -p "$BIN_DIR" || {
    printf 'Error: Could not create installation directory %s.\n' "$BIN_DIR" >&2
    rmdir "$BIN_DIR" "$INSTALL_DIR" 2>/dev/null || :
    exit 1
}

TEMP_BIN="${BIN_NAME}.tmp"
# Remove files and empty directories created by an incomplete installation.
cleanup_install() {
    rm -f "$TEMP_BIN" "$BIN_NAME"
    rmdir "$BIN_DIR" "$INSTALL_DIR" 2>/dev/null || :
}
trap 'cleanup_install' 0
trap 'cleanup_install; exit 1' 1 2 15

printf 'Downloading Parseable version %s, for OS: %s, CPU architecture: %s\n' "$release_tag" "$OS" "$CPU_ARCH"
printf 'Download URL: %s\n\n' "$download_url"

# Download the selected release asset to the temporary installation path.
download_binary() {
    case "$DOWNLOADER" in
        curl)
            curl -fL -o "$TEMP_BIN" "$download_url"
            ;;
        wget)
            wget -q -O "$TEMP_BIN" "$download_url"
            ;;
    esac
}
if ! download_binary; then
    printf 'Error: download failed.\n' >&2
    exit 1
fi

chmod +x "$TEMP_BIN" || {
    printf 'Error: Could not make the downloaded binary executable.\n' >&2
    exit 1
}
mv "$TEMP_BIN" "$BIN_NAME" || {
    printf 'Error: Could not finish installing %s.\n' "$BIN_NAME" >&2
    exit 1
}
trap - 0 1 2 15

printf 'Parseable Server was successfully installed at: %s\n' "$BIN_NAME"

# Use the user's login shell to select its startup file. Do not source that file
# here: this installer may be running under a different shell via `... | sh`.
SHELL_NAME=${SHELL##*/}
case "$SHELL_NAME" in
    zsh)
        RC_FILE_PATH="${ZDOTDIR:-$HOME}/.zshrc"
        ;;
    bash)
        RC_FILE_PATH="${HOME}/.bash_profile"
        for BASH_PROFILE in .bash_profile .bash_login .profile; do
            if [ -f "${HOME}/${BASH_PROFILE}" ]; then
                RC_FILE_PATH="${HOME}/${BASH_PROFILE}"
                break
            fi
        done
        ;;
    sh|dash|ksh)
        RC_FILE_PATH="${HOME}/.profile"
        ;;
    *)
        RC_FILE_PATH=""
        ;;
esac

PATH_STR="export PATH=\"${BIN_DIR}:\$PATH\""
if [ -n "$RC_FILE_PATH" ]; then
    if grep -Fqx "$PATH_STR" "$RC_FILE_PATH" 2>/dev/null; then
        printf 'Parseable is already configured in %s.\n' "$RC_FILE_PATH"
    elif printf '%s\n' "$PATH_STR" >> "$RC_FILE_PATH"; then
        printf 'Added Parseable to PATH in %s.\n' "$RC_FILE_PATH"
    else
        printf 'Warning: Could not update %s.\n' "$RC_FILE_PATH" >&2
    fi
else
    printf 'No supported shell profile was detected.\n'
fi
printf 'To use Parseable in the current shell, run:\n  %s\n' "$PATH_STR"
