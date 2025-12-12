#!/bin/zsh

# supported CPU architectures and operating systems
SUPPORTED_ARCH=("x86_64" "arm64")
SUPPORTED_OS=("linux" "darwin")
DOWNLOAD_BASE_URL="https://github.com/parseablehq/parseable/releases/download"
ARM_APPLE_PREFIX="Parseable_OSS_aarch64-apple-darwin"
ARM_LINUX_PREFIX="Parseable_OSS_aarch64-unknown-linux-gnu"
INTEL_LINUX_PREFIX="Parseable_OSS_x86_64-unknown-linux-gnu"
PARSEABLE_PREFIX=${ARM_APPLE_PREFIX}

# Get the system's CPU architecture and operating system
CPU_ARCH=$(uname -m)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

printf "\n=========================\n"
printf "Detected CPU architecture: %s\n" "$CPU_ARCH"
printf "Detected operating system: %s\n" "$OS"

SHELL_NAME=$(basename $SHELL)
RC_FILE=".${SHELL_NAME}rc"
RC_FILE_PATH="${HOME}/${RC_FILE}"
INSTALL_DIR="${HOME}/.parseable"
BIN_DIR="${INSTALL_DIR}/bin"
BIN_NAME="${BIN_DIR}/parseable"

# Check if the CPU architecture is supported
if ! echo "${SUPPORTED_ARCH[@]}" | grep -q "\\b${CPU_ARCH}\\b"; then
    echo "Error: Unsupported CPU architecture (${CPU_ARCH})."
    exit 1
fi
# Check if the OS is supported
if ! echo "${SUPPORTED_OS[@]}" | grep -q "\\b${OS}\\b"; then
    echo "Error: Unsupported operating system (${OS})."
    exit 1
fi

# Get the latest release information using GitHub API
release=$(curl -s "https://api.github.com/repos/parseablehq/parseable/releases/latest")
# find the release tag
release_tag=$(echo "$release" | grep -o "\"tag_name\":\s*\"[^\"]*\"" | cut -d '"' -f 4)
if [[ -z "$release_tag" ]]; then
    echo "Error: Could not determine the latest release version."
    exit 1
fi

printf "Latest Parseable version: $release_tag\n"

# Determine the appropriate binary prefix based on OS and CPU architecture
declare -A PREFIX_MAP=(
  ["darwin_arm64"]=$ARM_APPLE_PREFIX
  ["linux_arm64"]=$ARM_LINUX_PREFIX
  ["linux_x86_64"]=$INTEL_LINUX_PREFIX
)
key="${OS}_${CPU_ARCH}"
PARSEABLE_PREFIX=${PREFIX_MAP[$key]:-""} || {
  echo "Error: unsupported platform $OS/$CPU_ARCH"; exit 1;
}

download_url=${DOWNLOAD_BASE_URL}/${release_tag}/${PARSEABLE_PREFIX}

if [[ -d ${INSTALL_DIR} ]]; then
    printf "A Previous version of parseable already exists. Run 'parseable --version' to check the version."
    printf "or consider removing that before new installation\n"
    exit 1
else
    mkdir -p ${BIN_DIR}
fi

# Download the binary using curl or wget
printf "Downloading Parseable version $release_tag, for OS: $OS, CPU architecture: $CPU_ARCH\n"
printf "Download URL: $download_url\n\n"

if command -v curl &>/dev/null; then
    curl -fL -o "${BIN_NAME}" "$download_url" || { echo "Error: download failed"; exit 1; }
elif command -v wget &>/dev/null; then
    wget -q -O "${BIN_NAME}" "$download_url" || { echo "Error: download failed"; exit 1; }
else
    echo "Error: Neither curl nor wget found. Please install either curl or wget."
    exit 1
fi

printf "Parseable Server was successfully installed at: ${BIN_NAME}\n"

chmod +x "${BIN_NAME}"

PATH_STR="export PATH=${BIN_DIR}"':$PATH'
echo ${PATH_STR} >> ${RC_FILE_PATH}
source ${RC_FILE_PATH}
