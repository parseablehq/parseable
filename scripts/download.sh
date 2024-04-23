#!/bin/zsh

# supported CPU architectures and operating systems
SUPPORTED_ARCH=("x86_64" "arm64")
SUPPORTED_OS=("linux" "darwin")
DOWNLOAD_BASE_URL="parseable.gateway.scarf.sh/"

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
printf "Found latest release version: $release_tag\n"

download_url=${DOWNLOAD_BASE_URL}${CPU_ARCH}-${OS}.${release_tag}

if [[ -d ${INSTALL_DIR} ]]; then
    printf "A Previous version of parseable already exists. Run 'parseable --version' to check the version."
    printf "or consider removing that before new installation\n"
    exit 1
else
    mkdir -p ${BIN_DIR}
fi

# Download the binary using curl or wget
printf "Downloading Parseable version $release_tag, for OS: $OS, CPU architecture: $CPU_ARCH\n\n"
if command -v curl &>/dev/null; then
    curl -L -o "${BIN_NAME}" "$download_url"
elif command -v wget &>/dev/null; then
    wget -O "${BIN_NAME}" "$download_url"
else
    echo "Error: Neither curl nor wget found. Please install either curl or wget."
    exit 1
fi

printf "Parseable Server was successfully installed at: ${BIN_NAME}\n"

chmod +x "${BIN_NAME}"

printf "Adding parseable to the path\n"
PATH_STR="export PATH=${BIN_DIR}"':$PATH'
echo ${PATH_STR} >> ${RC_FILE_PATH}

echo "parseable was added to the path. Please refresh the environment by sourcing the ${RC_FILE_PATH}"
