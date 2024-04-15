#!/bin/zsh

# supported CPU architectures and operating systems
SUPPORTED_ARCH=("x86_64" "arm64")
SUPPORTED_OS=("linux" "darwin")
# Associate binaries with CPU architectures and operating systems
declare -A BINARIES=(
    ["x86_64-linux"]="Parseable_x86_64-unknown-linux-gnu"
    ["arm64-linux"]="Parseable_aarch64-unknown-linux-gnu"
    ["x86_64-darwin"]="Parseable_x86_64-apple-darwin"
    ["arm64-darwin"]="Parseable_aarch64-apple-darwin"
)
# Get the system's CPU architecture and operating system
CPU_ARCH=$(uname -m)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

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

printf "Fetching release information for parseable...\n"

# Loop through binaries in the release and find the appropriate one
for arch_os in "${CPU_ARCH}-${OS}"; do
    binary_name="${BINARIES[$arch_os]}"
    download_url=$(echo "$release" | grep -o "\"browser_download_url\":\s*\"[^\"]*${binary_name}\"" | cut -d '"' -f 4)
    if [ -n "$download_url" ]; then
        break
    fi
done

printf "Checking for existing installation...\n"
if [[ -d ${INSTALL_DIR} ]]; then
    printf "A Previous version of parseable already exists. Run 'parseable --version' to check the version."
    printf "or consider removing that before Installing"
    exit 1

else
    printf "No Previous installation found\n"
    printf "Installing parseable...\n"
    mkdir -p ${BIN_DIR}
fi


# Download the binary using curl or wget
if command -v curl &>/dev/null; then
    curl -L -o "${BIN_NAME}" "$download_url" 2&>> /dev/null
elif command -v wget &>/dev/null; then
    wget -O "${BIN_NAME}" "$download_url" 2&>> /dev/null
else
    echo "Error: Neither curl nor wget found. Please install either curl or wget."
    exit 1
fi

printf "Parseable Server was successfully installed at: ${BIN_NAME}\n"

chmod +x "${BIN_NAME}"

printf "Adding parseable to the path\n"
PATH_STR="export PATH=${BIN_DIR}"':$PATH'
echo ${PATH_STR} >> ${RC_FILE_PATH}

echo "parseable was added to the path. Please refresh the environment by sourcing the ${RC_PATH}"
