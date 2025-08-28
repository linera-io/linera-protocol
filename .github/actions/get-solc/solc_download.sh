#!/bin/bash

# ======================================================================================================================
# Solc folder
# ======================================================================================================================
SOLC_FOLDER="$HOME/.solc"
if [ ! -e $SOLC_FOLDER ]; then
  echo "Create solc folder: $SOLC_FOLDER"
  mkdir -p $SOLC_FOLDER
fi

# ======================================================================================================================
# Token
# ======================================================================================================================
if [[ -z $SECRET_TOKEN ]]; then
  SECRET_TOKEN=""
  if [[ ! -z $2 ]]; then
    SECRET_TOKEN=$2
  fi
fi
if [[ ! -z $SECRET_TOKEN ]]; then
  echo "Token: ***"
fi

# ======================================================================================================================
# releases.json
# ======================================================================================================================
RELEASES_PATH="$SOLC_FOLDER/releases.json"
if [ ! -e $RELEASES_PATH ] || [ $(($(date "+%s") - $(date -r $RELEASES_PATH "+%s"))) -ge 600 ]; then
  echo "Download: releases.json"
  if [ -z $SECRET_TOKEN ]; then
    curl -sL -o "$RELEASES_PATH.tmp" \
      -s https://api.github.com/repos/ethereum/solidity/releases
  else
    curl -sL -o "$RELEASES_PATH.tmp" \
      -H "Authorization: Bearer ${SECRET_TOKEN}" \
      -s https://api.github.com/repos/ethereum/solidity/releases
  fi
  mv "$RELEASES_PATH.tmp" $RELEASES_PATH
fi

# check release.json
MESSAGE=$(jq '.message?' -r $RELEASES_PATH)
if [[ ! -z $MESSAGE ]]; then
  echo "Message: $MESSAGE"
  rm $RELEASES_PATH
  exit 5
fi

# ======================================================================================================================
# pre-release (prerelease=true)
# ======================================================================================================================
if [[ -z $SOLC_PRERELEASE ]]; then
  SOLC_PRERELEASE="false"
  if [[ ! -z $3 ]]; then
    SOLC_PRERELEASE=$3
  fi
else
  if [ $SOLC_PRERELEASE != "true" ] && [ $SOLC_PRERELEASE != "false" ]; then
    SOLC_PRERELEASE="false"
  else
    SOLC_PRERELEASE="true"
  fi
fi

echo "Pre-release: $SOLC_PRERELEASE"

SELECT_PRERELEASE=""
if [ $SOLC_PRERELEASE == "false" ]; then
  SELECT_PRERELEASE=".prerelease==false"
else
  SELECT_PRERELEASE="."
fi

# ======================================================================================================================
# Solc VERSION
# ======================================================================================================================
VERSION=""
if [[ ! -z $SOLC_VERSION ]]; then
  VERSION=$SOLC_VERSION
elif [[ ! -z $1 ]]; then
  VERSION=$1
fi
if [[ $VERSION == "latest" || $VERSION == "new" || $VERSION == "last" || -z $VERSION ]]; then
  # Get the latest VERSION
  VERSION=$(cat "$RELEASES_PATH" | jq -r '.[] | select(("${SELECT_PRERELEASE}") and .tag_name) .tag_name' | head -n1)
  if [[ -z $VERSION ]]; then
    echo "{$VERSION|$SOLC_PRERELEASE} The specified version of solc was not found"
    exit 6
  fi
else
  if [ ! $(cat "$RELEASES_PATH" | jq ".[] | select("${SELECT_PRERELEASE}" and .tag_name==\"${VERSION}\") .tag_name") ]; then
    echo "{$VERSION} The specified version of solc was not found"
    exit 1
  fi
fi
echo "version: $VERSION"

if [[ "$OSTYPE" == "linux-gnu"* || "$OSTYPE" == "freebsd"* || "$OSTYPE" == "cygwin" ]]; then
  NAME="solc-static-linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  NAME="solc-macos"
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
  NAME="solc-windows.exe"
else
  echo "Unknown OS"
  exit 2
fi

# ======================================================================================================================
# Download
# ======================================================================================================================
DOWNLOAD_URL=$(
  cat "$RELEASES_PATH" |
    jq -r "
    .[]
    | select(${SELECT_PRERELEASE} and .tag_name==\"${VERSION}\") .assets
    | .[]
    | select(.name==\"${NAME}\")
    | .browser_download_url
  "
)

if [ -z $DOWNLOAD_URL ]; then
  echo "Releases \"${VERSION}/${NAME}\" not found"
  exit 3
fi

VERSION_DIR="$SOLC_FOLDER/$VERSION"
if [ ! -e $VERSION_DIR ]; then
  if ! mkdir -p $VERSION_DIR; then
    echo "Failed to create a directory ${VERSION_DIR}"
    exit 7
  fi
fi

FILE_PATH="$VERSION_DIR/$NAME"
if [ ! -e $FILE_PATH ]; then
  echo "Download: $DOWNLOAD_URL"
  if [ -z $SECRET_TOKEN ]; then
    curl -sL --fail \
      -H "Accept: application/octet-stream" \
      -o "$FILE_PATH.tmp" \
      -s $DOWNLOAD_URL
  else
    curl -sL --fail \
      -H "Accept: application/octet-stream" \
      -H "Authorization: Bearer ${SECRET_TOKEN}" \
      -o "$FILE_PATH.tmp" \
      -s $DOWNLOAD_URL
  fi
  mv "$FILE_PATH.tmp" $FILE_PATH
fi

echo "chmod 1755 $FILE_PATH"
chmod 1755 $FILE_PATH
echo "create link $FILE_PATH"
echo "OS: ${OSTYPE}"

if [[ "$OSTYPE" == "linux-gnu"* || "$OSTYPE" == "freebsd"* || "$OSTYPE" == "cygwin" ]]; then
  mkdir -p $HOME/.local/bin
  ln -sf "$FILE_PATH" $HOME/.local/bin/solc
  echo "$HOME/.local/bin" >>$GITHUB_PATH
elif [[ "$OSTYPE" == "darwin"* ]]; then
  ln -sf "$FILE_PATH" /usr/local/bin/solc
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
  mkdir -p "$HOME/.local/bin"
  ln -sf "$FILE_PATH" "$HOME/.local/bin/solc"
  echo "$HOME/.local/bin" >>$GITHUB_PATH
else
  echo "Unknown OS"
  exit 4
fi

# ======================================================================================================================
# run
# ======================================================================================================================
echo "run: $FILE_PATH --version"
$FILE_PATH --version
solc --version
