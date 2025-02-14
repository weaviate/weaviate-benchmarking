#!/bin/bash

set -eou pipefail

DOCKER_REPO="semitechnologies/weaviate-benchmarker"

function main() {
  init
  echo "git ref type is \"$GITHUB_REF_TYPE\""
  echo "git ref name is \"$GITHUB_REF_NAME\""
  build_and_push_tag
}

function init() {
  docker run --privileged --rm tonistiigi/binfmt --install all
  docker buildx create --use
  docker buildx inspect --bootstrap
}

function build_and_push_tag() {
  if [ ! -z "$GITHUB_REF_NAME" ] && [ "$GITHUB_REF_TYPE" == "tag" ]; then
    tag_git="$DOCKER_REPO:$GITHUB_REF_NAME"
    tag_latest="$DOCKER_REPO:latest"

    echo "Tag & Push $tag_latest, $tag_git"
    docker buildx build --platform=linux/arm64,linux/amd64 \
      --push \
      --tag "$tag_git" \
      --tag "$tag_latest" \
      ./benchmarker
  fi
}

main
