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
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  docker buildx create --use
}

function build_and_push_tag() {
  if [ ! -z "$GITHUB_REF_NAME" ] && [ "$GITHUB_REF_TYPE" == "tag" ]; then
    branch_name="$(echo -n $GITHUB_REF_NAME | sed 's/\//-/g')"
    tag_git="$DOCKER_REPO:$branch_name"
    tag_latest="$DOCKER_REPO:latest"

    echo "Tag & Push $tag_latest, $tag_git"
    docker buildx build --platform=linux/arm64,linux/amd64 \
      --push \
      --tag "$tag_git" \
      --tag "$tag_latest" \
      ./benchmarker
  fi

  if [ ! -z "$GITHUB_REF_NAME" ] && [ "$GITHUB_REF_TYPE" == "branch" ]; then
    branch_name="$(echo -n $GITHUB_REF_NAME | sed 's/\//-/g')"
    tag_git="$DOCKER_REPO:$branch_name"

    echo "Tag & Push $tag_git"
    docker buildx build --platform=linux/arm64,linux/amd64 \
      --push \
      --tag "$tag_git" \
      ./benchmarker
  fi

}

main
