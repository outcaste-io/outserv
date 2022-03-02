#!/bin/bash

main() {
  setup $@

  set -e
  build_compose_tool $@
  build_outserv_docker_image
  launch_environment
}

setup() {
  readonly ME=${0##*/}
  OUTSERV_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
  readonly COMPOSE_FILE="./docker-compose.yml"

  if [[ $1 == "-h" || $1 == "--help" ]]; then usage; fi

  check_environment
}

Info() {
    echo -e "INFO: $*"
}

usage() {
    cat <<EOF
usage: ./run.sh [./compose args ...]

description:

    Without arguments, rebuild outserv and bring up the docker-compose.yml
    config found here.

    With arguments, pass them all to ./compose to create a docker-compose.yml
    file first, then rebuild outserv and bring up the config.
EOF
    exit 0
}

check_environment() {
  command -v make > /dev/null || \
    { echo "ERROR: 'make' command not not found" 1>&2; exit 1; }
  command -v go > /dev/null || \
    { echo "ERROR: 'go' command not not found" 1>&2; exit 1; }
  command -v docker-compose > /dev/null || \
    { echo "ERROR: 'docker-compose' command not not found" 1>&2; exit 1; }
  ## GOPATH required for locally built docker images
  [[ -z "${GOPATH}" ]] && \
    { echo "ERROR: The env var of 'GOPATH' was not defined. Exiting" 1>&2; exit 1; }
}

build_compose_tool() {
  ## Always make compose if it doesn't exist
  make compose

  ## Create compose file if it does not exist or compose parameters passed
  if [[ $# -gt 0 ]] || ! [[ -f $COMPOSE_FILE ]]; then
      Info "creating compose file ..."
      ./compose "$@"
  fi

  if [[ ! -e $COMPOSE_FILE ]]; then
      echo >&2 "$ME: no '$COMPOSE_FILE' found"
      exit 1
  fi
}

build_outserv_docker_image() {
  ## linux binary required for docker image
  export GOOS=linux
  Info "rebuilding outserv ..."
  ( cd $OUTSERV_ROOT/outserv && make install )
}

launch_environment() {
  # Detect if $GOPATH/bin/$GOOS_$GOARCH path
  if [[ -f $GOPATH/bin/linux_amd64/outserv ]]; then
    Info "Found '$GOPATH/bin/linux_amd64/outserv'. Updating $COMPOSE_FILE."
    sed -i 's/\$GOPATH\/bin$/\$GOPATH\/bin\/linux_amd64/' $COMPOSE_FILE
  # if no outserv binary found, abort
  elif ! [[ -f $GOPATH/bin/outserv ]]; then
    echo "ERROR: '$GOPATH/bin/outserv' not found. Exiting" 1>&2
    exit 1
  else
    Info "Found '$GOPATH/bin/outserv'"
  fi

  # No need to down existing containers, if any.
  # The up command handles that automatically

  Info "Bringing up containers"
  docker-compose -p outserv down
  docker-compose --compatibility -p outserv up --force-recreate --remove-orphans
}

main $@
