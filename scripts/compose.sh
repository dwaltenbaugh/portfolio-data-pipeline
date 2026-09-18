#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <dev|prod> <docker compose arguments...>" >&2
    exit 2
fi

environment="$1"
shift

case "$environment" in
    dev|prod)
        ;;
    *)
        echo "Environment must be 'dev' or 'prod'." >&2
        exit 2
        ;;
esac

script_directory="$(
    cd "$(dirname "${BASH_SOURCE[0]}")"
    pwd
)"

project_directory="$(
    cd "$script_directory/.."
    pwd
)"

environment_file="$project_directory/.env.$environment"
overlay_file="$project_directory/docker-compose.$environment.yaml"

if [[ ! -f "$environment_file" ]]; then
    echo "Missing environment file: $environment_file" >&2
    echo "Create it from .env.$environment.example." >&2
    exit 1
fi

export ENV_FILE_PATH="$environment_file"

exec docker compose \
    --project-name "portfolio-data-pipeline-$environment" \
    --project-directory "$project_directory" \
    --env-file "$environment_file" \
    -f "$project_directory/docker-compose.yaml" \
    -f "$overlay_file" \
    "$@"