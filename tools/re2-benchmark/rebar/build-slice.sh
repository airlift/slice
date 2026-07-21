#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$repo_root"

maven_snapshot_arguments=(-Dmaven.gitcommitid.skip=true)

./mvnw "${maven_snapshot_arguments[@]}" -q test-compile
./mvnw "${maven_snapshot_arguments[@]}" -q dependency:build-classpath \
    -DincludeScope=test \
    -Dmdep.outputFile=target/rebar-classpath.txt
