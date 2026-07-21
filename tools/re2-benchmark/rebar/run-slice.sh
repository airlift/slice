#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$repo_root"

if [[ ! -f target/rebar-classpath.txt ]]; then
    echo "Rebar classpath is missing; run tools/re2-benchmark/rebar/build-slice.sh" >&2
    exit 1
fi

classpath="target/test-classes:target/classes:$(cat target/rebar-classpath.txt)"
java_options=(--add-modules jdk.incubator.vector)
unset JDK_JAVA_OPTIONS JAVA_TOOL_OPTIONS _JAVA_OPTIONS
case ${REBAR_NATIVE_ACCESS:?REBAR_NATIVE_ACCESS must be enabled or disabled} in
    enabled)
        java_options+=(
            --enable-native-access=ALL-UNNAMED
            --illegal-native-access=deny
        )
        ;;
    disabled) ;;
    *)
        echo "Invalid REBAR_NATIVE_ACCESS: ${REBAR_NATIVE_ACCESS}" >&2
        exit 1
        ;;
esac
if [[ -n ${REBAR_HEAP_SIZE:-} ]]; then
    java_options+=(
        "-Xms${REBAR_HEAP_SIZE}"
        "-Xmx${REBAR_HEAP_SIZE}"
        -XX:+AlwaysPreTouch
    )
fi
if [[ ${REBAR_PRINT_COMPILATION:-false} == true ]]; then
    java_options+=(-XX:+PrintCompilation)
fi

exec java "${java_options[@]}" \
    -cp "$classpath" \
    io.airlift.slice.re2.RebarRunner "$@"
