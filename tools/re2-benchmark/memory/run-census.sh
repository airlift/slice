#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
OUTPUT_DIR=${1:-"${ROOT_DIR}/target/re2-memory-census"}
PATTERN_FILE=${2:-}
HAYSTACK_FILE=${3:-}
MAXIMUM_MEMORY_MIB=${4:-}
JONI_VERSION=2.1.5.3
JOL_VERSION=0.17

if [[ -n "${PATTERN_FILE}" || -n "${HAYSTACK_FILE}" || -n "${MAXIMUM_MEMORY_MIB}" ]]; then
    if [[ -z "${PATTERN_FILE}" || -z "${HAYSTACK_FILE}" ]]; then
        echo "usage: $0 [output-directory [pattern-file haystack-file [maximum-memory-mib]]]" >&2
        exit 1
    fi
fi
if [[ -n "${MAXIMUM_MEMORY_MIB}" && ! "${MAXIMUM_MEMORY_MIB}" =~ ^[1-9][0-9]*$ ]]; then
    echo "maximum-memory-mib must be a positive integer" >&2
    exit 1
fi

cd "${ROOT_DIR}"
maven_arguments=()
if [[ ! -e .git ]]; then
    maven_arguments+=(-Dmaven.gitcommitid.skip=true)
fi

./mvnw "${maven_arguments[@]}" -q -DskipTests test-compile
./mvnw "${maven_arguments[@]}" -q dependency:get -Dartifact="io.airlift:joni:${JONI_VERSION}"
./mvnw "${maven_arguments[@]}" -q dependency:get -Dartifact="org.openjdk.jol:jol-core:${JOL_VERSION}"

SLICE_DEPENDENCIES="${ROOT_DIR}/target/re2-memory-census-slice-classpath.txt"
./mvnw "${maven_arguments[@]}" -q dependency:build-classpath \
    -DincludeScope=test \
    -Dmdep.outputFile="${SLICE_DEPENDENCIES}"

JONI_JAR="${HOME}/.m2/repository/io/airlift/joni/${JONI_VERSION}/joni-${JONI_VERSION}.jar"
JOL_JAR="${HOME}/.m2/repository/org/openjdk/jol/jol-core/${JOL_VERSION}/jol-core-${JOL_VERSION}.jar"
CLASSES_DIR="${ROOT_DIR}/target/re2-memory-census-classes"
mkdir -p "${CLASSES_DIR}" "${OUTPUT_DIR}"

CLASSPATH="${ROOT_DIR}/target/test-classes:${ROOT_DIR}/target/classes:$(cat "${SLICE_DEPENDENCIES}"):${JONI_JAR}:${JOL_JAR}"
javac \
    --add-modules jdk.incubator.vector \
    -cp "${CLASSPATH}" \
    -d "${CLASSES_DIR}" \
    "${ROOT_DIR}/tools/re2-benchmark/memory/RegexpMemoryCensus.java"

java_arguments=()
if [[ -n "${JAVA_CENSUS_ARGUMENTS:-}" ]]; then
    read -r -a java_arguments <<< "${JAVA_CENSUS_ARGUMENTS}"
fi

census_arguments=("${OUTPUT_DIR}")
if [[ -n "${PATTERN_FILE}" ]]; then
    census_arguments+=("${PATTERN_FILE}" "${HAYSTACK_FILE}")
fi
if [[ -n "${MAXIMUM_MEMORY_MIB}" ]]; then
    census_arguments+=("${MAXIMUM_MEMORY_MIB}")
fi

java \
    -Djol.magicFieldOffset=true \
    --add-modules jdk.incubator.vector \
    "${java_arguments[@]}" \
    -cp "${CLASSES_DIR}:${CLASSPATH}" \
    io.airlift.slice.re2.RegexpMemoryCensus \
    "${census_arguments[@]}"
