#!/usr/bin/env bash
set -euo pipefail

# EC2 user data invokes this script without guaranteeing HOME. Maven's wrapper
# requires the invoking account's home for distribution and repository caches.
if [[ -z ${HOME:-} ]]; then
    HOME=$(awk -F: -v user_id="$(id -u)" '$3 == user_id {print $6; exit}' /etc/passwd)
    HOME=${HOME:-/tmp/re2-engineering-home}
    export HOME
    mkdir -p "${HOME}"
fi

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <slice-directory> <trino-directory> <result-directory>" >&2
    exit 1
fi

SLICE_DIR=$(cd "$1" && pwd)
TRINO_DIR=$(cd "$2" && pwd)
RESULT_DIR=$(mkdir -p "$3" && cd "$3" && pwd)
PHYSICAL_CPU_LIST=$(lscpu -p=CPU,CORE,SOCKET | awk -F, '!/^#/ {key=$2 ":" $3; if (!seen[key]++) {cpus=(cpus == "" ? $1 : cpus "," $1)}} END {print cpus}')
PHYSICAL_CORE_COUNT=$(awk -F, '{print NF}' <<<"${PHYSICAL_CPU_LIST}")
MAVEN_SNAPSHOT_ARGS=(-Dmaven.gitcommitid.skip=true)
BENCHMARK_MODE=${RE2_BENCHMARK_MODE:-full}
CAPTURE_SHARD=${CAPTURE_SHARD:-direct}
CAPTURE_CONTROL=${CAPTURE_CONTROL:-instruction-scan}
CAPTURE_ENGINE=${CAPTURE_ENGINE:-nfa}
CAPTURE_GUARD_SIZES=${CAPTURE_GUARD_SIZES:-512,4096}
CAPTURE_PIPELINE_WORKLOAD=${CAPTURE_PIPELINE_WORKLOAD:-}
CAPTURE_PIPELINE_STAGES=${CAPTURE_PIPELINE_STAGES:-control,setup,forward,reverse,capture,composed,result}
COUNT_PIPELINE_MEMORY_MEGABYTES=${COUNT_PIPELINE_MEMORY_MEGABYTES:-8,12,16,24,32,48,64,96,128}
COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES=${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES:-8,32,64}
REBAR_MODEL_FILTER=${REBAR_MODEL_FILTER:-'^(?:compile|count|count-spans|count-captures|grep|grep-captures)$'}
REBAR_BENCHMARK_FILTER=${REBAR_BENCHMARK_FILTER:-'^curated/'}
REBAR_ALLOW_NATIVE_DRIFT=${REBAR_ALLOW_NATIVE_DRIFT:-false}
REBAR_EXCLUDE_NATIVE_DRIFT=${REBAR_EXCLUDE_NATIVE_DRIFT:-false}
TRADITIONAL_BENCHMARK_CLASS=${TRADITIONAL_BENCHMARK_CLASS:-}
VECTOR_SCANNER_FILTER=${VECTOR_SCANNER_FILTER:-'BenchmarkByteScanner\.(swar|vector128|vector256|vector512)$'}
VECTOR_SCANNER_SOURCE_LENGTHS=${VECTOR_SCANNER_SOURCE_LENGTHS:-16,64,256,1024,32768}
VECTOR_SCANNER_CANDIDATE_COUNTS=${VECTOR_SCANNER_CANDIDATE_COUNTS:-1,2,3}
VECTOR_SCANNER_INPUT_SHAPES=${VECTOR_SCANNER_INPUT_SHAPES:-ABSENT,EARLY}
VECTOR_SCANNER_ARRAY_OFFSETS=${VECTOR_SCANNER_ARRAY_OFFSETS:-0}
VECTOR_SCANNER_FORKS=${VECTOR_SCANNER_FORKS:-3}
VECTOR_LITERAL_FILTER=${VECTOR_LITERAL_FILTER:-'BenchmarkLiteralScanner\.(repeatedSwar|swar|vector128|vector256|vector512)$'}
VECTOR_LITERAL_LANGUAGES=${VECTOR_LITERAL_LANGUAGES:-RUSSIAN,CHINESE}
VECTOR_LITERAL_SOURCE_LENGTHS=${VECTOR_LITERAL_SOURCE_LENGTHS:-64,1024,32768}
VECTOR_LITERAL_INPUT_SHAPES=${VECTOR_LITERAL_INPUT_SHAPES:-ABSENT,DENSE_FIRST_BYTE_FALSE,DENSE_TWO_OFFSET_FALSE}
VECTOR_LITERAL_OFFSET_SELECTIONS=${VECTOR_LITERAL_OFFSET_SELECTIONS:-FRONT_BACK,TWO_RAREST}
VECTOR_LITERAL_FORKS=${VECTOR_LITERAL_FORKS:-1}

case "${BENCHMARK_MODE}" in
    full | targeted | trino-comparator | joni-focused | joni-memory | joni-memory-census | safere-comparator | safere-contains | bounded-count | byte-scan-fallback | capture-count | capture-engine | capture-pipeline | dfa-absolute-pointer-integrated | dfa-diagnostic | dfa-large-pointer | dfa-layout | dfa-layout-screen | dfa-layout-perfasm | dfa-real-layout-screen | dfa-pair-candidate | dfa-pair-diagnostic | dfa-pair-scaling | dfa-paired-corpus | dfa-partial-candidate | dfa-self-loop-final | dfa-self-loop-paired-protected | dfa-self-loop-protected | dfa-self-loop-rebar | fixed-distance | group-zero | historical-dfa | nullable-repeat | rebar-native-comparison | rebar-official | start-byte | traditional-native-comparison | vector-literal | vector-scanner) ;;
    *) echo "Unsupported benchmark mode: ${BENCHMARK_MODE}" >&2; exit 1 ;;
esac
if [[ "${BENCHMARK_MODE}" == dfa-large-pointer ]]; then
    COUNT_PIPELINE_MEMORY_MEGABYTES=96
fi
case "${CAPTURE_SHARD}" in
    direct | scaling | guard) ;;
    *) echo "Unsupported capture shard: ${CAPTURE_SHARD}" >&2; exit 1 ;;
esac
case "${CAPTURE_ENGINE}" in
    nfa | onepass | bitstate) ;;
    *) echo "CAPTURE_ENGINE must be nfa, onepass, or bitstate: ${CAPTURE_ENGINE}" >&2; exit 1 ;;
esac
case "${CAPTURE_ENGINE}:${CAPTURE_CONTROL}" in
    nfa:instruction-scan | nfa:capture-queue | nfa:capture-instruction-words | \
        onepass:onepass-finalization | \
        bitstate:bitstate-job-capacity | bitstate:bitstate-instruction-words) ;;
    *) echo "Unsupported capture engine/control pair: ${CAPTURE_ENGINE}:${CAPTURE_CONTROL}" >&2; exit 1 ;;
esac
case "${CAPTURE_ENGINE}:${CAPTURE_SHARD}" in
    nfa:direct | nfa:scaling | onepass:direct | bitstate:direct | bitstate:guard) ;;
    *) echo "Unsupported capture engine/shard pair: ${CAPTURE_ENGINE}:${CAPTURE_SHARD}" >&2; exit 1 ;;
esac
if [[ ! ${CAPTURE_GUARD_SIZES} =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]]; then
    echo "CAPTURE_GUARD_SIZES must be a comma-separated list of positive integers: ${CAPTURE_GUARD_SIZES}" >&2
    exit 1
fi
if [[ ! ${COUNT_PIPELINE_MEMORY_MEGABYTES} =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ||
        ! ${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES} =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]]; then
    echo "Count pipeline memory values must be comma-separated positive integers" >&2
    exit 1
fi
case "${REBAR_ALLOW_NATIVE_DRIFT}" in
    true | false) ;;
    *) echo "REBAR_ALLOW_NATIVE_DRIFT must be true or false: ${REBAR_ALLOW_NATIVE_DRIFT}" >&2; exit 1 ;;
esac
case "${REBAR_EXCLUDE_NATIVE_DRIFT}" in
    true | false) ;;
    *) echo "REBAR_EXCLUDE_NATIVE_DRIFT must be true or false: ${REBAR_EXCLUDE_NATIVE_DRIFT}" >&2; exit 1 ;;
esac
if [[ "${REBAR_ALLOW_NATIVE_DRIFT}" == true && "${REBAR_EXCLUDE_NATIVE_DRIFT}" == true ]]; then
    echo "Native drift cannot be both allowed and excluded" >&2
    exit 1
fi
case "${TRADITIONAL_BENCHMARK_CLASS}" in
    "" | BenchmarkRe2Search | BenchmarkRe2SearchNfa | BenchmarkRe2SearchExtra | BenchmarkRe2Parse | BenchmarkRe2FullMatch | BenchmarkRe2Practical | BenchmarkRe2Misc) ;;
    *) echo "Unsupported traditional benchmark class: ${TRADITIONAL_BENCHMARK_CLASS}" >&2; exit 1 ;;
esac

exec > >(tee "${RESULT_DIR}/run.log") 2>&1

install_packages()
{
    sudo dnf install -y cmake gcc-c++ git jq make ninja-build perf tar gzip
    if [[ "${BENCHMARK_MODE}" == rebar-official || "${BENCHMARK_MODE}" == rebar-native-comparison || "${BENCHMARK_MODE}" == bounded-count || "${BENCHMARK_MODE}" == dfa-large-pointer || "${BENCHMARK_MODE}" == byte-scan-fallback || "${BENCHMARK_MODE}" == capture-pipeline || "${BENCHMARK_MODE}" == vector-literal ]]; then
        sudo dnf install -y abseil-cpp-devel cargo rust
    fi
}

install_java()
{
    if [[ -n ${BENCHMARK_JAVA_ARCHIVE_URL:-} ]]; then
        curl --fail --location --retry 5 \
            "${BENCHMARK_JAVA_ARCHIVE_URL}" \
            --output /tmp/benchmark-jdk.tar.gz
        printf '%s  %s\n' "${BENCHMARK_JAVA_ARCHIVE_SHA256}" /tmp/benchmark-jdk.tar.gz | sha256sum --check -
        sudo mkdir -p /opt/benchmark-jdk
        sudo tar -xzf /tmp/benchmark-jdk.tar.gz -C /opt/benchmark-jdk --strip-components=1
        export JAVA_HOME=/opt/benchmark-jdk
        export PATH="${JAVA_HOME}/bin:${PATH}"
        return
    fi

    local adoptium_architecture
    case "$(uname -m)" in
        x86_64) adoptium_architecture=x64 ;;
        aarch64) adoptium_architecture=aarch64 ;;
        *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
    esac

    sudo mkdir -p /opt/temurin-25
    curl --fail --location --retry 5 \
        "https://api.adoptium.net/v3/binary/latest/25/ga/linux/${adoptium_architecture}/jdk/hotspot/normal/eclipse" \
        --output /tmp/temurin-25.tar.gz
    sudo tar -xzf /tmp/temurin-25.tar.gz -C /opt/temurin-25 --strip-components=1
    export JAVA_HOME=/opt/temurin-25
    export PATH="${JAVA_HOME}/bin:${PATH}"
}

install_hsdis()
{
    local hsdis_name
    local hsdis_sha256
    case "$(uname -m)" in
        x86_64)
            hsdis_name=hsdis-amd64.so
            hsdis_sha256=2ebd13ca0dd0a3f20c49b99c12b72e376b6c371975f734403048ddf3d7b51507
            ;;
        aarch64)
            hsdis_name=hsdis-aarch64.so
            hsdis_sha256=c531ae2f6002987b1d7ee5713a76e51bb54dc3da7b00c8b1214f021abda4dffb
            ;;
        *) echo "Unsupported architecture for hsdis: $(uname -m)" >&2; exit 1 ;;
    esac

    if [[ ${HSDIS_PATH:-} != "/tmp/${hsdis_name}" || ! -f ${HSDIS_PATH:-} ]]; then
        echo "Missing controller-provided hsdis binary: ${HSDIS_PATH:-unset}" >&2
        exit 1
    fi
    printf '%s  %s\n' "${hsdis_sha256}" "${HSDIS_PATH}" | sha256sum --check -
    sudo install -m 0755 "${HSDIS_PATH}" "${JAVA_HOME}/lib/server/${hsdis_name}"
}

record_environment()
{
    local metadata_token
    local instance_type
    metadata_token=$(curl --silent --fail --max-time 2 \
        --request PUT \
        --header 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
        http://169.254.169.254/latest/api/token || true)
    instance_type=$(curl --silent --fail --max-time 2 \
        --header "X-aws-ec2-metadata-token: ${metadata_token}" \
        http://169.254.169.254/latest/meta-data/instance-type || true)

    {
        date --iso-8601=seconds
        uname -a
        cat /etc/os-release
        lscpu
        free -h
        java -version
        java \
            -Xms"${BENCHMARK_HEAP_SIZE:-8g}" \
            -Xmx"${BENCHMARK_HEAP_SIZE:-8g}" \
            -Xlog:gc+heap+coops=debug \
            -XX:+PrintFlagsFinal \
            -version 2>&1 | grep -E 'Compressed Oops mode|ObjectAlignmentInBytes|UseCompressedClassPointers|UseCompressedOops'
        c++ --version
        cmake --version
        git --version
        if command -v cargo >/dev/null; then cargo --version; fi
        printf 'slice_snapshot_commit=%s\n' "${SLICE_SNAPSHOT_COMMIT:-unknown}"
        printf 'slice_snapshot_sha256=%s\n' "${SLICE_SNAPSHOT_SHA256:-unknown}"
        printf 'slice_content_sha256=%s\n' "${SLICE_CONTENT_SHA256:-unknown}"
        printf 'trino_snapshot_commit=%s\n' "${TRINO_SNAPSHOT_COMMIT:-unknown}"
        printf 'instance_type=%s\n' "${instance_type}"
        printf 'physical_cpu_list=%s\n' "${PHYSICAL_CPU_LIST}"
        printf 'physical_core_count=%s\n' "${PHYSICAL_CORE_COUNT}"
        printf 'benchmark_mode=%s\n' "${BENCHMARK_MODE}"
        printf 'instance_market_type=%s\n' "${INSTANCE_MARKET_TYPE:-unknown}"
        printf 'qualification_jmh_forks=5\n'
        printf 'qualification_jmh_warmup=10x1s\n'
        printf 'qualification_jmh_measurement=10x1s\n'
        printf 'qualification_native_repetitions=5\n'
        if [[ "${BENCHMARK_MODE}" == safere-comparator || "${BENCHMARK_MODE}" == safere-contains ]]; then
            printf 'safere_jmh_forks=3\n'
            printf 'safere_jmh_warmup=5x500ms\n'
            printf 'safere_jmh_measurement=5x500ms\n'
        fi
        printf 'capture_shard=%s\n' "${CAPTURE_SHARD}"
        printf 'capture_control=%s\n' "${CAPTURE_CONTROL}"
        printf 'capture_engine=%s\n' "${CAPTURE_ENGINE}"
        printf 'capture_guard_sizes=%s\n' "${CAPTURE_GUARD_SIZES}"
        printf 'count_pipeline_memory_megabytes=%s\n' "${COUNT_PIPELINE_MEMORY_MEGABYTES}"
        printf 'count_pipeline_control_memory_megabytes=%s\n' "${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES}"
        printf 'rebar_model_filter=%s\n' "${REBAR_MODEL_FILTER}"
        printf 'rebar_benchmark_filter=%s\n' "${REBAR_BENCHMARK_FILTER}"
        printf 'rebar_allow_native_drift=%s\n' "${REBAR_ALLOW_NATIVE_DRIFT}"
        printf 'rebar_exclude_native_drift=%s\n' "${REBAR_EXCLUDE_NATIVE_DRIFT}"
        printf 'vector_literal_filter=%s\n' "${VECTOR_LITERAL_FILTER}"
        printf 'vector_literal_languages=%s\n' "${VECTOR_LITERAL_LANGUAGES}"
        printf 'vector_literal_source_lengths=%s\n' "${VECTOR_LITERAL_SOURCE_LENGTHS}"
        printf 'vector_literal_input_shapes=%s\n' "${VECTOR_LITERAL_INPUT_SHAPES}"
        printf 'vector_literal_offset_selections=%s\n' "${VECTOR_LITERAL_OFFSET_SELECTIONS}"
        printf 'vector_literal_forks=%s\n' "${VECTOR_LITERAL_FORKS}"
    } > "${RESULT_DIR}/environment.txt" 2>&1
}

build_classpath()
{
    local project_dir=$1
    local output_file=$2
    (
        cd "${project_dir}"
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q dependency:build-classpath \
            -DincludeScope=test \
            -Dmdep.outputFile="${output_file}"
    )
}

run_jmh()
{
    local classpath=$1
    local filter=$2
    local output_file=$3
    local fork_count=${BENCHMARK_FORKS:-1}
    local heap_size=${BENCHMARK_HEAP_SIZE:-8g}
    local warmup_iterations=${BENCHMARK_WARMUP_ITERATIONS:-10}
    local measurement_iterations=${BENCHMARK_MEASUREMENT_ITERATIONS:-7}
    local warmup_time=${BENCHMARK_WARMUP_TIME:-500ms}
    local measurement_time=${BENCHMARK_MEASUREMENT_TIME:-500ms}
    local native_access_arguments=()
    shift 3

    if [[ "${BENCHMARK_NATIVE_ACCESS:-false}" == true || "${filter}" == *everythingSegmentPointers* ]]; then
        native_access_arguments+=(--enable-native-access=ALL-UNNAMED)
    fi
    if [[ "${BENCHMARK_DENY_NATIVE_ACCESS:-false}" == true ]]; then
        native_access_arguments+=(--illegal-native-access=deny)
    fi

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        java "${native_access_arguments[@]}" --add-modules jdk.incubator.vector \
        -cp "${classpath}" \
        org.openjdk.jmh.Main "${filter}" \
        -f "${fork_count}" \
        -wi "${warmup_iterations}" \
        -i "${measurement_iterations}" \
        -w "${warmup_time}" \
        -r "${measurement_time}" \
        -jvmArgsAppend "${native_access_arguments[*]} --add-modules=jdk.incubator.vector -Xms${heap_size} -Xmx${heap_size} -XX:+AlwaysPreTouch" \
        -rf json \
        -rff "${output_file}" \
        "$@"
}

run_qualification_jmh()
{
    BENCHMARK_FORKS=5 \
        BENCHMARK_WARMUP_ITERATIONS=10 \
        BENCHMARK_MEASUREMENT_ITERATIONS=10 \
        BENCHMARK_WARMUP_TIME=1s \
        BENCHMARK_MEASUREMENT_TIME=1s \
        run_jmh "$@"
}

run_native_qualification_jmh()
{
    BENCHMARK_NATIVE_ACCESS=true \
        BENCHMARK_DENY_NATIVE_ACCESS=true \
        run_qualification_jmh "$@"
}

prepare_trino_comparator()
{
    cd "${TRINO_DIR}"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -pl core/trino-main -am \
        -Dair.check.skip-all \
        -DskipTests \
        -Dskip.bun=true \
        -Dskip.installbun=true \
        -Dmaven.javadoc.skip=true \
        -Dmaven.source.skip=true \
        install | tee "${RESULT_DIR}/trino-build.log"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -pl core/trino-main \
        -Dtest=TestBenchmarkRegexpOperations \
        test | tee "${RESULT_DIR}/trino-tests.log"

    local trino_dependencies="${RESULT_DIR}/trino-classpath.txt"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q -pl core/trino-main dependency:build-classpath \
        -DincludeScope=test \
        -Dmdep.outputFile="${trino_dependencies}"
    TRINO_CLASSPATH="${TRINO_DIR}/core/trino-main/target/test-classes:${TRINO_DIR}/core/trino-main/target/classes:$(cat "${trino_dependencies}")"

    # JDK 25 does not discover JMH's processor from Trino's test dependency alone.
    local generated_sources="${TRINO_DIR}/core/trino-main/target/generated-test-sources/re2-jmh"
    rm -rf "${generated_sources}"
    rm -rf "${TRINO_DIR}/core/trino-main/target/test-classes/io/trino/operator/scalar/jmh_generated"
    rm -f \
        "${TRINO_DIR}/core/trino-main/target/test-classes/META-INF/BenchmarkList" \
        "${TRINO_DIR}/core/trino-main/target/test-classes/META-INF/CompilerHints"
    mkdir -p "${generated_sources}"
    javac \
        -cp "${TRINO_CLASSPATH}" \
        -processorpath "${TRINO_CLASSPATH}" \
        -processor org.openjdk.jmh.generators.BenchmarkProcessor \
        -d "${TRINO_DIR}/core/trino-main/target/test-classes" \
        -s "${generated_sources}" \
        "${TRINO_DIR}/core/trino-main/src/test/java/io/trino/operator/scalar/BenchmarkRegexpOperations.java"
    local benchmark_count
    benchmark_count=$(java -cp "${TRINO_CLASSPATH}" org.openjdk.jmh.Main -l | grep -c 'BenchmarkRegexpOperations\.')
    if [[ ${benchmark_count} -ne 16 ]]; then
        echo "Expected 16 Trino regexp benchmarks, found ${benchmark_count}" >&2
        exit 1
    fi

}

run_trino_comparator()
{
    prepare_trino_comparator
    run_qualification_jmh \
        "${TRINO_CLASSPATH}" \
        'BenchmarkRegexpOperations' \
        "${RESULT_DIR}/trino-joni-re2j-operations.json"
}

run_joni_memory_qualification()
{
    local slice_operation_filter='BenchmarkTrinoRegexp\.(contains|count|positionThird|extract|extractAll|split|replace|replaceLambda)$'
    local joni_operation_filter='BenchmarkRegexpOperations\.(contains|count|positionThird|extract|extractAll|split|replace|replaceLambda)Joni$'
    local slice_benchmark_count
    local joni_benchmark_count

    slice_benchmark_count=$(java --add-modules jdk.incubator.vector -cp "${SLICE_CLASSPATH}" org.openjdk.jmh.Main -l "${slice_operation_filter}" | grep -c '^io.airlift.slice.re2.BenchmarkTrinoRegexp\.')
    joni_benchmark_count=$(java -cp "${TRINO_CLASSPATH}" org.openjdk.jmh.Main -l "${joni_operation_filter}" | grep -c '^io.trino.operator.scalar.BenchmarkRegexpOperations\.')
    if [[ ${slice_benchmark_count} -ne 8 || ${joni_benchmark_count} -ne 8 ]]; then
        echo "Expected eight Slice and Joni operation benchmarks, found Slice=${slice_benchmark_count}, Joni=${joni_benchmark_count}" >&2
        exit 1
    fi

    run_native_qualification_jmh \
        "${SLICE_CLASSPATH}" \
        "${slice_operation_filter}" \
        "${RESULT_DIR}/slice-before.json" \
        -prof gc
    run_qualification_jmh \
        "${TRINO_CLASSPATH}" \
        "${joni_operation_filter}" \
        "${RESULT_DIR}/joni.json" \
        -prof gc
    run_native_qualification_jmh \
        "${SLICE_CLASSPATH}" \
        "${slice_operation_filter}" \
        "${RESULT_DIR}/slice-after.json" \
        -prof gc

    python3 "${SLICE_DIR}/tools/re2-benchmark/memory/summarize_jmh.py" \
        "${RESULT_DIR}/slice-before.json" \
        "${RESULT_DIR}/joni.json" \
        "${RESULT_DIR}/operation-summary" \
        --slice-after "${RESULT_DIR}/slice-after.json"

    run_joni_memory_census
}

run_safere_qualification()
{
    local operation_filter='contains'
    local expected_benchmark_count=1
    if [[ "${BENCHMARK_MODE}" == safere-comparator ]]; then
        operation_filter='contains|count|positionThird|extract|extractAll|split|replace|replaceLambda'
        expected_benchmark_count=8
    fi
    local slice_operation_filter="BenchmarkTrinoRegexp\.(${operation_filter})$"
    local safere_operation_filter="BenchmarkSafeReTrinoRegexp\.(${operation_filter})SafeRe$"
    local slice_benchmark_count
    local safere_benchmark_count

    slice_benchmark_count=$(java --add-modules jdk.incubator.vector -cp "${SLICE_CLASSPATH}" org.openjdk.jmh.Main -l "${slice_operation_filter}" | grep -c '^io.airlift.slice.re2.BenchmarkTrinoRegexp\.')
    safere_benchmark_count=$(java --add-modules jdk.incubator.vector -cp "${SLICE_CLASSPATH}" org.openjdk.jmh.Main -l "${safere_operation_filter}" | grep -c '^io.airlift.slice.re2.BenchmarkSafeReTrinoRegexp\.')
    if [[ ${slice_benchmark_count} -ne ${expected_benchmark_count} || ${safere_benchmark_count} -ne ${expected_benchmark_count} ]]; then
        echo "Expected ${expected_benchmark_count} Slice and SafeRE operation benchmarks, found Slice=${slice_benchmark_count}, SafeRE=${safere_benchmark_count}" >&2
        exit 1
    fi

    BENCHMARK_FORKS=3 \
        BENCHMARK_WARMUP_ITERATIONS=5 \
        BENCHMARK_MEASUREMENT_ITERATIONS=5 \
        BENCHMARK_WARMUP_TIME=500ms \
        BENCHMARK_MEASUREMENT_TIME=500ms \
        BENCHMARK_NATIVE_ACCESS=true \
        BENCHMARK_DENY_NATIVE_ACCESS=true \
        run_jmh \
            "${SLICE_CLASSPATH}" \
            "${slice_operation_filter}" \
            "${RESULT_DIR}/slice-before.json" \
            -prof gc
    BENCHMARK_FORKS=3 \
        BENCHMARK_WARMUP_ITERATIONS=5 \
        BENCHMARK_MEASUREMENT_ITERATIONS=5 \
        BENCHMARK_WARMUP_TIME=500ms \
        BENCHMARK_MEASUREMENT_TIME=500ms \
        run_jmh \
            "${SLICE_CLASSPATH}" \
            "${safere_operation_filter}" \
            "${RESULT_DIR}/safere.json" \
            -prof gc
    BENCHMARK_FORKS=3 \
        BENCHMARK_WARMUP_ITERATIONS=5 \
        BENCHMARK_MEASUREMENT_ITERATIONS=5 \
        BENCHMARK_WARMUP_TIME=500ms \
        BENCHMARK_MEASUREMENT_TIME=500ms \
        BENCHMARK_NATIVE_ACCESS=true \
        BENCHMARK_DENY_NATIVE_ACCESS=true \
        run_jmh \
            "${SLICE_CLASSPATH}" \
            "${slice_operation_filter}" \
            "${RESULT_DIR}/slice-after.json" \
            -prof gc

    python3 "${SLICE_DIR}/tools/re2-benchmark/safere/summarize.py" \
        "${RESULT_DIR}/slice-before.json" \
        "${RESULT_DIR}/safere.json" \
        "${RESULT_DIR}/operation-summary" \
        --slice-after "${RESULT_DIR}/slice-after.json"
}

run_boolean_partial_match_qualification()
{
    local benchmark_filter='BenchmarkRe2BooleanPartialMatch\.match$'
    local benchmark_count

    benchmark_count=$(java --add-modules jdk.incubator.vector -cp "${SLICE_CLASSPATH}" org.openjdk.jmh.Main -l "${benchmark_filter}" | grep -c '^io.airlift.slice.re2.BenchmarkRe2BooleanPartialMatch\.match$')
    if [[ ${benchmark_count} -ne 1 ]]; then
        echo "Expected one boolean partial-match benchmark, found ${benchmark_count}" >&2
        exit 1
    fi

    BENCHMARK_FORKS=3 \
        BENCHMARK_WARMUP_ITERATIONS=5 \
        BENCHMARK_MEASUREMENT_ITERATIONS=5 \
        BENCHMARK_WARMUP_TIME=500ms \
        BENCHMARK_MEASUREMENT_TIME=500ms \
        BENCHMARK_NATIVE_ACCESS=true \
        BENCHMARK_DENY_NATIVE_ACCESS=true \
        run_jmh \
            "${SLICE_CLASSPATH}" \
            "${benchmark_filter}" \
            "${RESULT_DIR}/boolean-partial-match.json"

    python3 "${SLICE_DIR}/tools/re2-benchmark/safere/summarize_boolean_partial_match.py" \
        "${RESULT_DIR}/boolean-partial-match.json" \
        "${RESULT_DIR}/boolean-partial-match-summary"
}

run_joni_memory_census()
{
    JAVA_CENSUS_ARGUMENTS='-Xms8g -Xmx8g -XX:+AlwaysPreTouch --enable-native-access=ALL-UNNAMED --illegal-native-access=deny' \
        taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${SLICE_DIR}/tools/re2-benchmark/memory/run-census.sh" \
        "${RESULT_DIR}/memory-census" \
        /opt/re2-work/context/pattern.txt \
        /opt/re2-work/context/haystack.bin \
        96
    python3 "${SLICE_DIR}/tools/re2-benchmark/memory/summarize.py" \
        "${RESULT_DIR}/memory-census/memory-census.csv" \
        "${RESULT_DIR}/memory-census"
}

run_official_rebar()
{
    local rebar_root=${REBAR_ROOT:?REBAR_ROOT is required}
    local benchmark_directory="${SLICE_DIR}/target/rebar-slice-benchmarks"
    local abseil_source="${SLICE_DIR}/target/re2-golden-dependencies/abseil-cpp"
    local abseil_build="${SLICE_DIR}/target/rebar-abseil-build"
    local abseil_install="${SLICE_DIR}/target/rebar-abseil-install"
    local rebar="${rebar_root}/target/release/rebar"
    local engine_filter='^(?:re2|slice/re2)$'
    local model_filter='^(?:compile|count|count-spans|count-captures|grep|grep-captures)$'
    local benchmark_filter=${REBAR_BENCHMARK_FILTER}

    "${SLICE_DIR}/tools/re2-benchmark/rebar/prepare.sh" "${rebar_root}" "${benchmark_directory}"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/build-slice.sh"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/run-slice.sh" --version \
        > "${RESULT_DIR}/slice-version.txt" \
        2> "${RESULT_DIR}/slice-version-error.txt"

    # Amazon Linux's Abseil package predates the logging modules required by Rebar's vendored RE2.
    "${SLICE_DIR}/tools/re2-golden/fetch-dependencies.sh" >/dev/null
    cmake -S "${abseil_source}" -B "${abseil_build}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_INSTALL_PREFIX="${abseil_install}" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DABSL_ENABLE_INSTALL=ON
    cmake --build "${abseil_build}" --target install --parallel
    export CXXFLAGS="-I${abseil_install}/include ${CXXFLAGS:-}"
    export PKG_CONFIG_PATH="${abseil_install}/lib64/pkgconfig:${abseil_install}/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"

    cargo build --release --manifest-path "${rebar_root}/Cargo.toml" | tee "${RESULT_DIR}/rebar-build.log"
    "${rebar}" build \
        -d "${benchmark_directory}" \
        -e "${engine_filter}" | tee "${RESULT_DIR}/rebar-engine-build.log"

    set +e
    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${rebar}" measure \
        -d "${benchmark_directory}" \
        -e "${engine_filter}" \
        -f "${benchmark_filter}" \
        -m "${model_filter}" \
        --timeout 30s \
        --test \
        > "${RESULT_DIR}/rebar-verification.csv" \
        2> "${RESULT_DIR}/rebar-verification.log"
    local verification_status=$?
    set -e
    printf '%s\n' "${verification_status}" > "${RESULT_DIR}/rebar-verification-status"

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${rebar}" measure \
        -d "${benchmark_directory}" \
        -e "${engine_filter}" \
        -f "${benchmark_filter}" \
        -m "${model_filter}" \
        --timeout 30s \
        > "${RESULT_DIR}/rebar-measurements.csv"
}

run_rebar_native_comparison()
{
    local rebar_root=${REBAR_ROOT:?REBAR_ROOT is required}
    local benchmark_directory="${SLICE_DIR}/target/rebar-slice-benchmarks"
    local portable_native_root="${SLICE_DIR}/target/rebar-native-portable"
    local tuned_native_root="${SLICE_DIR}/target/rebar-native-tuned"
    local pinned_re2_root="${SLICE_DIR}/target/re2-golden-dependencies/re2"
    local abseil_source="${SLICE_DIR}/target/re2-golden-dependencies/abseil-cpp"
    local abseil_build="${SLICE_DIR}/target/rebar-abseil-build"
    local abseil_install="${SLICE_DIR}/target/rebar-abseil-install"
    local rebar="${rebar_root}/target/release/rebar"
    local engine_filter='^(?:re2|re2/pinned-portable|re2/pinned-host-tuned-before|re2/pinned-host-tuned-after|slice/re2|slice/re2-object)$'
    local model_filter=${REBAR_MODEL_FILTER}
    local benchmark_filter=${REBAR_BENCHMARK_FILTER}

    "${SLICE_DIR}/tools/re2-golden/fetch-dependencies.sh" | tee "${RESULT_DIR}/native-fetch.log"
    cmake -S "${abseil_source}" -B "${abseil_build}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_INSTALL_PREFIX="${abseil_install}" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DABSL_ENABLE_INSTALL=ON
    cmake --build "${abseil_build}" --target install --parallel
    export CXXFLAGS="-I${abseil_install}/include ${CXXFLAGS:-}"
    export PKG_CONFIG_PATH="${abseil_install}/lib64/pkgconfig:${abseil_install}/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"

    cargo build --release --locked --manifest-path "${rebar_root}/Cargo.toml" \
        | tee "${RESULT_DIR}/rebar-build.log"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/build-native.sh" \
        "${rebar_root}" \
        "${portable_native_root}" \
        "${tuned_native_root}" \
        "${pinned_re2_root}" \
        2>&1 | tee "${RESULT_DIR}/pinned-native-build.log"
    cp "${portable_native_root}/cxx-commands.log" \
        "${RESULT_DIR}/pinned-portable-cxx-commands.log"
    cp "${tuned_native_root}/cxx-commands.log" \
        "${RESULT_DIR}/pinned-host-tuned-cxx-commands.log"
    sha256sum \
        "${portable_native_root}/target/release/main" \
        "${tuned_native_root}/target/release/main" \
        > "${RESULT_DIR}/pinned-native-binaries.sha256"
    "${portable_native_root}/target/release/main" --version > "${RESULT_DIR}/pinned-portable-version.txt"
    "${tuned_native_root}/target/release/main" --version > "${RESULT_DIR}/pinned-host-tuned-version.txt"
    c++ --version > "${RESULT_DIR}/native-compiler-version.txt"
    nm -C "${tuned_native_root}/target/release/main" > "${RESULT_DIR}/pinned-host-tuned-symbols.txt"
    objdump -d -C "${tuned_native_root}/target/release/main" | gzip -9 \
        > "${RESULT_DIR}/pinned-host-tuned-disassembly.txt.gz"

    grep -Eq 're2::RE2::(Match|RE2)' "${RESULT_DIR}/pinned-host-tuned-symbols.txt"

    "${SLICE_DIR}/tools/re2-benchmark/rebar/prepare.sh" \
        "${rebar_root}" \
        "${benchmark_directory}" \
        "${portable_native_root}" \
        "${tuned_native_root}"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/build-slice.sh"
    "${rebar}" build \
        -d "${benchmark_directory}" \
        -e "${engine_filter}" | tee "${RESULT_DIR}/rebar-engine-build.log"

    "${rebar}" measure \
        -d "${benchmark_directory}" \
        -e "${engine_filter}" \
        -f "${benchmark_filter}" \
        -m "${model_filter}" \
        --list \
        > "${RESULT_DIR}/rebar-engine-manifest.csv"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/generate-manifest.sh" \
        "${rebar}" \
        "${benchmark_directory}" \
        "${RESULT_DIR}/rebar-workload-manifest.csv"
    sha256sum \
        "${benchmark_directory}/engines.toml" \
        "${RESULT_DIR}/rebar-engine-manifest.csv" \
        "${RESULT_DIR}/rebar-workload-manifest.csv" \
        > "${RESULT_DIR}/rebar-manifests.sha256"

    local compile_calibration
    local search_calibration
    local search_compilation_pattern
    compile_calibration=$(awk -F, 'NR > 1 && $2 == "compile" {print $1; exit}' "${RESULT_DIR}/rebar-workload-manifest.csv")
    search_calibration=$(awk -F, 'NR > 1 && $2 == "count" && $18 > 0 {print $1; exit}' "${RESULT_DIR}/rebar-workload-manifest.csv")
    if [[ -n ${search_calibration} ]]; then
        search_compilation_pattern='io\.airlift\.slice\.re2\.Dfa::searchForwardAbsolutePointers'
    else
        search_calibration=$(awk -F, 'NR > 1 && $2 == "count" {print $1; exit}' "${RESULT_DIR}/rebar-workload-manifest.csv")
        search_compilation_pattern='io\.airlift\.slice\.re2\.Dfa::search'
    fi
    if [[ -z ${compile_calibration} || -z ${search_calibration} ]]; then
        echo "Unable to select Rebar tier-4 calibration workloads" >&2
        exit 1
    fi
    "${rebar}" klv -d "${benchmark_directory}" "${compile_calibration}" \
        | taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        env REBAR_NATIVE_ACCESS=enabled REBAR_HEAP_SIZE=8g REBAR_PRINT_COMPILATION=true \
        "${SLICE_DIR}/tools/re2-benchmark/rebar/run-slice.sh" --calibrate \
        > "${RESULT_DIR}/java-compile-calibration.log"
    "${rebar}" klv -d "${benchmark_directory}" "${search_calibration}" \
        | taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        env REBAR_NATIVE_ACCESS=enabled REBAR_HEAP_SIZE=8g REBAR_PRINT_COMPILATION=true \
        "${SLICE_DIR}/tools/re2-benchmark/rebar/run-slice.sh" --calibrate \
        > "${RESULT_DIR}/java-search-calibration.log"
    grep -Eq '[[:space:]]4[[:space:]].*io\.airlift\.slice\.re2\.Simplifier::simplifyRecursive' \
        "${RESULT_DIR}/java-compile-calibration.log"
    grep -Eq "[[:space:]]4[[:space:]].*${search_compilation_pattern}" \
        "${RESULT_DIR}/java-search-calibration.log"

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${rebar}" measure \
        -d "${benchmark_directory}" \
        -e "${engine_filter}" \
        -f "${benchmark_filter}" \
        -m "${model_filter}" \
        --max-time 5s \
        --max-warmup-time 5s \
        --timeout 30s \
        --test \
        > "${RESULT_DIR}/rebar-verification.csv" \
        2> "${RESULT_DIR}/rebar-verification.log"
    python3 "${SLICE_DIR}/tools/re2-benchmark/rebar/validate_verification.py" \
        "${RESULT_DIR}/rebar-verification.csv" \
        --manifest "${RESULT_DIR}/rebar-engine-manifest.csv"
    printf '0\n' > "${RESULT_DIR}/rebar-verification-status"

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${rebar}" measure \
        -d "${benchmark_directory}" \
        -e "${engine_filter}" \
        -f "${benchmark_filter}" \
        -m "${model_filter}" \
        --max-time 5s \
        --max-warmup-time 5s \
        --timeout 30s \
        > "${RESULT_DIR}/rebar-measurements.csv"
    local summarize_arguments=()
    if [[ "${REBAR_ALLOW_NATIVE_DRIFT}" == true ]]; then
        summarize_arguments+=(--allow-native-drift)
    fi
    if [[ "${REBAR_EXCLUDE_NATIVE_DRIFT}" == true ]]; then
        summarize_arguments+=(--exclude-native-drift)
    fi
    python3 "${SLICE_DIR}/tools/re2-benchmark/rebar/summarize.py" \
        "${RESULT_DIR}/rebar-measurements.csv" \
        --workload-manifest "${RESULT_DIR}/rebar-workload-manifest.csv" \
        --engine-manifest "${RESULT_DIR}/rebar-engine-manifest.csv" \
        "${summarize_arguments[@]}" \
        --output-directory "${RESULT_DIR}/rebar-normalized"
}

run_capture_pipeline()
{
    local rebar_root=${REBAR_ROOT:?REBAR_ROOT is required}
    local benchmark_directory="${SLICE_DIR}/target/rebar-slice-benchmarks"
    local output_directory="${RESULT_DIR}/capture-pipeline"
    local native_diagnostic=

    if [[ "${CAPTURE_PIPELINE_NATIVE_DIAGNOSTICS:-false}" == true ]]; then
        local native_build_directory="${SLICE_DIR}/target/re2-capture-diagnostics-build"
        local native_source="${SLICE_DIR}/target/re2-golden-dependencies/re2"
        local abseil_source="${SLICE_DIR}/target/re2-golden-dependencies/abseil-cpp"
        local native_flag
        case "$(uname -m)" in
            x86_64) native_flag=-march=native ;;
            aarch64) native_flag=-mcpu=native ;;
            *) echo "Unsupported capture diagnostic architecture: $(uname -m)" >&2; exit 1 ;;
        esac

        "${SLICE_DIR}/tools/re2-golden/fetch-dependencies.sh" \
            | tee "${RESULT_DIR}/native-fetch.log"
        cmake -S "${SLICE_DIR}/tools/re2-golden" -B "${native_build_directory}" \
            -DRE2_DIR="${native_source}" \
            -DABSL_DIR="${abseil_source}" \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
            -DCMAKE_CXX_FLAGS="-O3 -DNDEBUG ${native_flag}"
        cmake --build "${native_build_directory}" --target re2_capture_diagnostics --parallel \
            | tee "${RESULT_DIR}/native-build.log"
        cp "${native_build_directory}/compile_commands.json" \
            "${RESULT_DIR}/native-compile-commands.json"
        native_diagnostic="${native_build_directory}/re2_capture_diagnostics"
    fi

    cargo build --release --locked --manifest-path "${rebar_root}/Cargo.toml" \
        | tee "${RESULT_DIR}/rebar-build.log"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/prepare.sh" \
        "${rebar_root}" \
        "${benchmark_directory}"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/build-slice.sh"

    CAPTURE_PIPELINE_OUTPUT_DIR="${output_directory}" \
    CAPTURE_PIPELINE_WORKLOAD="${CAPTURE_PIPELINE_WORKLOAD:?CAPTURE_PIPELINE_WORKLOAD is required}" \
    CAPTURE_PIPELINE_STAGES="${CAPTURE_PIPELINE_STAGES}" \
    CAPTURE_PIPELINE_NATIVE_DIAGNOSTIC="${native_diagnostic}" \
    CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS="${CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS:-false}" \
    CAPTURE_PIPELINE_ITERATIONS="${CAPTURE_PIPELINE_ITERATIONS:-20}" \
    CAPTURE_PIPELINE_WARMUP_ITERATIONS="${CAPTURE_PIPELINE_WARMUP_ITERATIONS:-20}" \
    REBAR_BENCHMARK_DIR="${benchmark_directory}" \
    REBAR_EXECUTABLE="${rebar_root}/target/release/rebar" \
    REBAR_NATIVE_ACCESS=enabled \
    REBAR_HEAP_SIZE="${BENCHMARK_HEAP_SIZE:-8g}" \
        "${SLICE_DIR}/tools/re2-benchmark/rebar/run-capture-pipeline.sh"
}

run_bounded_count_pipeline()
{
    local rebar_root=${REBAR_ROOT:?REBAR_ROOT is required}
    local benchmark_directory="${SLICE_DIR}/target/rebar-slice-benchmarks"
    local native_build_directory="${SLICE_DIR}/target/re2-count-diagnostics-build"
    local native_source="${SLICE_DIR}/target/re2-golden-dependencies/re2"
    local abseil_source="${SLICE_DIR}/target/re2-golden-dependencies/abseil-cpp"
    local native_flag

    case "$(uname -m)" in
        x86_64) native_flag=-march=native ;;
        aarch64) native_flag=-mcpu=native ;;
        *) echo "Unsupported bounded-count architecture: $(uname -m)" >&2; exit 1 ;;
    esac

    cargo build --release --locked --manifest-path "${rebar_root}/Cargo.toml" \
        | tee "${RESULT_DIR}/rebar-build.log"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/prepare.sh" \
        "${rebar_root}" \
        "${benchmark_directory}"
    "${SLICE_DIR}/tools/re2-benchmark/rebar/build-slice.sh"

    "${SLICE_DIR}/tools/re2-golden/fetch-dependencies.sh" \
        | tee "${RESULT_DIR}/native-fetch.log"
    cmake -S "${SLICE_DIR}/tools/re2-golden" -B "${native_build_directory}" \
        -DRE2_DIR="${native_source}" \
        -DABSL_DIR="${abseil_source}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_CXX_FLAGS="-O3 -DNDEBUG ${native_flag}"
    cmake --build "${native_build_directory}" --target re2_count_diagnostics --parallel
    cp "${native_build_directory}/compile_commands.json" \
        "${RESULT_DIR}/count-native-compile-commands.json"
    grep -Fq -- '-O3' "${RESULT_DIR}/count-native-compile-commands.json"
    grep -Fq -- '-DNDEBUG' "${RESULT_DIR}/count-native-compile-commands.json"
    grep -Fq -- "${native_flag}" "${RESULT_DIR}/count-native-compile-commands.json"
    sha256sum "${native_build_directory}/re2_count_diagnostics" \
        > "${RESULT_DIR}/count-native-binary.sha256"

    run_count_pipeline_workload()
    {
        local label=$1
        local workload=$2
        local expected_count=$3
        local memory_megabytes=$4
        local native_access=$5
        local native_diagnostics=$6

        taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
            env \
            COUNT_PIPELINE_OUTPUT_DIR="${RESULT_DIR}/${label}" \
            COUNT_PIPELINE_WORKLOAD="${workload}" \
            COUNT_PIPELINE_EXPECTED_COUNT="${expected_count}" \
            COUNT_PIPELINE_MEMORY_MEGABYTES="${memory_megabytes}" \
            COUNT_PIPELINE_ITERATIONS=7 \
            COUNT_PIPELINE_WARMUP_ITERATIONS=5 \
            COUNT_PIPELINE_NATIVE_ITERATIONS=7 \
            RE2_COUNT_DIAGNOSTICS="${native_diagnostics}" \
            REBAR_BENCHMARK_DIR="${benchmark_directory}" \
            REBAR_EXECUTABLE="${rebar_root}/target/release/rebar" \
            REBAR_NATIVE_ACCESS="${native_access}" \
            REBAR_HEAP_SIZE="${BENCHMARK_HEAP_SIZE:-8g}" \
                "${SLICE_DIR}/tools/re2-benchmark/rebar/run-count-pipeline.sh"
    }

    if [[ "${BENCHMARK_MODE}" == dfa-large-pointer ]]; then
        run_count_pipeline_workload \
            object-before \
            curated/10-bounded-repeat/context \
            53 \
            "${COUNT_PIPELINE_MEMORY_MEGABYTES}" \
            disabled \
            /nonexistent
        run_count_pipeline_workload \
            pointer \
            curated/10-bounded-repeat/context \
            53 \
            "${COUNT_PIPELINE_MEMORY_MEGABYTES}" \
            enabled \
            "${native_build_directory}/re2_count_diagnostics"
        run_count_pipeline_workload \
            object-after \
            curated/10-bounded-repeat/context \
            53 \
            "${COUNT_PIPELINE_MEMORY_MEGABYTES}" \
            disabled \
            /nonexistent

        awk -F, 'FNR > 1 && ($4 != 53 || $5 != 0 || $6 != 75850 || $11 != 0) {exit 1}' \
            "${RESULT_DIR}/object-before/java.csv" \
            "${RESULT_DIR}/object-after/java.csv"
        awk -F, 'FNR > 1 && ($4 != 53 || $5 != 0 || $6 != 75850 || $11 < 6291456 || $12 == 0) {exit 1}' \
            "${RESULT_DIR}/pointer/java.csv"
        awk -F, 'FNR > 1 && ($4 != 53 || $5 != 0) {exit 1}' \
            "${RESULT_DIR}/pointer/native.csv"
        return
    fi

    run_count_pipeline_workload \
        context \
        curated/10-bounded-repeat/context \
        53 \
        "${COUNT_PIPELINE_MEMORY_MEGABYTES}" \
        enabled \
        "${native_build_directory}/re2_count_diagnostics"
    run_count_pipeline_workload \
        capitals \
        curated/10-bounded-repeat/capitals \
        11 \
        "${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES}" \
        enabled \
        "${native_build_directory}/re2_count_diagnostics"
    run_count_pipeline_workload \
        letters-en \
        curated/10-bounded-repeat/letters-en \
        1833 \
        "${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES}" \
        enabled \
        "${native_build_directory}/re2_count_diagnostics"
}

run_traditional_native_comparison()
{
    local benchmark_tool="${SLICE_DIR}/tools/re2-benchmark/traditional/summarize.py"
    local native_build_directory="${SLICE_DIR}/target/re2-benchmark-build"
    local native_binary="${native_build_directory}/regexp_benchmark"
    local native_source="${SLICE_DIR}/target/re2-golden-dependencies/re2"
    local pinned_commit=972a15cedd008d846f1a39b2e88ce48d7f166cbd
    local native_filter
    local benchmark_class
    local java_filter
    local java_output
    local native_flag
    local java_outputs=()
    local benchmark_classes=(
        BenchmarkRe2Search
        BenchmarkRe2SearchNfa
        BenchmarkRe2SearchExtra
        BenchmarkRe2Parse
        BenchmarkRe2FullMatch
        BenchmarkRe2Practical
        BenchmarkRe2Misc
    )
    local pair_count
    local selector_arguments=()

    if [[ -n "${TRADITIONAL_BENCHMARK_CLASS}" ]]; then
        benchmark_classes=("${TRADITIONAL_BENCHMARK_CLASS}")
        selector_arguments=(--class-name "${TRADITIONAL_BENCHMARK_CLASS}")
    fi

    python3 "${benchmark_tool}" manifest "${RESULT_DIR}/traditional-pairs.csv" "${selector_arguments[@]}"
    pair_count=$(( $(wc -l < "${RESULT_DIR}/traditional-pairs.csv") - 1 ))
    native_filter=$(python3 "${benchmark_tool}" native-filter "${selector_arguments[@]}")

    RE2_NATIVE_TUNING=host "${SLICE_DIR}/tools/re2-benchmark/build.sh" \
        2>&1 | tee "${RESULT_DIR}/traditional-native-build.log"
    cp "${native_build_directory}/compile_commands.json" \
        "${RESULT_DIR}/traditional-native-compile-commands.json"
    printf '%s\n' "$(git -C "${native_source}" rev-parse HEAD)" \
        > "${RESULT_DIR}/traditional-native-source-commit.txt"
    grep -Fxq "${pinned_commit}" "${RESULT_DIR}/traditional-native-source-commit.txt"
    case "$(uname -m)" in
        x86_64) native_flag=-march=native ;;
        aarch64) native_flag=-mcpu=native ;;
        *) echo "Unsupported traditional benchmark architecture: $(uname -m)" >&2; exit 1 ;;
    esac
    grep -Fq -- '-O3' "${RESULT_DIR}/traditional-native-compile-commands.json"
    grep -Fq -- '-DNDEBUG' "${RESULT_DIR}/traditional-native-compile-commands.json"
    grep -Fq -- "${native_flag}" "${RESULT_DIR}/traditional-native-compile-commands.json"
    sha256sum "${native_binary}" > "${RESULT_DIR}/traditional-native-binary.sha256"
    c++ --version > "${RESULT_DIR}/traditional-native-compiler-version.txt"
    "${native_binary}" \
        --benchmark_filter="${native_filter}" \
        --benchmark_list_tests \
        > "${RESULT_DIR}/traditional-native-benchmark-list.txt"
    if [[ $(wc -l < "${RESULT_DIR}/traditional-native-benchmark-list.txt") -ne ${pair_count} ]]; then
        echo "Traditional native filter did not select exactly ${pair_count} benchmarks" >&2
        exit 1
    fi

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${native_binary}" \
        --benchmark_filter="${native_filter}" \
        --benchmark_min_time="${TRADITIONAL_NATIVE_MINIMUM_TIME:-1s}" \
        --benchmark_repetitions="${TRADITIONAL_NATIVE_REPETITIONS:-5}" \
        --benchmark_report_aggregates_only=true \
        --benchmark_out_format=json \
        --benchmark_out="${RESULT_DIR}/traditional-native-before.json"
    test -s "${RESULT_DIR}/traditional-native-before.json"

    for benchmark_class in "${benchmark_classes[@]}"; do
        java_filter=$(python3 "${benchmark_tool}" java-filter --class-name "${benchmark_class}")
        java_output="${RESULT_DIR}/traditional-java-${benchmark_class}.json"
        BENCHMARK_FORKS="${TRADITIONAL_JMH_FORKS:-5}" \
        BENCHMARK_WARMUP_ITERATIONS=10 \
        BENCHMARK_MEASUREMENT_ITERATIONS=10 \
        BENCHMARK_WARMUP_TIME=1s \
        BENCHMARK_MEASUREMENT_TIME=1s \
        BENCHMARK_NATIVE_ACCESS=true \
        BENCHMARK_DENY_NATIVE_ACCESS=true \
            run_jmh \
                "${SLICE_CLASSPATH}" \
                "${java_filter}" \
                "${java_output}" \
                -foe true \
                -to 10m
        java_outputs+=("${java_output}")
    done
    jq -s 'add' "${java_outputs[@]}" > "${RESULT_DIR}/traditional-java-all.json"

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${native_binary}" \
        --benchmark_filter="${native_filter}" \
        --benchmark_min_time="${TRADITIONAL_NATIVE_MINIMUM_TIME:-1s}" \
        --benchmark_repetitions="${TRADITIONAL_NATIVE_REPETITIONS:-5}" \
        --benchmark_report_aggregates_only=true \
        --benchmark_out_format=json \
        --benchmark_out="${RESULT_DIR}/traditional-native-after.json"
    test -s "${RESULT_DIR}/traditional-native-after.json"

    python3 "${benchmark_tool}" summarize \
        "${RESULT_DIR}/traditional-java-all.json" \
        "${RESULT_DIR}/traditional-native-before.json" \
        "${RESULT_DIR}/traditional-native-after.json" \
        "${RESULT_DIR}/traditional-normalized" \
        "${selector_arguments[@]}"
}

prepare_byte_scan_rebar()
{
    local rebar_root=${REBAR_ROOT:?REBAR_ROOT is required}
    local benchmark_directory="${SLICE_DIR}/target/rebar-slice-benchmarks"
    local rebar="${rebar_root}/target/release/rebar"

    "${SLICE_DIR}/tools/re2-benchmark/rebar/prepare.sh" "${rebar_root}" "${benchmark_directory}"
    cargo build --release --manifest-path "${rebar_root}/Cargo.toml" | tee "${RESULT_DIR}/rebar-build.log"
    "${rebar}" build \
        -d "${benchmark_directory}" \
        -e '^slice/re2$' | tee "${RESULT_DIR}/rebar-engine-build.log"
}

run_byte_scan_rebar()
{
    local label=$1
    local rebar_root=${REBAR_ROOT:?REBAR_ROOT is required}
    local benchmark_directory="${SLICE_DIR}/target/rebar-slice-benchmarks"
    local rebar="${rebar_root}/target/release/rebar"
    local benchmark_filter='^curated/(01-literal|02-literal-alternate)/sherlock(-casei)?-(en|ru|zh)$'

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${rebar}" measure \
        -d "${benchmark_directory}" \
        -e '^slice/re2$' \
        -f "${benchmark_filter}" \
        -m '^count$' \
        --timeout 30s \
        --test \
        > "${RESULT_DIR}/${label}-rebar-verification.csv" \
        2> "${RESULT_DIR}/${label}-rebar-verification.log"

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${rebar}" measure \
        -d "${benchmark_directory}" \
        -e '^slice/re2$' \
        -f "${benchmark_filter}" \
        -m '^count$' \
        --timeout 30s \
        > "${RESULT_DIR}/${label}-rebar-measurements.csv"
}

install_packages
install_java
if [[ "${BENCHMARK_MODE}" == dfa-layout-perfasm || "${BENCHMARK_MODE}" == dfa-absolute-pointer-integrated ]]; then
    install_hsdis
fi
record_environment

if [[ "${BENCHMARK_MODE}" == trino-comparator ]]; then
    run_trino_comparator
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == joni-focused || "${BENCHMARK_MODE}" == joni-memory ]]; then
    prepare_trino_comparator
fi

cd "${SLICE_DIR}"
if [[ "${BENCHMARK_MODE}" == full || "${BENCHMARK_MODE}" == rebar-native-comparison || "${BENCHMARK_MODE}" == traditional-native-comparison ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" "-Dtest=**/re2/**/Test*" test \
        | tee "${RESULT_DIR}/slice-tests-object.log"
    JDK_JAVA_OPTIONS='--enable-native-access=ALL-UNNAMED --illegal-native-access=deny' \
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" "-Dtest=**/re2/**/Test*" test \
        | tee "${RESULT_DIR}/slice-tests-native-access.log"
elif [[ "${BENCHMARK_MODE}" == joni-memory || "${BENCHMARK_MODE}" == joni-memory-census ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestBenchmarkTrinoRegexp,TestTraditionalBenchmarkInputs \
        test | tee "${RESULT_DIR}/slice-tests-object.log"
    JDK_JAVA_OPTIONS='--enable-native-access=ALL-UNNAMED --illegal-native-access=deny' \
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestBenchmarkTrinoRegexp,TestTraditionalBenchmarkInputs \
        test | tee "${RESULT_DIR}/slice-tests-native-access.log"
elif [[ "${BENCHMARK_MODE}" == safere-comparator || "${BENCHMARK_MODE}" == safere-contains ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestBenchmarkRe2BooleanPartialMatch,TestBenchmarkSafeReTrinoRegexp,TestBenchmarkTrinoRegexp,TestRe2BooleanMatchOptimizations \
        test | tee "${RESULT_DIR}/slice-tests-object.log"
    JDK_JAVA_OPTIONS='--enable-native-access=ALL-UNNAMED --illegal-native-access=deny' \
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestBenchmarkRe2BooleanPartialMatch,TestBenchmarkSafeReTrinoRegexp,TestBenchmarkTrinoRegexp,TestRe2BooleanMatchOptimizations \
        test | tee "${RESULT_DIR}/slice-tests-native-access.log"
elif [[ "${BENCHMARK_MODE}" == bounded-count || "${BENCHMARK_MODE}" == dfa-large-pointer ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestCountPipelineRunner,TestDfaCountMatches,TestRebarRunner \
        test | tee "${RESULT_DIR}/slice-tests-object.log"
    JDK_JAVA_OPTIONS='--enable-native-access=ALL-UNNAMED --illegal-native-access=deny' \
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestCountPipelineRunner,TestDfaCountMatches,TestRebarRunner \
        test | tee "${RESULT_DIR}/slice-tests-native-access.log"
elif [[ "${BENCHMARK_MODE}" == capture-pipeline ]]; then
    JDK_JAVA_OPTIONS='--enable-native-access=ALL-UNNAMED --illegal-native-access=deny' \
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestCapturePipelineRunner,TestRebarRunner,TestTraditionalBenchmarkInputs \
        test | tee "${RESULT_DIR}/slice-tests.log"
elif [[ "${BENCHMARK_MODE}" == dfa-absolute-pointer-integrated ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -Dtest=TestDfaAbsolutePointerTransitions,TestBenchmarkDfaSampledSelfLoopSearch,TestDfaPairedTransitions test | tee "${RESULT_DIR}/slice-tests.log"
elif [[ "${BENCHMARK_MODE}" == dfa-real-layout-screen ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -Dtest=TestBenchmarkDfaRealTransitionLayout test | tee "${RESULT_DIR}/slice-tests.log"
elif [[ "${BENCHMARK_MODE}" == dfa-layout-perfasm ]]; then
    case "${DFA_LAYOUT_PERFASM_BENCHMARK:-}" in
        BenchmarkDfaByteTransitionLayout.*) layout_test=TestBenchmarkDfaByteTransitionLayout ;;
        BenchmarkDfaCharacterOffsetLayout.*) layout_test=TestBenchmarkDfaCharacterOffsetLayout ;;
        BenchmarkDfaCompactTransitionLayout.*) layout_test=TestBenchmarkDfaCompactTransitionLayout ;;
        BenchmarkDfaRealTransitionLayout.*) layout_test=TestBenchmarkDfaRealTransitionLayout ;;
        *) echo "Unsupported DFA layout perfasm benchmark: ${DFA_LAYOUT_PERFASM_BENCHMARK:-}" >&2; exit 1 ;;
    esac
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -Dtest="${layout_test}" test | tee "${RESULT_DIR}/slice-tests.log"
elif [[ "${BENCHMARK_MODE}" == dfa-layout-screen ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -Dtest=TestBenchmarkDfaByteTransitionLayout,TestBenchmarkDfaCharacterOffsetLayout,TestBenchmarkDfaCompactTransitionLayout,TestBenchmarkDfaTransitionLayout,TestBenchmarkDfaRealTransitionLayout,TestBenchmarkDfaPairedTransitionScaling,TestBenchmarkDfaPartialPairedTransitions,TestBenchmarkRebarPairedTransitions,TestDfaPairedTransitions test | tee "${RESULT_DIR}/slice-tests.log"
elif [[ "${BENCHMARK_MODE}" == dfa-layout || "${BENCHMARK_MODE}" == dfa-pair-candidate || "${BENCHMARK_MODE}" == dfa-pair-diagnostic || "${BENCHMARK_MODE}" == dfa-pair-scaling || "${BENCHMARK_MODE}" == dfa-paired-corpus || "${BENCHMARK_MODE}" == dfa-partial-candidate || "${BENCHMARK_MODE}" == dfa-self-loop-final || "${BENCHMARK_MODE}" == dfa-self-loop-paired-protected || "${BENCHMARK_MODE}" == dfa-self-loop-protected || "${BENCHMARK_MODE}" == dfa-self-loop-rebar ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -Dtest=TestBenchmarkDfaTransitionLayout,TestBenchmarkDfaRealTransitionLayout,TestBenchmarkDfaSampledSelfLoopSearch,TestBenchmarkDfaSelfLoopSearch,TestBenchmarkDfaPairedTransitionScaling,TestBenchmarkDfaPartialPairedTransitions,TestBenchmarkRebarPairedTransitions,TestDfaPairedTransitions test | tee "${RESULT_DIR}/slice-tests.log"
elif [[ "${BENCHMARK_MODE}" == vector-scanner || "${BENCHMARK_MODE}" == vector-literal ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -Dtest=TestBenchmarkByteScanner test | tee "${RESULT_DIR}/slice-tests.log"
elif [[ "${BENCHMARK_MODE}" != historical-dfa ]]; then
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" "-Dtest=**/re2/**/Test*" test | tee "${RESULT_DIR}/slice-tests.log"
fi
./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile

SLICE_DEPENDENCIES="${RESULT_DIR}/slice-classpath.txt"
build_classpath "${SLICE_DIR}" "${SLICE_DEPENDENCIES}"
SLICE_CLASSPATH="${SLICE_DIR}/target/test-classes:${SLICE_DIR}/target/classes:$(cat "${SLICE_DEPENDENCIES}")"

if [[ "${BENCHMARK_MODE}" == rebar-official ]]; then
    run_official_rebar
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == rebar-native-comparison ]]; then
    run_rebar_native_comparison
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == bounded-count || "${BENCHMARK_MODE}" == dfa-large-pointer ]]; then
    run_bounded_count_pipeline
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == capture-pipeline ]]; then
    run_capture_pipeline
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == traditional-native-comparison ]]; then
    run_traditional_native_comparison
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == joni-memory ]]; then
    run_joni_memory_qualification
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == joni-memory-census ]]; then
    run_joni_memory_census
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == safere-comparator || "${BENCHMARK_MODE}" == safere-contains ]]; then
    if [[ "${BENCHMARK_MODE}" == safere-contains ]]; then
        run_boolean_partial_match_qualification
    fi
    run_safere_qualification
    exit 0
fi
if [[ "${BENCHMARK_MODE}" == vector-scanner ]]; then
    BENCHMARK_FORKS="${VECTOR_SCANNER_FORKS}" \
        BENCHMARK_WARMUP_ITERATIONS=5 \
        BENCHMARK_MEASUREMENT_ITERATIONS=5 \
        BENCHMARK_WARMUP_TIME=200ms \
        BENCHMARK_MEASUREMENT_TIME=200ms \
        run_jmh \
            "${SLICE_CLASSPATH}" \
            "${VECTOR_SCANNER_FILTER}" \
            "${RESULT_DIR}/vector-scanner.json" \
            -p "sourceLength=${VECTOR_SCANNER_SOURCE_LENGTHS}" \
            -p "candidateCount=${VECTOR_SCANNER_CANDIDATE_COUNTS}" \
            -p "inputShape=${VECTOR_SCANNER_INPUT_SHAPES}" \
            -p "arrayOffset=${VECTOR_SCANNER_ARRAY_OFFSETS}"
    exit 0
fi

run_capture_engine_direct()
{
    local label=$1
    local benchmark_filter
    local benchmark_arguments=()
    case "${CAPTURE_ENGINE}" in
        nfa) benchmark_filter='BenchmarkRe2Parse\.parse(1Split|3DigitDs|3Digits|SplitHard)Nfa$' ;;
        onepass) benchmark_filter='BenchmarkRe2Parse\.parse(1Split|3DigitDs|3Digits)OnePass$|BenchmarkRe2SearchExtra\.search(Success|AltMatch)OnePass$' ;;
        bitstate)
            case "${CAPTURE_SHARD}" in
                direct) benchmark_filter='BenchmarkRe2Parse\.parse(1Split|3DigitDs|3Digits|SplitHard)BitState$' ;;
                guard)
                    benchmark_filter='BenchmarkRe2SearchExtra\.search(Success1|AltMatch)BitState$'
                    benchmark_arguments=(-p "textSize=${CAPTURE_GUARD_SIZES}")
                    ;;
            esac
            ;;
    esac
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        "${benchmark_filter}" \
        "${RESULT_DIR}/${label}-capture-${CAPTURE_ENGINE}-${CAPTURE_SHARD}.json" \
        "${benchmark_arguments[@]}" \
        -prof gc
}

run_capture_engine_native()
{
    local label=$1
    local native_binary="${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark"
    local native_filter
    case "${CAPTURE_ENGINE}" in
        nfa) native_filter='^Parse_Cached(DigitDs|Digits|Split|SplitHard)_NFA/threads:1$' ;;
        onepass) native_filter='^Parse_Cached(DigitDs|Digits|Split)_OnePass/threads:1$' ;;
        bitstate) native_filter='^Parse_Cached(DigitDs|Digits|Split|SplitHard)_BitState/threads:1$' ;;
    esac

    taskset --cpu-list "${BENCHMARK_CPU_LIST:-0}" \
        "${native_binary}" \
        --benchmark_filter="${native_filter}" \
        --benchmark_min_time=0.2s \
        --benchmark_repetitions=5 \
        --benchmark_report_aggregates_only=true \
        --benchmark_out_format=json \
        --benchmark_out="${RESULT_DIR}/${label}-capture-${CAPTURE_ENGINE}-native.json"
}

capture_source_hash()
{
    find \
        "${SLICE_DIR}/src/main/java/io/airlift/slice/re2" \
        "${SLICE_DIR}/src/test/java/io/airlift/slice/re2" \
        -type f -print0 \
        | sort -z \
        | xargs -0 sha256sum \
        | sha256sum \
        | awk '{print $1}'
}

if [[ "${BENCHMARK_MODE}" == capture-engine ]]; then
    if [[ "${CAPTURE_SHARD}" == scaling ]]; then
        BENCHMARK_FORKS=3 run_jmh \
            "${SLICE_CLASSPATH}" \
            'BenchmarkNfaCaptureScaling' \
            "${RESULT_DIR}/capture-scaling.json" \
            -prof gc
        exit 0
    fi

    RE2_NATIVE_TUNING=host "${SLICE_DIR}/tools/re2-benchmark/build.sh" \
        2>&1 | tee "${RESULT_DIR}/capture-engine-native-build.log"
    control_patch="${RESULT_DIR}/${CAPTURE_CONTROL}-control.patch"
    cp "${SLICE_DIR}/tools/re2-benchmark/capture-engine/${CAPTURE_CONTROL}-control.patch" \
        "${control_patch}"
    git -C "${SLICE_DIR}" apply --check "${control_patch}"

    if [[ ${CAPTURE_SHARD} == direct ]]; then
        run_capture_engine_native native-before
    fi
    run_capture_engine_direct candidate-before

    candidate_source_hash=$(capture_source_hash)
    git -C "${SLICE_DIR}" apply "${control_patch}"
    control_applied=true
    restore_capture_candidate()
    {
        if [[ ${control_applied} == true ]]; then
            git -C "${SLICE_DIR}" apply --reverse "${control_patch}"
        fi
    }
    trap restore_capture_candidate EXIT

    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" \
        -Dtest=TestUpstreamNfaSearch,TestUpstreamOnePassSearch,TestUpstreamBitStateSearch,TestPublicEngineAgreement,TestRe2MatchInto \
        test | tee "${RESULT_DIR}/${CAPTURE_CONTROL}-control-tests.log"
    run_capture_engine_direct "${CAPTURE_CONTROL}-control"

    git -C "${SLICE_DIR}" apply --reverse --check "${control_patch}"
    git -C "${SLICE_DIR}" apply --reverse "${control_patch}"
    control_applied=false
    trap - EXIT
    restored_source_hash=$(capture_source_hash)
    if [[ ${restored_source_hash} != "${candidate_source_hash}" ]]; then
        echo "Capture candidate source was not restored exactly" >&2
        exit 1
    fi
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    run_capture_engine_direct candidate-after
    if [[ ${CAPTURE_SHARD} == direct ]]; then
        run_capture_engine_native native-after
    fi
    exit 0
fi

set_byte_scan_productivity_check_enabled()
{
    local enabled=$1
    local dfa_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/Dfa.java"
    local enabled_declaration='    private static final boolean BYTE_SCAN_PRODUCTIVITY_CHECK_ENABLED = true;'
    local disabled_declaration='    private static final boolean BYTE_SCAN_PRODUCTIVITY_CHECK_ENABLED = false;'
    local expected_declaration
    local replacement_declaration

    if [[ "${enabled}" == true ]]; then
        expected_declaration=${disabled_declaration}
        replacement_declaration=${enabled_declaration}
    else
        expected_declaration=${enabled_declaration}
        replacement_declaration=${disabled_declaration}
    fi

    if [[ $(grep -Fxc "${expected_declaration}" "${dfa_source}") -ne 1 ]]; then
        echo "Unable to set byte-scan productivity check enabled=${enabled}: expected source not found" >&2
        exit 1
    fi
    sed -i "s|${expected_declaration}|${replacement_declaration}|" "${dfa_source}"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

run_byte_scan_targets()
{
    local label=$1
    run_byte_scan_rebar "${label}"
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.(contains|count|extract|replace)$' \
        "${RESULT_DIR}/${label}-trino-capture-sparse.json" \
        -p workload=captureSparse \
        -p sourceLength=32768
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaFixedDistanceByte\.(partialMatch|matchBoundary)$' \
        "${RESULT_DIR}/${label}-fixed-distance.json" \
        -p inputShape=SPARSE_MATCH,DENSE_FALSE_POSITIVE,NO_MATCH \
        -p sourceLength=32768
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Easy0|Easy1|Hard|Parens)Dfa$' \
        "${RESULT_DIR}/${label}-protected-dfa.json" \
        -p textSize=16777216
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaSampledSelfLoopSearch\.search$' \
        "${RESULT_DIR}/${label}-state-changing-compact.json" \
        -p textLength=16777216
}

run_byte_scan_source_comparison()
{
    local candidate_source="${RESULT_DIR}/candidate-Dfa.java"
    cp src/main/java/io/airlift/slice/re2/Dfa.java "${candidate_source}"
    sha256sum "${candidate_source}" > "${RESULT_DIR}/candidate-source-sha256.txt"

    restore_candidate()
    {
        cp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    }
    trap restore_candidate EXIT

    prepare_byte_scan_rebar
    run_byte_scan_targets candidate-before

    set_byte_scan_productivity_check_enabled false
    diff -u "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]
    run_byte_scan_targets control

    restore_candidate
    cmp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
    sha256sum src/main/java/io/airlift/slice/re2/Dfa.java >> "${RESULT_DIR}/candidate-source-sha256.txt"
    trap - EXIT
    run_byte_scan_targets candidate-after
}

if [[ "${BENCHMARK_MODE}" == byte-scan-fallback ]]; then
    run_byte_scan_source_comparison
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == vector-literal ]]; then
    BENCHMARK_FORKS="${VECTOR_LITERAL_FORKS}" \
        BENCHMARK_WARMUP_ITERATIONS=5 \
        BENCHMARK_MEASUREMENT_ITERATIONS=5 \
        BENCHMARK_WARMUP_TIME=200ms \
        BENCHMARK_MEASUREMENT_TIME=200ms \
        run_jmh \
            "${SLICE_CLASSPATH}" \
            "${VECTOR_LITERAL_FILTER}" \
            "${RESULT_DIR}/vector-literal.json" \
            -p "language=${VECTOR_LITERAL_LANGUAGES}" \
            -p "sourceLength=${VECTOR_LITERAL_SOURCE_LENGTHS}" \
            -p "inputShape=${VECTOR_LITERAL_INPUT_SHAPES}" \
            -p "offsetSelection=${VECTOR_LITERAL_OFFSET_SELECTIONS}"

    prepare_byte_scan_rebar
    run_byte_scan_rebar candidate-before

    candidate_source="${RESULT_DIR}/candidate-Prog.java"
    cp src/main/java/io/airlift/slice/re2/Prog.java "${candidate_source}"
    restore_vector_literal_candidate()
    {
        cp "${candidate_source}" src/main/java/io/airlift/slice/re2/Prog.java
    }
    trap restore_vector_literal_candidate EXIT
    sed -i 's/if (prefixFoldCase || prefixSize <= 1 || prefix\[0\] >= 0) {/if (true) {/' \
        src/main/java/io/airlift/slice/re2/Prog.java
    if cmp -s "${candidate_source}" src/main/java/io/airlift/slice/re2/Prog.java; then
        echo "Unable to disable the fused prefix route" >&2
        exit 1
    fi
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    run_byte_scan_rebar control

    restore_vector_literal_candidate
    trap - EXIT
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    run_byte_scan_rebar candidate-after
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == joni-focused ]]; then
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.extract$' \
        "${RESULT_DIR}/slice-before.json" \
        -p workload=unicodeSparse \
        -p sourceLength=1024
    BENCHMARK_FORKS=5 run_jmh \
        "${TRINO_CLASSPATH}" \
        'BenchmarkRegexpOperations\.extractJoni$' \
        "${RESULT_DIR}/joni.json" \
        -p workload=unicodeSparse \
        -p sourceLength=1024
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.extract$' \
        "${RESULT_DIR}/slice-after.json" \
        -p workload=unicodeSparse \
        -p sourceLength=1024
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-layout-screen ]]; then
    if [[ "${DFA_LAYOUT_FILTER:-}" != *BenchmarkDfaCompactTransitionLayout* || "${DFA_LAYOUT_FILTER:-}" != *objectReferences* ]]; then
        echo "DFA_LAYOUT_FILTER must select BenchmarkDfaCompactTransitionLayout and include the objectReferences control" >&2
        exit 1
    fi
    if [[ ! "${DFA_LAYOUT_FORKS:-5}" =~ ^[1-9][0-9]*$ ]]; then
        echo "DFA_LAYOUT_FORKS must be a positive integer: ${DFA_LAYOUT_FORKS:-}" >&2
        exit 1
    fi

    BENCHMARK_HEAP_SIZE=8g BENCHMARK_FORKS="${DFA_LAYOUT_FORKS:-5}" run_jmh \
        "${SLICE_CLASSPATH}" \
        "${DFA_LAYOUT_FILTER}" \
        "${RESULT_DIR}/slice-dfa-layout-screen.json" \
        -p "stateCount=${DFA_LAYOUT_STATE_COUNTS:-32,128,512,2048}" \
        -p "classCount=${DFA_LAYOUT_CLASS_COUNTS:-8,29,64}"

    case "${DFA_LAYOUT_RUN_NATIVE:-false}" in
        false) ;;
        true)
            RE2_BENCHMARK_BUILD_DIR="${SLICE_DIR}/target/re2-benchmark-build-host" \
                RE2_NATIVE_TUNING=host \
                RE2_BENCHMARK_TARGETS='regexp_benchmark dfa_pointer_benchmark' \
                tools/re2-benchmark/build.sh | tee "${RESULT_DIR}/native-host-build.log"
            taskset --cpu-list 0 \
                "${SLICE_DIR}/target/re2-benchmark-build-host/regexp_benchmark" \
                --benchmark_filter='^Search_Hard_CachedDFA/16777216/threads:1$' \
                --benchmark_min_time=0.5s \
                --benchmark_repetitions=5 \
                --benchmark_out="${RESULT_DIR}/native-hard-host.json" \
                --benchmark_out_format=json
            taskset --cpu-list 0 \
                "${SLICE_DIR}/target/re2-benchmark-build-host/dfa_pointer_benchmark" \
                --benchmark_min_time=0.5s \
                --benchmark_repetitions=5 \
                --benchmark_out="${RESULT_DIR}/native-dfa-pointer-host.json" \
                --benchmark_out_format=json
            ;;
        *)
            echo "DFA_LAYOUT_RUN_NATIVE must be true or false: ${DFA_LAYOUT_RUN_NATIVE}" >&2
            exit 1
            ;;
    esac
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-real-layout-screen ]]; then
    if [[ "${DFA_REAL_LAYOUT_FILTER:-}" != *BenchmarkDfaRealTransitionLayout* ||
            "${DFA_REAL_LAYOUT_FILTER:-}" != *compactObjectReferences* ]]; then
        echo "DFA_REAL_LAYOUT_FILTER must select BenchmarkDfaRealTransitionLayout and include the compactObjectReferences control" >&2
        exit 1
    fi
    if [[ ! "${DFA_REAL_LAYOUT_PATTERNS:-HARD,PARENS}" =~ ^(HARD|PARENS|SELF_LOOP|LATE_TRANSITION|ALTERNATING|MULTIBYTE)(,(HARD|PARENS|SELF_LOOP|LATE_TRANSITION|ALTERNATING|MULTIBYTE))*$ ]]; then
        echo "DFA_REAL_LAYOUT_PATTERNS contains an unsupported topology: ${DFA_REAL_LAYOUT_PATTERNS:-}" >&2
        exit 1
    fi
    if [[ ! "${DFA_REAL_LAYOUT_FORKS:-5}" =~ ^[1-9][0-9]*$ ]]; then
        echo "DFA_REAL_LAYOUT_FORKS must be a positive integer: ${DFA_REAL_LAYOUT_FORKS:-}" >&2
        exit 1
    fi

    BENCHMARK_HEAP_SIZE=8g BENCHMARK_FORKS="${DFA_REAL_LAYOUT_FORKS:-5}" run_jmh \
        "${SLICE_CLASSPATH}" \
        "${DFA_REAL_LAYOUT_FILTER}" \
        "${RESULT_DIR}/slice-dfa-real-layout-screen.json" \
        -p "pattern=${DFA_REAL_LAYOUT_PATTERNS:-HARD,PARENS}"

    case "${DFA_REAL_LAYOUT_RUN_NATIVE:-false}" in
        false) ;;
        true)
            if [[ ! "${DFA_REAL_LAYOUT_PATTERNS}" =~ ^(HARD|PARENS)(,(HARD|PARENS))*$ ]]; then
                echo "Native real-layout comparison only supports HARD and PARENS" >&2
                exit 1
            fi
            native_patterns=${DFA_REAL_LAYOUT_PATTERNS//HARD/Hard}
            native_patterns=${native_patterns//PARENS/Parens}
            native_patterns=${native_patterns//,/|}
            RE2_BENCHMARK_BUILD_DIR="${SLICE_DIR}/target/re2-benchmark-build-host" \
                RE2_NATIVE_TUNING=host \
                tools/re2-benchmark/build.sh | tee "${RESULT_DIR}/native-host-build.log"
            taskset --cpu-list 0 \
                "${SLICE_DIR}/target/re2-benchmark-build-host/regexp_benchmark" \
                --benchmark_filter="^Search_(${native_patterns})_CachedDFA/16777216/threads:1$" \
                --benchmark_min_time=0.5s \
                --benchmark_repetitions=5 \
                --benchmark_out="${RESULT_DIR}/native-real-layout-host.json" \
                --benchmark_out_format=json
            ;;
        *)
            echo "DFA_REAL_LAYOUT_RUN_NATIVE must be true or false: ${DFA_REAL_LAYOUT_RUN_NATIVE}" >&2
            exit 1
            ;;
    esac
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-layout-perfasm ]]; then
    benchmark=${DFA_LAYOUT_PERFASM_BENCHMARK:-BenchmarkDfaCompactTransitionLayout.exactStrideHeapIntegers}
    native_access_arguments=()
    jvm_arguments='--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch'
    if [[ "${benchmark}" == BenchmarkDfaCompactTransitionLayout.everythingSegment*Pointers ]]; then
        native_access_arguments=(--enable-native-access=ALL-UNNAMED)
        jvm_arguments+=' --enable-native-access=ALL-UNNAMED'
    fi
    case "${benchmark}" in
        BenchmarkDfaByteTransitionLayout.*)
            control=BenchmarkDfaCompactTransitionLayout.objectReferences
            parameters=(-p "stateCount=${DFA_LAYOUT_PERFASM_STATE_COUNT:-128}" -p "classCount=${DFA_LAYOUT_PERFASM_CLASS_COUNT:-29}")
            ;;
        BenchmarkDfaCharacterOffsetLayout.*)
            control=BenchmarkDfaCompactTransitionLayout.objectReferences
            parameters=(-p "stateCount=${DFA_LAYOUT_PERFASM_STATE_COUNT:-128}" -p "classCount=${DFA_LAYOUT_PERFASM_CLASS_COUNT:-29}")
            ;;
        BenchmarkDfaCompactTransitionLayout.*)
            control=BenchmarkDfaCompactTransitionLayout.objectReferences
            parameters=(-p "stateCount=${DFA_LAYOUT_PERFASM_STATE_COUNT:-512}" -p "classCount=${DFA_LAYOUT_PERFASM_CLASS_COUNT:-29}")
            ;;
        BenchmarkDfaRealTransitionLayout.*)
            if java --add-modules jdk.incubator.vector -cp "${SLICE_CLASSPATH}" org.openjdk.jmh.Main -l |
                    grep -q 'BenchmarkDfaRealTransitionLayout\.objectCompactTransitions$'; then
                control=BenchmarkDfaRealTransitionLayout.objectCompactTransitions
            else
                control=BenchmarkDfaRealTransitionLayout.compactObjectReferences
            fi
            parameters=(-p "pattern=${DFA_LAYOUT_PERFASM_PATTERN:-HARD}")
            ;;
        *)
            echo "Unsupported DFA layout perfasm benchmark: ${benchmark}" >&2
            exit 1
            ;;
    esac

    benchmarks=("${control}")
    if [[ "${benchmark}" != "${control}" ]]; then
        benchmarks+=("${benchmark}")
    fi

    sudo sysctl -w kernel.perf_event_paranoid=-1
    sudo sysctl -w kernel.kptr_restrict=0
    for selected_benchmark in "${benchmarks[@]}"; do
        method=${selected_benchmark##*.}
        escaped_benchmark=${selected_benchmark//./\\.}
        taskset --cpu-list 0 \
            java "${native_access_arguments[@]}" --add-modules jdk.incubator.vector \
            -cp "${SLICE_CLASSPATH}" \
            org.openjdk.jmh.Main "${escaped_benchmark}$" \
            "${parameters[@]}" \
            -f 1 \
            -wi 15 \
            -i 7 \
            -w 500ms \
            -r 500ms \
            -prof 'perf:events=cycles,instructions,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses' \
            -jvmArgsAppend "${jvm_arguments}" \
            -rf json \
            -rff "${RESULT_DIR}/${method}-counters.json" \
            | tee "${RESULT_DIR}/${method}-counters.log"

        taskset --cpu-list 0 \
            java "${native_access_arguments[@]}" --add-modules jdk.incubator.vector \
            -cp "${SLICE_CLASSPATH}" \
            org.openjdk.jmh.Main "${escaped_benchmark}$" \
            "${parameters[@]}" \
            -f 1 \
            -wi 15 \
            -i 7 \
            -w 500ms \
            -r 500ms \
            -prof "perfasm:events=cycles;hotThreshold=0.02;top=10;printMargin=32;savePerf=true;savePerfTo=${RESULT_DIR};savePerfBin=true;savePerfBinTo=${RESULT_DIR};saveLog=true;saveLogTo=${RESULT_DIR}" \
            -jvmArgsAppend "${jvm_arguments}" \
            -rf json \
            -rff "${RESULT_DIR}/${method}-perfasm.json" \
            | tee "${RESULT_DIR}/${method}-perfasm.log"
    done

    if [[ "${benchmark}" == BenchmarkDfaCompactTransitionLayout.everythingSegment*Pointers ]]; then
        state_count=${DFA_LAYOUT_PERFASM_STATE_COUNT:-512}
        class_count=${DFA_LAYOUT_PERFASM_CLASS_COUNT:-29}
        native_filter="^DfaAbsolutePointers(SeparateAddress)?/${state_count}/${class_count}$"
        native_binary="${SLICE_DIR}/target/re2-benchmark-build-host/dfa_pointer_benchmark"
        RE2_BENCHMARK_BUILD_DIR="${SLICE_DIR}/target/re2-benchmark-build-host" \
            RE2_NATIVE_TUNING=host \
            RE2_BENCHMARK_TARGETS=dfa_pointer_benchmark \
            tools/re2-benchmark/build.sh | tee "${RESULT_DIR}/native-pointer-build.log"

        DFA_LAYOUT_STATE_COUNTS="${state_count}" \
        DFA_LAYOUT_CLASS_COUNTS="${class_count}" \
            taskset --cpu-list 0 "${native_binary}" \
                --benchmark_filter="${native_filter}" \
                --benchmark_min_time=0.5s \
                --benchmark_repetitions=5 \
                --benchmark_out="${RESULT_DIR}/native-pointer.json" \
                --benchmark_out_format=json \
                | tee "${RESULT_DIR}/native-pointer.log"

        for native_method in DfaAbsolutePointers DfaAbsolutePointersSeparateAddress; do
            method_filter="^${native_method}/${state_count}/${class_count}$"
            case "${native_method}" in
                DfaAbsolutePointers) method_file=native-pointer ;;
                DfaAbsolutePointersSeparateAddress) method_file=native-pointer-separate-address ;;
            esac
            DFA_LAYOUT_STATE_COUNTS="${state_count}" \
            DFA_LAYOUT_CLASS_COUNTS="${class_count}" \
                perf stat -x, -r 5 \
                    -e cycles,instructions,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses \
                    -o "${RESULT_DIR}/${method_file}-counters.csv" \
                    -- taskset --cpu-list 0 "${native_binary}" \
                        --benchmark_filter="${method_filter}" \
                        --benchmark_min_time=2s \
                        --benchmark_repetitions=1 \
                        > "${RESULT_DIR}/${method_file}-counters.log" 2>&1

            DFA_LAYOUT_STATE_COUNTS="${state_count}" \
            DFA_LAYOUT_CLASS_COUNTS="${class_count}" \
                perf record -e cycles:u -o "${RESULT_DIR}/${method_file}.perf" -- \
                    taskset --cpu-list 0 "${native_binary}" \
                        --benchmark_filter="${method_filter}" \
                        --benchmark_min_time=2s \
                        --benchmark_repetitions=1 \
                        > "${RESULT_DIR}/${method_file}-perf.log" 2>&1
            perf report --stdio -i "${RESULT_DIR}/${method_file}.perf" > "${RESULT_DIR}/${method_file}-perf-report.txt"
        done
        objdump -d -C "${native_binary}" > "${RESULT_DIR}/native-pointer-disassembly.txt"
    fi
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-pair-scaling ]]; then
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaPairedTransitionScaling\.(compactObjectReferences|pairedObjectReferences)$' \
        "${RESULT_DIR}/slice-dfa-pair-scaling.json" \
        -p "stateCount=${PAIR_SCALING_STATE_COUNTS:-4,8,16,32,64,128,256}" \
        -p "classCount=${PAIR_SCALING_CLASS_COUNTS:-16,29,64}"
    exit 0
fi

set_start_byte_acceleration_enabled()
{
    local enabled=$1
    local dfa_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/Dfa.java"
    local enabled_assignment='            this.startByteCandidates = prog.buildStartByteCandidates(stack);'
    local disabled_assignment='            this.startByteCandidates = null;'
    local enabled_dispatch='                if (canUseStartByteAcceleration(dfa, anchoredEffective, true, startData.flag())) {'
    local disabled_dispatch='                if (false) {'
    local expected_assignment
    local replacement_assignment
    local expected_dispatch
    local replacement_dispatch

    if [[ "${enabled}" == true ]]; then
        expected_assignment=${disabled_assignment}
        replacement_assignment=${enabled_assignment}
        expected_dispatch=${disabled_dispatch}
        replacement_dispatch=${enabled_dispatch}
    else
        expected_assignment=${enabled_assignment}
        replacement_assignment=${disabled_assignment}
        expected_dispatch=${enabled_dispatch}
        replacement_dispatch=${disabled_dispatch}
    fi

    if [[ $(grep -Fxc "${expected_assignment}" "${dfa_source}") -ne 1 ||
            $(grep -Fxc "${expected_dispatch}" "${dfa_source}") -ne 1 ]]; then
        echo "Unable to set start-byte acceleration enabled=${enabled}: expected source not found" >&2
        exit 1
    fi
    sed -i "s|${expected_assignment}|${replacement_assignment}|" "${dfa_source}"
    sed -i "s|${expected_dispatch}|${replacement_dispatch}|" "${dfa_source}"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

run_start_byte_target()
{
    local label=$1
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.(contains|count|extract|replace)$' \
        "${RESULT_DIR}/${label}-trino-capture-sparse.json" \
        -p workload=captureSparse \
        -p sourceLength=32768
}

run_start_byte_compile_gates()
{
    local label=$1
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2CompileFocused\.(compileToProgFromParsed|compileToProgAndConstructFirstMatchDfa|re2CompileTotal)$' \
        "${RESULT_DIR}/${label}-compile.json" \
        -p 'pattern=([a-z]+)-([0-9]+)'
}

run_start_byte_protected_gates()
{
    local label=$1
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Easy0|Easy1|Hard|Parens)Dfa$' \
        "${RESULT_DIR}/${label}-protected-dfa.json" \
        -p textSize=16777216
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Easy0|Easy1|Hard|Parens)Dfa$' \
        "${RESULT_DIR}/${label}-protected-dfa-tiny.json" \
        -p textSize=8
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2SearchExtra\.searchBigFixed(Dfa|Re2)$' \
        "${RESULT_DIR}/${label}-protected-big-fixed.json" \
        -p textSize=32768
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2SearchExtra\.searchEasy2(Dfa|Re2)$' \
        "${RESULT_DIR}/${label}-protected-easy2.json" \
        -p textSize=16777216
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.count$' \
        "${RESULT_DIR}/${label}-protected-trino.json" \
        -p workload=delimiterDense,emptyMatches \
        -p sourceLength=32768
}

set_partial_pairing_enabled()
{
    local enabled=$1
    local dfa_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/Dfa.java"
    local maximum_rows='            int maximumRows = (int) Math.min(stateCount, (maximumMemory - outerMemory) / rowMemory);'
    local disabled_gate='            if (maximumRows < stateCount) {'
    local gate_count
    local maximum_rows_line
    gate_count=$(grep -Fxc "${disabled_gate}" "${dfa_source}" || true)

    if [[ "${enabled}" == true ]]; then
        if [[ ${gate_count} -ne 1 ]]; then
            echo "Unable to enable partial pairing: disabled gate not found" >&2
            exit 1
        fi
        sed -i '/^            if (maximumRows < stateCount) {$/,+2d' "${dfa_source}"
    else
        if [[ ${gate_count} -ne 0 || $(grep -Fxc "${maximum_rows}" "${dfa_source}") -ne 1 ]]; then
            echo "Unable to disable partial pairing: expected source not found" >&2
            exit 1
        fi
        maximum_rows_line=$(grep -Fnx "${maximum_rows}" "${dfa_source}" | cut -d: -f1)
        sed -i "${maximum_rows_line}a\\
            if (maximumRows < stateCount) {\\
                return;\\
            }" "${dfa_source}"
    fi
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

run_partial_pairing_target()
{
    local label=$1
    local expect_partial_table=$2
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaPartialPairedTransitions\.search$' \
        "${RESULT_DIR}/${label}-partial-paired.json" \
        -p textLength=32768,1048576,16777216 \
        -p "expectPartialTable=${expect_partial_table}"
}

run_partial_pairing_protected_gates()
{
    local label=$1
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Easy0|Easy1|Hard|Parens)Dfa$' \
        "${RESULT_DIR}/${label}-protected-dfa.json" \
        -p textSize=16777216
}

run_rebar_pairing_corpus()
{
    local label=$1
    local expect_paired_table=$2
    local workloads=$3
    BENCHMARK_FORKS="${BENCHMARK_FORKS:-3}" run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRebarPairedTransitions\.searchAll$' \
        "${RESULT_DIR}/${label}-rebar-paired.json" \
        -p "workload=${workloads}" \
        -p "expectPairedTable=${expect_paired_table}"
}

run_self_loop_protected_paths()
{
    local label=$1
    BENCHMARK_FORKS=2 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaPartialPairedTransitions\.search$' \
        "${RESULT_DIR}/${label}-partial-paired.json" \
        -p workload=MATCH,NO_MATCH \
        -p textLength=16777216 \
        -p expectPartialTable=true
    BENCHMARK_FORKS=2 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Medium|Hard|Parens)Dfa$' \
        "${RESULT_DIR}/${label}-protected-dfa.json" \
        -p textSize=16777216
    BENCHMARK_FORKS=2 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaSampledSelfLoopSearch\.search$' \
        "${RESULT_DIR}/${label}-state-changing-compact.json" \
        -p textLength=16777216
    BENCHMARK_FORKS=1 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaFixedDistanceByte\.partialMatch$' \
        "${RESULT_DIR}/${label}-fixed-distance.json" \
        -p inputShape=SPARSE_MATCH,DENSE_FALSE_POSITIVE,NO_MATCH \
        -p sourceLength=32768
}

run_self_loop_final_path()
{
    local label=$1
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaSelfLoopSearch\.search$' \
        "${RESULT_DIR}/${label}-self-loop.json" \
        -p textLength=16777216
    run_self_loop_paired_protected_paths "${label}"
    BENCHMARK_FORKS=2 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaSampledSelfLoopSearch\.search$' \
        "${RESULT_DIR}/${label}-state-changing-compact.json" \
        -p textLength=16777216
}

run_self_loop_paired_protected_paths()
{
    local label=$1
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Hard|Parens)Dfa$' \
        "${RESULT_DIR}/${label}-paired-protected.json" \
        -p textSize=16777216
}

set_self_loop_transition_reuse_enabled()
{
    local enabled=$1
    local dfa_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/Dfa.java"
    local candidate_selector='        return !pairedSearch && endMatch && remainingBytes >= MIN_SELF_LOOP_SAMPLE_SEARCH_BYTES;'
    local control_selector='        return false;'

    if [[ "${enabled}" == true ]]; then
        if [[ $(grep -Fxc "${candidate_selector}" "${dfa_source}" || true) -ne 1 ]]; then
            echo "Self-loop transition sampling is not present in the candidate source" >&2
            exit 1
        fi
        return
    fi
    if [[ $(grep -Fxc "${candidate_selector}" "${dfa_source}" || true) -ne 1 ||
            $(grep -Fxc "${control_selector}" "${dfa_source}" || true) -ne 0 ]]; then
        echo "Unable to disable self-loop transition sampling: expected source not found" >&2
        exit 1
    fi
    sed -i "s#${candidate_selector}#${control_selector}#" "${dfa_source}"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

run_self_loop_source_comparison()
{
    local workload=$1
    local candidate_source="${RESULT_DIR}/candidate-Dfa.java"
    cp src/main/java/io/airlift/slice/re2/Dfa.java "${candidate_source}"
    sha256sum "${candidate_source}" > "${RESULT_DIR}/candidate-source-sha256.txt"
    restore_candidate()
    {
        cp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    }
    trap restore_candidate EXIT

    case "${workload}" in
        final) run_self_loop_final_path candidate-before ;;
        paired-protected) run_self_loop_paired_protected_paths candidate-before ;;
        protected) run_self_loop_protected_paths candidate-before ;;
        rebar) BENCHMARK_FORKS=2 run_rebar_pairing_corpus candidate-before false NO_QUADRATIC,KEYWORDS,URL ;;
        *) echo "Unknown self-loop comparison workload: ${workload}" >&2; exit 1 ;;
    esac

    set_self_loop_transition_reuse_enabled false
    diff -u "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]
    case "${workload}" in
        final) run_self_loop_final_path control ;;
        paired-protected) run_self_loop_paired_protected_paths control ;;
        protected) run_self_loop_protected_paths control ;;
        rebar) BENCHMARK_FORKS=2 run_rebar_pairing_corpus control false NO_QUADRATIC,KEYWORDS,URL ;;
    esac

    restore_candidate
    cmp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
    sha256sum src/main/java/io/airlift/slice/re2/Dfa.java >> "${RESULT_DIR}/candidate-source-sha256.txt"
    trap - EXIT
    case "${workload}" in
        final) run_self_loop_final_path candidate-after ;;
        paired-protected) run_self_loop_paired_protected_paths candidate-after ;;
        protected) run_self_loop_protected_paths candidate-after ;;
        rebar) BENCHMARK_FORKS=2 run_rebar_pairing_corpus candidate-after false NO_QUADRATIC,KEYWORDS,URL ;;
    esac

    if [[ "${workload}" == final ]]; then
        RE2_BENCHMARK_BUILD_DIR="${SLICE_DIR}/target/re2-benchmark-build-host" \
            RE2_NATIVE_TUNING=host \
            tools/re2-benchmark/build.sh | tee "${RESULT_DIR}/native-host-build.log"
        taskset --cpu-list 0 \
            "${SLICE_DIR}/target/re2-benchmark-build-host/regexp_benchmark" \
            --benchmark_filter='^Search_SelfLoopSuffixNoMatch_CachedDFA/16777216/threads:1$' \
            --benchmark_min_time=0.5s \
            --benchmark_repetitions=5 \
            --benchmark_out="${RESULT_DIR}/native-self-loop-host.json" \
            --benchmark_out_format=json
    fi
}

set_all_pairing_enabled()
{
    local enabled=$1
    local dfa_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/Dfa.java"
    local maximum_rows='            int maximumRows = (int) Math.min(stateCount, (maximumMemory - outerMemory) / rowMemory);'
    local disabled_gate='            if (maximumRows <= stateCount) {'
    local gate_count
    local maximum_rows_line
    gate_count=$(grep -Fxc "${disabled_gate}" "${dfa_source}" || true)

    if [[ "${enabled}" == true ]]; then
        if [[ ${gate_count} -ne 1 ]]; then
            echo "Unable to enable paired transitions: disabled gate not found" >&2
            exit 1
        fi
        sed -i '/^            if (maximumRows <= stateCount) {$/,+2d' "${dfa_source}"
    else
        if [[ ${gate_count} -ne 0 || $(grep -Fxc "${maximum_rows}" "${dfa_source}") -ne 1 ]]; then
            echo "Unable to disable paired transitions: expected source not found" >&2
            exit 1
        fi
        maximum_rows_line=$(grep -Fnx "${maximum_rows}" "${dfa_source}" | cut -d: -f1)
        sed -i "${maximum_rows_line}a\\
            if (maximumRows <= stateCount) {\\
                return;\\
            }" "${dfa_source}"
    fi
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

if [[ "${BENCHMARK_MODE}" == start-byte ]]; then
    run_start_byte_target candidate-before
    run_start_byte_compile_gates candidate
    run_start_byte_protected_gates candidate

    cp src/main/java/io/airlift/slice/re2/Dfa.java "${RESULT_DIR}/candidate-Dfa.java"
    set_start_byte_acceleration_enabled false
    diff -u "${RESULT_DIR}/candidate-Dfa.java" src/main/java/io/airlift/slice/re2/Dfa.java \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]
    rm "${RESULT_DIR}/candidate-Dfa.java"

    run_start_byte_target disabled
    run_start_byte_compile_gates disabled
    run_start_byte_protected_gates disabled

    set_start_byte_acceleration_enabled true
    run_start_byte_target candidate-after
    exit 0
fi

set_fixed_distance_enabled()
{
    local enabled=$1
    local dfa_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/Dfa.java"
    local enabled_expression='? dfa.fixedDistanceByteCandidates() : null;'
    local disabled_expression='? null : null;'
    local expected_expression
    local replacement_expression

    if [[ "${enabled}" == true ]]; then
        expected_expression=${disabled_expression}
        replacement_expression=${enabled_expression}
    else
        expected_expression=${enabled_expression}
        replacement_expression=${disabled_expression}
    fi

    if [[ $(grep -Fc "${expected_expression}" "${dfa_source}") -ne 1 ]]; then
        echo "Unable to set fixed-distance acceleration enabled=${enabled}: expected source not found" >&2
        exit 1
    fi
    sed -i "s|${expected_expression}|${replacement_expression}|" "${dfa_source}"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

run_fixed_distance_target()
{
    local label=$1
    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaFixedDistanceByte' \
        "${RESULT_DIR}/${label}-fixed-distance.json" \
        -p sourceLength=32768
}

if [[ "${BENCHMARK_MODE}" == fixed-distance ]]; then
    run_fixed_distance_target candidate-before

    cp src/main/java/io/airlift/slice/re2/Dfa.java "${RESULT_DIR}/candidate-Dfa.java"
    set_fixed_distance_enabled false
    diff -u "${RESULT_DIR}/candidate-Dfa.java" src/main/java/io/airlift/slice/re2/Dfa.java \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]
    rm "${RESULT_DIR}/candidate-Dfa.java"

    run_fixed_distance_target disabled
    set_fixed_distance_enabled true
    run_fixed_distance_target candidate-after
    exit 0
fi

set_nullable_repeat_cursor_enabled()
{
    local enabled=$1
    local trino_regexp_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/TrinoRegexp.java"
    local enabled_eligibility='        this.mayHaveSingleByteRepeatMatcher = pattern.canMatchEmpty();'
    local disabled_eligibility='        this.mayHaveSingleByteRepeatMatcher = false;'
    local enabled_position_guard='        if (codePointStart > source.length()) {'
    local disabled_position_guard='        if (codePointStart > SliceUtf8.countCodePoints(source)) {'
    local expected_eligibility
    local replacement_eligibility

    if [[ "${enabled}" == true ]]; then
        expected_eligibility=${disabled_eligibility}
        replacement_eligibility=${enabled_eligibility}
        if [[ $(grep -Fxc "${disabled_position_guard}" "${trino_regexp_source}") -ne 1 ]]; then
            echo "Unable to enable nullable-repeat cursor: position control not found" >&2
            exit 1
        fi
        sed -i "s|${disabled_position_guard}|${enabled_position_guard}|" "${trino_regexp_source}"
    else
        expected_eligibility=${enabled_eligibility}
        replacement_eligibility=${disabled_eligibility}
        if [[ $(grep -Fxc "${enabled_position_guard}" "${trino_regexp_source}") -ne 1 ]]; then
            echo "Unable to disable nullable-repeat cursor: position source not found" >&2
            exit 1
        fi
        sed -i "s|${enabled_position_guard}|${disabled_position_guard}|" "${trino_regexp_source}"
    fi

    if [[ $(grep -Fxc "${expected_eligibility}" "${trino_regexp_source}") -ne 1 ]]; then
        echo "Unable to set nullable-repeat cursor enabled=${enabled}: expected eligibility source not found" >&2
        exit 1
    fi
    sed -i "s|${expected_eligibility}|${replacement_eligibility}|" "${trino_regexp_source}"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

run_nullable_repeat_operations()
{
    local label=$1
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.(positionThird|extractAll|split|replace|replaceLambda)$' \
        "${RESULT_DIR}/${label}-nullable-repeat-operations.json" \
        -p workload=emptyMatches,delimiterDense \
        -p sourceLength=32768
}

if [[ "${BENCHMARK_MODE}" == nullable-repeat ]]; then
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkSingleByteRepeatMatcher' \
        "${RESULT_DIR}/slice-nullable-repeat-control.json"
    run_nullable_repeat_operations candidate-before

    cp src/main/java/io/airlift/slice/re2/TrinoRegexp.java "${RESULT_DIR}/candidate-TrinoRegexp.java"
    set_nullable_repeat_cursor_enabled false
    diff -u "${RESULT_DIR}/candidate-TrinoRegexp.java" src/main/java/io/airlift/slice/re2/TrinoRegexp.java \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]

    run_nullable_repeat_operations disabled
    set_nullable_repeat_cursor_enabled true
    cmp "${RESULT_DIR}/candidate-TrinoRegexp.java" src/main/java/io/airlift/slice/re2/TrinoRegexp.java
    rm "${RESULT_DIR}/candidate-TrinoRegexp.java"
    run_nullable_repeat_operations candidate-after
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == capture-count ]]; then
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaCountMatches' \
        "${RESULT_DIR}/slice-count-session.json"
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkBoundedCharacterClassCounter' \
        "${RESULT_DIR}/slice-bounded-character-class.json"
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Parse\.(parse3DigitsRe2|parse3DigitDsRe2|parse1SplitRe2|parseSplitHardRe2)$' \
        "${RESULT_DIR}/slice-capture-controls.json"
    exit 0
fi

disable_group_zero_matcher()
{
    local trino_regexp_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/TrinoRegexp.java"
    local direct_candidate='        Re2Matcher matcher = newGroupZeroMatcher(source);'
    local direct_control='        Re2Matcher matcher = newMatcher(source);'
    local selected_candidate='        Re2Matcher matcher = group == 0 ? newGroupZeroMatcher(source) : newMatcher(source);'
    local selected_control='        Re2Matcher matcher = newMatcher(source);'
    local replacement_candidate='        Re2Matcher matcher = needsCapturingGroups ? newMatcher(source) : newGroupZeroMatcher(source);'

    if [[ $(grep -Fxc "${direct_candidate}" "${trino_regexp_source}") -ne 3 ||
            $(grep -Fxc "${selected_candidate}" "${trino_regexp_source}") -ne 2 ||
            $(grep -Fxc "${replacement_candidate}" "${trino_regexp_source}") -ne 1 ]]; then
        echo "Unable to disable group-zero matcher: expected sources not found" >&2
        exit 1
    fi
    sed -i "s|${direct_candidate}|${direct_control}|g; s|${selected_candidate}|${selected_control}|g; s|${replacement_candidate}|${direct_control}|g" "${trino_regexp_source}"
    ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
}

run_group_zero_operations()
{
    local label=$1
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.(positionThird|extract|extractAll|split|replace)$' \
        "${RESULT_DIR}/${label}-group-zero-operations.json" \
        -p workload=unicodeSparse,captureSparse,delimiterDense \
        -p sourceLength=32768

    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp\.(positionThird|extract|replace)$' \
        "${RESULT_DIR}/${label}-group-zero-unicode-short.json" \
        -p workload=unicodeSparse \
        -p sourceLength=1024

    BENCHMARK_FORKS=3 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexpSearchEdges\.(extract|replace)$' \
        "${RESULT_DIR}/${label}-group-zero-search-edges.json" \
        -p workload=captureNoMatch,captureLateMatch \
        -p sourceLength=1024,32768
}

run_absolute_pointer_integrated()
{
    local label=$1
    local native_access=$2
    BENCHMARK_FORKS=3 BENCHMARK_NATIVE_ACCESS="${native_access}" BENCHMARK_DENY_NATIVE_ACCESS=true run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaSampledSelfLoopSearch\.search$' \
        "${RESULT_DIR}/${label}-state-changing.json" \
        -p textLength=16777216
    BENCHMARK_FORKS=2 BENCHMARK_NATIVE_ACCESS="${native_access}" BENCHMARK_DENY_NATIVE_ACCESS=true run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Hard|Parens)Dfa$' \
        "${RESULT_DIR}/${label}-paired-protected.json" \
        -p textSize=16777216
}

if [[ "${BENCHMARK_MODE}" == dfa-absolute-pointer-integrated ]]; then
    run_absolute_pointer_integrated candidate-before true
    run_absolute_pointer_integrated object-control false
    run_absolute_pointer_integrated candidate-after true

    RE2_BENCHMARK_BUILD_DIR="${SLICE_DIR}/target/re2-benchmark-build-host" \
        RE2_NATIVE_TUNING=host \
        tools/re2-benchmark/build.sh | tee "${RESULT_DIR}/native-host-build.log"
    taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build-host/regexp_benchmark" \
        --benchmark_filter='^Search_StateChangingNoMatch_CachedDFA/16777216/threads:1$' \
        --benchmark_min_time=0.5s \
        --benchmark_repetitions=5 \
        --benchmark_out="${RESULT_DIR}/native-state-changing.json" \
        --benchmark_out_format=json

    sudo sysctl -w kernel.perf_event_paranoid=-1
    sudo sysctl -w kernel.kptr_restrict=0
    taskset --cpu-list 0 \
        java --enable-native-access=ALL-UNNAMED --illegal-native-access=deny --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main 'BenchmarkDfaSampledSelfLoopSearch\.search$' \
        -p textLength=16777216 \
        -f 1 \
        -wi 15 \
        -i 7 \
        -w 500ms \
        -r 500ms \
        -prof "perfasm:events=cycles;hotThreshold=0.02;top=10;printMargin=32;savePerf=true;savePerfTo=${RESULT_DIR};savePerfBin=true;savePerfBinTo=${RESULT_DIR};saveLog=true;saveLogTo=${RESULT_DIR}" \
        -jvmArgsAppend '--enable-native-access=ALL-UNNAMED --illegal-native-access=deny --add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch -XX:CompileCommand=print,io.airlift.slice.re2.Dfa::searchForwardAbsolutePointers' \
        -rf json \
        -rff "${RESULT_DIR}/candidate-perfasm.json" \
        | tee "${RESULT_DIR}/candidate-perfasm.log"
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == group-zero ]]; then
    local_source="${SLICE_DIR}/src/main/java/io/airlift/slice/re2/TrinoRegexp.java"
    candidate_source="${RESULT_DIR}/candidate-TrinoRegexp.java"
    cp "${local_source}" "${candidate_source}"

    restore_group_zero_source()
    {
        cp "${candidate_source}" "${local_source}"
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    }
    trap restore_group_zero_source EXIT

    run_group_zero_operations candidate-before
    disable_group_zero_matcher
    diff -u "${candidate_source}" "${local_source}" \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]
    run_group_zero_operations disabled

    restore_group_zero_source
    cmp "${candidate_source}" "${local_source}"
    trap - EXIT
    run_group_zero_operations candidate-after
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == targeted ]]; then
    run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkTrinoRegexp' \
        "${RESULT_DIR}/slice-trino-operations.json"
    run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2CompileFocused.re2CompileTotal' \
        "${RESULT_DIR}/slice-compile.json"
    run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2CompileFocused.re2CompileTotal' \
        "${RESULT_DIR}/slice-character-class-compile.json" \
        -p 'pattern=[;x]'
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-paired-corpus ]]; then
    compact_workloads=LEIPZIG,KEYWORDS,URL,NO_QUADRATIC
    complete_workloads=WORD_BOUNDARY,WORD_ENDING,ANY_CODE_POINT,BOUNDED_ENDING,AROUND_HOLMES,LINE_BOUNDARY

    run_rebar_pairing_corpus candidate-before-compact false "${compact_workloads}"
    run_rebar_pairing_corpus candidate-before-complete true "${complete_workloads}"

    candidate_source="${RESULT_DIR}/candidate-Dfa.java"
    cp src/main/java/io/airlift/slice/re2/Dfa.java "${candidate_source}"
    sha256sum "${candidate_source}" > "${RESULT_DIR}/candidate-source-sha256.txt"
    restore_candidate()
    {
        cp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    }
    trap restore_candidate EXIT

    set_all_pairing_enabled false
    diff -u "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]
    run_rebar_pairing_corpus control-compact false "${compact_workloads}"
    run_rebar_pairing_corpus control-complete false "${complete_workloads}"

    restore_candidate
    cmp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
    sha256sum src/main/java/io/airlift/slice/re2/Dfa.java >> "${RESULT_DIR}/candidate-source-sha256.txt"
    trap - EXIT
    run_rebar_pairing_corpus candidate-after-compact false "${compact_workloads}"
    run_rebar_pairing_corpus candidate-after-complete true "${complete_workloads}"
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-self-loop-final ]]; then
    run_self_loop_source_comparison final
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-self-loop-paired-protected ]]; then
    run_self_loop_source_comparison paired-protected
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-self-loop-protected ]]; then
    run_self_loop_source_comparison protected
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-self-loop-rebar ]]; then
    run_self_loop_source_comparison rebar
    exit 0
fi

tools/re2-benchmark/build.sh | tee "${RESULT_DIR}/native-build.log"

if [[ "${BENCHMARK_MODE}" == dfa-partial-candidate ]]; then
    run_partial_pairing_target candidate-before true
    run_partial_pairing_protected_gates candidate
    run_rebar_pairing_corpus candidate-before false LEIPZIG,KEYWORDS,URL,NO_QUADRATIC

    candidate_source="${RESULT_DIR}/candidate-Dfa.java"
    cp src/main/java/io/airlift/slice/re2/Dfa.java "${candidate_source}"
    sha256sum "${candidate_source}" > "${RESULT_DIR}/candidate-source-sha256.txt"
    restore_candidate()
    {
        cp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
        ./mvnw "${MAVEN_SNAPSHOT_ARGS[@]}" -q test-compile
    }
    trap restore_candidate EXIT

    set_partial_pairing_enabled false
    diff -u "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java \
        > "${RESULT_DIR}/disabled-control.patch" || [[ $? -eq 1 ]]

    run_partial_pairing_target control false
    run_partial_pairing_protected_gates control
    run_rebar_pairing_corpus control false LEIPZIG,KEYWORDS,URL,NO_QUADRATIC

    restore_candidate
    cmp "${candidate_source}" src/main/java/io/airlift/slice/re2/Dfa.java
    sha256sum src/main/java/io/airlift/slice/re2/Dfa.java >> "${RESULT_DIR}/candidate-source-sha256.txt"
    trap - EXIT
    run_partial_pairing_target candidate-after true
    run_rebar_pairing_corpus candidate-after false LEIPZIG,KEYWORDS,URL,NO_QUADRATIC

    taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
        --benchmark_filter='^Search_HardTail(Match|NoMatch)_CachedDFA/(32768|1048576|16777216)/threads:1$' \
        --benchmark_min_time=0.5s \
        --benchmark_repetitions=5 \
        --benchmark_out="${RESULT_DIR}/native-generic-tail.json" \
        --benchmark_out_format=json

    RE2_BENCHMARK_BUILD_DIR="${SLICE_DIR}/target/re2-benchmark-build-host" \
        RE2_NATIVE_TUNING=host \
        tools/re2-benchmark/build.sh | tee "${RESULT_DIR}/native-host-build.log"
    taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build-host/regexp_benchmark" \
        --benchmark_filter='^Search_HardTail(Match|NoMatch)_CachedDFA/(32768|1048576|16777216)/threads:1$' \
        --benchmark_min_time=0.5s \
        --benchmark_repetitions=5 \
        --benchmark_out="${RESULT_DIR}/native-host-tail.json" \
        --benchmark_out_format=json
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-pair-candidate ]]; then
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Easy0|Easy1|Hard|Parens)Dfa$' \
        "${RESULT_DIR}/slice-dfa-protected.json" \
        -p textSize=16777216

    taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
        --benchmark_filter='^Search_(Hard|Parens)_CachedDFA/16777216/threads:1$' \
        --benchmark_min_time=0.5s \
        --benchmark_repetitions=5 \
        --benchmark_out="${RESULT_DIR}/native-dfa.json" \
        --benchmark_out_format=json
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == historical-dfa ]]; then
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Hard|Parens)Dfa$' \
        "${RESULT_DIR}/slice-dfa-baseline.json" \
        -p textSize=16777216

    sudo sysctl -w kernel.perf_event_paranoid=-1
    sudo sysctl -w kernel.kptr_restrict=0

    taskset --cpu-list 0 \
        java --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main 'BenchmarkRe2Search\.searchHardDfa$' \
        -p textSize=16777216 \
        -f 1 \
        -wi 15 \
        -i 7 \
        -w 500ms \
        -r 500ms \
        -prof 'perf:events=cycles,instructions,branches,branch-misses' \
        -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch' \
        -rf json \
        -rff "${RESULT_DIR}/slice-dfa-counters.json" \
        | tee "${RESULT_DIR}/slice-dfa-counters.log"

    taskset --cpu-list 0 \
        java --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main 'BenchmarkRe2Search\.searchHardDfa$' \
        -p textSize=16777216 \
        -f 1 \
        -wi 15 \
        -i 7 \
        -w 500ms \
        -r 500ms \
        -prof "perfasm:events=cycles;hotThreshold=0.02;top=10;printMargin=32;savePerf=true;savePerfTo=${RESULT_DIR};savePerfBin=true;savePerfBinTo=${RESULT_DIR};saveLog=true;saveLogTo=${RESULT_DIR}" \
        -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch -XX:CompileCommand=print,io.airlift.slice.re2.Dfa::searchForward' \
        -rf json \
        -rff "${RESULT_DIR}/slice-dfa-perfasm.json" \
        | tee "${RESULT_DIR}/slice-dfa-perfasm.log"

    taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
        --benchmark_filter='^Search_(Hard|Parens)_CachedDFA/16777216/threads:1$' \
        --benchmark_min_time=0.5s \
        --benchmark_repetitions=5 \
        --benchmark_out="${RESULT_DIR}/native-dfa.json" \
        --benchmark_out_format=json
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-pair-diagnostic ]]; then
    sudo sysctl -w kernel.perf_event_paranoid=-1
    sudo sysctl -w kernel.kptr_restrict=0

    for layout in compactObjectReferences pairedObjectReferences; do
        taskset --cpu-list 0 \
            java --add-modules jdk.incubator.vector \
            -cp "${SLICE_CLASSPATH}" \
            org.openjdk.jmh.Main "BenchmarkDfaRealTransitionLayout\\.${layout}$" \
            -p pattern=HARD \
            -f 1 \
            -wi 15 \
            -i 7 \
            -w 500ms \
            -r 500ms \
            -prof 'perf:events=cycles,instructions,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses' \
            -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch' \
            -rf json \
            -rff "${RESULT_DIR}/${layout}-counters.json" \
            | tee "${RESULT_DIR}/${layout}-counters.log"

        taskset --cpu-list 0 \
            java --add-modules jdk.incubator.vector \
            -cp "${SLICE_CLASSPATH}" \
            org.openjdk.jmh.Main "BenchmarkDfaRealTransitionLayout\\.${layout}$" \
            -p pattern=HARD \
            -f 1 \
            -wi 15 \
            -i 7 \
            -w 500ms \
            -r 500ms \
            -prof "perfasm:events=cycles;hotThreshold=0.02;top=10;printMargin=32;savePerf=true;savePerfTo=${RESULT_DIR};savePerfBin=true;savePerfBinTo=${RESULT_DIR};saveLog=true;saveLogTo=${RESULT_DIR}" \
            -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch' \
            -rf json \
            -rff "${RESULT_DIR}/${layout}-perfasm.json" \
            | tee "${RESULT_DIR}/${layout}-perfasm.log"
    done

    sudo perf record -e cycles:u -o "${RESULT_DIR}/native-hard.perf" -- \
        taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
        --benchmark_filter='^Search_Hard_CachedDFA/16777216/threads:1$' \
        --benchmark_min_time=2s \
        --benchmark_repetitions=1
    sudo perf report --stdio -i "${RESULT_DIR}/native-hard.perf" > "${RESULT_DIR}/native-hard-perf-report.txt"
    objdump -d -C "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" > "${RESULT_DIR}/native-hard-disassembly.txt"
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-layout ]]; then
    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaTransitionLayout\.(directObjectReferences|objectReferences|pairedFlatIntegers|pairedObjectReferences)$' \
        "${RESULT_DIR}/slice-dfa-layout.json" \
        -p classCount=29

    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkDfaRealTransitionLayout\.(compactObjectReferences|pairedObjectReferences)$' \
        "${RESULT_DIR}/slice-dfa-real-layout.json" \
        -p pattern=HARD,PARENS

    BENCHMARK_FORKS=5 run_jmh \
        "${SLICE_CLASSPATH}" \
        'BenchmarkRe2Search\.search(Hard|Parens)Dfa$' \
        "${RESULT_DIR}/slice-dfa-baseline.json" \
        -p textSize=16777216

    taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
        --benchmark_filter='^Search_(Hard|Parens)_CachedDFA/16777216/threads:1$' \
        --benchmark_min_time=0.5s \
        --benchmark_repetitions=5 \
        --benchmark_out="${RESULT_DIR}/native-dfa.json" \
        --benchmark_out_format=json
    exit 0
fi

if [[ "${BENCHMARK_MODE}" == dfa-diagnostic ]]; then
    sudo sysctl -w kernel.perf_event_paranoid=-1
    sudo sysctl -w kernel.kptr_restrict=0

    taskset --cpu-list 0 \
        java --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main 'BenchmarkRe2Search.search(Hard|Parens)Dfa' \
        -p textSize=16777216 \
        -f 10 \
        -wi 10 \
        -i 5 \
        -w 300ms \
        -r 300ms \
        -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch' \
        -rf json \
        -rff "${RESULT_DIR}/slice-dfa-multifork.json" \
        | tee "${RESULT_DIR}/slice-dfa-multifork.log"

    taskset --cpu-list 0 \
        java --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main 'BenchmarkRe2Search.searchHardDfa' \
        -p textSize=16777216 \
        -f 1 \
        -wi 15 \
        -i 7 \
        -w 500ms \
        -r 500ms \
        -prof 'perf:events=cycles,instructions,branches,branch-misses' \
        -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch' \
        -rf json \
        -rff "${RESULT_DIR}/slice-dfa-counters.json" \
        | tee "${RESULT_DIR}/slice-dfa-counters.log"

    taskset --cpu-list 0 \
        java --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main 'BenchmarkRe2Search.searchHardDfa' \
        -p textSize=16777216 \
        -f 1 \
        -wi 15 \
        -i 7 \
        -w 500ms \
        -r 500ms \
        -prof "perfasm:events=cycles;hotThreshold=0.02;top=10;printMargin=32;savePerf=true;savePerfTo=${RESULT_DIR};savePerfBin=true;savePerfBinTo=${RESULT_DIR};saveLog=true;saveLogTo=${RESULT_DIR}" \
        -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch -XX:CompileCommand=print,io.airlift.slice.re2.Dfa::searchForward' \
        -rf json \
        -rff "${RESULT_DIR}/slice-dfa-perfasm.json" \
        | tee "${RESULT_DIR}/slice-dfa-perfasm.log"

    taskset --cpu-list 0 \
        java --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main 'BenchmarkRe2Search.searchHardDfa' \
        -p textSize=16777216 \
        -f 5 \
        -wi 10 \
        -i 5 \
        -w 300ms \
        -r 300ms \
        -jvmArgsAppend '--add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch -XX:-UseCompressedOops' \
        -rf json \
        -rff "${RESULT_DIR}/slice-dfa-uncompressed.json" \
        | tee "${RESULT_DIR}/slice-dfa-uncompressed.log"

    taskset --cpu-list 0 \
        "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
        --benchmark_filter='^Search_(Hard|Parens)_CachedDFA/16777216/threads:1$' \
        --benchmark_min_time=0.5s \
        --benchmark_repetitions=5 \
        --benchmark_out="${RESULT_DIR}/native-dfa.json" \
        --benchmark_out_format=json
    exit 0
fi

qualification_suites=(dfa big-fixed public-api trino shared-one shared-all cold-wave)
mapfile -t qualification_suites < <(printf '%s\n' "${qualification_suites[@]}" | shuf)
printf '%s\n' "${qualification_suites[@]}" > "${RESULT_DIR}/qualification-suite-order.txt"
for qualification_suite in "${qualification_suites[@]}"; do
    case "${qualification_suite}" in
        dfa)
            run_native_qualification_jmh \
                "${SLICE_CLASSPATH}" \
                'BenchmarkRe2Search.search(Easy0|Easy1|Hard|Parens)Dfa' \
                "${RESULT_DIR}/slice-dfa.json"
            ;;
        big-fixed)
            run_native_qualification_jmh \
                "${SLICE_CLASSPATH}" \
                'BenchmarkRe2SearchExtra.searchBigFixed(Dfa|Re2)' \
                "${RESULT_DIR}/slice-big-fixed.json"
            ;;
        public-api)
            run_native_qualification_jmh \
                "${SLICE_CLASSPATH}" \
                'BenchmarkRe2PublicApi' \
                "${RESULT_DIR}/slice-public-api.json"
            ;;
        trino)
            run_native_qualification_jmh \
                "${SLICE_CLASSPATH}" \
                'BenchmarkTrinoRegexp' \
                "${RESULT_DIR}/slice-trino-operations.json"
            ;;
        shared-one)
            run_native_qualification_jmh \
                "${SLICE_CLASSPATH}" \
                'BenchmarkDfaCache.searchSharedWarm' \
                "${RESULT_DIR}/slice-dfa-shared-1.json" \
                -t 1
            ;;
        shared-all)
            BENCHMARK_CPU_LIST="${PHYSICAL_CPU_LIST}" run_native_qualification_jmh \
                "${SLICE_CLASSPATH}" \
                'BenchmarkDfaCache.searchSharedWarm' \
                "${RESULT_DIR}/slice-dfa-shared-all-cores.json" \
                -t "${PHYSICAL_CORE_COUNT}"
            ;;
        cold-wave)
            BENCHMARK_CPU_LIST="${PHYSICAL_CPU_LIST}" run_native_qualification_jmh \
                "${SLICE_CLASSPATH}" \
                'BenchmarkDfaCache.searchSharedColdWave' \
                "${RESULT_DIR}/slice-dfa-cold-wave.json"
            ;;
    esac
done

taskset --cpu-list 0 \
    java --enable-native-access=ALL-UNNAMED --illegal-native-access=deny --add-modules jdk.incubator.vector \
        -cp "${SLICE_CLASSPATH}" \
        org.openjdk.jmh.Main BenchmarkRe2PublicApi \
    -f 5 \
    -wi 10 \
    -i 10 \
    -w 1s \
    -r 1s \
    -prof gc \
    -jvmArgsAppend "--enable-native-access=ALL-UNNAMED --illegal-native-access=deny --add-modules=jdk.incubator.vector -Xms8g -Xmx8g -XX:+AlwaysPreTouch" \
    -rf json \
    -rff "${RESULT_DIR}/slice-public-api-allocation.json"

taskset --cpu-list "${PHYSICAL_CPU_LIST}" \
    "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
    --benchmark_filter="^Search_(Easy0|Easy1|Hard|Parens)_CachedDFA/.*/threads:(1|${PHYSICAL_CORE_COUNT})$" \
    --benchmark_min_time=1s \
    --benchmark_repetitions=5 \
    --benchmark_out="${RESULT_DIR}/native-dfa.json" \
    --benchmark_out_format=json

taskset --cpu-list 0 \
    "${SLICE_DIR}/target/re2-benchmark-build/regexp_benchmark" \
    --benchmark_filter='^Search_BigFixed_Cached(DFA|RE2)/.*/threads:1$' \
    --benchmark_min_time=1s \
    --benchmark_repetitions=5 \
    --benchmark_out="${RESULT_DIR}/native-big-fixed.json" \
    --benchmark_out_format=json

run_trino_comparator
