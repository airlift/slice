#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SLICE_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)
TRINO_DIR=${TRINO_DIR:-/Users/dain/.codex/worktrees/trino-re2-benchmark}
AWS_PROFILE=${AWS_PROFILE:-dev}
AWS_REGION=${AWS_REGION:-us-west-2}
INTEL_INSTANCE_TYPE=${INTEL_INSTANCE_TYPE:-c8i.8xlarge}
ARM_INSTANCE_TYPE=${ARM_INSTANCE_TYPE:-c8g.4xlarge}
INSTANCE_MARKET_TYPE=${INSTANCE_MARKET_TYPE:-on-demand}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-10800}
RESULT_ROOT=${RESULT_ROOT:-"${SLICE_DIR}/benchmark-results/re2-engineering"}
HSDIS_CACHE_DIR=${HSDIS_CACHE_DIR:-"${SLICE_DIR}/target/re2-hsdis"}
CAMPAIGN_MODE=${CAMPAIGN_MODE:-full}
CAMPAIGN_ARCHITECTURES=${CAMPAIGN_ARCHITECTURES:-intel,arm}
CAPTURE_SHARD=${CAPTURE_SHARD:-direct}
CAPTURE_CONTROL=${CAPTURE_CONTROL:-instruction-scan}
CAPTURE_ENGINE=${CAPTURE_ENGINE:-nfa}
CAPTURE_GUARD_SIZES=${CAPTURE_GUARD_SIZES:-512,4096}
CAPTURE_PIPELINE_WORKLOAD=${CAPTURE_PIPELINE_WORKLOAD:-}
CAPTURE_PIPELINE_STAGES=${CAPTURE_PIPELINE_STAGES:-control,setup,forward,reverse,capture,composed,result}
CAPTURE_PIPELINE_NATIVE_DIAGNOSTICS=${CAPTURE_PIPELINE_NATIVE_DIAGNOSTICS:-false}
CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS=${CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS:-false}
CAPTURE_PIPELINE_ITERATIONS=${CAPTURE_PIPELINE_ITERATIONS:-20}
CAPTURE_PIPELINE_WARMUP_ITERATIONS=${CAPTURE_PIPELINE_WARMUP_ITERATIONS:-20}
COUNT_PIPELINE_MEMORY_MEGABYTES=${COUNT_PIPELINE_MEMORY_MEGABYTES:-8,12,16,24,32,48,64,96,128}
COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES=${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES:-8,32,64}
BENCHMARK_JAVA_ARCHIVE_URL=${BENCHMARK_JAVA_ARCHIVE_URL:-}
BENCHMARK_JAVA_ARCHIVE_SHA256=${BENCHMARK_JAVA_ARCHIVE_SHA256:-}
REBAR_CORPUS_DIR=${REBAR_CORPUS_DIR:-"${SLICE_DIR}/target/rebar-selected"}
REBAR_OFFICIAL_DIR=${REBAR_OFFICIAL_DIR:-"${SLICE_DIR}/target/rebar-corpus"}
REBAR_COMPARATOR_ORDER=${REBAR_COMPARATOR_ORDER:-forward}
REBAR_MODEL_FILTER=${REBAR_MODEL_FILTER:-'^(?:compile|count|count-spans|count-captures|grep|grep-captures)$'}
REBAR_ALLOW_NATIVE_DRIFT=${REBAR_ALLOW_NATIVE_DRIFT:-false}
REBAR_EXCLUDE_NATIVE_DRIFT=${REBAR_EXCLUDE_NATIVE_DRIFT:-false}
JONI_MEMORY_CONTEXT_DIR=${JONI_MEMORY_CONTEXT_DIR:-"${SLICE_DIR}/benchmark-results/re2-engineering/20260718T192636Z-67046/intel/re2-results/context"}
DEFAULT_DFA_LAYOUT_FILTER='BenchmarkDfaCompactTransitionLayout\.(objectReferences|exactStrideHeapIntegers|powerOfTwoStateIds|preShiftedHeapRowIndexes|foreign64BitRowOffsets|foreign32BitRowOffsets)$'
DFA_LAYOUT_FILTER=${DFA_LAYOUT_FILTER:-${DEFAULT_DFA_LAYOUT_FILTER}}
DFA_LAYOUT_STATE_COUNTS=${DFA_LAYOUT_STATE_COUNTS:-32,128,512,2048}
DFA_LAYOUT_CLASS_COUNTS=${DFA_LAYOUT_CLASS_COUNTS:-8,29,64}
DFA_LAYOUT_FORKS=${DFA_LAYOUT_FORKS:-5}
DFA_LAYOUT_RUN_NATIVE=${DFA_LAYOUT_RUN_NATIVE:-false}
DEFAULT_DFA_REAL_LAYOUT_FILTER='BenchmarkDfaRealTransitionLayout\.(compactObjectReferences|selfLoopObjectReferences|selfLoopFirstObjectReferences)$'
DFA_REAL_LAYOUT_FILTER=${DFA_REAL_LAYOUT_FILTER:-${DEFAULT_DFA_REAL_LAYOUT_FILTER}}
DFA_REAL_LAYOUT_PATTERNS=${DFA_REAL_LAYOUT_PATTERNS:-HARD,PARENS}
DFA_REAL_LAYOUT_FORKS=${DFA_REAL_LAYOUT_FORKS:-5}
DFA_REAL_LAYOUT_RUN_NATIVE=${DFA_REAL_LAYOUT_RUN_NATIVE:-false}
DFA_LAYOUT_PERFASM_BENCHMARK=${DFA_LAYOUT_PERFASM_BENCHMARK:-BenchmarkDfaCompactTransitionLayout.exactStrideHeapIntegers}
DFA_LAYOUT_PERFASM_STATE_COUNT=${DFA_LAYOUT_PERFASM_STATE_COUNT:-512}
DFA_LAYOUT_PERFASM_CLASS_COUNT=${DFA_LAYOUT_PERFASM_CLASS_COUNT:-29}
DFA_LAYOUT_PERFASM_PATTERN=${DFA_LAYOUT_PERFASM_PATTERN:-HARD}
TRADITIONAL_JMH_FORKS=${TRADITIONAL_JMH_FORKS:-5}
TRADITIONAL_NATIVE_REPETITIONS=${TRADITIONAL_NATIVE_REPETITIONS:-5}
TRADITIONAL_NATIVE_MINIMUM_TIME=${TRADITIONAL_NATIVE_MINIMUM_TIME:-1s}
TRADITIONAL_BENCHMARK_CLASS=${TRADITIONAL_BENCHMARK_CLASS:-}
HSDIS_AMD64_SHA256=2ebd13ca0dd0a3f20c49b99c12b72e376b6c371975f734403048ddf3d7b51507
HSDIS_AARCH64_SHA256=c531ae2f6002987b1d7ee5713a76e51bb54dc3da7b00c8b1214f021abda4dffb

case "${CAMPAIGN_MODE}" in
    full | targeted | trino-comparator | joni-focused | joni-memory | joni-memory-census | bounded-count | byte-scan-fallback | capture-count | capture-engine | capture-pipeline | dfa-absolute-pointer-integrated | dfa-diagnostic | dfa-large-pointer | dfa-layout | dfa-layout-screen | dfa-layout-perfasm | dfa-real-layout-screen | dfa-pair-candidate | dfa-pair-diagnostic | dfa-pair-scaling | dfa-paired-corpus | dfa-partial-candidate | dfa-self-loop-final | dfa-self-loop-paired-protected | dfa-self-loop-protected | dfa-self-loop-rebar | fixed-distance | group-zero | historical-dfa | nullable-repeat | rebar-native-comparison | rebar-official | start-byte | traditional-native-comparison) ;;
    *) echo "Unsupported campaign mode: ${CAMPAIGN_MODE}" >&2; exit 1 ;;
esac
if [[ "${CAMPAIGN_MODE}" == dfa-large-pointer ]]; then
    # This mode qualifies the accepted reset-free 96 MiB graph, not the
    # exploratory cache-capacity sweep used by bounded-count.
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
if [[ "${CAMPAIGN_MODE}" == capture-pipeline && -z "${CAPTURE_PIPELINE_WORKLOAD}" ]]; then
    echo "CAPTURE_PIPELINE_WORKLOAD is required for capture-pipeline" >&2
    exit 1
fi
case "${CAPTURE_PIPELINE_NATIVE_DIAGNOSTICS}:${CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS}" in
    true:true | true:false | false:false) ;;
    false:true) echo "Extended OnePass comparison requires native diagnostics" >&2; exit 1 ;;
    *) echo "Capture-pipeline diagnostic flags must be true or false" >&2; exit 1 ;;
esac
if [[ ! "${CAPTURE_PIPELINE_ITERATIONS}" =~ ^[1-9][0-9]*$ ||
        ! "${CAPTURE_PIPELINE_WARMUP_ITERATIONS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Capture-pipeline iteration counts must be positive integers" >&2
    exit 1
fi
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
case "${CAMPAIGN_ARCHITECTURES}" in
    intel | arm | intel,arm | arm,intel) ;;
    *) echo "CAMPAIGN_ARCHITECTURES must be intel, arm, or both: ${CAMPAIGN_ARCHITECTURES}" >&2; exit 1 ;;
esac
case "${INSTANCE_MARKET_TYPE}" in
    on-demand | spot) ;;
    *) echo "INSTANCE_MARKET_TYPE must be on-demand or spot: ${INSTANCE_MARKET_TYPE}" >&2; exit 1 ;;
esac
case "${REBAR_COMPARATOR_ORDER}" in
    forward | reverse) ;;
    *) echo "REBAR_COMPARATOR_ORDER must be forward or reverse: ${REBAR_COMPARATOR_ORDER}" >&2; exit 1 ;;
esac
case "${TRADITIONAL_BENCHMARK_CLASS}" in
    "" | BenchmarkRe2Search | BenchmarkRe2SearchNfa | BenchmarkRe2SearchExtra | BenchmarkRe2Parse | BenchmarkRe2FullMatch | BenchmarkRe2Practical | BenchmarkRe2Misc) ;;
    *) echo "Unsupported traditional benchmark class: ${TRADITIONAL_BENCHMARK_CLASS}" >&2; exit 1 ;;
esac
if [[ -n "${BENCHMARK_JAVA_ARCHIVE_URL}" || -n "${BENCHMARK_JAVA_ARCHIVE_SHA256}" ]]; then
    if [[ -z "${BENCHMARK_JAVA_ARCHIVE_URL}" || ! "${BENCHMARK_JAVA_ARCHIVE_SHA256}" =~ ^[0-9a-f]{64}$ ]]; then
        echo "BENCHMARK_JAVA_ARCHIVE_URL requires a lowercase SHA-256 checksum" >&2
        exit 1
    fi
fi

includes_architecture()
{
    [[ ",${CAMPAIGN_ARCHITECTURES}," == *",$1,"* ]]
}

case "${DFA_LAYOUT_RUN_NATIVE}" in
    true | false) ;;
    *) echo "DFA_LAYOUT_RUN_NATIVE must be true or false: ${DFA_LAYOUT_RUN_NATIVE}" >&2; exit 1 ;;
esac
case "${DFA_REAL_LAYOUT_RUN_NATIVE}" in
    true | false) ;;
    *) echo "DFA_REAL_LAYOUT_RUN_NATIVE must be true or false: ${DFA_REAL_LAYOUT_RUN_NATIVE}" >&2; exit 1 ;;
esac
if [[ "${CAMPAIGN_MODE}" == dfa-layout-screen ]]; then
    if [[ "${DFA_LAYOUT_FILTER}" != *BenchmarkDfaCompactTransitionLayout* || "${DFA_LAYOUT_FILTER}" != *objectReferences* ]]; then
        echo "DFA_LAYOUT_FILTER must select BenchmarkDfaCompactTransitionLayout and include the objectReferences control" >&2
        exit 1
    fi
    if [[ ! "${DFA_LAYOUT_FORKS}" =~ ^[1-9][0-9]*$ ]]; then
        echo "DFA_LAYOUT_FORKS must be a positive integer: ${DFA_LAYOUT_FORKS}" >&2
        exit 1
    fi
fi
if [[ "${CAMPAIGN_MODE}" == dfa-real-layout-screen ]]; then
    if [[ "${DFA_REAL_LAYOUT_FILTER}" != *BenchmarkDfaRealTransitionLayout* || "${DFA_REAL_LAYOUT_FILTER}" != *compactObjectReferences* ]]; then
        echo "DFA_REAL_LAYOUT_FILTER must select BenchmarkDfaRealTransitionLayout and include the compactObjectReferences control" >&2
        exit 1
    fi
    if [[ ! "${DFA_REAL_LAYOUT_PATTERNS}" =~ ^(HARD|PARENS|SELF_LOOP|LATE_TRANSITION|ALTERNATING|MULTIBYTE)(,(HARD|PARENS|SELF_LOOP|LATE_TRANSITION|ALTERNATING|MULTIBYTE))*$ ]]; then
        echo "DFA_REAL_LAYOUT_PATTERNS contains an unsupported topology: ${DFA_REAL_LAYOUT_PATTERNS}" >&2
        exit 1
    fi
    if [[ ! "${DFA_REAL_LAYOUT_FORKS}" =~ ^[1-9][0-9]*$ ]]; then
        echo "DFA_REAL_LAYOUT_FORKS must be a positive integer: ${DFA_REAL_LAYOUT_FORKS}" >&2
        exit 1
    fi
fi
if [[ "${CAMPAIGN_MODE}" == dfa-layout-perfasm ]]; then
    if [[ ! "${DFA_LAYOUT_PERFASM_BENCHMARK}" =~ ^BenchmarkDfa(Compact|Real)TransitionLayout\.[A-Za-z][A-Za-z0-9]*$ ]]; then
        echo "DFA_LAYOUT_PERFASM_BENCHMARK must name one compact or real layout benchmark method: ${DFA_LAYOUT_PERFASM_BENCHMARK}" >&2
        exit 1
    fi
    if [[ ! "${DFA_LAYOUT_PERFASM_STATE_COUNT}" =~ ^[1-9][0-9]*$ || ! "${DFA_LAYOUT_PERFASM_CLASS_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
        echo "DFA_LAYOUT_PERFASM_STATE_COUNT and DFA_LAYOUT_PERFASM_CLASS_COUNT must be positive integers" >&2
        exit 1
    fi
    if [[ ! "${DFA_LAYOUT_PERFASM_PATTERN}" =~ ^(HARD|PARENS|SELF_LOOP|LATE_TRANSITION|ALTERNATING|MULTIBYTE)$ ]]; then
        echo "DFA_LAYOUT_PERFASM_PATTERN contains an unsupported topology: ${DFA_LAYOUT_PERFASM_PATTERN}" >&2
        exit 1
    fi
fi

SESSION_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
SESSION_DIR="${RESULT_ROOT}/${SESSION_ID}"
mkdir -p "${SESSION_DIR}"

INSTANCE_IDS=()
BUCKET=
REBAR_CORPUS_SHA256=
REBAR_OFFICIAL_SHA256=
JONI_MEMORY_CONTEXT_SHA256=
INSTANCE_ROLE_NAME=
INSTANCE_PROFILE_NAME=

aws_cli()
{
    aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" "$@"
}

retry_cleanup_command()
{
    local attempt
    for attempt in {1..5}; do
        if aws_cli "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep "${attempt}"
    done
    return 1
}

verify_aws_resource_absent()
{
    local description=$1
    local absent_pattern=$2
    local output
    shift 2

    if output=$(aws_cli "$@" 2>&1); then
        echo "Cleanup failed: ${description} still exists" >&2
        return 1
    fi
    if grep -Eq "${absent_pattern}" <<<"${output}"; then
        return 0
    fi
    echo "Unable to verify cleanup of ${description}: ${output}" >&2
    return 1
}

cleanup()
{
    local status=$?
    local association_id
    local cleanup_failed=0
    trap - EXIT INT TERM

    if [[ ${#INSTANCE_IDS[@]} -gt 0 ]]; then
        for instance_id in "${INSTANCE_IDS[@]}"; do
            association_id=$(aws_cli ec2 describe-iam-instance-profile-associations \
                --filters "Name=instance-id,Values=${instance_id}" \
                --query 'IamInstanceProfileAssociations[0].AssociationId' \
                --output text 2>/dev/null || true)
            if [[ -n "${association_id}" && "${association_id}" != None ]]; then
                retry_cleanup_command ec2 disassociate-iam-instance-profile --association-id "${association_id}" || true
            fi
        done
        retry_cleanup_command ec2 terminate-instances --instance-ids "${INSTANCE_IDS[@]}" || true
        if ! aws_cli ec2 wait instance-terminated --instance-ids "${INSTANCE_IDS[@]}"; then
            echo "Cleanup failed: benchmark instances did not reach terminated state" >&2
            cleanup_failed=1
        fi
    fi
    if [[ -n "${INSTANCE_PROFILE_NAME}" && -n "${INSTANCE_ROLE_NAME}" ]]; then
        retry_cleanup_command iam remove-role-from-instance-profile \
            --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
            --role-name "${INSTANCE_ROLE_NAME}" || true
        retry_cleanup_command iam delete-instance-profile --instance-profile-name "${INSTANCE_PROFILE_NAME}" || true
        retry_cleanup_command iam delete-role-policy --role-name "${INSTANCE_ROLE_NAME}" --policy-name CampaignTransfer || true
        retry_cleanup_command iam delete-role --role-name "${INSTANCE_ROLE_NAME}" || true
        verify_aws_resource_absent \
            "instance profile ${INSTANCE_PROFILE_NAME}" \
            'NoSuchEntity' \
            iam get-instance-profile --instance-profile-name "${INSTANCE_PROFILE_NAME}" || cleanup_failed=1
        verify_aws_resource_absent \
            "role ${INSTANCE_ROLE_NAME}" \
            'NoSuchEntity' \
            iam get-role --role-name "${INSTANCE_ROLE_NAME}" || cleanup_failed=1
    fi
    if [[ -n "${BUCKET}" && ${status} -eq 0 ]]; then
        retry_cleanup_command s3 rm "s3://${BUCKET}" --recursive || true
        retry_cleanup_command s3api delete-bucket --bucket "${BUCKET}" || true
        verify_aws_resource_absent \
            "bucket ${BUCKET}" \
            '404|NoSuchBucket|Not Found' \
            s3api head-bucket --bucket "${BUCKET}" || cleanup_failed=1
    fi
    if [[ -n "${BUCKET}" && ${status} -ne 0 ]]; then
        echo "Campaign failed; preserving the private transfer bucket for artifact recovery" >&2
    fi
    if [[ ${status} -eq 0 && ${cleanup_failed} -ne 0 ]]; then
        status=1
    fi
    exit "${status}"
}
trap cleanup EXIT INT TERM

package_worktree()
{
    local worktree=$1
    local output=$2

    COPYFILE_DISABLE=1 tar --no-xattrs -czf "${output}" \
        --exclude='./.git' \
        --exclude='./.agents' \
        --exclude='./benchmark-results' \
        --exclude='./target' \
        --exclude='*/__pycache__' \
        --exclude='*.pyc' \
        --exclude='*/benchmark-results' \
        --exclude='*/target' \
        -C "${worktree}" .
}

sha256()
{
    shasum -a 256 "$1" | awk '{print $1}'
}

archive_content_sha256()
{
    gzip -dc "$1" | shasum -a 256 | awk '{print $1}'
}

create_instance_profile()
{
    INSTANCE_ROLE_NAME="re2-benchmark-${LOWER_SESSION_ID}"
    INSTANCE_PROFILE_NAME="${INSTANCE_ROLE_NAME}"

    cat > "${SESSION_DIR}/instance-trust-policy.json" <<'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF
    cat > "${SESSION_DIR}/instance-transfer-policy.json" <<EOF
{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${BUCKET}/input/*"},
  {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${BUCKET}/results/*"}
]}
EOF

    aws_cli iam create-role \
        --role-name "${INSTANCE_ROLE_NAME}" \
        --assume-role-policy-document "file://${SESSION_DIR}/instance-trust-policy.json" >/dev/null
    aws_cli iam put-role-policy \
        --role-name "${INSTANCE_ROLE_NAME}" \
        --policy-name CampaignTransfer \
        --policy-document "file://${SESSION_DIR}/instance-transfer-policy.json"
    aws_cli iam create-instance-profile --instance-profile-name "${INSTANCE_PROFILE_NAME}" >/dev/null
    aws_cli iam add-role-to-instance-profile \
        --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
        --role-name "${INSTANCE_ROLE_NAME}"

    # IAM instance profiles are eventually consistent with EC2 launches.
    sleep 10
}

select_subnet()
{
    local instance_type=$1
    local available_zones
    local zone_values
    available_zones=$(aws_cli ec2 describe-instance-type-offerings \
        --location-type availability-zone \
        --filters "Name=instance-type,Values=${instance_type}" \
        --query 'InstanceTypeOfferings[].Location' \
        --output text)
    zone_values=$(tr '\t ' '\n' <<<"${available_zones}" | sed '/^$/d' | paste -sd, -)

    aws_cli ec2 describe-subnets \
        --filters \
            "Name=vpc-id,Values=${VPC_ID}" \
            "Name=state,Values=available" \
            "Name=availability-zone,Values=${zone_values}" \
        --query 'Subnets[?MapPublicIpOnLaunch] | [0].SubnetId' \
        --output text
}

create_user_data()
{
    local architecture=$1
    local result_key=$2
    local output=$3
    local hsdis_name=unused
    if [[ "${CAMPAIGN_MODE}" == dfa-layout-perfasm || "${CAMPAIGN_MODE}" == dfa-absolute-pointer-integrated ]]; then
        case "${architecture}" in
            x86_64) hsdis_name=hsdis-amd64.so ;;
            aarch64) hsdis_name=hsdis-aarch64.so ;;
            *) echo "Unsupported hsdis architecture: ${architecture}" >&2; exit 1 ;;
        esac
    fi
    cat > "${output}" <<EOF
#!/usr/bin/env bash
set -Eeuo pipefail

RESULT_DIR=/var/tmp/re2-results
mkdir -p "\${RESULT_DIR}"
exec > >(tee /var/log/re2-engineering-user-data.log) 2>&1

finish()
{
    status=\$?
    trap - EXIT
    printf '%s\n' "\${status}" > "\${RESULT_DIR}/exit-status"
    if [[ -d /opt/re2-work/slice/target/surefire-reports ]]; then
        cp -a /opt/re2-work/slice/target/surefire-reports "\${RESULT_DIR}/slice-surefire-reports"
    fi
    if [[ -d /opt/re2-work/trino/core/trino-main/target/surefire-reports ]]; then
        cp -a /opt/re2-work/trino/core/trino-main/target/surefire-reports "\${RESULT_DIR}/trino-surefire-reports"
    fi
    cp /var/log/re2-engineering-user-data.log "\${RESULT_DIR}/user-data.log" || true
    tar -czf /var/tmp/re2-results.tar.gz -C /var/tmp re2-results || true
    aws s3 cp /var/tmp/re2-results.tar.gz 's3://${BUCKET}/${result_key}' --region '${AWS_REGION}' --only-show-errors || true
    shutdown -h now
    exit "\${status}"
}
trap finish EXIT

mkdir -p /opt/re2-work/slice /opt/re2-work/trino
aws s3 cp 's3://${BUCKET}/input/slice.tar.gz' /tmp/slice.tar.gz --region '${AWS_REGION}' --only-show-errors
aws s3 cp 's3://${BUCKET}/input/trino.tar.gz' /tmp/trino.tar.gz --region '${AWS_REGION}' --only-show-errors
printf '%s  %s\n' '${SLICE_SHA256}' /tmp/slice.tar.gz | sha256sum --check -
printf '%s  %s\n' '${TRINO_SHA256}' /tmp/trino.tar.gz | sha256sum --check -
tar -xzf /tmp/slice.tar.gz -C /opt/re2-work/slice
tar -xzf /tmp/trino.tar.gz -C /opt/re2-work/trino
if [[ '${REBAR_CORPUS_SHA256}' != '' ]]; then
    mkdir -p /opt/re2-work/slice/target
    aws s3 cp 's3://${BUCKET}/input/rebar-selected.tar.gz' /tmp/rebar-selected.tar.gz --region '${AWS_REGION}' --only-show-errors
    printf '%s  %s\n' '${REBAR_CORPUS_SHA256}' /tmp/rebar-selected.tar.gz | sha256sum --check -
    tar -xzf /tmp/rebar-selected.tar.gz -C /opt/re2-work/slice/target
fi
if [[ '${REBAR_OFFICIAL_SHA256}' != '' ]]; then
    mkdir -p /opt/re2-work/rebar
    aws s3 cp 's3://${BUCKET}/input/rebar-official.tar.gz' /tmp/rebar-official.tar.gz --region '${AWS_REGION}' --only-show-errors
    printf '%s  %s\n' '${REBAR_OFFICIAL_SHA256}' /tmp/rebar-official.tar.gz | sha256sum --check -
    tar --no-same-owner -xzf /tmp/rebar-official.tar.gz -C /opt/re2-work/rebar --strip-components=1
fi
if [[ '${JONI_MEMORY_CONTEXT_SHA256}' != '' ]]; then
    mkdir -p /opt/re2-work/context
    aws s3 cp 's3://${BUCKET}/input/joni-memory-context.tar.gz' /tmp/joni-memory-context.tar.gz --region '${AWS_REGION}' --only-show-errors
    printf '%s  %s\n' '${JONI_MEMORY_CONTEXT_SHA256}' /tmp/joni-memory-context.tar.gz | sha256sum --check -
    tar --no-same-owner -xzf /tmp/joni-memory-context.tar.gz -C /opt/re2-work/context
fi
if [[ '${CAMPAIGN_MODE}' == dfa-layout-perfasm || '${CAMPAIGN_MODE}' == dfa-absolute-pointer-integrated ]]; then
    aws s3 cp 's3://${BUCKET}/input/${hsdis_name}' '/tmp/${hsdis_name}' --region '${AWS_REGION}' --only-show-errors
    export HSDIS_PATH='/tmp/${hsdis_name}'
fi

export SLICE_SNAPSHOT_COMMIT='${SLICE_COMMIT}'
export SLICE_SNAPSHOT_SHA256='${SLICE_SHA256}'
export SLICE_CONTENT_SHA256='${SLICE_CONTENT_SHA256}'
export TRINO_SNAPSHOT_COMMIT='${TRINO_COMMIT}'
export RE2_ENGINEERING_ARCHITECTURE='${architecture}'
export RE2_BENCHMARK_MODE='${CAMPAIGN_MODE}'
export INSTANCE_MARKET_TYPE='${INSTANCE_MARKET_TYPE}'
export CAPTURE_SHARD='${CAPTURE_SHARD}'
export CAPTURE_CONTROL='${CAPTURE_CONTROL}'
export CAPTURE_ENGINE='${CAPTURE_ENGINE}'
export CAPTURE_GUARD_SIZES='${CAPTURE_GUARD_SIZES}'
export CAPTURE_PIPELINE_WORKLOAD='${CAPTURE_PIPELINE_WORKLOAD}'
export CAPTURE_PIPELINE_STAGES='${CAPTURE_PIPELINE_STAGES}'
export CAPTURE_PIPELINE_NATIVE_DIAGNOSTICS='${CAPTURE_PIPELINE_NATIVE_DIAGNOSTICS}'
export CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS='${CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS}'
export CAPTURE_PIPELINE_ITERATIONS='${CAPTURE_PIPELINE_ITERATIONS}'
export CAPTURE_PIPELINE_WARMUP_ITERATIONS='${CAPTURE_PIPELINE_WARMUP_ITERATIONS}'
export COUNT_PIPELINE_MEMORY_MEGABYTES='${COUNT_PIPELINE_MEMORY_MEGABYTES}'
export COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES='${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES}'
export BENCHMARK_JAVA_ARCHIVE_URL='${BENCHMARK_JAVA_ARCHIVE_URL}'
export BENCHMARK_JAVA_ARCHIVE_SHA256='${BENCHMARK_JAVA_ARCHIVE_SHA256}'
export REBAR_ROOT='/opt/re2-work/rebar'
export REBAR_COMPARATOR_ORDER='${REBAR_COMPARATOR_ORDER}'
export REBAR_MODEL_FILTER='${REBAR_MODEL_FILTER}'
export REBAR_ALLOW_NATIVE_DRIFT='${REBAR_ALLOW_NATIVE_DRIFT}'
export REBAR_EXCLUDE_NATIVE_DRIFT='${REBAR_EXCLUDE_NATIVE_DRIFT}'
export BENCHMARK_HEAP_SIZE='${BENCHMARK_HEAP_SIZE:-8g}'
export PAIR_SCALING_STATE_COUNTS='${PAIR_SCALING_STATE_COUNTS:-4,8,16,32,64,128,256}'
export PAIR_SCALING_CLASS_COUNTS='${PAIR_SCALING_CLASS_COUNTS:-16,29,64}'
export DFA_LAYOUT_FILTER='${DFA_LAYOUT_FILTER}'
export DFA_LAYOUT_STATE_COUNTS='${DFA_LAYOUT_STATE_COUNTS}'
export DFA_LAYOUT_CLASS_COUNTS='${DFA_LAYOUT_CLASS_COUNTS}'
export DFA_LAYOUT_FORKS='${DFA_LAYOUT_FORKS}'
export DFA_LAYOUT_RUN_NATIVE='${DFA_LAYOUT_RUN_NATIVE}'
export DFA_REAL_LAYOUT_FILTER='${DFA_REAL_LAYOUT_FILTER}'
export DFA_REAL_LAYOUT_PATTERNS='${DFA_REAL_LAYOUT_PATTERNS}'
export DFA_REAL_LAYOUT_FORKS='${DFA_REAL_LAYOUT_FORKS}'
export DFA_REAL_LAYOUT_RUN_NATIVE='${DFA_REAL_LAYOUT_RUN_NATIVE}'
export DFA_LAYOUT_PERFASM_BENCHMARK='${DFA_LAYOUT_PERFASM_BENCHMARK}'
export DFA_LAYOUT_PERFASM_STATE_COUNT='${DFA_LAYOUT_PERFASM_STATE_COUNT}'
export DFA_LAYOUT_PERFASM_CLASS_COUNT='${DFA_LAYOUT_PERFASM_CLASS_COUNT}'
export DFA_LAYOUT_PERFASM_PATTERN='${DFA_LAYOUT_PERFASM_PATTERN}'
export TRADITIONAL_JMH_FORKS='${TRADITIONAL_JMH_FORKS}'
export TRADITIONAL_NATIVE_REPETITIONS='${TRADITIONAL_NATIVE_REPETITIONS}'
export TRADITIONAL_NATIVE_MINIMUM_TIME='${TRADITIONAL_NATIVE_MINIMUM_TIME}'
export TRADITIONAL_BENCHMARK_CLASS='${TRADITIONAL_BENCHMARK_CLASS}'

/opt/re2-work/slice/tools/re2-benchmark/aws/run-host.sh \
    /opt/re2-work/slice \
    /opt/re2-work/trino \
    "\${RESULT_DIR}"
EOF
}

launch_instance()
{
    local label=$1
    local architecture=$2
    local instance_type=$3
    local ami_parameter=$4
    local result_key=$5
    local user_data="${SESSION_DIR}/user-data-${label}.sh"
    local ami
    local root_device
    local subnet
    local market_arguments=()

    ami=$(aws_cli ssm get-parameter --name "${ami_parameter}" --query Parameter.Value --output text)
    root_device=$(aws_cli ec2 describe-images --image-ids "${ami}" --query 'Images[0].RootDeviceName' --output text)
    subnet=$(select_subnet "${instance_type}")
    if [[ -z "${subnet}" || "${subnet}" == "None" ]]; then
        echo "No public default-VPC subnet offers ${instance_type}" >&2
        exit 1
    fi

    create_user_data "${architecture}" "${result_key}" "${user_data}"

    if [[ "${INSTANCE_MARKET_TYPE}" == spot ]]; then
        market_arguments=(
            --instance-market-options
            'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}')
    fi

    aws_cli ec2 run-instances \
        --image-id "${ami}" \
        --instance-type "${instance_type}" \
        --count 1 \
        --network-interfaces "DeviceIndex=0,SubnetId=${subnet},Groups=${SECURITY_GROUP_ID},AssociatePublicIpAddress=true" \
        --block-device-mappings "DeviceName=${root_device},Ebs={VolumeSize=100,VolumeType=gp3,DeleteOnTermination=true,Encrypted=true}" \
        --metadata-options HttpTokens=required,HttpEndpoint=enabled \
        --iam-instance-profile "Name=${INSTANCE_PROFILE_NAME}" \
        --instance-initiated-shutdown-behavior terminate \
        "${market_arguments[@]}" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=re2-engineering-${label}-${SESSION_ID}},{Key=Project,Value=re2-port-benchmark}]" \
        --user-data "file://${user_data}" \
        --query 'Instances[0].InstanceId' \
        --output text
}

package_worktree "${SLICE_DIR}" "${SESSION_DIR}/slice.tar.gz"
package_worktree "${TRINO_DIR}" "${SESSION_DIR}/trino.tar.gz"
if [[ "${CAMPAIGN_MODE}" == dfa-layout-perfasm || "${CAMPAIGN_MODE}" == dfa-absolute-pointer-integrated ]]; then
    mkdir -p "${HSDIS_CACHE_DIR}"
    for hsdis_architecture in amd64 aarch64; do
        case "${hsdis_architecture}" in
            amd64) hsdis_sha256=${HSDIS_AMD64_SHA256} ;;
            aarch64) hsdis_sha256=${HSDIS_AARCH64_SHA256} ;;
        esac
        hsdis_cache_path="${HSDIS_CACHE_DIR}/hsdis-${hsdis_architecture}.so"
        if ! printf '%s  %s\n' "${hsdis_sha256}" "${hsdis_cache_path}" | shasum -a 256 --check --status 2>/dev/null; then
            hsdis_download_path="${hsdis_cache_path}.download"
            rm -f "${hsdis_download_path}"
            curl --fail --location --retry 5 \
                "https://chriswhocodes.com/hsdis/hsdis-${hsdis_architecture}.so" \
                --output "${hsdis_download_path}"
            printf '%s  %s\n' "${hsdis_sha256}" "${hsdis_download_path}" | shasum -a 256 --check
            mv "${hsdis_download_path}" "${hsdis_cache_path}"
        fi
        cp "${hsdis_cache_path}" "${SESSION_DIR}/hsdis-${hsdis_architecture}.so"
    done
fi
if [[ "${CAMPAIGN_MODE}" == dfa-paired-corpus || "${CAMPAIGN_MODE}" == dfa-partial-candidate || "${CAMPAIGN_MODE}" == dfa-self-loop-rebar ]]; then
    if [[ ! -d "${REBAR_CORPUS_DIR}" ]]; then
        echo "Rebar corpus directory does not exist: ${REBAR_CORPUS_DIR}" >&2
        exit 1
    fi
    COPYFILE_DISABLE=1 tar --no-xattrs -czf "${SESSION_DIR}/rebar-selected.tar.gz" \
        -C "$(dirname "${REBAR_CORPUS_DIR}")" "$(basename "${REBAR_CORPUS_DIR}")"
    REBAR_CORPUS_SHA256=$(sha256 "${SESSION_DIR}/rebar-selected.tar.gz")
fi
if [[ "${CAMPAIGN_MODE}" == rebar-official || "${CAMPAIGN_MODE}" == rebar-native-comparison || "${CAMPAIGN_MODE}" == bounded-count || "${CAMPAIGN_MODE}" == dfa-large-pointer || "${CAMPAIGN_MODE}" == byte-scan-fallback || "${CAMPAIGN_MODE}" == capture-pipeline ]]; then
    if [[ ! -d "${REBAR_OFFICIAL_DIR}/.git" ]]; then
        echo "Pinned Rebar checkout does not exist: ${REBAR_OFFICIAL_DIR}" >&2
        exit 1
    fi
    if [[ "$(git -C "${REBAR_OFFICIAL_DIR}" rev-parse HEAD)" != 463d00f31887e84c38467805b9e3122c314b9521 ]]; then
        echo "Unexpected Rebar revision in ${REBAR_OFFICIAL_DIR}" >&2
        exit 1
    fi
    if [[ -n $(git -C "${REBAR_OFFICIAL_DIR}" status --porcelain --untracked-files=no) ]]; then
        echo "Pinned Rebar checkout has tracked modifications" >&2
        exit 1
    fi
    COPYFILE_DISABLE=1 tar --no-xattrs -czf "${SESSION_DIR}/rebar-official.tar.gz" \
        --exclude='rebar-corpus/target' \
        -C "$(dirname "${REBAR_OFFICIAL_DIR}")" "$(basename "${REBAR_OFFICIAL_DIR}")"
    REBAR_OFFICIAL_SHA256=$(sha256 "${SESSION_DIR}/rebar-official.tar.gz")
fi
if [[ "${CAMPAIGN_MODE}" == joni-memory || "${CAMPAIGN_MODE}" == joni-memory-census ]]; then
    for context_file in pattern.txt haystack.bin; do
        if [[ ! -f "${JONI_MEMORY_CONTEXT_DIR}/${context_file}" ]]; then
            echo "Joni memory context file does not exist: ${JONI_MEMORY_CONTEXT_DIR}/${context_file}" >&2
            exit 1
        fi
    done
    COPYFILE_DISABLE=1 tar --no-xattrs -czf "${SESSION_DIR}/joni-memory-context.tar.gz" \
        -C "${JONI_MEMORY_CONTEXT_DIR}" pattern.txt haystack.bin
    JONI_MEMORY_CONTEXT_SHA256=$(sha256 "${SESSION_DIR}/joni-memory-context.tar.gz")
fi

SLICE_COMMIT=$(git -C "${SLICE_DIR}" rev-parse HEAD)
TRINO_COMMIT=$(git -C "${TRINO_DIR}" rev-parse HEAD)
SLICE_SHA256=$(sha256 "${SESSION_DIR}/slice.tar.gz")
SLICE_CONTENT_SHA256=$(archive_content_sha256 "${SESSION_DIR}/slice.tar.gz")
TRINO_SHA256=$(sha256 "${SESSION_DIR}/trino.tar.gz")
ACCOUNT_ID=$(aws_cli sts get-caller-identity --query Account --output text)
LOWER_SESSION_ID=$(printf '%s' "${SESSION_ID}" | tr '[:upper:]' '[:lower:]')
BUCKET="re2-engineering-${ACCOUNT_ID}-${LOWER_SESSION_ID}-$RANDOM"

aws_cli s3api create-bucket \
    --bucket "${BUCKET}" \
    --create-bucket-configuration "LocationConstraint=${AWS_REGION}" >/dev/null
aws_cli s3api put-public-access-block \
    --bucket "${BUCKET}" \
    --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws_cli s3api put-bucket-encryption \
    --bucket "${BUCKET}" \
    --server-side-encryption-configuration \
        'Rules=[{ApplyServerSideEncryptionByDefault={SSEAlgorithm=AES256},BucketKeyEnabled=true}]'
aws_cli s3api put-bucket-lifecycle-configuration \
    --bucket "${BUCKET}" \
    --lifecycle-configuration \
        '{"Rules":[{"ID":"ExpireCampaignArtifacts","Status":"Enabled","Filter":{"Prefix":""},"Expiration":{"Days":1}}]}'
aws_cli s3 cp "${SESSION_DIR}/slice.tar.gz" "s3://${BUCKET}/input/slice.tar.gz" --only-show-errors
aws_cli s3 cp "${SESSION_DIR}/trino.tar.gz" "s3://${BUCKET}/input/trino.tar.gz" --only-show-errors
if [[ "${CAMPAIGN_MODE}" == dfa-layout-perfasm || "${CAMPAIGN_MODE}" == dfa-absolute-pointer-integrated ]]; then
    aws_cli s3 cp "${SESSION_DIR}/hsdis-amd64.so" "s3://${BUCKET}/input/hsdis-amd64.so" --only-show-errors
    aws_cli s3 cp "${SESSION_DIR}/hsdis-aarch64.so" "s3://${BUCKET}/input/hsdis-aarch64.so" --only-show-errors
fi
if [[ -f "${SESSION_DIR}/rebar-selected.tar.gz" ]]; then
    aws_cli s3 cp "${SESSION_DIR}/rebar-selected.tar.gz" "s3://${BUCKET}/input/rebar-selected.tar.gz" --only-show-errors
fi
if [[ -f "${SESSION_DIR}/rebar-official.tar.gz" ]]; then
    aws_cli s3 cp "${SESSION_DIR}/rebar-official.tar.gz" "s3://${BUCKET}/input/rebar-official.tar.gz" --only-show-errors
fi
if [[ -f "${SESSION_DIR}/joni-memory-context.tar.gz" ]]; then
    aws_cli s3 cp "${SESSION_DIR}/joni-memory-context.tar.gz" "s3://${BUCKET}/input/joni-memory-context.tar.gz" --only-show-errors
fi

create_instance_profile

VPC_ID=$(aws_cli ec2 describe-vpcs \
    --filters Name=is-default,Values=true \
    --query 'Vpcs[0].VpcId' \
    --output text)
SECURITY_GROUP_ID=$(aws_cli ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=${VPC_ID}" Name=group-name,Values=default \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

INTEL_RESULT_KEY=results/intel.tar.gz
ARM_RESULT_KEY=results/arm.tar.gz
INTEL_INSTANCE_ID=none
ARM_INSTANCE_ID=none
pending=()
if includes_architecture intel; then
    INTEL_INSTANCE_ID=$(launch_instance \
        intel x86_64 "${INTEL_INSTANCE_TYPE}" \
        /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
        "${INTEL_RESULT_KEY}")
    INSTANCE_IDS+=("${INTEL_INSTANCE_ID}")
    pending+=(intel)
fi
if includes_architecture arm; then
    ARM_INSTANCE_ID=$(launch_instance \
        arm aarch64 "${ARM_INSTANCE_TYPE}" \
        /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
        "${ARM_RESULT_KEY}")
    INSTANCE_IDS+=("${ARM_INSTANCE_ID}")
    pending+=(arm)
fi

cat > "${SESSION_DIR}/session.txt" <<EOF
session=${SESSION_ID}
aws_profile=${AWS_PROFILE}
aws_region=${AWS_REGION}
bucket=${BUCKET}
slice_commit=${SLICE_COMMIT}
slice_snapshot_sha256=${SLICE_SHA256}
slice_content_sha256=${SLICE_CONTENT_SHA256}
trino_commit=${TRINO_COMMIT}
trino_snapshot_sha256=${TRINO_SHA256}
rebar_corpus_sha256=${REBAR_CORPUS_SHA256:-none}
rebar_official_sha256=${REBAR_OFFICIAL_SHA256:-none}
joni_memory_context_sha256=${JONI_MEMORY_CONTEXT_SHA256:-none}
campaign_mode=${CAMPAIGN_MODE}
campaign_architectures=${CAMPAIGN_ARCHITECTURES}
instance_market_type=${INSTANCE_MARKET_TYPE}
capture_shard=${CAPTURE_SHARD}
capture_control=${CAPTURE_CONTROL}
capture_engine=${CAPTURE_ENGINE}
capture_guard_sizes=${CAPTURE_GUARD_SIZES}
capture_pipeline_workload=${CAPTURE_PIPELINE_WORKLOAD:-none}
capture_pipeline_stages=${CAPTURE_PIPELINE_STAGES}
capture_pipeline_native_diagnostics=${CAPTURE_PIPELINE_NATIVE_DIAGNOSTICS}
capture_pipeline_allow_extended_one_pass=${CAPTURE_PIPELINE_ALLOW_EXTENDED_ONE_PASS}
capture_pipeline_iterations=${CAPTURE_PIPELINE_ITERATIONS}
capture_pipeline_warmup_iterations=${CAPTURE_PIPELINE_WARMUP_ITERATIONS}
count_pipeline_memory_megabytes=${COUNT_PIPELINE_MEMORY_MEGABYTES}
count_pipeline_control_memory_megabytes=${COUNT_PIPELINE_CONTROL_MEMORY_MEGABYTES}
rebar_comparator_order=${REBAR_COMPARATOR_ORDER}
rebar_model_filter=${REBAR_MODEL_FILTER}
rebar_allow_native_drift=${REBAR_ALLOW_NATIVE_DRIFT}
rebar_exclude_native_drift=${REBAR_EXCLUDE_NATIVE_DRIFT}
benchmark_java_archive_url=${BENCHMARK_JAVA_ARCHIVE_URL:-default-temurin-25}
benchmark_java_archive_sha256=${BENCHMARK_JAVA_ARCHIVE_SHA256:-none}
benchmark_heap_size=${BENCHMARK_HEAP_SIZE:-8g}
pair_scaling_state_counts=${PAIR_SCALING_STATE_COUNTS:-4,8,16,32,64,128,256}
pair_scaling_class_counts=${PAIR_SCALING_CLASS_COUNTS:-16,29,64}
dfa_layout_filter=${DFA_LAYOUT_FILTER}
dfa_layout_state_counts=${DFA_LAYOUT_STATE_COUNTS}
dfa_layout_class_counts=${DFA_LAYOUT_CLASS_COUNTS}
dfa_layout_forks=${DFA_LAYOUT_FORKS}
dfa_layout_run_native=${DFA_LAYOUT_RUN_NATIVE}
dfa_real_layout_filter=${DFA_REAL_LAYOUT_FILTER}
dfa_real_layout_patterns=${DFA_REAL_LAYOUT_PATTERNS}
dfa_real_layout_forks=${DFA_REAL_LAYOUT_FORKS}
dfa_real_layout_run_native=${DFA_REAL_LAYOUT_RUN_NATIVE}
dfa_layout_perfasm_benchmark=${DFA_LAYOUT_PERFASM_BENCHMARK}
dfa_layout_perfasm_state_count=${DFA_LAYOUT_PERFASM_STATE_COUNT}
dfa_layout_perfasm_class_count=${DFA_LAYOUT_PERFASM_CLASS_COUNT}
dfa_layout_perfasm_pattern=${DFA_LAYOUT_PERFASM_PATTERN}
traditional_jmh_forks=${TRADITIONAL_JMH_FORKS}
traditional_native_repetitions=${TRADITIONAL_NATIVE_REPETITIONS}
traditional_native_minimum_time=${TRADITIONAL_NATIVE_MINIMUM_TIME}
traditional_benchmark_class=${TRADITIONAL_BENCHMARK_CLASS:-all}
intel_instance_type=${INTEL_INSTANCE_TYPE}
intel_instance_id=${INTEL_INSTANCE_ID}
arm_instance_type=${ARM_INSTANCE_TYPE}
arm_instance_id=${ARM_INSTANCE_ID}
EOF

echo "Started ${CAMPAIGN_ARCHITECTURES}: Intel ${INTEL_INSTANCE_ID}, Arm ${ARM_INSTANCE_ID}"
echo "Artifacts: ${SESSION_DIR}"

deadline=$((SECONDS + TIMEOUT_SECONDS))
campaign_failed=0
while [[ ${#pending[@]} -gt 0 && ${SECONDS} -lt ${deadline} ]]; do
    next_pending=()
    for label in "${pending[@]}"; do
        if [[ "${label}" == intel ]]; then
            key=${INTEL_RESULT_KEY}
            instance_id=${INTEL_INSTANCE_ID}
        else
            key=${ARM_RESULT_KEY}
            instance_id=${ARM_INSTANCE_ID}
        fi

        if aws_cli s3api head-object --bucket "${BUCKET}" --key "${key}" >/dev/null 2>&1; then
            aws_cli s3 cp "s3://${BUCKET}/${key}" "${SESSION_DIR}/${label}.tar.gz" --only-show-errors
            mkdir -p "${SESSION_DIR}/${label}"
            tar -xzf "${SESSION_DIR}/${label}.tar.gz" -C "${SESSION_DIR}/${label}"
            result_status=$(cat "${SESSION_DIR}/${label}/re2-results/exit-status")
            if [[ "${result_status}" != 0 ]]; then
                echo "${label} run failed with status ${result_status}" >&2
                campaign_failed=1
                continue
            fi
            echo "Downloaded ${label} results"
            continue
        fi

        if ! state=$(aws_cli ec2 describe-instances \
                --instance-ids "${instance_id}" \
                --query 'Reservations[0].Instances[0].State.Name' \
                --output text); then
            echo "Unable to query AWS while waiting for ${label}; results can be recovered from s3://${BUCKET}/${key}" >&2
            exit 1
        fi
        if [[ "${state}" == terminated || "${state}" == shutting-down ]]; then
            aws_cli ec2 get-console-output \
                --instance-id "${instance_id}" \
                --latest \
                --query Output \
                --output text > "${SESSION_DIR}/${label}-console.log" 2>&1 || true
            echo "${label} terminated without uploading results" >&2
            campaign_failed=1
            continue
        fi
        next_pending+=("${label}")
    done
    set +u
    pending=("${next_pending[@]}")
    set -u
    if [[ ${#pending[@]} -gt 0 ]]; then
        sleep 60
    fi
done

if [[ ${#pending[@]} -gt 0 ]]; then
    echo "Timed out waiting for: ${pending[*]}" >&2
    exit 1
fi

if [[ ${campaign_failed} -ne 0 ]]; then
    echo "One or more engineering runs failed; inspect ${SESSION_DIR}" >&2
    exit 1
fi

echo "Engineering run complete: ${SESSION_DIR}"
