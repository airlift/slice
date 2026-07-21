#!/usr/bin/env bash

set -euo pipefail

readonly pinned_re2_commit=972a15cedd008d846f1a39b2e88ce48d7f166cbd
repo_root=$(cd "$(dirname "$0")/../../.." && pwd)
rebar_root=${1:-"$repo_root/target/rebar-corpus"}
portable_native_root=${2:-"$repo_root/target/rebar-native-portable"}
tuned_native_root=${3:-"$repo_root/target/rebar-native-tuned"}
pinned_re2_root=${4:-"$repo_root/target/re2-golden-dependencies/re2"}

if [[ ! -f "$rebar_root/engines/re2/Cargo.toml" ]]; then
    echo "Rebar native runner not found: $rebar_root/engines/re2" >&2
    exit 1
fi
if [[ ! -d "$pinned_re2_root/.git" ]]; then
    echo "Pinned RE2 checkout not found: $pinned_re2_root" >&2
    exit 1
fi
actual_commit=$(git -C "$pinned_re2_root" rev-parse HEAD)
if [[ "$actual_commit" != "$pinned_re2_commit" ]]; then
    echo "Expected RE2 $pinned_re2_commit but found $actual_commit" >&2
    exit 1
fi
if [[ -n $(git -C "$pinned_re2_root" status --porcelain) ]]; then
    echo "Pinned RE2 checkout is dirty: $pinned_re2_root" >&2
    exit 1
fi
if [[ -n $(git -C "$rebar_root" status --porcelain --untracked-files=no) ]]; then
    echo "Rebar checkout has tracked modifications: $rebar_root" >&2
    exit 1
fi

prepare_runner()
{
    local destination=$1
    rm -rf "$destination"
    mkdir -p "$destination"
    cp -R "$rebar_root/engines/re2/." "$destination"
    rm -rf "$destination/target" "$destination/upstream"
    ln -s "$pinned_re2_root" "$destination/upstream"
    REBAR_ROOT="$rebar_root" perl -pi -e \
        's#path = "\.\./\.\./shared/#path = "$ENV{REBAR_ROOT}/shared/#' \
        "$destination/Cargo.toml"
    printf 'pub(crate) const VERSION: &str = "%s";\n' "$pinned_re2_commit" \
        > "$destination/version.rs"
}

prepare_runner "$portable_native_root"
prepare_runner "$tuned_native_root"

case $(uname -m) in
    x86_64) tuned_flag=-march=native ;;
    aarch64 | arm64) tuned_flag=-mcpu=native ;;
    *)
        echo "Unsupported architecture: $(uname -m)" >&2
        exit 1
        ;;
esac

build_runner()
{
    local runner_root=$1
    local flags=$2
    local command_log="$runner_root/cxx-commands.log"
    local compiler_wrapper="$runner_root/cxx-wrapper.sh"
    local real_compiler
    real_compiler=$(command -v c++)

    cat > "$compiler_wrapper" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

printf -v command '%q ' "$@"
printf '%s\n' "$command" >> "$CXX_COMMAND_LOG"
exec "$REAL_CXX" "$@"
EOF
    chmod +x "$compiler_wrapper"
    : > "$command_log"

    (
        cd "$runner_root"
        REAL_CXX="$real_compiler" \
            CXX_COMMAND_LOG="$command_log" \
            CXX="$compiler_wrapper" \
            CXXFLAGS="${CXXFLAGS:-} $flags" \
            cargo build --release --verbose --locked
    )
}

verify_compile_flags()
{
    local command_log=$1
    shift

    local compile_commands
    compile_commands=$(grep -F 'binding.cpp' "$command_log" | grep -F -- ' -c ' || true)
    if [[ -z "$compile_commands" ]]; then
        echo "No binding.cpp compilation command found in $command_log" >&2
        exit 1
    fi

    local flag
    for flag in "$@"; do
        if ! grep -Fq -- "$flag" <<< "$compile_commands"; then
            echo "Compilation command in $command_log does not contain $flag" >&2
            exit 1
        fi
    done
}

build_runner "$portable_native_root" '-O3 -DNDEBUG'
build_runner "$tuned_native_root" "-O3 -DNDEBUG $tuned_flag"

verify_compile_flags "$portable_native_root/cxx-commands.log" -O3 -DNDEBUG
verify_compile_flags "$tuned_native_root/cxx-commands.log" -O3 -DNDEBUG "$tuned_flag"

sha256sum "$portable_native_root/target/release/main"
sha256sum "$tuned_native_root/target/release/main"
