#!/usr/bin/env bash
# Print the path of a JDK that Spark 3.5 can use (major version 17 or 21),
# or print nothing and exit 1 if there is none.
#
# Needed because `/usr/libexec/java_home -v 17` on macOS means "17 or newer"
# and happily returns a JDK 26 that Spark cannot start on, so every candidate
# is checked by actually asking it for its version.
set -uo pipefail

major_of() {
    local home="$1"
    [[ -x "$home/bin/java" ]] || return 1
    "$home/bin/java" -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+).*/\1/'
}

candidates=()
[[ -n "${JAVA_HOME:-}" ]] && candidates+=("$JAVA_HOME")

if [[ -x /usr/libexec/java_home ]]; then
    while IFS= read -r line; do
        candidates+=("$line")
    done < <(/usr/libexec/java_home -V 2>&1 | sed -nE 's#.*(/[^ ]*/(Contents/Home|jdk[^ ]*))$#\1#p')
fi

for path in /opt/homebrew/opt/openjdk@17 /opt/homebrew/opt/openjdk@21 \
            /usr/local/opt/openjdk@17 /usr/local/opt/openjdk@21 \
            /usr/lib/jvm/java-17-openjdk* /usr/lib/jvm/java-21-openjdk* \
            /usr/lib/jvm/temurin-17* /usr/lib/jvm/temurin-21*; do
    [[ -d "$path" ]] && candidates+=("$path")
done

for candidate in "${candidates[@]:-}"; do
    [[ -n "$candidate" ]] || continue
    version="$(major_of "$candidate" || true)"
    if [[ "$version" == "17" || "$version" == "21" ]]; then
        echo "$candidate"
        exit 0
    fi
done

exit 1
