#!/bin/sh
set -eu
APP_HOME=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
JAR="$APP_HOME/gradle/wrapper/gradle-wrapper.jar"
URL="https://raw.githubusercontent.com/gradle/gradle/v9.6.1/gradle/wrapper/gradle-wrapper.jar"
SHA="497c8c2a7e5031f6aa847f88104aa80a93532ec32ee17bdb8d1d2f67a194a9c7"
verify() {
  if command -v sha256sum >/dev/null 2>&1; then actual=$(sha256sum "$1" | awk '{print $1}'); else actual=$(shasum -a 256 "$1" | awk '{print $1}'); fi
  [ "$actual" = "$SHA" ] || { echo "Gradle wrapper checksum mismatch" >&2; rm -f "$1"; exit 1; }
}
if [ ! -f "$JAR" ]; then
  tmp="$JAR.tmp"; mkdir -p "$(dirname "$JAR")"; rm -f "$tmp"
  if command -v curl >/dev/null 2>&1; then curl -fsSL "$URL" -o "$tmp"; elif command -v wget >/dev/null 2>&1; then wget -q "$URL" -O "$tmp"; else echo "curl or wget is required" >&2; exit 1; fi
  verify "$tmp"; mv "$tmp" "$JAR"
fi
verify "$JAR"
JAVACMD=${JAVA_HOME:+$JAVA_HOME/bin/}java
exec "$JAVACMD" -Xmx64m -Xms64m -classpath "$JAR" org.gradle.wrapper.GradleWrapperMain "$@"
