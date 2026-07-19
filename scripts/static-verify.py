#!/usr/bin/env python3
"""Performs dependency-free structural verification of the repository artifact."""

from __future__ import annotations

import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ERRORS: list[str] = []


def fail(message: str) -> None:
    """Records one verification failure."""
    ERRORS.append(message)


def verify_required_files() -> None:
    """Checks that all portfolio and operational assets exist."""
    required = [
        "README.md",
        "REPOSITORY.md",
        "VERIFICATION.md",
        "CHANGELOG.md",
        "build.gradle",
        "settings.gradle",
        "gradlew",
        "gradlew.bat",
        "gradle/wrapper/gradle-wrapper.properties",
        "Dockerfile",
        "docker-compose.yml",
        ".github/workflows/ci.yml",
        "src/main/resources/db/migration/V1__create_reconciliation_schema.sql",
        "src/main/resources/db/migration/V2__strengthen_reconciliation_evidence.sql",
        "src/main/resources/application-local.yml",
        ".github/workflows/dependency-submission.yml",
        "postman/Reconciliation-Pipeline.postman_collection.json",
        "postman/Local.postman_environment.json",
    ]
    for relative in required:
        if not (ROOT / relative).is_file():
            fail(f"missing required file: {relative}")


def verify_serialized_files() -> None:
    """Parses every checked-in JSON, YAML, and XML configuration file."""
    for path in ROOT.rglob("*.json"):
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - verifier must report all parse failures.
            fail(f"invalid JSON {path.relative_to(ROOT)}: {exc}")
    for pattern in ("*.yml", "*.yaml"):
        for path in ROOT.rglob(pattern):
            try:
                yaml.safe_load(path.read_text(encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001
                fail(f"invalid YAML {path.relative_to(ROOT)}: {exc}")
    for path in ROOT.rglob("*.xml"):
        try:
            ET.parse(path)
        except Exception as exc:  # noqa: BLE001
            fail(f"invalid XML {path.relative_to(ROOT)}: {exc}")


def verify_text_hygiene() -> None:
    """Rejects tabs, trailing whitespace, placeholders, and generated output."""
    ignored_parts = {".git", ".gradle", "build"}
    text_suffixes = {
        ".java", ".gradle", ".md", ".yml", ".yaml", ".json", ".xml",
        ".sql", ".sh", ".bat", ".properties", ".example", ".gitignore",
        ".gitattributes", ".editorconfig", ".dockerignore",
    }
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(part in ignored_parts for part in path.parts):
            continue
        if path.suffix not in text_suffixes and path.name not in {
            "Dockerfile", "LICENSE", "gradlew",
        }:
            continue
        text = path.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if line.rstrip(" \t") != line:
                fail(f"trailing whitespace: {path.relative_to(ROOT)}:{line_number}")
            if "\t" in line and path.suffix not in {".bat"}:
                fail(f"tab character: {path.relative_to(ROOT)}:{line_number}")
        if re.search(r"\b(TODO|FIXME|XXX)\b", text):
            fail(f"unfinished marker in {path.relative_to(ROOT)}")


def verify_java_layout_and_javadocs() -> None:
    """Checks package paths and Javadoc immediately preceding public declarations."""
    source_root = ROOT / "src/main/java"
    declaration = re.compile(
        r"^\s*public\s+(?:(?:static|final|abstract|sealed|non-sealed)\s+)*"
        r"(?:class|interface|record|enum|@interface|[\w<>?,.\[\] ]+\s+\w+\s*\()"
    )
    for path in source_root.rglob("*.java"):
        text = path.read_text(encoding="utf-8")
        package_match = re.search(r"^package\s+([\w.]+);", text, re.MULTILINE)
        if not package_match:
            fail(f"missing package declaration: {path.relative_to(ROOT)}")
            continue
        expected = Path(*package_match.group(1).split(".")) / path.name
        actual = path.relative_to(source_root)
        if actual != expected:
            fail(f"package/path mismatch: {actual} != {expected}")

        lines = text.splitlines()
        for index, line in enumerate(lines):
            if not declaration.match(line):
                continue
            window = "\n".join(lines[max(0, index - 20):index])
            close = window.rfind("*/")
            opening = window.rfind("/**", 0, close + 2)
            tail = window[close + 2:] if close >= 0 else window
            intervening_declaration = re.search(
                r"(?:^|\n)\s*(?:public|protected|private)\s+", tail)
            if opening < 0 or close < opening or intervening_declaration:
                fail(f"missing Javadoc before public declaration: {actual}:{index + 1}")


def verify_architecture_contracts() -> None:
    """Checks the critical low-load, audit, replay, and CI design invariants."""
    expectations = {
        "build.gradle": [
            "id 'org.springframework.boot' version '4.1.0'",
            "org.testcontainers:testcontainers-postgresql",
            "testClassesDirs = sourceSets.test.output.classesDirs",
            "jacoco { toolVersion = '0.8.14' }",
            "toolVersion = '13.7.0'",
        ],
        "src/main/java/com/dxi/reconciliation/config/KafkaTopicConfiguration.java": [
            "LogAppendTime", "RETENTION_MS_CONFIG",
        ],
        "src/main/java/com/dxi/reconciliation/adapter/db/R2dbcEventProjectionStore.java": [
            "source.recordTimestamp().atZone(ZoneOffset.UTC).toLocalDate()",
            "source_event_observations",
            "ON CONFLICT (event_id) DO NOTHING",
        ],
        "src/main/java/com/dxi/reconciliation/service/ReplayService.java": [
            "Idempotency-Key is already associated with a different replay request",
            "recoverStale",
            "ReplayCheckpointStore",
            "RequestResolution",
            "Failed replay jobs require an explicit retry request",
        ],
        "src/main/java/com/dxi/reconciliation/adapter/db/R2dbcReplayJobStore.java": [
            "attempt_count = :executionAttempt",
            "Replay execution fence rejected a stale worker",
        ],
        "src/main/resources/db/migration/V1__create_reconciliation_schema.sql": [
            "CREATE TABLE business_event_ledger",
            "CREATE TABLE reconciliation_reports",
            "CREATE TABLE replay_jobs",
            "CREATE TRIGGER",
        ],
        "src/main/resources/db/migration/V2__strengthen_reconciliation_evidence.sql": [
            "CREATE TABLE source_event_observations",
            "CREATE TABLE replay_partition_checkpoints",
            "CREATE TABLE data_mutation_audit",
            "source_offsets JSONB",
            "ck_replay_progress",
            "data_mutation_audit_immutable",
            "uq_reconciliation_alert_report_channel",
            "idx_reconciliation_alerts_recovery",
        ],
        ".github/workflows/ci.yml": [
            "actions/checkout@v7", "actions/setup-java@v5",
            "gradle/actions/setup-gradle@v6", "./gradlew --no-daemon",
            "actions/upload-artifact@v7",
        ],
        "docker-compose.yml": [
            "postgres:18.4-alpine", "apache/kafka:4.3.1", "redis:8.8-alpine",
            "/var/lib/postgresql", "read_only: true",
        ],
        "gradlew": [
            "Gradle wrapper checksum mismatch", "org.gradle.wrapper.GradleWrapperMain",
            "497c8c2a7e5031f6aa847f88104aa80a93532ec32ee17bdb8d1d2f67a194a9c7",
        ],
        "gradle/wrapper/gradle-wrapper.properties": [
            "gradle-9.6.1-bin.zip",
            "distributionSha256Sum=9c0f7faeeb306cb14e4279a3e084ca6b596894089a0638e68a07c945a32c9e14",
            "validateDistributionUrl=true",
        ],
        "README.md": [
            "SOURCE_OBSERVATIONS_VS_UNIQUE_EVENTS",
            "/api/v1/replays/{jobId}/checkpoints",
            "SPRING_PROFILES_ACTIVE=local",
            "transactional alert outbox",
        ],
        "src/main/java/com/dxi/reconciliation/scheduler/AlertRecoveryScheduler.java": [
            "findRecoverable", "alertPublisher.retry",
        ],
    }
    for relative, needles in expectations.items():
        path = ROOT / relative
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for needle in needles:
            if needle not in text:
                fail(f"missing architecture marker {needle!r} in {relative}")

    tests = list((ROOT / "src/test/java").rglob("*Test.java"))
    if len(tests) < 15:
        fail(f"expected at least 15 test classes, found {len(tests)}")


def main() -> int:
    """Runs all structural verification checks."""
    verify_required_files()
    verify_serialized_files()
    verify_text_hygiene()
    verify_java_layout_and_javadocs()
    verify_architecture_contracts()
    if ERRORS:
        print("Static verification failed:", file=sys.stderr)
        for error in ERRORS:
            print(f" - {error}", file=sys.stderr)
        return 1
    java_files = len(list((ROOT / "src/main/java").rglob("*.java")))
    test_files = len(list((ROOT / "src/test/java").rglob("*Test.java")))
    print(f"Static verification passed: {java_files} main Java files, {test_files} test classes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
