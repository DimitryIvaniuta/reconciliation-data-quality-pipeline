# Contributing

Use Java 25 and Gradle 9.6.1. Keep domain services reactive, isolate blocking Kafka administration on bounded elastic schedulers, add Javadoc for public types and methods, and include tests for changed behavior.

Run `./scripts/verify-project.sh` before opening a pull request. Database changes must use a new Flyway migration and document operational impact.
