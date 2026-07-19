# GitHub repository metadata

**Repository name:** `reconciliation-data-quality-pipeline`

**Description:** Reactive reconciliation and data-quality monitoring with independent Kafka/PostgreSQL evidence, immutable reports, transactional alert recovery, and resumable idempotent replay.

**Topics:** `java-25`, `spring-boot-4`, `webflux`, `r2dbc`, `postgresql`, `flyway`, `apache-kafka`, `kraft`, `redis`, `reconciliation`, `data-quality`, `event-driven`, `testcontainers`, `gradle`

## Publish the initialized repository

After authenticating GitHub CLI, run from this directory:

```bash
gh repo create reconciliation-data-quality-pipeline \
  --public \
  --description "Reactive reconciliation and data-quality monitoring with independent Kafka/PostgreSQL evidence, immutable reports, transactional alert recovery, and resumable idempotent replay." \
  --source . \
  --remote origin \
  --push

gh repo edit --add-topic java-25 --add-topic spring-boot-4 \
  --add-topic webflux --add-topic r2dbc --add-topic postgresql \
  --add-topic flyway --add-topic apache-kafka --add-topic kraft \
  --add-topic redis --add-topic reconciliation --add-topic data-quality \
  --add-topic event-driven --add-topic testcontainers --add-topic gradle
```
