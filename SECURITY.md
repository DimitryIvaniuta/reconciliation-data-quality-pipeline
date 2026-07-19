# Security policy

Report vulnerabilities privately to the repository owner. Do not open public issues containing credentials, exploit details, customer data, production offsets, or topology.

## Required production controls

- Put the service behind an identity-aware gateway or configure it as an OAuth2 resource server; the included API-key filter is an intentionally small portfolio boundary.
- Supply `APP_SECURITY_API_KEY` from a managed secret store and rotate it. Never deploy with the `local` Spring profile.
- Enable TLS/SASL and least-privilege ACLs for Kafka, TLS/ACLs for Redis, and TLS plus separate migration/runtime roles for PostgreSQL.
- Restrict Actuator and replay APIs to trusted networks and operator identities.
- Set `application_name` on PostgreSQL clients so `data_mutation_audit` identifies administrative changes.
- Treat report partition offsets, business keys, event payloads, and replay ranges as operationally sensitive.
- Validate the Gradle wrapper/distribution checksums and review dependency graph changes before merging.
- Preserve append-only reports and mutation audit according to the organization’s evidence-retention policy.
