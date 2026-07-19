package com.dxi.reconciliation.adapter.kafka;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.AlertAudit;
import com.dxi.reconciliation.domain.AlertStatus;
import com.dxi.reconciliation.domain.ReconciliationReport;
import com.dxi.reconciliation.port.AlertAuditStore;
import com.dxi.reconciliation.port.AlertPublisher;
import com.dxi.reconciliation.service.JsonCodec;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Delivers durable mismatch-alert outbox entries through Kafka and an optional webhook. */
public class AuditedAlertPublisher implements AlertPublisher {

    private static final String KAFKA_CHANNEL = "KAFKA";
    private static final String WEBHOOK_CHANNEL = "WEBHOOK";

    private final KafkaTemplate<String, String> template;
    private final WebClient webClient;
    private final AlertAuditStore auditStore;
    private final JsonCodec jsonCodec;
    private final AppProperties properties;
    private final Clock clock;

    /** Creates the durable multi-channel alert publisher. */
    public AuditedAlertPublisher(
            KafkaTemplate<String, String> template,
            WebClient webClient,
            AlertAuditStore auditStore,
            JsonCodec jsonCodec,
            AppProperties properties,
            Clock clock) {
        this.template = template;
        this.webClient = webClient;
        this.auditStore = auditStore;
        this.jsonCodec = jsonCodec;
        this.properties = properties;
        this.clock = clock;
    }

    /** Delivers each configured report/channel outbox entry. */
    @Override
    public Mono<Void> publish(ReconciliationReport report) {
        return Flux.fromIterable(configuredChannels())
                .concatMap(channel -> auditStore.createOrGet(pendingAudit(report, channel))
                        .flatMap(audit -> retry(audit, report)))
                .then();
    }

    /** Retries one existing outbox entry unless it is already delivered. */
    @Override
    public Mono<Void> retry(AlertAudit audit, ReconciliationReport report) {
        if (audit.status() == AlertStatus.DELIVERED) {
            return Mono.empty();
        }
        Mono<Void> delivery = switch (audit.channel()) {
            case KAFKA_CHANNEL -> Mono.fromFuture(template.send(
                            properties.kafka().alertTopic(),
                            audit.alertId().toString(),
                            jsonCodec.write(report)))
                    .then();
            case WEBHOOK_CHANNEL -> webhookDelivery(report);
            default -> Mono.error(new IllegalArgumentException(
                    "Unsupported alert channel: " + audit.channel()));
        };
        return delivery
                .timeout(properties.reconciliation().queryTimeout())
                .then(auditStore.complete(
                        audit.alertId(), AlertStatus.DELIVERED, null, clock.instant()))
                .onErrorResume(exception -> auditStore.complete(
                                audit.alertId(),
                                AlertStatus.FAILED,
                                safeMessage(exception),
                                clock.instant())
                        .then(Mono.error(exception)));
    }

    private Mono<Void> webhookDelivery(ReconciliationReport report) {
        if (properties.alert().webhookUrl() == null) {
            return Mono.error(new IllegalStateException("Webhook alert channel is disabled"));
        }
        return webClient.post()
                .uri(properties.alert().webhookUrl())
                .header("Idempotency-Key", report.reportId().toString())
                .bodyValue(report)
                .retrieve()
                .toBodilessEntity()
                .then();
    }

    private List<String> configuredChannels() {
        List<String> channels = new ArrayList<>();
        channels.add(KAFKA_CHANNEL);
        if (properties.alert().webhookUrl() != null) {
            channels.add(WEBHOOK_CHANNEL);
        }
        return List.copyOf(channels);
    }

    private AlertAudit pendingAudit(ReconciliationReport report, String channel) {
        Instant now = clock.instant();
        return new AlertAudit(
                UUID.randomUUID(), report.reportId(), channel, AlertStatus.PENDING,
                0, null, now, now);
    }

    private String safeMessage(Throwable exception) {
        String message = exception.getMessage() == null
                ? exception.getClass().getSimpleName()
                : exception.getMessage();
        return message.length() <= 2_000 ? message : message.substring(0, 2_000);
    }
}
