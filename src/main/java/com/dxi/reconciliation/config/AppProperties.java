package com.dxi.reconciliation.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Strongly typed operational configuration for the service. */
@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @Valid @NotNull Security security,
        @Valid @NotNull Reconciliation reconciliation,
        @Valid @NotNull Kafka kafka,
        @Valid @NotNull Alert alert) {

    /** Ensures replay requests cannot exceed the retained Kafka source window. */
    @AssertTrue(message = "Kafka source retention must cover the maximum replay range")
    public boolean isReplayRetentionSufficient() {
        return kafka == null
                || reconciliation == null
                || kafka.sourceRetention() == null
                || kafka.sourceRetention().compareTo(
                        Duration.ofDays(reconciliation.maximumReplayDays())) >= 0;
    }

    /** API authentication settings. */
    public record Security(@NotBlank String apiKey) { }

    /** Scheduling, locking, timeout, tolerance, and replay safety settings. */
    public record Reconciliation(
            @NotBlank String scheduleCron,
            @NotBlank String zone,
            @NotNull Duration settlementDelay,
            @NotNull Duration queryTimeout,
            @NotNull Duration lockTtl,
            @NotNull Duration commandRedispatchInterval,
            @NotNull Duration alertRedispatchInterval,
            @NotNull Duration replayStaleTimeout,
            @DecimalMin("0.0000") @NotNull BigDecimal amountTolerance,
            @Min(1) @Max(365) int maximumReplayDays,
            @Min(1) @Max(20) int maximumReplayAttempts,
            @Min(1) @Max(10_000) int replayCheckpointInterval) { }

    /** Kafka topics and bounded administrative operation settings. */
    public record Kafka(
            @NotBlank String sourceTopic,
            @NotBlank String replayCommandTopic,
            @NotBlank String alertTopic,
            @NotBlank String deadLetterTopic,
            @Min(1) int partitions,
            @Min(1) int replicationFactor,
            @NotNull Duration sourceRetention,
            @NotNull Duration adminTimeout,
            @NotNull Duration replayPollTimeout) { }

    /** Alert transport settings. */
    public record Alert(URI webhookUrl) { }
}
