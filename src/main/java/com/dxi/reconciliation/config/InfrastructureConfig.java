package com.dxi.reconciliation.config;

import com.dxi.reconciliation.adapter.db.R2dbcAlertAuditStore;
import com.dxi.reconciliation.adapter.db.R2dbcDailyAggregateRepairStore;
import com.dxi.reconciliation.adapter.db.R2dbcDailyMetricsStore;
import com.dxi.reconciliation.adapter.db.R2dbcEventProjectionStore;
import com.dxi.reconciliation.adapter.db.R2dbcReplayCheckpointStore;
import com.dxi.reconciliation.adapter.db.R2dbcReplayJobStore;
import com.dxi.reconciliation.adapter.db.R2dbcReportStore;
import com.dxi.reconciliation.adapter.kafka.AuditedAlertPublisher;
import com.dxi.reconciliation.adapter.kafka.BusinessEventListener;
import com.dxi.reconciliation.adapter.kafka.DeadLetterListener;
import com.dxi.reconciliation.adapter.kafka.KafkaEventPublisher;
import com.dxi.reconciliation.adapter.kafka.KafkaOffsetEventCountProvider;
import com.dxi.reconciliation.adapter.kafka.KafkaReplayCommandPublisher;
import com.dxi.reconciliation.adapter.kafka.KafkaReplayExecutor;
import com.dxi.reconciliation.adapter.kafka.ReplayCommandListener;
import com.dxi.reconciliation.adapter.redis.RedisDistributedLockService;
import com.dxi.reconciliation.port.AlertAuditStore;
import com.dxi.reconciliation.port.AlertPublisher;
import com.dxi.reconciliation.port.DailyAggregateRepairStore;
import com.dxi.reconciliation.port.DailyMetricsStore;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.EventProjectionStore;
import com.dxi.reconciliation.port.EventPublisher;
import com.dxi.reconciliation.port.KafkaEventCountProvider;
import com.dxi.reconciliation.port.ReplayCheckpointStore;
import com.dxi.reconciliation.port.ReplayCommandPublisher;
import com.dxi.reconciliation.port.ReplayExecutor;
import com.dxi.reconciliation.port.ReplayJobStore;
import com.dxi.reconciliation.port.ReportStore;
import com.dxi.reconciliation.service.BusinessEventProcessor;
import com.dxi.reconciliation.service.JsonCodec;
import com.dxi.reconciliation.service.ReconciliationService;
import com.dxi.reconciliation.service.ReplayService;
import com.dxi.reconciliation.service.ReportQueryService;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Validator;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.reactive.function.client.WebClient;
import tools.jackson.databind.ObjectMapper;

/** Wires application services to production infrastructure adapters. */
@Configuration(proxyBeanMethods = false)
public class InfrastructureConfig {

    /** Provides a UTC clock for deterministic date boundaries and tests. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /** Provides the shared JSON boundary. */
    @Bean
    public JsonCodec jsonCodec(ObjectMapper objectMapper) {
        return new JsonCodec(objectMapper);
    }

    /** Provides the reactive transaction operator. */
    @Bean
    public TransactionalOperator transactionalOperator(ReactiveTransactionManager manager) {
        return TransactionalOperator.create(manager);
    }

    /** Provides the Kafka administrative client. */
    @Bean(destroyMethod = "close")
    public Admin kafkaAdmin(KafkaProperties kafkaProperties) {
        return Admin.create(kafkaProperties.buildAdminProperties());
    }

    /** Provides a dedicated manually assigned consumer for replay scans. */
    @Bean(destroyMethod = "close")
    public Consumer<String, String> replayKafkaConsumer(KafkaProperties kafkaProperties) {
        Map<String, Object> configuration = new HashMap<>(
                kafkaProperties.buildConsumerProperties());
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "reconciliation-replay-scanner");
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(configuration, new StringDeserializer(), new StringDeserializer());
    }

    /** Provides a reusable non-blocking HTTP client. */
    @Bean
    public WebClient webClient(WebClient.Builder builder) {
        return builder.build();
    }

    /** Provides the event projection repository. */
    @Bean
    public EventProjectionStore eventProjectionStore(
            DatabaseClient client,
            TransactionalOperator transactions) {
        return new R2dbcEventProjectionStore(client, transactions);
    }

    /** Provides compact metrics access. */
    @Bean
    public DailyMetricsStore dailyMetricsStore(DatabaseClient client) {
        return new R2dbcDailyMetricsStore(client);
    }

    /** Provides immutable report persistence. */
    @Bean
    public ReportStore reportStore(
            DatabaseClient client,
            JsonCodec jsonCodec,
            TransactionalOperator transactions,
            AppProperties properties) {
        return new R2dbcReportStore(client, jsonCodec, transactions, properties);
    }

    /** Provides alert audit persistence. */
    @Bean
    public AlertAuditStore alertAuditStore(DatabaseClient client) {
        return new R2dbcAlertAuditStore(client);
    }

    /** Provides durable replay-job persistence. */
    @Bean
    public ReplayJobStore replayJobStore(DatabaseClient client) {
        return new R2dbcReplayJobStore(client);
    }

    /** Provides durable per-partition replay checkpoints. */
    @Bean
    public ReplayCheckpointStore replayCheckpointStore(DatabaseClient client) {
        return new R2dbcReplayCheckpointStore(client);
    }

    /** Provides exact aggregate repair. */
    @Bean
    public DailyAggregateRepairStore dailyAggregateRepairStore(
            DatabaseClient client,
            TransactionalOperator transactions) {
        return new R2dbcDailyAggregateRepairStore(client, transactions);
    }

    /** Provides token-owned Redis distributed locking. */
    @Bean
    public DistributedLockService distributedLockService(ReactiveStringRedisTemplate redis) {
        return new RedisDistributedLockService(redis);
    }

    /** Provides offset-based Kafka event counting. */
    @Bean
    public KafkaEventCountProvider kafkaEventCountProvider(Admin admin, AppProperties properties) {
        return new KafkaOffsetEventCountProvider(admin, properties);
    }

    /** Provides public event publication. */
    @Bean
    public EventPublisher eventPublisher(
            KafkaTemplate<String, String> template,
            JsonCodec jsonCodec,
            AppProperties properties) {
        return new KafkaEventPublisher(template, jsonCodec, properties);
    }

    /** Provides replay command publication. */
    @Bean
    public ReplayCommandPublisher replayCommandPublisher(
            KafkaTemplate<String, String> template,
            JsonCodec jsonCodec,
            AppProperties properties) {
        return new KafkaReplayCommandPublisher(template, jsonCodec, properties);
    }

    /** Provides the replay source scanner. */
    @Bean
    public ReplayExecutor replayExecutor(
            Consumer<String, String> replayKafkaConsumer,
            BusinessEventProcessor processor,
            ReplayCheckpointStore checkpointStore,
            ReplayJobStore jobStore,
            AppProperties properties,
            Clock clock) {
        return new KafkaReplayExecutor(
                replayKafkaConsumer, processor, checkpointStore, jobStore, properties, clock);
    }

    /** Provides audited mismatch alert publication. */
    @Bean
    public AlertPublisher alertPublisher(
            KafkaTemplate<String, String> template,
            WebClient webClient,
            AlertAuditStore auditStore,
            JsonCodec jsonCodec,
            AppProperties properties,
            Clock clock) {
        return new AuditedAlertPublisher(
                template, webClient, auditStore, jsonCodec, properties, clock);
    }

    /** Provides event validation and projection orchestration. */
    @Bean
    public BusinessEventProcessor businessEventProcessor(
            Validator validator,
            EventProjectionStore store,
            JsonCodec jsonCodec,
            Clock clock) {
        return new BusinessEventProcessor(validator, store, jsonCodec, clock);
    }

    /** Provides daily reconciliation orchestration. */
    @Bean
    public ReconciliationService reconciliationService(
            KafkaEventCountProvider kafkaCounts,
            DailyMetricsStore metricsStore,
            ReportStore reportStore,
            AlertPublisher alertPublisher,
            DistributedLockService locks,
            AppProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        return new ReconciliationService(
                kafkaCounts, metricsStore, reportStore, alertPublisher,
                locks, properties, clock, meterRegistry);
    }

    /** Provides report queries. */
    @Bean
    public ReportQueryService reportQueryService(ReportStore reportStore) {
        return new ReportQueryService(reportStore);
    }

    /** Provides durable replay orchestration. */
    @Bean
    public ReplayService replayService(
            ReplayJobStore jobStore,
            ReplayCheckpointStore checkpointStore,
            ReplayCommandPublisher commandPublisher,
            ReplayExecutor replayExecutor,
            DailyAggregateRepairStore repairStore,
            ReconciliationService reconciliationService,
            DistributedLockService locks,
            AppProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        return new ReplayService(
                jobStore, checkpointStore, commandPublisher, replayExecutor, repairStore,
                reconciliationService, locks, properties, clock, meterRegistry);
    }

    /** Registers the source-topic listener. */
    @Bean
    public BusinessEventListener businessEventListener(
            BusinessEventProcessor processor,
            AppProperties properties) {
        return new BusinessEventListener(processor, properties);
    }

    /** Registers the replay command listener. */
    @Bean
    public ReplayCommandListener replayCommandListener(
            JsonCodec jsonCodec,
            ReplayService replayService) {
        return new ReplayCommandListener(jsonCodec, replayService);
    }

    /** Registers a safe dead-letter observer. */
    @Bean
    public DeadLetterListener deadLetterListener() {
        return new DeadLetterListener();
    }
}
