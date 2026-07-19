package com.dxi.reconciliation.adapter.kafka;

import com.dxi.reconciliation.domain.ReplayCommand;
import com.dxi.reconciliation.service.JsonCodec;
import com.dxi.reconciliation.service.ReplayService;
import org.springframework.kafka.annotation.KafkaListener;

/** Executes durable replay jobs in response to Kafka wake-up commands. */
public class ReplayCommandListener {

    private final JsonCodec jsonCodec;
    private final ReplayService replayService;

    /** Creates the replay command listener. */
    public ReplayCommandListener(JsonCodec jsonCodec, ReplayService replayService) {
        this.jsonCodec = jsonCodec;
        this.replayService = replayService;
    }

    /** Parses and executes a replay command before acknowledging it. */
    @KafkaListener(
            topics = "${app.kafka.replay-command-topic}",
            groupId = "reconciliation-replay-worker")
    public void listen(String payload) {
        ReplayCommand command = jsonCodec.read(payload, ReplayCommand.class);
        replayService.execute(command.jobId()).block();
    }
}
