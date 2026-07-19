package com.dxi.reconciliation.web;

import com.dxi.reconciliation.domain.ReplayJob;
import com.dxi.reconciliation.domain.ReplayPartitionCheckpoint;
import com.dxi.reconciliation.service.ReplayService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Durable replay/backfill request and status API. */
@RestController
@RequestMapping("/api/v1/replays")
public class ReplayController {

    private final ReplayService replayService;

    /** Creates the replay controller. */
    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    /** Creates or returns an idempotent asynchronous replay request. */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<ReplayJob> request(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestHeader("X-Requested-By") String requestedBy,
            @Valid @RequestBody ReplayRequest request,
            ServerWebExchange exchange) {
        return replayService.request(
                idempotencyKey,
                request.fromDate(),
                request.toDate(),
                request.dryRun(),
                requestedBy,
                RequestContext.correlationId(exchange));
    }

    /** Returns durable progress for each bounded Kafka partition range. */
    @GetMapping("/{jobId}/checkpoints")
    public Flux<ReplayPartitionCheckpoint> checkpoints(@PathVariable UUID jobId) {
        return replayService.checkpoints(jobId);
    }

    /** Explicitly retries a failed job while preserving existing checkpoints. */
    @PostMapping("/{jobId}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<ReplayJob> retry(@PathVariable UUID jobId) {
        return replayService.retry(jobId);
    }

    /** Returns durable replay lifecycle and counters. */
    @GetMapping("/{jobId}")
    public Mono<ReplayJob> get(@PathVariable UUID jobId) {
        return replayService.get(jobId);
    }
}
