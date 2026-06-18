package com.flowforge.common.event;

import java.time.Instant;
import java.util.Map;

public record DataProcessRequestedEvent(
        String eventId,
        String eventType,
        String jobId,
        String idempotencyKey,
        int retryCount,
        Instant occurredAt,
        Map<String, Object> payload
) {
}
