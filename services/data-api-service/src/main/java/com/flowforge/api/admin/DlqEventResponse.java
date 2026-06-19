package com.flowforge.api.admin;

import java.time.Instant;

public record DlqEventResponse(
        String eventId,
        String jobId,
        String topicName,
        String errorMessage,
        int retryCount,
        String status,
        Instant createdAt,
        Instant updatedAt
) {
    static DlqEventResponse from(FailedEventEntity event) {
        return new DlqEventResponse(
                event.getEventId(),
                event.getJobId(),
                event.getTopicName(),
                event.getErrorMessage(),
                event.getRetryCount(),
                event.getStatus(),
                event.getCreatedAt(),
                event.getUpdatedAt()
        );
    }
}
