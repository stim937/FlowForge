package com.flowforge.api.admin;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "failed_events")
public class FailedEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @Column(name = "job_id", nullable = false, length = 100)
    private String jobId;

    @Column(name = "topic_name", nullable = false, length = 200)
    private String topicName;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(nullable = false, length = 30)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected FailedEventEntity() {
    }

    private FailedEventEntity(String eventId, String jobId, String topicName, String errorMessage, String payload, int retryCount) {
        this.eventId = eventId;
        this.jobId = jobId;
        this.topicName = topicName;
        this.errorMessage = errorMessage;
        this.payload = payload;
        this.retryCount = retryCount;
        this.status = "DLQ";
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public static FailedEventEntity dlq(String eventId, String jobId, String topicName, String errorMessage, String payload, int retryCount) {
        return new FailedEventEntity(eventId, jobId, topicName, errorMessage, payload, retryCount);
    }

    public void markRetrying() {
        this.status = "RETRYING";
        this.updatedAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getPayload() {
        return payload;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
