package com.flowforge.worker.job;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "processed_events")
public class ProcessedEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true, length = 100)
    private String eventId;

    @Column(name = "job_id", nullable = false, length = 100)
    private String jobId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEventEntity() {
    }

    private ProcessedEventEntity(String eventId, String jobId, String eventType) {
        this.eventId = eventId;
        this.jobId = jobId;
        this.eventType = eventType;
        this.processedAt = Instant.now();
    }

    public static ProcessedEventEntity processed(String eventId, String jobId, String eventType) {
        return new ProcessedEventEntity(eventId, jobId, eventType);
    }

    public String getEventId() {
        return eventId;
    }
}
