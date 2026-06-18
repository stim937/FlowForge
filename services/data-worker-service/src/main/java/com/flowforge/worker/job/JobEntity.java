package com.flowforge.worker.job;

import com.flowforge.common.domain.JobStatus;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "jobs")
public class JobEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false, unique = true, length = 100)
    private String jobId;

    @Column(nullable = false, length = 100)
    private String requester;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private JobStatus status;

    @Column(name = "data_type", nullable = false, length = 30)
    private String dataType;

    @Column(name = "item_count", nullable = false)
    private int itemCount;

    @Column(name = "processed_count", nullable = false)
    private int processedCount;

    @Column(name = "failed_count", nullable = false)
    private int failedCount;

    @Column(name = "idempotency_key", length = 200)
    private String idempotencyKey;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected JobEntity() {
    }

    private JobEntity(String jobId, String requester, String dataType, int itemCount, String idempotencyKey) {
        this.jobId = jobId;
        this.requester = requester;
        this.status = JobStatus.REQUESTED;
        this.dataType = dataType;
        this.itemCount = itemCount;
        this.processedCount = 0;
        this.failedCount = 0;
        this.idempotencyKey = idempotencyKey;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public static JobEntity requested(String jobId, String requester, String dataType, int itemCount, String idempotencyKey) {
        return new JobEntity(jobId, requester, dataType, itemCount, idempotencyKey);
    }

    public void markProcessing() {
        this.status = JobStatus.PROCESSING;
        this.updatedAt = Instant.now();
    }

    public void markCompleted() {
        this.status = JobStatus.COMPLETED;
        this.processedCount = this.itemCount;
        this.failedCount = 0;
        this.updatedAt = Instant.now();
    }

    public String getJobId() {
        return jobId;
    }

    public JobStatus getStatus() {
        return status;
    }

    public int getItemCount() {
        return itemCount;
    }

    public int getProcessedCount() {
        return processedCount;
    }
}
