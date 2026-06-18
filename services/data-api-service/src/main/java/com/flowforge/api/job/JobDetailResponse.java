package com.flowforge.api.job;

import com.flowforge.common.domain.JobStatus;

import java.time.Instant;

public record JobDetailResponse(
        String jobId,
        String requester,
        JobStatus status,
        String dataType,
        int itemCount,
        int processedCount,
        int failedCount,
        Instant createdAt,
        Instant updatedAt
) {
    static JobDetailResponse from(JobEntity job) {
        return new JobDetailResponse(
                job.getJobId(),
                job.getRequester(),
                job.getStatus(),
                job.getDataType(),
                job.getItemCount(),
                job.getProcessedCount(),
                job.getFailedCount(),
                job.getCreatedAt(),
                job.getUpdatedAt()
        );
    }
}
