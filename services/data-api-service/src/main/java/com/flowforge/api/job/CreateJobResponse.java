package com.flowforge.api.job;

import com.flowforge.common.domain.JobStatus;

public record CreateJobResponse(String jobId, JobStatus status, String message) {
}
