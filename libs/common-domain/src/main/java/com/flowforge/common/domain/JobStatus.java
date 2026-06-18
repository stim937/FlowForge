package com.flowforge.common.domain;

public enum JobStatus {
    REQUESTED,
    PROCESSING,
    COMPLETED,
    FAILED,
    PARTIAL_FAILED,
    RETRYING,
    DLQ,
    CANCELLED
}
