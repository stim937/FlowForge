package com.flowforge.api.job;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record CreateJobRequest(
        @NotBlank String requester,
        @NotBlank String dataType,
        @Min(1) int itemCount,
        String idempotencyKey,
        Map<String, Object> payload
) {
}
