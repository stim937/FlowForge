package com.flowforge.api.job;

import com.flowforge.api.common.ApiResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    ApiResponse<CreateJobResponse> createJob(@Valid @RequestBody CreateJobRequest request) {
        return ApiResponse.success(jobService.createJob(request));
    }

    @GetMapping("/{jobId}")
    ApiResponse<JobDetailResponse> getJob(@PathVariable String jobId) {
        return ApiResponse.success(jobService.getJob(jobId));
    }
}
