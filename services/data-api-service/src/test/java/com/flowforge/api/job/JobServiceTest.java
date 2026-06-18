package com.flowforge.api.job;

import com.flowforge.common.domain.JobStatus;
import com.flowforge.common.event.DataProcessRequestedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class JobServiceTest {

    @Test
    void createJobPersistsRequestedJobAndPublishesKafkaEvent() {
        JobRepository jobRepository = new InMemoryJobRepository();
        KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        JobService jobService = new JobService(jobRepository, kafkaTemplate, "data.process.requested");

        CreateJobRequest request = new CreateJobRequest("test-user", "CSV", 1000, null, null);

        CreateJobResponse response = jobService.createJob(request);

        Optional<JobEntity> savedJob = jobRepository.findByJobId(response.jobId());
        assertThat(savedJob).isPresent();
        assertThat(savedJob.get().getStatus()).isEqualTo(JobStatus.REQUESTED);
        assertThat(savedJob.get().getRequester()).isEqualTo("test-user");
        verify(kafkaTemplate).send(eq("data.process.requested"), eq(response.jobId()), org.mockito.ArgumentMatchers.any(DataProcessRequestedEvent.class));
    }
}
