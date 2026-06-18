package com.flowforge.worker.job;

import com.flowforge.common.domain.JobStatus;
import com.flowforge.common.event.DataProcessRequestedEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataProcessRequestedListenerTest {

    @Test
    void consumeRequestedEventMarksJobCompleted() {
        JobRepository jobRepository = new InMemoryJobRepository();
        JobEntity job = JobEntity.requested("JOB-test", "test-user", "CSV", 1000, "idem-1");
        jobRepository.save(job);
        DataProcessRequestedListener listener = new DataProcessRequestedListener(jobRepository);
        DataProcessRequestedEvent event = new DataProcessRequestedEvent(
                "EVT-test",
                "DATA_PROCESS_REQUESTED",
                "JOB-test",
                "idem-1",
                0,
                Instant.now(),
                Map.of("itemCount", 1000)
        );

        listener.handle(event);

        JobEntity updatedJob = jobRepository.findByJobId("JOB-test").orElseThrow();
        assertThat(updatedJob.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(updatedJob.getProcessedCount()).isEqualTo(1000);
    }
}
