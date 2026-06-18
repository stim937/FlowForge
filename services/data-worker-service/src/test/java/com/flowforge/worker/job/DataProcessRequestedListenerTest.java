package com.flowforge.worker.job;

import com.flowforge.common.domain.JobStatus;
import com.flowforge.common.event.DataProcessRequestedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class DataProcessRequestedListenerTest {

    @Test
    void consumeRequestedEventMarksJobCompleted() {
        JobRepository jobRepository = new InMemoryJobRepository();
        ProcessedEventRepository processedEventRepository = new InMemoryProcessedEventRepository();
        FailedEventRepository failedEventRepository = new InMemoryFailedEventRepository();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        JobEntity job = JobEntity.requested("JOB-test", "test-user", "CSV", 1000, "idem-1");
        jobRepository.save(job);
        DataProcessRequestedListener listener = new DataProcessRequestedListener(
                jobRepository,
                processedEventRepository,
                failedEventRepository,
                kafkaTemplate,
                "data.process.retry",
                "data.process.dlq",
                3,
                meterRegistry
        );
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
        assertThat(processedEventRepository.existsByEventId("EVT-test")).isTrue();
        assertThat(meterRegistry.counter("flowforge.worker.jobs.completed").count()).isEqualTo(1);
        verify(kafkaTemplate, never()).send(eq("data.process.retry"), any(), any());
        verify(kafkaTemplate, never()).send(eq("data.process.dlq"), any(), any());
    }

    @Test
    void duplicateEventIsIgnoredWhenEventAlreadyProcessed() {
        JobRepository jobRepository = new InMemoryJobRepository();
        ProcessedEventRepository processedEventRepository = new InMemoryProcessedEventRepository();
        FailedEventRepository failedEventRepository = new InMemoryFailedEventRepository();
        KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        JobEntity job = JobEntity.requested("JOB-test", "test-user", "CSV", 1000, "idem-1");
        job.markCompleted();
        jobRepository.save(job);
        processedEventRepository.save(ProcessedEventEntity.processed("EVT-test", "JOB-test", "DATA_PROCESS_REQUESTED"));
        DataProcessRequestedListener listener = new DataProcessRequestedListener(
                jobRepository,
                processedEventRepository,
                failedEventRepository,
                kafkaTemplate,
                "data.process.retry",
                "data.process.dlq",
                3
        );

        listener.handle(requestEvent("EVT-test", "JOB-test", 0, false));

        JobEntity updatedJob = jobRepository.findByJobId("JOB-test").orElseThrow();
        assertThat(updatedJob.getStatus()).isEqualTo(JobStatus.COMPLETED);
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    void failedEventUnderRetryLimitPublishesRetryEvent() {
        JobRepository jobRepository = new InMemoryJobRepository();
        ProcessedEventRepository processedEventRepository = new InMemoryProcessedEventRepository();
        FailedEventRepository failedEventRepository = new InMemoryFailedEventRepository();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        jobRepository.save(JobEntity.requested("JOB-test", "test-user", "CSV", 1000, "idem-1"));
        DataProcessRequestedListener listener = new DataProcessRequestedListener(
                jobRepository,
                processedEventRepository,
                failedEventRepository,
                kafkaTemplate,
                "data.process.retry",
                "data.process.dlq",
                3,
                meterRegistry
        );

        listener.handle(requestEvent("EVT-retry", "JOB-test", 1, true));

        JobEntity updatedJob = jobRepository.findByJobId("JOB-test").orElseThrow();
        assertThat(updatedJob.getStatus()).isEqualTo(JobStatus.RETRYING);
        assertThat(meterRegistry.counter("flowforge.worker.jobs.retried").count()).isEqualTo(1);
        verify(kafkaTemplate).send(eq("data.process.retry"), eq("JOB-test"), any(DataProcessRequestedEvent.class));
        assertThat(failedEventRepository.findByEventId("EVT-retry")).isEmpty();
    }

    @Test
    void failedEventAtRetryLimitMovesToDlq() {
        JobRepository jobRepository = new InMemoryJobRepository();
        ProcessedEventRepository processedEventRepository = new InMemoryProcessedEventRepository();
        FailedEventRepository failedEventRepository = new InMemoryFailedEventRepository();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        jobRepository.save(JobEntity.requested("JOB-test", "test-user", "CSV", 1000, "idem-1"));
        DataProcessRequestedListener listener = new DataProcessRequestedListener(
                jobRepository,
                processedEventRepository,
                failedEventRepository,
                kafkaTemplate,
                "data.process.retry",
                "data.process.dlq",
                3,
                meterRegistry
        );

        listener.handle(requestEvent("EVT-dlq", "JOB-test", 3, true));

        JobEntity updatedJob = jobRepository.findByJobId("JOB-test").orElseThrow();
        assertThat(updatedJob.getStatus()).isEqualTo(JobStatus.DLQ);
        assertThat(failedEventRepository.findByEventId("EVT-dlq")).isPresent();
        assertThat(meterRegistry.counter("flowforge.worker.jobs.dlq").count()).isEqualTo(1);
        verify(kafkaTemplate).send(eq("data.process.dlq"), eq("JOB-test"), any(DataProcessRequestedEvent.class));
    }

    private DataProcessRequestedEvent requestEvent(String eventId, String jobId, int retryCount, boolean forceFail) {
        return new DataProcessRequestedEvent(
                eventId,
                "DATA_PROCESS_REQUESTED",
                jobId,
                "idem-1",
                retryCount,
                Instant.now(),
                Map.of("itemCount", 1000, "forceFail", forceFail)
        );
    }
}
