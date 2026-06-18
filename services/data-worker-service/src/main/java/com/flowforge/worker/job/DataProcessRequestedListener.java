package com.flowforge.worker.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.common.event.DataProcessRequestedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Component
public class DataProcessRequestedListener {

    private static final Logger log = LoggerFactory.getLogger(DataProcessRequestedListener.class);

    private final JobRepository jobRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final FailedEventRepository failedEventRepository;
    private final KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String retryTopic;
    private final String dlqTopic;
    private final int maxRetryCount;
    private final StringRedisTemplate redisTemplate;
    private final MeterRegistry meterRegistry;

    public DataProcessRequestedListener(JobRepository jobRepository) {
        this(jobRepository, null, null, null, new ObjectMapper().findAndRegisterModules(), "data.process.retry", "data.process.dlq", 3, (StringRedisTemplate) null, null);
    }

    public DataProcessRequestedListener(
            JobRepository jobRepository,
            ProcessedEventRepository processedEventRepository,
            FailedEventRepository failedEventRepository,
            KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate,
            String retryTopic,
            String dlqTopic,
            int maxRetryCount
    ) {
        this(jobRepository, processedEventRepository, failedEventRepository, kafkaTemplate, new ObjectMapper().findAndRegisterModules(), retryTopic, dlqTopic, maxRetryCount, (StringRedisTemplate) null, null);
    }

    public DataProcessRequestedListener(
            JobRepository jobRepository,
            ProcessedEventRepository processedEventRepository,
            FailedEventRepository failedEventRepository,
            KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate,
            String retryTopic,
            String dlqTopic,
            int maxRetryCount,
            MeterRegistry meterRegistry
    ) {
        this(jobRepository, processedEventRepository, failedEventRepository, kafkaTemplate, new ObjectMapper().findAndRegisterModules(), retryTopic, dlqTopic, maxRetryCount, null, meterRegistry);
    }

    @Autowired
    public DataProcessRequestedListener(
            JobRepository jobRepository,
            ProcessedEventRepository processedEventRepository,
            FailedEventRepository failedEventRepository,
            KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topics.retry}") String retryTopic,
            @Value("${app.kafka.topics.dlq}") String dlqTopic,
            @Value("${app.worker.max-retry-count}") int maxRetryCount,
            ObjectProvider<StringRedisTemplate> redisTemplateProvider,
            ObjectProvider<MeterRegistry> meterRegistryProvider
    ) {
        this(
                jobRepository,
                processedEventRepository,
                failedEventRepository,
                kafkaTemplate,
                objectMapper,
                retryTopic,
                dlqTopic,
                maxRetryCount,
                redisTemplateProvider.getIfAvailable(),
                meterRegistryProvider.getIfAvailable()
        );
    }

    private DataProcessRequestedListener(
            JobRepository jobRepository,
            ProcessedEventRepository processedEventRepository,
            FailedEventRepository failedEventRepository,
            KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate,
            ObjectMapper objectMapper,
            String retryTopic,
            String dlqTopic,
            int maxRetryCount,
            StringRedisTemplate redisTemplate,
            MeterRegistry meterRegistry
    ) {
        this.jobRepository = jobRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedEventRepository = failedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.retryTopic = retryTopic;
        this.dlqTopic = dlqTopic;
        this.maxRetryCount = maxRetryCount;
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = {"${app.kafka.topics.requested}", "${app.kafka.topics.retry}"}, groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void handle(DataProcessRequestedEvent event) {
        log.info("Received data process request jobId={} eventId={}", event.jobId(), event.eventId());
        if (processedEventRepository != null && processedEventRepository.existsByEventId(event.eventId())) {
            log.info("Skip already processed event jobId={} eventId={}", event.jobId(), event.eventId());
            return;
        }

        JobEntity job = jobRepository.findByJobId(event.jobId())
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + event.jobId()));

        try {
            process(event, job);
        } catch (RuntimeException exception) {
            handleFailure(event, job, exception);
        }
    }

    private void process(DataProcessRequestedEvent event, JobEntity job) {
        job.markProcessing();
        cacheProgress(job.getJobId(), "50");
        if (isForceFail(event)) {
            throw new IllegalStateException("Forced failure for test event");
        }
        job.markCompleted();
        jobRepository.save(job);
        if (processedEventRepository != null) {
            processedEventRepository.save(ProcessedEventEntity.processed(event.eventId(), event.jobId(), event.eventType()));
        }
        cacheProgress(job.getJobId(), "100");
        incrementCounter("flowforge.worker.jobs.completed");
        log.info("Completed data process request jobId={} eventId={}", event.jobId(), event.eventId());
    }

    private void handleFailure(DataProcessRequestedEvent event, JobEntity job, RuntimeException exception) {
        if (event.retryCount() < maxRetryCount) {
            job.markRetrying();
            jobRepository.save(job);
            kafkaTemplate.send(retryTopic, event.jobId(), retryEvent(event));
            incrementCounter("flowforge.worker.jobs.retried");
            log.warn("Published retry event jobId={} eventId={} retryCount={}", event.jobId(), event.eventId(), event.retryCount() + 1);
            return;
        }

        job.markDlq();
        jobRepository.save(job);
        String payload = toJson(event);
        FailedEventEntity failedEvent = failedEventRepository.findByEventId(event.eventId())
                .map(existingEvent -> {
                    existingEvent.markDlq(dlqTopic, exception.getMessage(), payload, event.retryCount());
                    return existingEvent;
                })
                .orElseGet(() -> FailedEventEntity.dlq(
                        event.eventId(),
                        event.jobId(),
                        dlqTopic,
                        exception.getMessage(),
                        payload,
                        event.retryCount()
                ));
        failedEventRepository.save(failedEvent);
        kafkaTemplate.send(dlqTopic, event.jobId(), event);
        cacheProgress(job.getJobId(), "0");
        incrementCounter("flowforge.worker.jobs.dlq");
        log.error("Moved event to DLQ jobId={} eventId={}", event.jobId(), event.eventId(), exception);
    }

    private DataProcessRequestedEvent retryEvent(DataProcessRequestedEvent event) {
        Map<String, Object> payload = new HashMap<>(event.payload());
        return new DataProcessRequestedEvent(
                event.eventId(),
                event.eventType(),
                event.jobId(),
                event.idempotencyKey(),
                event.retryCount() + 1,
                Instant.now(),
                payload
        );
    }

    private boolean isForceFail(DataProcessRequestedEvent event) {
        Object forceFail = event.payload().get("forceFail");
        if (forceFail instanceof Boolean value) {
            return value;
        }
        Object nestedPayload = event.payload().get("payload");
        if (nestedPayload instanceof Map<?, ?> payload) {
            Object nestedForceFail = payload.get("forceFail");
            return nestedForceFail instanceof Boolean value && value;
        }
        return false;
    }

    private String toJson(DataProcessRequestedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize event", exception);
        }
    }

    private void cacheProgress(String jobId, String progress) {
        if (redisTemplate == null) {
            return;
        }
        redisTemplate.opsForValue().set("job:" + jobId + ":progress", progress);
    }

    private void incrementCounter(String counterName) {
        if (meterRegistry == null) {
            return;
        }
        meterRegistry.counter(counterName).increment();
    }
}
