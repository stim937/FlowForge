package com.flowforge.api.job;

import com.flowforge.common.event.DataProcessRequestedEvent;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class JobService {

    private static final DateTimeFormatter JOB_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private final JobRepository jobRepository;
    private final KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate;
    private final String requestedTopic;
    private final StringRedisTemplate redisTemplate;

    public JobService(JobRepository jobRepository, KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate, String requestedTopic) {
        this(jobRepository, kafkaTemplate, requestedTopic, (StringRedisTemplate) null);
    }

    @Autowired
    public JobService(
            JobRepository jobRepository,
            KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate,
            @Value("${app.kafka.topics.requested}") String requestedTopic,
            ObjectProvider<StringRedisTemplate> redisTemplateProvider
    ) {
        this(jobRepository, kafkaTemplate, requestedTopic, redisTemplateProvider.getIfAvailable());
    }

    private JobService(
            JobRepository jobRepository,
            KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate,
            String requestedTopic,
            StringRedisTemplate redisTemplate
    ) {
        this.jobRepository = jobRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.requestedTopic = requestedTopic;
        this.redisTemplate = redisTemplate;
    }

    @Transactional
    public CreateJobResponse createJob(CreateJobRequest request) {
        String idempotencyKey = normalizeIdempotencyKey(request.idempotencyKey());
        if (idempotencyKey != null) {
            var existingJob = jobRepository.findByIdempotencyKey(idempotencyKey);
            if (existingJob.isPresent()) {
                JobEntity job = existingJob.get();
                return new CreateJobResponse(job.getJobId(), job.getStatus(), "Data processing job has already been accepted.");
            }
        }

        JobEntity job = JobEntity.requested(newJobId(), request.requester(), request.dataType(), request.itemCount(), idempotencyKey);
        jobRepository.save(job);
        cacheInitialProgress(job);

        DataProcessRequestedEvent event = new DataProcessRequestedEvent(
                "EVT-" + UUID.randomUUID(),
                "DATA_PROCESS_REQUESTED",
                job.getJobId(),
                idempotencyKey,
                0,
                Instant.now(),
                eventPayload(request)
        );
        publishAfterCommit(job.getJobId(), event);

        return new CreateJobResponse(job.getJobId(), job.getStatus(), "Data processing job has been accepted.");
    }

    @Transactional(readOnly = true)
    public JobDetailResponse getJob(String jobId) {
        return jobRepository.findByJobId(jobId)
                .map(JobDetailResponse::from)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));
    }

    private void cacheInitialProgress(JobEntity job) {
        if (redisTemplate == null) {
            return;
        }
        redisTemplate.opsForValue().set("job:" + job.getJobId() + ":progress", "0");
    }

    private Map<String, Object> eventPayload(CreateJobRequest request) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("requester", request.requester());
        payload.put("dataType", request.dataType());
        payload.put("itemCount", request.itemCount());
        if (request.payload() != null) {
            payload.put("payload", request.payload());
        }
        return payload;
    }

    private void publishAfterCommit(String jobId, DataProcessRequestedEvent event) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            kafkaTemplate.send(requestedTopic, jobId, event);
            return;
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(requestedTopic, jobId, event);
            }
        });
    }

    private String newJobId() {
        return "JOB-" + JOB_DATE_FORMAT.format(Instant.now()) + "-" + UUID.randomUUID();
    }

    private String normalizeIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return null;
        }
        return idempotencyKey.trim();
    }
}
