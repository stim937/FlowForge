package com.flowforge.worker.job;

import com.flowforge.common.event.DataProcessRequestedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DataProcessRequestedListener {

    private static final Logger log = LoggerFactory.getLogger(DataProcessRequestedListener.class);

    private final JobRepository jobRepository;
    private final StringRedisTemplate redisTemplate;

    public DataProcessRequestedListener(JobRepository jobRepository) {
        this(jobRepository, (StringRedisTemplate) null);
    }

    @Autowired
    public DataProcessRequestedListener(JobRepository jobRepository, ObjectProvider<StringRedisTemplate> redisTemplateProvider) {
        this(jobRepository, redisTemplateProvider.getIfAvailable());
    }

    private DataProcessRequestedListener(JobRepository jobRepository, StringRedisTemplate redisTemplate) {
        this.jobRepository = jobRepository;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(topics = "${app.kafka.topics.requested}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void handle(DataProcessRequestedEvent event) {
        log.info("Received data process request jobId={} eventId={}", event.jobId(), event.eventId());
        JobEntity job = jobRepository.findByJobId(event.jobId())
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + event.jobId()));

        job.markProcessing();
        cacheProgress(job.getJobId(), "50");
        job.markCompleted();
        jobRepository.save(job);
        cacheProgress(job.getJobId(), "100");
        log.info("Completed data process request jobId={} eventId={}", event.jobId(), event.eventId());
    }

    private void cacheProgress(String jobId, String progress) {
        if (redisTemplate == null) {
            return;
        }
        redisTemplate.opsForValue().set("job:" + jobId + ":progress", progress);
    }
}
