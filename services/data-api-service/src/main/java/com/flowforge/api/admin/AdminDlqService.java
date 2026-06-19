package com.flowforge.api.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.common.event.DataProcessRequestedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.List;

@Service
public class AdminDlqService {

    private final FailedEventRepository failedEventRepository;
    private final KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String retryTopic;

    public AdminDlqService(FailedEventRepository failedEventRepository, KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate, String retryTopic) {
        this.failedEventRepository = failedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
        this.retryTopic = retryTopic;
    }

    @Autowired
    public AdminDlqService(
            FailedEventRepository failedEventRepository,
            KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topics.retry}") String retryTopic
    ) {
        this.failedEventRepository = failedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.retryTopic = retryTopic;
    }

    @Transactional(readOnly = true)
    public List<DlqEventResponse> listDlqEvents() {
        return failedEventRepository.findByStatusOrderByCreatedAtDesc("DLQ").stream()
                .map(DlqEventResponse::from)
                .toList();
    }

    @Transactional
    public DlqEventResponse reprocess(String eventId) {
        FailedEventEntity failedEvent = failedEventRepository.findByEventId(eventId)
                .orElseThrow(() -> new IllegalArgumentException("DLQ event not found: " + eventId));
        DataProcessRequestedEvent originalEvent = readEvent(failedEvent.getPayload());
        DataProcessRequestedEvent retryEvent = new DataProcessRequestedEvent(
                originalEvent.eventId(),
                originalEvent.eventType(),
                originalEvent.jobId(),
                originalEvent.idempotencyKey(),
                0,
                Instant.now(),
                originalEvent.payload()
        );

        failedEvent.markRetrying();
        failedEventRepository.save(failedEvent);
        publishAfterCommit(retryEvent);
        return DlqEventResponse.from(failedEvent);
    }

    private DataProcessRequestedEvent readEvent(String payload) {
        try {
            return objectMapper.readValue(payload, DataProcessRequestedEvent.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Invalid DLQ event payload.", exception);
        }
    }

    private void publishAfterCommit(DataProcessRequestedEvent event) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            kafkaTemplate.send(retryTopic, event.jobId(), event);
            return;
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(retryTopic, event.jobId(), event);
            }
        });
    }
}
