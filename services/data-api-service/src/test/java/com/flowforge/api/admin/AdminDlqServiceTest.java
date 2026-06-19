package com.flowforge.api.admin;

import com.flowforge.common.event.DataProcessRequestedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AdminDlqServiceTest {

    @Test
    void listDlqEventsReturnsOnlyDlqStatusEvents() {
        InMemoryFailedEventRepository failedEventRepository = new InMemoryFailedEventRepository();
        failedEventRepository.save(FailedEventEntity.dlq(
                "EVT-dlq",
                "JOB-test",
                "data.process.dlq",
                "forced",
                eventPayload("EVT-dlq", "JOB-test"),
                3
        ));
        KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        AdminDlqService service = new AdminDlqService(failedEventRepository, kafkaTemplate, "data.process.retry");

        assertThat(service.listDlqEvents())
                .extracting(DlqEventResponse::eventId)
                .containsExactly("EVT-dlq");
    }

    @Test
    void reprocessDlqEventMarksRetryingAndPublishesRetryEvent() {
        InMemoryFailedEventRepository failedEventRepository = new InMemoryFailedEventRepository();
        failedEventRepository.save(FailedEventEntity.dlq(
                "EVT-dlq",
                "JOB-test",
                "data.process.dlq",
                "forced",
                eventPayload("EVT-dlq", "JOB-test"),
                3
        ));
        KafkaTemplate<String, DataProcessRequestedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        AdminDlqService service = new AdminDlqService(failedEventRepository, kafkaTemplate, "data.process.retry");

        DlqEventResponse response = service.reprocess("EVT-dlq");

        assertThat(response.status()).isEqualTo("RETRYING");
        verify(kafkaTemplate).send(eq("data.process.retry"), eq("JOB-test"), any(DataProcessRequestedEvent.class));
    }

    private String eventPayload(String eventId, String jobId) {
        return """
                {
                  "eventId": "%s",
                  "eventType": "DATA_PROCESS_REQUESTED",
                  "jobId": "%s",
                  "idempotencyKey": "idem-1",
                  "retryCount": 3,
                  "occurredAt": "%s",
                  "payload": {
                    "itemCount": 1000,
                    "forceFail": false
                  }
                }
                """.formatted(eventId, jobId, Instant.now());
    }
}
