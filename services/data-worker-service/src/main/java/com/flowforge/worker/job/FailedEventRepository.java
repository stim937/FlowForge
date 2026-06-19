package com.flowforge.worker.job;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface FailedEventRepository extends JpaRepository<FailedEventEntity, Long> {
    Optional<FailedEventEntity> findByEventId(String eventId);
}
