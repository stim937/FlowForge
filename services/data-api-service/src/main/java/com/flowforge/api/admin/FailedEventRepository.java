package com.flowforge.api.admin;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface FailedEventRepository extends JpaRepository<FailedEventEntity, Long> {
    List<FailedEventEntity> findByStatusOrderByCreatedAtDesc(String status);

    Optional<FailedEventEntity> findByEventId(String eventId);
}
