package com.flowforge.api.admin;

import java.util.*;

class InMemoryFailedEventRepository implements FailedEventRepository {

    private final Map<String, FailedEventEntity> events = new LinkedHashMap<>();

    @Override
    public List<FailedEventEntity> findByStatusOrderByCreatedAtDesc(String status) {
        return events.values().stream()
                .filter(event -> status.equals(event.getStatus()))
                .sorted(Comparator.comparing(FailedEventEntity::getCreatedAt).reversed())
                .toList();
    }

    @Override
    public Optional<FailedEventEntity> findByEventId(String eventId) {
        return Optional.ofNullable(events.get(eventId));
    }

    @Override
    public <S extends FailedEventEntity> S save(S entity) {
        events.put(entity.getEventId(), entity);
        return entity;
    }

    @Override public void flush() {}
    @Override public <S extends FailedEventEntity> S saveAndFlush(S entity) { return save(entity); }
    @Override public <S extends FailedEventEntity> List<S> saveAllAndFlush(Iterable<S> entities) { return saveAll(entities); }
    @Override public void deleteAllInBatch(Iterable<FailedEventEntity> entities) {}
    @Override public void deleteAllByIdInBatch(Iterable<Long> longs) {}
    @Override public void deleteAllInBatch() {}
    @Override public FailedEventEntity getOne(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public FailedEventEntity getById(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public FailedEventEntity getReferenceById(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public <S extends FailedEventEntity> Optional<S> findOne(org.springframework.data.domain.Example<S> example) { return Optional.empty(); }
    @Override public <S extends FailedEventEntity> List<S> findAll(org.springframework.data.domain.Example<S> example) { return List.of(); }
    @Override public <S extends FailedEventEntity> List<S> findAll(org.springframework.data.domain.Example<S> example, org.springframework.data.domain.Sort sort) { return List.of(); }
    @Override public <S extends FailedEventEntity> org.springframework.data.domain.Page<S> findAll(org.springframework.data.domain.Example<S> example, org.springframework.data.domain.Pageable pageable) { return org.springframework.data.domain.Page.empty(); }
    @Override public <S extends FailedEventEntity> long count(org.springframework.data.domain.Example<S> example) { return 0; }
    @Override public <S extends FailedEventEntity> boolean exists(org.springframework.data.domain.Example<S> example) { return false; }
    @Override public <S extends FailedEventEntity, R> R findBy(org.springframework.data.domain.Example<S> example, java.util.function.Function<org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery<S>, R> queryFunction) { throw new UnsupportedOperationException(); }
    @Override public <S extends FailedEventEntity> List<S> saveAll(Iterable<S> entities) { List<S> saved = new ArrayList<>(); entities.forEach(entity -> saved.add(save(entity))); return saved; }
    @Override public Optional<FailedEventEntity> findById(Long aLong) { return Optional.empty(); }
    @Override public boolean existsById(Long aLong) { return false; }
    @Override public List<FailedEventEntity> findAll() { return List.copyOf(events.values()); }
    @Override public List<FailedEventEntity> findAllById(Iterable<Long> longs) { return List.of(); }
    @Override public long count() { return events.size(); }
    @Override public void deleteById(Long aLong) {}
    @Override public void delete(FailedEventEntity entity) { events.remove(entity.getEventId()); }
    @Override public void deleteAllById(Iterable<? extends Long> longs) {}
    @Override public void deleteAll(Iterable<? extends FailedEventEntity> entities) { entities.forEach(this::delete); }
    @Override public void deleteAll() { events.clear(); }
    @Override public List<FailedEventEntity> findAll(org.springframework.data.domain.Sort sort) { return findAll(); }
    @Override public org.springframework.data.domain.Page<FailedEventEntity> findAll(org.springframework.data.domain.Pageable pageable) { return org.springframework.data.domain.Page.empty(); }
}
