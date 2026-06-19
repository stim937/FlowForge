package com.flowforge.worker.job;

import java.util.*;

class InMemoryProcessedEventRepository implements ProcessedEventRepository {

    private final Map<String, ProcessedEventEntity> events = new LinkedHashMap<>();

    @Override
    public boolean existsByEventId(String eventId) {
        return events.containsKey(eventId);
    }

    @Override
    public <S extends ProcessedEventEntity> S save(S entity) {
        events.put(entity.getEventId(), entity);
        return entity;
    }

    @Override public void flush() {}
    @Override public <S extends ProcessedEventEntity> S saveAndFlush(S entity) { return save(entity); }
    @Override public <S extends ProcessedEventEntity> List<S> saveAllAndFlush(Iterable<S> entities) { return saveAll(entities); }
    @Override public void deleteAllInBatch(Iterable<ProcessedEventEntity> entities) {}
    @Override public void deleteAllByIdInBatch(Iterable<Long> longs) {}
    @Override public void deleteAllInBatch() {}
    @Override public ProcessedEventEntity getOne(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public ProcessedEventEntity getById(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public ProcessedEventEntity getReferenceById(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public <S extends ProcessedEventEntity> Optional<S> findOne(org.springframework.data.domain.Example<S> example) { return Optional.empty(); }
    @Override public <S extends ProcessedEventEntity> List<S> findAll(org.springframework.data.domain.Example<S> example) { return List.of(); }
    @Override public <S extends ProcessedEventEntity> List<S> findAll(org.springframework.data.domain.Example<S> example, org.springframework.data.domain.Sort sort) { return List.of(); }
    @Override public <S extends ProcessedEventEntity> org.springframework.data.domain.Page<S> findAll(org.springframework.data.domain.Example<S> example, org.springframework.data.domain.Pageable pageable) { return org.springframework.data.domain.Page.empty(); }
    @Override public <S extends ProcessedEventEntity> long count(org.springframework.data.domain.Example<S> example) { return 0; }
    @Override public <S extends ProcessedEventEntity> boolean exists(org.springframework.data.domain.Example<S> example) { return false; }
    @Override public <S extends ProcessedEventEntity, R> R findBy(org.springframework.data.domain.Example<S> example, java.util.function.Function<org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery<S>, R> queryFunction) { throw new UnsupportedOperationException(); }
    @Override public <S extends ProcessedEventEntity> List<S> saveAll(Iterable<S> entities) { List<S> saved = new ArrayList<>(); entities.forEach(entity -> saved.add(save(entity))); return saved; }
    @Override public Optional<ProcessedEventEntity> findById(Long aLong) { return Optional.empty(); }
    @Override public boolean existsById(Long aLong) { return false; }
    @Override public List<ProcessedEventEntity> findAll() { return List.copyOf(events.values()); }
    @Override public List<ProcessedEventEntity> findAllById(Iterable<Long> longs) { return List.of(); }
    @Override public long count() { return events.size(); }
    @Override public void deleteById(Long aLong) {}
    @Override public void delete(ProcessedEventEntity entity) { events.remove(entity.getEventId()); }
    @Override public void deleteAllById(Iterable<? extends Long> longs) {}
    @Override public void deleteAll(Iterable<? extends ProcessedEventEntity> entities) { entities.forEach(this::delete); }
    @Override public void deleteAll() { events.clear(); }
    @Override public List<ProcessedEventEntity> findAll(org.springframework.data.domain.Sort sort) { return findAll(); }
    @Override public org.springframework.data.domain.Page<ProcessedEventEntity> findAll(org.springframework.data.domain.Pageable pageable) { return org.springframework.data.domain.Page.empty(); }
}
