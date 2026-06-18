package com.flowforge.worker.job;

import java.util.*;

class InMemoryJobRepository implements JobRepository {

    private final Map<String, JobEntity> jobs = new LinkedHashMap<>();

    @Override
    public Optional<JobEntity> findByJobId(String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public <S extends JobEntity> S save(S entity) {
        jobs.put(entity.getJobId(), entity);
        return entity;
    }

    @Override public void flush() {}
    @Override public <S extends JobEntity> S saveAndFlush(S entity) { return save(entity); }
    @Override public <S extends JobEntity> List<S> saveAllAndFlush(Iterable<S> entities) { return saveAll(entities); }
    @Override public void deleteAllInBatch(Iterable<JobEntity> entities) {}
    @Override public void deleteAllByIdInBatch(Iterable<Long> longs) {}
    @Override public void deleteAllInBatch() {}
    @Override public JobEntity getOne(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public JobEntity getById(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public JobEntity getReferenceById(Long aLong) { throw new UnsupportedOperationException(); }
    @Override public <S extends JobEntity> Optional<S> findOne(org.springframework.data.domain.Example<S> example) { return Optional.empty(); }
    @Override public <S extends JobEntity> List<S> findAll(org.springframework.data.domain.Example<S> example) { return List.of(); }
    @Override public <S extends JobEntity> List<S> findAll(org.springframework.data.domain.Example<S> example, org.springframework.data.domain.Sort sort) { return List.of(); }
    @Override public <S extends JobEntity> org.springframework.data.domain.Page<S> findAll(org.springframework.data.domain.Example<S> example, org.springframework.data.domain.Pageable pageable) { return org.springframework.data.domain.Page.empty(); }
    @Override public <S extends JobEntity> long count(org.springframework.data.domain.Example<S> example) { return 0; }
    @Override public <S extends JobEntity> boolean exists(org.springframework.data.domain.Example<S> example) { return false; }
    @Override public <S extends JobEntity, R> R findBy(org.springframework.data.domain.Example<S> example, java.util.function.Function<org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery<S>, R> queryFunction) { throw new UnsupportedOperationException(); }
    @Override public <S extends JobEntity> List<S> saveAll(Iterable<S> entities) { List<S> saved = new ArrayList<>(); entities.forEach(entity -> saved.add(save(entity))); return saved; }
    @Override public Optional<JobEntity> findById(Long aLong) { return Optional.empty(); }
    @Override public boolean existsById(Long aLong) { return false; }
    @Override public List<JobEntity> findAll() { return List.copyOf(jobs.values()); }
    @Override public List<JobEntity> findAllById(Iterable<Long> longs) { return List.of(); }
    @Override public long count() { return jobs.size(); }
    @Override public void deleteById(Long aLong) {}
    @Override public void delete(JobEntity entity) { jobs.remove(entity.getJobId()); }
    @Override public void deleteAllById(Iterable<? extends Long> longs) {}
    @Override public void deleteAll(Iterable<? extends JobEntity> entities) { entities.forEach(this::delete); }
    @Override public void deleteAll() { jobs.clear(); }
    @Override public List<JobEntity> findAll(org.springframework.data.domain.Sort sort) { return findAll(); }
    @Override public org.springframework.data.domain.Page<JobEntity> findAll(org.springframework.data.domain.Pageable pageable) { return org.springframework.data.domain.Page.empty(); }
}
