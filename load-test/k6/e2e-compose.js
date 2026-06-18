import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { BASE_URL, requestHeaders } from './lib/jobs.js';

export const options = {
  scenarios: {
    compose_e2e: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: __ENV.MAX_DURATION || '2m',
    },
  },
  thresholds: {
    checks: ['rate==1'],
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

const POLL_SECONDS = Number(__ENV.POLL_SECONDS || '1');
const MAX_POLLS = Number(__ENV.MAX_POLLS || '60');

export default function () {
  const completedJob = createJob('e2e-completed', false);
  const completed = waitForJobStatus(completedJob.jobId, 'COMPLETED');

  check(completed, {
    'completed job reached COMPLETED': (job) => job.status === 'COMPLETED',
    'completed job processed all items': (job) => job.processedCount === completedJob.itemCount,
  });

  const dlqJob = createJob('e2e-dlq', true);
  const dlq = waitForJobStatus(dlqJob.jobId, 'DLQ');

  check(dlq, {
    'forced failure job reached DLQ': (job) => job.status === 'DLQ',
  });

  const dlqEvent = findDlqEvent(dlqJob.jobId);

  check(dlqEvent, {
    'DLQ event exists for forced failure job': (event) => Boolean(event && event.eventId),
    'DLQ event status is DLQ': (event) => event.status === 'DLQ',
  });
}

function createJob(scenario, forceFail) {
  const itemCount = forceFail ? 10 : 123;
  const body = JSON.stringify({
    requester: 'k6-e2e',
    dataType: 'CSV',
    itemCount,
    idempotencyKey: `${scenario}-${Date.now()}-${randomString(6)}`,
    payload: {
      source: 'k6-e2e',
      scenario,
      forceFail,
    },
  });

  const response = http.post(`${BASE_URL}/api/v1/jobs`, body, {
    headers: requestHeaders(),
    tags: { scenario },
  });

  if (response.status !== 202 && response.status !== 200) {
    fail(`Failed to create ${scenario} job: status=${response.status} body=${response.body}`);
  }

  const data = response.json('data');
  if (!data || !data.jobId) {
    fail(`Create ${scenario} response did not include data.jobId: ${response.body}`);
  }

  return {
    jobId: data.jobId,
    itemCount,
  };
}

function waitForJobStatus(jobId, expectedStatus) {
  let lastJob = null;

  for (let attempt = 0; attempt < MAX_POLLS; attempt += 1) {
    const response = http.get(`${BASE_URL}/api/v1/jobs/${jobId}`, {
      headers: requestHeaders(),
      tags: { scenario: `wait-${expectedStatus}` },
    });

    if (response.status !== 200) {
      fail(`Failed to read job ${jobId}: status=${response.status} body=${response.body}`);
    }

    lastJob = response.json('data');
    if (lastJob && lastJob.status === expectedStatus) {
      return lastJob;
    }

    sleep(POLL_SECONDS);
  }

  fail(`Job ${jobId} did not reach ${expectedStatus}; last=${JSON.stringify(lastJob)}`);
}

function findDlqEvent(jobId) {
  const response = http.get(`${BASE_URL}/api/v1/admin/dlq-events`, {
    headers: requestHeaders(),
    tags: { scenario: 'list-dlq-events' },
  });

  if (response.status !== 200) {
    fail(`Failed to list DLQ events: status=${response.status} body=${response.body}`);
  }

  const events = response.json('data') || [];
  return events.find((event) => event.jobId === jobId);
}

function randomString(length) {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let value = '';
  for (let index = 0; index < length; index += 1) {
    value += alphabet.charAt(Math.floor(Math.random() * alphabet.length));
  }
  return value;
}
