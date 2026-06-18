import { createJob } from './lib/jobs.js';

export const options = {
  scenarios: {
    bulk_messages: {
      executor: 'shared-iterations',
      vus: Number(__ENV.VUS || '50'),
      iterations: Number(__ENV.JOB_REQUESTS || '10000'),
      maxDuration: __ENV.MAX_DURATION || '15m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1500'],
  },
};

export default function () {
  createJob({ scenario: 'soak-test' });
}
