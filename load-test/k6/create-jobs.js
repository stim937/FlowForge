import { createJob } from './lib/jobs.js';

export const options = {
  scenarios: {
    create_jobs: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || '50'),
      duration: __ENV.DURATION || '3m',
      gracefulStop: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

export default function () {
  createJob({ scenario: 'create-jobs' });
}
