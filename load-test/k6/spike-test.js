import { createJob } from './lib/jobs.js';

export const options = {
  scenarios: {
    spike_jobs: {
      executor: 'ramping-vus',
      stages: [
        { duration: '1m', target: Number(__ENV.START_VUS || '10') },
        { duration: '1m', target: Number(__ENV.SPIKE_VUS || '300') },
        { duration: '2m', target: Number(__ENV.SPIKE_VUS || '300') },
        { duration: '1m', target: 0 },
      ],
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<2000'],
  },
};

export default function () {
  createJob({ scenario: 'spike-test' });
}
