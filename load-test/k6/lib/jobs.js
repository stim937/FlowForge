import http from 'k6/http';
import { check, sleep } from 'k6';

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
export const HOST_HEADER = __ENV.HOST_HEADER || '';
export const MIN_ITEM_COUNT = Number(__ENV.MIN_ITEM_COUNT || '1000');
export const MAX_ITEM_COUNT = Number(__ENV.MAX_ITEM_COUNT || '10000');

export function requestHeaders() {
  const headers = {
    'Content-Type': 'application/json',
  };

  if (HOST_HEADER) {
    headers.Host = HOST_HEADER;
  }

  return headers;
}

export function createJob(tags = {}) {
  const idempotencyKey = `k6-${__VU}-${__ITER}-${Date.now()}-${randomString(6)}`;
  const payload = JSON.stringify({
    requester: 'k6-user',
    dataType: 'CSV',
    itemCount: randomIntBetween(MIN_ITEM_COUNT, MAX_ITEM_COUNT),
    idempotencyKey,
    payload: {
      source: 'k6',
      scenario: tags.scenario || 'default',
    },
  });

  const response = http.post(`${BASE_URL}/api/v1/jobs`, payload, {
    headers: requestHeaders(),
    tags,
  });

  check(response, {
    'job accepted': (res) => res.status === 200 || res.status === 202,
    'response success': (res) => {
      try {
        return res.json('success') === true && Boolean(res.json('data.jobId'));
      } catch (_) {
        return false;
      }
    },
  });

  sleep(Number(__ENV.SLEEP_SECONDS || '0.1'));
  return response;
}

function randomIntBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomString(length) {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let value = '';
  for (let index = 0; index < length; index += 1) {
    value += alphabet.charAt(Math.floor(Math.random() * alphabet.length));
  }
  return value;
}
