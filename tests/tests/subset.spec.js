import { test, expect } from '@playwright/test';

// Runs against the shared namespace alone (okteto.shared.yaml), which only
// deploys catalog + Kafka. There's no ingress on catalog, so these tests hit
// it directly over in-cluster DNS instead of going through BASE_URL. The
// unqualified "catalog" hostname doesn't reliably resolve here — the test
// Job's DNS search path doesn't include the target namespace the way an
// app pod deployed into it would — so use the namespace-qualified FQDN,
// which resolves cluster-wide regardless of which namespace the test Job
// itself runs in.
const CATALOG_URL = process.env.CATALOG_INTERNAL_URL
  || `http://catalog.${process.env.OKTETO_NAMESPACE}.svc.cluster.local:8080`;

test('catalog healthz responds ok', { tag: '@subset' }, async ({ request }) => {
  const response = await request.get(`${CATALOG_URL}/catalog/healthz`);
  expect(response.status()).toBe(200);
});

test('catalog has entries', { tag: '@subset' }, async ({ request }) => {
  const response = await request.get(`${CATALOG_URL}/catalog`);
  expect(response.status()).toBe(200);
  const data = await response.json();
  expect(data.length).toBe(6);

  const expectedTitles = [
    'Moby Dock',
    'The Finalizer',
    'Crash Loop Backoff',
    'Kube',
    'Cloud Atlas',
    'Aliens'
  ];

  const actualTitles = data.map(item => item.original_title);
  expect(actualTitles).toEqual(expectedTitles);
});
