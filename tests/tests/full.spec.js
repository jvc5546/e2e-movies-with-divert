import { test, expect } from '@playwright/test';

// Runs against a personal namespace (okteto.personal.yaml), which deploys
// the full app tier (frontend + api + rent-api + worker + local Postgres)
// and diverts catalog + Kafka from the shared namespace.

test('environment variables are set', { tag: '@full' }, async () => {
  expect(process.env.OKTETO_NAMESPACE).toBeDefined();
  expect(process.env.OKTETO_DOMAIN).toBeDefined();
  expect(process.env.OKTETO_NAMESPACE).not.toBe('');
  expect(process.env.OKTETO_DOMAIN).not.toBe('');
});

test('movies has title', { tag: '@full' }, async ({ page }) => {
  await page.goto('/');

  // The page title
  await expect(page).toHaveTitle('Movies');
});

test('catalog has entries', { tag: '@full' }, async ({ request }) => {
  // Real gateway route is /api/catalog (api/chart/templates/api-ingress.yaml),
  // not /catalog — the original spec hit the wrong path.
  const response = await request.get('/api/catalog');
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

test('rent and return a movie round-trip, tagged with our own namespace', { tag: '@full' }, async ({ request }) => {
  const catalogResponse = await request.get('/api/catalog');
  const catalog = await catalogResponse.json();
  const movie = catalog[0];

  // No baggage header set here on purpose — this exercises the same path a
  // real browser click takes, proving rent-api's self-tagging fallback
  // (RentController.java's effectiveBaggage) works, not just the explicit
  // header case.
  const rentResponse = await request.post('/api/rent', {
    data: { id: String(movie.id), price: String(movie.price), title: movie.original_title },
  });
  expect(rentResponse.status()).toBe(200);

  // The write lands via Kafka -> worker -> Postgres, so it's eventually
  // consistent — poll until it shows up.
  await expect(async () => {
    const rentals = await (await request.get('/api/rent')).json();
    const mine = rentals.find(r => String(r.id) === String(movie.id));
    expect(mine).toBeDefined();
    expect(mine.namespace).toBe(process.env.OKTETO_NAMESPACE);
  }).toPass({ timeout: 15000 });

  const returnResponse = await request.post('/api/rent/return', {
    data: { id: String(movie.id), title: movie.original_title },
  });
  expect(returnResponse.status()).toBe(200);

  await expect(async () => {
    const rentals = await (await request.get('/api/rent')).json();
    expect(rentals.find(r => String(r.id) === String(movie.id))).toBeUndefined();
  }).toPass({ timeout: 15000 });
});
