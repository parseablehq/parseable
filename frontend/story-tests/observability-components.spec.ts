import { expect, test, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.use({ timezoneId: 'UTC', locale: 'en-US', reducedMotion: 'reduce' });

async function story(page: Page, id: string) {
  await page.goto(`/iframe.html?id=${id}&viewMode=story`);
  await expect(page.locator('#storybook-root')).not.toBeEmpty();
}

async function selectJsonDestination(page: Page) {
  await page
    .getByRole('combobox', { name: 'Destination dataset' })
    .selectOption('application_logs');
  await expect(page.getByRole('button', { name: 'Copy Ingestion command' })).toBeVisible();
}

test('trace table sorts and paginates actual records while distinguishing root errors from explicit error spans', async ({
  page,
}) => {
  await story(page, 'traces-explorer--default');
  const results = page.getByRole('region', { name: 'Trace results', exact: true });
  await expect(results.getByRole('row')).toHaveCount(26);
  await expect(results.getByRole('row').nth(1)).toContainText('240 ms');
  await expect(results.getByRole('row').nth(1)).toContainText('1 error');
  const duration = results.getByRole('columnheader', { name: 'Sort Duration' });
  await page.getByRole('button', { name: 'Sort Duration', exact: true }).click();
  await expect(duration).toHaveAttribute('aria-sort', 'descending');
  await expect(results.getByRole('row').nth(1)).toContainText('600 ms');
  await expect(page.getByRole('combobox', { name: 'Sort by' })).toHaveValue('longestFirst');
  await page.getByRole('button', { name: 'Next page', exact: true }).click();
  await expect(page.getByText('Showing 26–31 of 31 traces', { exact: true })).toBeVisible();
  await expect(results.getByRole('row')).toHaveCount(7);
  await expect(page.getByRole('button', { name: 'Next page', exact: true })).toBeDisabled();
  await page.getByRole('combobox', { name: 'Status', exact: true }).selectOption('error');
  await expect(page.getByRole('combobox', { name: 'View', exact: true })).toBeDisabled();
  await expect(results.getByRole('row')).toHaveCount(2);
  await expect(results.getByRole('row').nth(1)).toContainText('create-order');
  await expect(results.getByRole('row').nth(1)).toContainText('checkout');
  await expect(page.getByText('Showing 1–1 of 1 error spans', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
  await expect(page.getByRole('combobox', { name: 'Status', exact: true })).toHaveValue('all');
  await expect(page.getByRole('combobox', { name: 'View', exact: true })).toBeEnabled();
  await expect(results.getByRole('row')).toHaveCount(26);
});

test('APM service handoff includes child spans and keeps the selected bounds in transport requests', async ({
  page,
}) => {
  await story(page, 'traces-explorer--service-handoff');
  await expect(page.getByRole('textbox', { name: 'Service', exact: true })).toHaveValue('checkout');
  await expect(page.getByRole('combobox', { name: 'View', exact: true })).toHaveValue('spans');
  await expect(page.getByRole('region', { name: 'Trace results', exact: true })).toContainText(
    'create-order',
  );
  const requests = page.getByTestId('trace-list-requests');
  await expect
    .poll(async () => JSON.parse((await requests.textContent()) ?? '[]').at(-1))
    .toMatchObject({
      dataset: 'orders_traces',
      serviceName: 'checkout',
      options: 'spans',
      startTime: '2026-10-12T10:00:00.000Z',
      endTime: '2026-10-12T11:00:00.000Z',
    });
  const service = page.getByRole('textbox', { name: 'Service', exact: true });
  await service.fill('inventory');
  await page.getByRole('button', { name: 'Apply service' }).click();
  await expect(page.getByRole('region', { name: 'Trace results', exact: true })).toContainText(
    'reserve-stock',
  );
  await expect(page.getByRole('region', { name: 'Trace results', exact: true })).not.toContainText(
    'create-order',
  );
});

test('waterfall keyboard selection reveals stored fields and deduplicates events for error spans', async ({
  page,
}) => {
  await story(page, 'traces-explorer--detail');
  const dialog = page.getByRole('dialog', { name: 'Trace detail', exact: true });
  const waterfall = dialog.getByRole('region', { name: 'Span timing', exact: true });
  const root = waterfall.getByRole('button', {
    name: 'POST /orders, api-gateway, 240 ms',
    exact: true,
  });
  const checkout = waterfall.getByRole('button', {
    name: 'create-order, checkout, 150 ms, Error',
    exact: true,
  });
  await expect(root).toHaveAttribute('aria-pressed', 'true');
  await root.focus();
  await page.keyboard.press('ArrowDown');
  await expect(checkout).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(checkout).toHaveAttribute('aria-pressed', 'true');
  await expect(root).toHaveAttribute('aria-pressed', 'false');
  const detail = dialog.getByRole('region', { name: 'Span detail', exact: true });
  await expect(detail.getByRole('heading', { name: 'create-order', exact: true })).toBeVisible();
  await expect(detail).toContainText('Payment provider timed out');
  await expect(
    detail.getByRole('combobox', { name: 'Stored record' }).getByRole('option'),
  ).toHaveCount(2);
  await expect(detail.locator('.traces-events').getByRole('listitem')).toHaveCount(1);
  await expect(detail.getByRole('heading', { name: 'exception', exact: true })).toBeVisible();
  await expect(detail).toContainText('Upstream timeout');
  await checkout.focus();
  await page.keyboard.press('End');
  await expect(
    waterfall.getByRole('button', { name: /authorize-payment.*Parent unavailable/ }),
  ).toBeFocused();
  await page.keyboard.press('Home');
  await expect(root).toBeFocused();
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('ArrowUp');
  await expect(root).toBeFocused();
});

test('waterfall search matches operations, services and span IDs and recovers from no matches', async ({
  page,
}) => {
  await story(page, 'traces-explorer--detail');
  const dialog = page.getByRole('dialog', { name: 'Trace detail', exact: true });
  const search = dialog.getByRole('textbox', { name: 'Search spans' });
  const waterfall = dialog.getByRole('region', { name: 'Span timing', exact: true });
  for (const query of ['INSERT', 'orders-db', '0000000000000003']) {
    await search.fill(query);
    await expect(waterfall.getByRole('button')).toHaveCount(1);
    await expect(waterfall.getByRole('button')).toHaveAccessibleName(
      'INSERT orders, orders-db, 80 ms',
    );
    await expect(dialog.getByRole('status').filter({ hasText: 'Showing' })).toHaveText(
      'Showing 1 of 5 spans',
    );
  }
  await search.fill('missing-span');
  await expect(dialog.getByRole('heading', { name: 'No matching spans' })).toBeVisible();
  await expect(waterfall.getByRole('button')).toHaveCount(0);
  await search.fill('');
  await expect(waterfall.getByRole('button')).toHaveCount(5);
  await expect(dialog.getByRole('heading', { name: 'No matching spans' })).toHaveCount(0);
});

test('opening and dismissing trace detail restores keyboard focus to its table opener', async ({
  page,
}) => {
  await story(page, 'traces-explorer--default');
  const opener = page.getByRole('button', {
    name: 'Open trace 0123456789abcdef0123456789abcdef, POST /orders',
    exact: true,
  });
  await opener.click();
  const dialog = page.getByRole('dialog', { name: 'Trace detail', exact: true });
  await expect(dialog).toBeVisible();
  await expect(
    dialog.getByRole('region', { name: 'Span timing', exact: true }).getByRole('button'),
  ).toHaveCount(5);
  await page.keyboard.press('Escape');
  await expect(dialog).toHaveCount(0);
  await expect(opener).toBeFocused();
});

for (const [id, heading, message] of [
  ['permission-denied', 'Permission denied', 'You do not have access to orders_traces'],
  [
    'missing-fields',
    'Could not load data',
    'missing required fields for the traces API: span_status_code',
  ],
  ['error', 'Could not load data', 'Trace query failed: query node unavailable'],
] as const) {
  test(`trace ${id} reports the server failure and permits retry`, async ({ page }) => {
    await story(page, `traces-explorer--${id}`);
    await expect(page.getByRole('heading', { name: heading, exact: true })).toBeVisible();
    await expect(page.getByRole('alert')).toContainText(message);
    await expect(page.getByRole('region', { name: 'Trace results', exact: true })).toHaveCount(0);
    await page.getByRole('button', { name: 'Try again', exact: true }).click();
    await expect
      .poll(
        async () =>
          JSON.parse((await page.getByTestId('trace-list-requests').textContent()) ?? '[]').length,
      )
      .toBeGreaterThanOrEqual(2);
    await expect(page.getByRole('alert')).toContainText(message);
  });
}

test('trace empty, missing-dataset, loading and not-found states explain available evidence', async ({
  page,
}) => {
  await story(page, 'traces-explorer--empty');
  await expect(page.getByRole('heading', { name: 'No matching traces' })).toBeVisible();
  await story(page, 'traces-explorer--no-datasets');
  await expect(page.getByRole('heading', { name: 'No trace datasets available' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Check again' })).toBeVisible();
  await story(page, 'traces-explorer--loading');
  await expect(page.getByRole('status').filter({ hasText: 'Loading data' })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Trace results', exact: true })).toHaveCount(0);
  await story(page, 'traces-explorer--lookup-not-found');
  const dialog = page.getByRole('dialog', { name: 'Trace detail', exact: true });
  await expect(dialog.getByRole('heading', { name: 'Trace not found' })).toBeVisible();
  await expect(dialog.getByText(/try a wider time range/)).toBeVisible();
  await expect(dialog.getByRole('region', { name: 'Span timing', exact: true })).toHaveCount(0);
});

test('stored-record permission failure retains span summary without fabricated field or event data', async ({
  page,
}) => {
  await story(page, 'traces-explorer--metadata-permission');
  const detail = page.getByRole('region', { name: 'Span detail', exact: true });
  await expect(detail.getByRole('heading', { name: 'POST /orders', exact: true })).toBeVisible();
  await expect(detail).toContainText('240 ms');
  await expect(detail.getByRole('alert')).toContainText('Stored-record query is not permitted');
  await expect(detail.getByRole('heading', { name: 'Recorded fields' })).toBeVisible();
  await expect(detail.locator('.traces-recorded-fields')).toHaveCount(0);
  await expect(detail.locator('.traces-events')).toHaveCount(0);
});

test('service inventory searches, exposes measured error rates and preserves missing latency', async ({
  page,
}) => {
  await story(page, 'apm-serviceinventory--services');
  const table = page.getByRole('table');
  await expect(table.getByRole('row')).toHaveCount(5);
  const checkout = table
    .getByRole('row')
    .filter({ has: page.getByRole('link', { name: 'checkout', exact: true }) });
  await expect(checkout).toContainText('7,840');
  await expect(checkout).toContainText('5%');
  await expect(checkout).toContainText('392 error spans');
  await expect(checkout).toContainText('540 ms');
  const missing = table
    .getByRole('row')
    .filter({ has: page.getByRole('link', { name: 'background-worker', exact: true }) });
  await expect(missing.getByRole('cell').last()).toContainText('—');
  const search = page.getByRole('textbox', { name: 'Search services' });
  await search.fill('CHECK');
  await expect(table.getByRole('row')).toHaveCount(2);
  await expect(page.getByRole('status')).toHaveText('1 of 4 services');
  await search.fill('missing-service');
  await expect(page.getByRole('heading', { name: 'No matching services' })).toBeVisible();
  await expect(page.getByRole('table')).toHaveCount(0);
  await search.fill('');
  await expect(page.getByRole('status')).toHaveText('4 of 4 services');
});

test('service inventory latency selection changes values and sorting keeps missing measurements last', async ({
  page,
}) => {
  await story(page, 'apm-serviceinventory--services');
  const table = page.getByRole('table');
  await expect(table.getByRole('columnheader', { name: 'Spans', exact: true })).toHaveAttribute(
    'aria-sort',
    'descending',
  );
  await page.getByRole('combobox', { name: 'Select latency metric' }).selectOption('p99Ms');
  const latency = table.getByRole('columnheader', { name: 'P99 latency' });
  await latency.getByRole('button').click();
  await expect(latency).toHaveAttribute('aria-sort', 'descending');
  await expect(table.locator('tbody').getByRole('link')).toHaveText([
    'checkout',
    'api-gateway',
    'inventory',
    'background-worker',
  ]);
  await expect(table.locator('tbody').getByRole('row').first()).toContainText('1.1 s');
  await latency.getByRole('button').click();
  await expect(latency).toHaveAttribute('aria-sort', 'ascending');
  await expect(table.locator('tbody').getByRole('link')).toHaveText([
    'inventory',
    'api-gateway',
    'checkout',
    'background-worker',
  ]);
  await expect(page.getByRole('link', { name: 'checkout', exact: true })).toHaveAttribute(
    'href',
    '/apm/demo_traces/checkout',
  );
});

test('service inventory empty and no-match states explain recovery', async ({ page }) => {
  await story(page, 'apm-serviceinventory--empty');
  await expect(page.getByRole('heading', { name: 'No services in this range' })).toBeVisible();
  await expect(
    page.getByText('Choose a wider time range or ingest OpenTelemetry traces.'),
  ).toBeVisible();
  await story(page, 'apm-serviceinventory--no-matches');
  await expect(page.getByRole('heading', { name: 'No matching services' })).toBeVisible();
  await expect(page.getByText('Try a different service name or clear your search.')).toBeVisible();
});

test('dependency insights report observed child-span metrics and focused services exclude unrelated edges', async ({
  page,
}) => {
  await story(page, 'apm-servicetopology--observed-dependencies');
  const edges = page.locator('.apm-edge-list').getByRole('listitem');
  await expect(edges).toHaveCount(3);
  await expect(edges.first()).toContainText('api-gateway');
  await expect(edges.first()).toContainText('checkout');
  await expect(edges.first()).toContainText('Child spans7,840');
  await expect(edges.first()).toContainText('Span error rate5%');
  await expect(edges.first()).toContainText('Average latency168.4 ms');
  await story(page, 'apm-servicetopology--focused-service');
  await expect(edges).toHaveCount(1);
  await expect(edges.first()).toContainText('checkout');
  await expect(edges.first()).toContainText('inventory');
  await expect(page.getByRole('link', { name: 'payments' })).toHaveCount(0);
  await story(page, 'apm-servicetopology--no-dependencies');
  await expect(
    page.getByRole('heading', { name: 'No observed service dependencies' }),
  ).toBeVisible();
  await expect(
    page.getByText(/Missing parents and ambiguous span identities are excluded/),
  ).toBeVisible();
});

test('service insights switch charts by keyboard, filter operations and link a prioritized span with its time window', async ({
  page,
}) => {
  await story(page, 'apm-serviceinsights--overview');
  await expect(page.locator('.apm-stat-grid')).toContainText('360');
  await expect(page.locator('.apm-stat-grid')).toContainText('5%');
  await expect(page.locator('.apm-stat-grid')).toContainText('420 ms');
  const rate = page.getByRole('tab', { name: 'Rate & errors', exact: true });
  await rate.focus();
  await page.keyboard.press('ArrowRight');
  const latency = page.getByRole('tab', { name: 'Latency', exact: true });
  await expect(latency).toBeFocused();
  await expect(latency).toHaveAttribute('aria-selected', 'true');
  await expect(page.getByRole('figure', { name: 'Span latency', exact: true })).toBeVisible();
  await expect(page.locator('.apm-percentiles')).toContainText('P99920 ms');
  const operations = page.getByRole('region', { name: 'Service operations', exact: true });
  await expect(operations.getByRole('table')).toContainText('POST /checkout');
  await page.getByRole('textbox', { name: 'Search operations' }).fill('missing');
  await expect(operations.getByRole('heading', { name: 'No matching operations' })).toBeVisible();
  await page.getByRole('textbox', { name: 'Search operations' }).fill('CHECKOUT');
  await expect(operations.getByRole('table')).toContainText('POST /checkout');
  const spans = page.getByRole('region', { name: 'Spans to investigate', exact: true });
  await expect(spans.getByRole('row').nth(1)).toContainText('Error');
  await expect(spans.getByRole('row').nth(1)).toContainText('3.1 s');
  await expect(spans.getByRole('row').nth(2)).toContainText('No explicit error');
  const link = spans.getByRole('link', { name: 'POST /checkout', exact: true }).first();
  const href = await link.getAttribute('href');
  expect(href).not.toBeNull();
  const target = new URL(href!, 'https://parseable.example');
  expect(target.pathname).toBe('/traces/explore/demo_traces');
  expect(Object.fromEntries(target.searchParams)).toEqual({
    service: 'checkout',
    start: '2026-10-12T15:00:00.000Z',
    end: '2026-10-12T16:00:00.000Z',
    traceId: '8e74f620af734b9086bb000000000001',
  });
});

test('service insights disclose limited result sets and provide recovery for an empty range', async ({
  page,
}) => {
  await story(page, 'apm-serviceinsights--limited-results');
  await expect(
    page.getByText(
      'Showing the 100 operations with the most spans. Search applies to these operations.',
    ),
  ).toBeVisible();
  await expect(
    page.getByText('Showing 50 prioritized spans. Open the trace explorer for more.'),
  ).toBeVisible();
  await story(page, 'apm-serviceinsights--empty');
  await expect(page.getByRole('heading', { name: 'No spans for this service' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Back to services' })).toHaveAttribute(
    'href',
    '/apm/demo_traces',
  );
  await expect(page.getByRole('figure')).toHaveCount(0);
});

test('directed service map exposes keyboard links, observed edge metrics and the current service', async ({
  page,
}) => {
  await story(page, 'apm-servicemap--directed-services');
  const map = page.getByRole('region', {
    name: 'Directed service map; scroll to explore',
    exact: true,
  });
  await expect(map.getByRole('link')).toHaveCount(4);
  const gateway = map.getByRole('link', { name: 'Open api-gateway service insights', exact: true });
  await expect(gateway).toHaveAttribute('href', '/apm/demo_traces/api-gateway');
  await map.focus();
  await page.keyboard.press('Tab');
  await expect(gateway).toBeFocused();
  await page.keyboard.press('Tab');
  await expect(
    map.getByRole('link', { name: 'Open checkout service insights', exact: true }),
  ).toBeFocused();
  await expect(map.locator('.apm-map-edge title')).toHaveText([
    'api-gateway → checkout: 7,840 child spans',
    'checkout → inventory: 5,240 child spans',
    'checkout → payments: 4,880 child spans',
  ]);
  await story(page, 'apm-servicemap--focused-service');
  await expect(
    map.getByRole('link', { name: 'Open checkout service insights', exact: true }),
  ).toHaveAttribute('aria-current', 'page');
  await expect(map.locator('.apm-map-edge-focused')).toHaveCount(3);
});

test('service map keeps cyclic edges finite and full service names accessible at narrow width', async ({
  page,
}) => {
  await story(page, 'apm-servicemap--cyclic-dependencies');
  const map = page.getByRole('region', {
    name: 'Directed service map; scroll to explore',
    exact: true,
  });
  await expect(map.getByRole('link')).toHaveCount(4);
  await expect(map.locator('.apm-map-edge')).toHaveCount(4);
  for (const edge of await map.locator('.apm-map-edge').all()) {
    await expect(edge).not.toHaveAttribute('d', /NaN|Infinity/);
    await expect(edge).toHaveAttribute('marker-end', /^url\(#apm-arrow-/);
  }
  await page.setViewportSize({ width: 420, height: 850 });
  await story(page, 'apm-servicemap--long-names');
  const fullName = 'production/europe/checkout-and-payment-orchestration-service';
  const destination = map.getByRole('link', {
    name: `Open ${fullName} service insights`,
    exact: true,
  });
  await expect(destination).toHaveAttribute(
    'href',
    `/apm/demo_traces/${encodeURIComponent(fullName)}`,
  );
  await expect(destination.locator('text')).toContainText('…');
  await map.focus();
  await page.keyboard.press('ArrowRight');
  await expect.poll(async () => map.evaluate((element) => element.scrollLeft)).toBeGreaterThan(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
    ),
  ).toBeLessThanOrEqual(1);
});

test('JSON setup validates the endpoint, copies the displayed command and invalidates copied feedback after edits', async ({
  page,
}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: {
        writeText: async (value: string) => {
          (window as Window & { copiedConfiguration?: string }).copiedConfiguration = value;
        },
      },
    });
  });
  await story(page, 'ingestion-setup--default');
  await selectJsonDestination(page);
  const endpoint = page.getByRole('textbox', { name: 'Public ingestion endpoint' });
  await endpoint.fill('https://ingest.example.com/parseable');
  const code = page.locator('pre[aria-label="Ingestion command"]');
  await expect(code).toContainText("'https://ingest.example.com/parseable/api/v1/ingest'");
  await expect(code).toContainText('X-P-Stream: application_logs');
  await expect(code).toContainText('${PARSEABLE_API_KEY:?Set PARSEABLE_API_KEY}');
  const copy = page.getByRole('button', { name: 'Copy Ingestion command' });
  await copy.click();
  await expect(copy).toHaveText('Copied');
  expect(
    await page.evaluate(
      () => (window as Window & { copiedConfiguration?: string }).copiedConfiguration,
    ),
  ).toBe(await code.textContent());
  await endpoint.fill('https://other.example.com');
  await expect(copy).toHaveText('Copy');
  await endpoint.fill('https://user:secret@ingest.example.com');
  await expect(endpoint).toHaveAttribute('aria-invalid', 'true');
  await expect(endpoint).toHaveAccessibleDescription('Keep credentials out of the URL.');
  await expect(page.getByRole('alert')).toHaveText('Keep credentials out of the URL.');
  await expect(code).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Check ingestion' })).toBeDisabled();
  await endpoint.fill('https://ingest.example.com');
  await expect(endpoint).not.toHaveAttribute('aria-invalid', 'true');
  await expect(code).toBeVisible();
});

test('clipboard failure offers manual copying and retains the generated configuration', async ({
  page,
}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: {
        writeText: async () => {
          throw new DOMException('Clipboard denied', 'NotAllowedError');
        },
      },
    });
  });
  await story(page, 'ingestion-components--copyable-code');
  await page.getByRole('button', { name: 'Copy Example command' }).click();
  await expect(page.getByRole('alert')).toHaveText(
    'Clipboard is unavailable. Select and copy the configuration below.',
  );
  await expect(page.locator('pre[aria-label="Example command"]')).toHaveText(
    'curl --fail-with-body https://ingest.example.com/api/v1/ingest',
  );
  await expect(page.getByRole('button', { name: 'Copy Example command' })).toBeEnabled();
});

test('OTLP signal and destination compatibility gate configuration and collector pipelines', async ({
  page,
}) => {
  await story(page, 'ingestion-setup--default');
  await selectJsonDestination(page);
  await page.getByRole('radio', { name: /^OTLP HTTP/ }).check();
  await page.getByRole('combobox', { name: 'Signal', exact: true }).selectOption('traces');
  await expect(page.getByRole('alert')).toContainText(
    'This is a logs dataset. Select a traces dataset or create one.',
  );
  await expect(page.locator('.ingestion-code-block pre')).toHaveCount(0);
  await page.getByRole('combobox', { name: 'Destination dataset' }).selectOption('otel_traces');
  const endpoint = page.getByRole('textbox', { name: 'Public ingestion endpoint' });
  await endpoint.fill('https://ingest.example.com');
  await expect(page.locator('pre[aria-label="Ingestion command"]')).toContainText('/v1/traces');
  await expect(page.locator('.ingestion-code-block pre')).toContainText(
    '--data-binary @- <<PARSEABLE_OTLP_JSON',
  );
  await expect(page.locator('.ingestion-code-block pre')).toContainText('"resourceSpans"');
  await page.getByRole('radio', { name: /^OpenTelemetry Collector/ }).check();
  const collector = page.locator('pre[aria-label="collector.yaml"]');
  await expect(collector).toContainText('encoding: proto');
  await expect(collector).toContainText('compression: none');
  await expect(collector).toContainText('X-P-Stream: "otel_traces"');
  await expect(collector).toContainText('pipelines:\n    traces:');
  await expect(collector).not.toContainText('pipelines:\n    logs:');
  await page.getByRole('combobox', { name: 'Signal', exact: true }).selectOption('metrics');
  await expect(collector).toHaveCount(0);
  await page.getByRole('combobox', { name: 'Destination dataset' }).selectOption('otel_metrics');
  await expect(collector).toContainText('pipelines:\n    metrics:');
});

test('new dataset validation blocks invalid names and successful creation prepares the destination', async ({
  page,
}) => {
  await story(page, 'ingestion-setup--empty-workspace');
  const opener = page.getByRole('button', { name: 'Create new dataset' });
  await opener.click();
  await expect(opener).toHaveAttribute('aria-expanded', 'true');
  const name = page.getByRole('textbox', { name: 'New dataset name' });
  const create = page.getByRole('button', { name: 'Create dataset', exact: true });
  await expect(create).toBeDisabled();
  await name.fill('invalid dataset');
  await expect(name).toHaveAttribute('aria-invalid', 'true');
  await expect(create).toBeDisabled();
  await name.fill('empty_checkout');
  await expect(create).toBeEnabled();
  await create.click();
  await expect(name).toHaveCount(0);
  await expect(page.getByRole('combobox', { name: 'Destination dataset' })).toHaveValue(
    'empty_checkout',
  );
  await expect(page.locator('pre[aria-label="Ingestion command"]')).toContainText(
    'X-P-Stream: empty_checkout',
  );
  await page.getByRole('button', { name: 'Check ingestion' }).click();
  await expect(page.getByText('No events yet', { exact: true })).toBeVisible();
  await expect(page.getByText(/The server reports no current events/)).toBeVisible();
  await expect(page.getByRole('link', { name: 'Explore logs' })).toHaveCount(0);
});

test('verification reports server evidence and a recoverable failure without claiming sender connectivity', async ({
  page,
}) => {
  await story(page, 'ingestion-setup--default');
  await selectJsonDestination(page);
  await page.getByRole('button', { name: 'Check ingestion' }).click();
  const result = page.locator('.ingestion-check-result');
  await expect(result).toContainText('Existing data');
  await expect(result.locator('.ui-badge')).toHaveAttribute('data-tone', 'neutral');
  await expect(result).toContainText('42');
  await expect(result).toContainText('Schema fields4');
  await expect(result).toContainText('the server response does not verify this source connection');
  await expect(page.getByRole('link', { name: 'Explore logs' })).toHaveAttribute(
    'href',
    '/logs/explore/application_logs',
  );
  await story(page, 'ingestion-setup--verification-error');
  await selectJsonDestination(page);
  await page.getByRole('button', { name: 'Check ingestion' }).click();
  await expect(page.getByRole('alert')).toHaveText(
    'Ingestion statistics are temporarily unavailable.',
  );
  await expect(page.getByRole('button', { name: 'Check ingestion' })).toBeEnabled();
  await expect(page.locator('.ingestion-check-result')).toHaveCount(0);
});

test('ingestion arrivals survive endpoint edits and reset for a different destination', async ({
  page,
}) => {
  await story(page, 'ingestion-setup--new-arrivals');
  await selectJsonDestination(page);
  const check = page.getByRole('button', { name: 'Check ingestion', exact: true });
  const result = page.locator('.ingestion-check-result');
  await check.click();
  await expect(result).toContainText('Existing data');
  await expect(result.locator('.ui-badge')).toHaveAttribute('data-tone', 'neutral');
  await check.click();
  await expect(result).toContainText('3 new events');
  await expect(result.locator('.ui-badge')).toHaveAttribute('data-tone', 'success');
  await expect(result).toContainText('These arrivals may include other senders');
  await page
    .getByRole('textbox', { name: 'Public ingestion endpoint' })
    .fill('https://other.example.com');
  await expect(result).toContainText('3 new events');
  await check.click();
  await expect(result).toContainText('3 new events');
  await page.getByRole('radio', { name: /^OTLP HTTP/ }).check();
  await page.getByRole('combobox', { name: 'Destination dataset' }).selectOption('otel_logs');
  await expect(result).toHaveCount(0);
  await check.click();
  await expect(result).toContainText('Existing data');
  await expect(result.locator('.ui-badge')).toHaveAttribute('data-tone', 'neutral');
});

test('ingestion permission, loading and demo states remain explicit', async ({ page }) => {
  await story(page, 'ingestion-setup--permission-denied');
  await expect(page.getByRole('alert')).toHaveText(
    'Permission denied: dataset access is required.',
  );
  await expect(page.locator('.ingestion-code-block pre')).toHaveCount(0);
  await story(page, 'ingestion-setup--loading');
  await expect(page.getByRole('combobox', { name: 'Destination dataset' })).toBeDisabled();
  await expect(page.getByRole('combobox', { name: 'Destination dataset' })).toContainText(
    'Loading datasets…',
  );
  await story(page, 'ingestion-setup--demo-mode');
  await selectJsonDestination(page);
  await expect(page.getByText('Example configuration', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Check ingestion' })).toBeDisabled();
  await expect(page.getByText('Live verification is unavailable in demo mode.')).toBeVisible();
});

for (const theme of ['light', 'dark'] as const) {
  for (const id of [
    'traces-explorer--default',
    'traces-explorer--detail',
    'traces-explorer--permission-denied',
    'apm-serviceinventory--services',
    'apm-serviceinventory--empty',
    'apm-servicetopology--observed-dependencies',
    'apm-servicetopology--no-dependencies',
    'apm-servicemap--cyclic-dependencies',
    'apm-servicemap--long-names',
    'apm-serviceinsights--overview',
    'apm-serviceinsights--empty',
    'ingestion-setup--default',
    'ingestion-setup--permission-denied',
    'ingestion-components--invalid-endpoint',
  ]) {
    test(`WCAG observability: ${id} in ${theme}`, async ({ page }) => {
      await story(page, id);
      if (id === 'traces-explorer--default')
        await expect(page.getByTestId('trace-table')).toBeVisible();
      if (id === 'traces-explorer--detail') {
        await expect(
          page
            .getByRole('region', { name: 'Span detail', exact: true })
            .getByRole('button', { name: 'Reload fields' }),
        ).toBeVisible();
      }
      if (id === 'traces-explorer--permission-denied')
        await expect(page.getByRole('alert')).toBeVisible();
      if (id === 'ingestion-setup--default') await selectJsonDestination(page);
      if (id === 'ingestion-setup--permission-denied')
        await expect(page.getByRole('alert')).toBeVisible();
      await page.evaluate((value) => {
        document.documentElement.dataset.theme = value;
      }, theme);
      await page.evaluate(async () => {
        await document.fonts.ready;
        await Promise.allSettled(document.getAnimations().map((animation) => animation.finished));
      });
      const result = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
        .analyze();
      expect(result.violations).toEqual([]);
    });
  }
}

test('ingestion custom partition guard survives source changes and selecting an unpartitioned dataset restores setup', async ({
  page,
}) => {
  await story(page, 'ingestion-setup--custom-partitions');
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('partitioned_logs');
  const error = page
    .getByRole('alert')
    .filter({ hasText: 'custom partition fields: tenant, region' });
  const check = page.getByRole('button', { name: 'Check ingestion', exact: true });
  for (const source of [/^JSON HTTP/, /^OTLP HTTP/, /^OpenTelemetry Collector/]) {
    await page.getByRole('radio', { name: source }).check();
    await expect(error).toContainText('Select or create a dataset without custom partitions');
    await expect(page.locator('.ingestion-code-block')).toHaveCount(0);
    await expect(page.getByRole('button', { name: /^Copy / })).toHaveCount(0);
    await expect(check).toBeDisabled();
  }
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('application_logs');
  await expect(error).toHaveCount(0);
  await expect(page.getByLabel('collector.yaml', { exact: true })).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Copy collector.yaml', exact: true }),
  ).toBeVisible();
  await expect(check).toBeEnabled();
});
