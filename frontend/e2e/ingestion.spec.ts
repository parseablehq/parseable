import { test, expect, type Page, type Route } from '@playwright/test';

type Call = { path: string; method: string; headers: Record<string, string> };
type FixtureInfo = {
  customPartition?: string;
  telemetryType: string;
  logSource: Array<{ log_source_format: string }>;
};
const info = (signal: string, source: string): FixtureInfo => ({
  telemetryType: signal,
  logSource: [{ log_source_format: source }],
});
const seed = () =>
  new Map<string, FixtureInfo>([
    ['web_logs', info('logs', 'json')],
    ['other_logs', info('logs', 'json')],
    ['metrics_a', info('metrics', 'otel-metrics')],
    ['traces_a', info('traces', 'otel-traces')],
  ]);
const stats = (name: string, count = 42) => ({
  stream: name,
  time: '2026-10-12T15:30:00Z',
  ingestion: {
    count,
    size: 1024,
    format: 'json',
    lifetime_count: count,
    lifetime_size: 1024,
    deleted_count: 0,
    deleted_size: 0,
  },
  storage: { size: 512, format: 'parquet', lifetime_size: 512, deleted_size: 0 },
});

async function mockLive(
  page: Page,
  options: {
    empty?: boolean;
    listStatus?: number;
    override?: (route: Route, call: Call) => Promise<void> | undefined;
  } = {},
) {
  const calls: Call[] = [];
  const entries = options.empty ? new Map<string, FixtureInfo>() : seed();
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const call = { path, method: request.method(), headers: request.headers() };
    calls.push(call);
    const override = options.override?.(route, call);
    if (override) return override;
    if (path === '/api/v1/about')
      return route.fulfill({ json: { capabilities: { promql: true } } });
    if (path === '/api/v1/logstream')
      return route.fulfill(
        options.listStatus
          ? {
              status: options.listStatus,
              body:
                options.listStatus === 403
                  ? 'Dataset access denied'
                  : 'Dataset inventory unavailable',
            }
          : { json: [...entries.keys()].map((name) => ({ name })) },
      );
    const match = path.match(/^\/api\/v1\/logstream\/([^/]+)(?:\/(info|stats|schema))?$/);
    if (!match) return route.fulfill({ status: 404, body: 'Not found' });
    const name = decodeURIComponent(match[1]);
    if (call.method === 'PUT') {
      entries.set(
        name,
        info(call.headers['x-p-telemetry-type'], call.headers['x-p-log-source'] || 'json'),
      );
      return route.fulfill({ status: 200, body: 'Log stream created' });
    }
    if (!entries.has(name)) return route.fulfill({ status: 404, body: 'Dataset not found' });
    if (match[2] === 'info') return route.fulfill({ json: entries.get(name) });
    if (match[2] === 'schema')
      return route.fulfill({ json: { fields: [{ name: 'p_timestamp' }, { name: 'message' }] } });
    if (match[2] === 'stats') return route.fulfill({ json: stats(name) });
    return route.fulfill({ status: 404, body: 'Not found' });
  });
  return calls;
}
async function open(page: Page, demo = false) {
  if (demo) await page.addInitScript(() => sessionStorage.setItem('parseable-mode', 'demo'));
  await page.goto('/add-data');
  await expect(page.getByRole('heading', { name: 'Add data', exact: true })).toBeVisible();
}
async function choose(page: Page, dataset: string) {
  await page.getByLabel('Destination dataset', { exact: true }).selectOption(dataset);
  await expect(page.getByLabel('Ingestion command', { exact: true })).toBeVisible();
}
async function clipboard(page: Page, fail = false) {
  await page.addInitScript(
    (failure) =>
      Object.defineProperty(navigator, 'clipboard', {
        configurable: true,
        value: {
          writeText: async (value: string) => {
            if (failure) throw new Error('Denied');
            sessionStorage.setItem('ingestion-copied', value);
          },
        },
      }),
    fail,
  );
}

test('demo switches source and signal, blocks incompatible datasets, and copies placeholder credentials', async ({
  page,
}) => {
  await clipboard(page);
  await open(page, true);
  await choose(page, 'application_logs');
  await page.getByLabel('Public ingestion endpoint').fill('https://ingest.example.com/proxy');
  await page.getByRole('button', { name: 'Copy Ingestion command', exact: true }).click();
  await expect(
    page.getByRole('button', { name: 'Copy Ingestion command', exact: true }),
  ).toHaveText('Copied');
  const copied = await page.evaluate(() => sessionStorage.getItem('ingestion-copied'));
  expect(copied).toContain('https://ingest.example.com/proxy/api/v1/ingest');
  expect(copied).toContain('Bearer ${PARSEABLE_API_KEY:?Set PARSEABLE_API_KEY}');
  await page.getByRole('radio', { name: /^OTLP HTTP/ }).check();
  await page.getByLabel('Signal', { exact: true }).selectOption('metrics');
  await expect(page.getByRole('alert')).toContainText('Select a metrics dataset');
  await expect(page.getByLabel('Ingestion command', { exact: true })).toHaveCount(0);
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('demo_metrics');
  await expect(page.getByLabel('Ingestion command', { exact: true })).toContainText('/v1/metrics');
  // The note under the snippet names the encoding the snippet uses
  await expect(
    page.getByText('This configuration uses JSON and sends only metrics to this dataset.'),
  ).toBeVisible();
  await page.getByRole('radio', { name: /^OpenTelemetry Collector/ }).check();
  await expect(page.getByLabel('collector.yaml', { exact: true })).toContainText('encoding: proto');
  await expect(
    page.getByText('This configuration uses protobuf and sends only metrics to this dataset.'),
  ).toBeVisible();
  await expect(page.getByLabel('collector.yaml', { exact: true })).toContainText(
    'X-P-Stream: "demo_metrics"',
  );
  await page.getByLabel('Signal', { exact: true }).selectOption('traces');
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('demo_traces');
  await expect(page.getByLabel('collector.yaml', { exact: true })).toContainText('    traces:');
  await expect(page.getByRole('button', { name: 'Check ingestion', exact: true })).toBeDisabled();
  await expect(
    page.getByText('Live verification is unavailable in demo mode.', { exact: true }),
  ).toBeVisible();
});

test('endpoint validation hides configuration and clipboard failures remain recoverable', async ({
  page,
}) => {
  await clipboard(page, true);
  await open(page, true);
  await choose(page, 'application_logs');
  await page.getByLabel('Public ingestion endpoint').fill('https://user:secret@example.com');
  await expect(page.getByRole('alert')).toContainText('Keep credentials out of the URL');
  await expect(page.getByLabel('Ingestion command', { exact: true })).toHaveCount(0);
  await page.getByLabel('Public ingestion endpoint').fill('https://ingest.example.com');
  await page.getByRole('button', { name: 'Copy Ingestion command', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Clipboard is unavailable');
  await expect(page.getByLabel('Ingestion command', { exact: true })).toBeVisible();
});

for (const [source, signal, format] of [
  ['json', 'logs', 'json'],
  ['otlp', 'metrics', 'otel-metrics'],
  ['collector', 'traces', 'otel-traces'],
] as const)
  test(`live creation sends ${format} source and ${signal} telemetry headers`, async ({ page }) => {
    const calls = await mockLive(page, { empty: true });
    await open(page);
    if (source !== 'json') {
      await page
        .getByRole('radio', { name: source === 'otlp' ? /^OTLP HTTP/ : /^OpenTelemetry Collector/ })
        .check();
      await page.getByLabel('Signal', { exact: true }).selectOption(signal);
    }
    await page.getByRole('button', { name: 'Create new dataset', exact: true }).click();
    await page.getByLabel('New dataset name').fill('select');
    await expect(page.getByRole('button', { name: 'Create dataset', exact: true })).toBeDisabled();
    await expect(page.getByRole('alert')).toContainText('SQL keyword');
    const name = `ingestion_${signal}`;
    await page.getByLabel('New dataset name').fill(name);
    await page.getByRole('button', { name: 'Create dataset', exact: true }).click();
    await expect(page.getByLabel('Destination dataset', { exact: true })).toHaveValue(name);
    await expect(
      page.getByLabel(source === 'collector' ? 'collector.yaml' : 'Ingestion command', {
        exact: true,
      }),
    ).toContainText(name);
    const creates = calls.filter((call) => call.method === 'PUT');
    expect(creates).toHaveLength(1);
    expect(creates[0].path).toBe(`/api/v1/logstream/${name}`);
    expect(creates[0].headers['x-p-telemetry-type']).toBe(signal);
    expect(creates[0].headers['x-p-log-source']).toBe(format);
  });

for (const status of [403, 503])
  test(`live dataset inventory failure ${status} reports the error and disables verification`, async ({
    page,
  }) => {
    await mockLive(page, { listStatus: status });
    await open(page);
    await expect(page.getByRole('alert')).toContainText(
      status === 403
        ? "You don't have permission to list datasets"
        : 'Dataset inventory unavailable',
    );
    await expect(page.getByLabel('Ingestion command', { exact: true })).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Check ingestion', exact: true })).toBeDisabled();
  });

test('live verification reads the selected dataset and reports existing events without claiming a source connection', async ({
  page,
}) => {
  const calls = await mockLive(page);
  await open(page);
  await choose(page, 'other_logs');
  await page.getByRole('button', { name: 'Check ingestion', exact: true }).click();
  await expect(page.getByText('Existing data', { exact: true })).toBeVisible();
  await expect(page.locator('.ingestion-check-result .ui-badge')).toHaveAttribute(
    'data-tone',
    'neutral',
  );
  await expect(page.locator('.ingestion-check-result')).toContainText('42');
  await expect(page.locator('.ingestion-check-result')).toContainText(
    'This check establishes a baseline',
  );
  await expect(page.getByRole('link', { name: 'Explore logs', exact: true })).toHaveAttribute(
    'href',
    '/logs/explore/other_logs',
  );
  for (const suffix of ['stats', 'schema', 'info'])
    expect(calls.some((call) => call.path === `/api/v1/logstream/other_logs/${suffix}`)).toBe(true);
  expect(calls.filter((call) => /\/stats$/.test(call.path))).toHaveLength(1);
});

test('live verification retries errors and reports no events honestly', async ({ page }) => {
  let failed = true;
  await mockLive(page, {
    override: (route, call) =>
      call.path.endsWith('/stats')
        ? route.fulfill(
            failed
              ? { status: 503, body: 'Ingestion statistics unavailable' }
              : { json: stats('web_logs', 0) },
          )
        : undefined,
  });
  await open(page);
  await choose(page, 'web_logs');
  await page.getByRole('button', { name: 'Check ingestion', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Ingestion statistics unavailable');
  await expect(page.getByText('Existing data', { exact: true })).toHaveCount(0);
  failed = false;
  await page.getByRole('button', { name: 'Check ingestion', exact: true }).click();
  await expect(page.getByText('No events yet', { exact: true })).toBeVisible();
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page.getByRole('link', { name: 'Explore logs', exact: true })).toHaveCount(0);
});

test('verification retains its baseline through endpoint edits and resets for a new dataset', async ({
  page,
}) => {
  let checks = 0;
  await mockLive(page, {
    override: (route, call) =>
      call.path.endsWith('/web_logs/stats')
        ? route.fulfill({
            json: {
              ...stats('web_logs', checks++ > 0 ? 20 : 42),
              ingestion: {
                ...stats('web_logs', 20).ingestion,
                count: checks === 1 ? 42 : 20,
                lifetime_count: checks === 1 ? 42 : 45,
              },
            },
          })
        : undefined,
  });
  await open(page);
  await choose(page, 'web_logs');
  const check = page.getByRole('button', { name: 'Check ingestion', exact: true });
  await check.click();
  await expect(page.getByText('Existing data', { exact: true })).toBeVisible();
  await page.getByLabel('Public ingestion endpoint').fill('https://corrected.example.test');
  await check.click();
  await expect(page.getByText('3 new events', { exact: true })).toBeVisible();
  await expect(page.locator('.ingestion-check-result .ui-badge')).toHaveAttribute(
    'data-tone',
    'success',
  );
  await expect(page.locator('.ingestion-check-result')).toContainText(
    'These arrivals may include other senders',
  );
  await check.click();
  await expect(page.getByText('No new events', { exact: true })).toBeVisible();
  await choose(page, 'other_logs');
  await expect(page.locator('.ingestion-check-result')).toHaveCount(0);
  await check.click();
  await expect(page.getByText('Existing data', { exact: true })).toBeVisible();
});

test('non-ASCII dataset headers are rejected inline for existing and new datasets', async ({
  page,
}) => {
  await mockLive(page, {
    override: (route, call) =>
      call.path === '/api/v1/logstream'
        ? route.fulfill({ json: [{ name: 'café' }] })
        : call.path === '/api/v1/logstream/caf%C3%A9/info'
          ? route.fulfill({ json: info('logs', 'json') })
          : undefined,
  });
  await open(page);
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('café');
  await expect(page.getByRole('alert')).toContainText('ASCII letters');
  await expect(page.getByRole('heading', { name: 'Add data', exact: true })).toBeVisible();
  await expect(page.getByLabel('Ingestion command', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Check ingestion', exact: true })).toBeDisabled();
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('');
  await page.getByRole('button', { name: 'Create new dataset', exact: true }).click();
  await page.getByLabel('New dataset name').fill('日本語');
  await expect(page.getByRole('alert')).toContainText('ASCII letters');
  await expect(page.getByRole('button', { name: 'Create dataset', exact: true })).toBeDisabled();
});

test('cancelling and reopening creation clears errors from the old attempt', async ({ page }) => {
  await mockLive(page, {
    override: (route, call) =>
      call.method === 'PUT'
        ? route.fulfill({ status: 403, body: 'Dataset creation denied' })
        : undefined,
  });
  await open(page);
  await page.getByRole('button', { name: 'Create new dataset', exact: true }).click();
  await page.getByLabel('New dataset name').fill('denied_dataset');
  await page.getByRole('button', { name: 'Create dataset', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Dataset creation denied');
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  await page.getByRole('button', { name: 'Create new dataset', exact: true }).click();
  await expect(page.getByRole('alert')).toHaveCount(0);
});

test('dataset metadata errors can retry and never generate unverified configuration', async ({
  page,
}) => {
  let failed = true;
  await mockLive(page, {
    override: (route, call) =>
      call.path.endsWith('/web_logs/info') && failed
        ? route.fulfill({ status: 403, body: 'Cannot inspect this dataset' })
        : undefined,
  });
  await open(page);
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('web_logs');
  await expect(page.getByRole('alert')).toContainText(
    "You don't have permission to read dataset information",
  );
  await expect(page.getByLabel('Ingestion command', { exact: true })).toHaveCount(0);
  failed = false;
  await page.getByRole('button', { name: 'Retry dataset check', exact: true }).click();
  await expect(page.getByLabel('Ingestion command', { exact: true })).toBeVisible();
});

for (const change of ['source', 'signal', 'dataset', 'endpoint'] as const)
  test(
    change === 'endpoint'
      ? 'changing endpoint preserves pending dataset verification'
      : `changing ${change} cancels stale verification and clears its result`,
    async ({ page }) => {
      let release = () => {};
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      let holding = true;
      let started = false;
      await mockLive(page, {
        override: (route, call) =>
          call.path.endsWith('/web_logs/stats') && holding
            ? (async () => {
                started = true;
                await held;
                await route.fulfill({ json: stats('web_logs', 999) }).catch(() => {});
              })()
            : undefined,
      });
      await open(page);
      await page.getByRole('radio', { name: /^OTLP HTTP/ }).check();
      await choose(page, 'web_logs');
      await page.getByRole('button', { name: 'Check ingestion', exact: true }).click();
      await expect.poll(() => started).toBe(true);
      if (change === 'endpoint') {
        await page.getByLabel('Public ingestion endpoint').fill('https://other-ingest.example.com');
        holding = false;
        release();
        await expect(page.locator('.ingestion-check-result')).toContainText('999');
        await expect(page.locator('.ingestion-check-result')).toContainText('Existing data');
        return;
      }
      const cancelled = page.waitForEvent('requestfailed', (request) =>
        new URL(request.url()).pathname.endsWith('/web_logs/stats'),
      );
      if (change === 'source')
        await page.getByRole('radio', { name: /^OpenTelemetry Collector/ }).check();
      if (change === 'signal')
        await page.getByLabel('Signal', { exact: true }).selectOption('metrics');
      if (change === 'dataset')
        await page.getByLabel('Destination dataset', { exact: true }).selectOption('other_logs');
      await cancelled;
      holding = false;
      release();
      await expect(page.locator('.ingestion-check-result')).toHaveCount(0);
      if (change === 'signal')
        await page.getByLabel('Destination dataset', { exact: true }).selectOption('metrics_a');
      await page.getByRole('button', { name: 'Check ingestion', exact: true }).click();
      await expect(page.locator('.ingestion-check-result')).toContainText('42');
      await expect(page.locator('.ingestion-check-result')).not.toContainText('999');
    },
  );

test('custom partition datasets block guided senders and creating an unpartitioned dataset restores setup', async ({
  page,
}) => {
  const calls = await mockLive(page, {
    override: (route, call) =>
      call.path.endsWith('/web_logs/info')
        ? route.fulfill({ json: { ...info('logs', 'json'), customPartition: 'tenant, region' } })
        : undefined,
  });
  await open(page);
  await page.getByLabel('Destination dataset', { exact: true }).selectOption('web_logs');
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
  expect(calls.filter((call) => call.path.endsWith('/stats'))).toHaveLength(0);
  await page.getByRole('button', { name: 'Create new dataset', exact: true }).click();
  await page.getByLabel('New dataset name').fill('unpartitioned_logs');
  await page.getByRole('button', { name: 'Create dataset', exact: true }).click();
  await expect(page.getByLabel('Destination dataset', { exact: true })).toHaveValue(
    'unpartitioned_logs',
  );
  await expect(error).toHaveCount(0);
  await expect(page.getByLabel('collector.yaml', { exact: true })).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Copy collector.yaml', exact: true }),
  ).toBeVisible();
  await expect(check).toBeEnabled();
});
