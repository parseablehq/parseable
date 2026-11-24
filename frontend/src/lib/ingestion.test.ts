import { describe, expect, it } from 'vitest';
import {
  generateIngestionConfig,
  ingestionCompatibility,
  quoteShell,
  validateIngestionEndpoint,
  validateIngestionDataset,
  otlpSamplePayload,
  ingestionProgress,
} from './ingestion';
import type { DatasetStats } from './types';
import { execFileSync } from 'node:child_process';
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

describe('public ingestion endpoints', () => {
  it('accepts HTTP origins and reverse proxy base paths', () => {
    for (const value of ['http://localhost:8000', 'https://ingest.example.com/proxy/'])
      expect(validateIngestionEndpoint(value)).toBeUndefined();
  });
  it.each([
    '',
    '/relative',
    'https:example.com',
    'https://example.com/\u0000value',
    'file:///tmp/server',
    'https://user:secret@example.com',
    'https://example.com/?secret=1',
    'https://example.com/#fragment',
    'https://example.com/v1/logs',
    'https://example.com/api/v1/ingest/',
    'https://example.com/${env:KEY}',
    'https://example.com/\nheader',
    'https://example.com/\\path',
  ])('rejects unsafe or ambiguous endpoint %s', (value) => {
    expect(validateIngestionEndpoint(value)).toBeTypeOf('string');
  });
});

describe('configuration generation', () => {
  it('uses the native JSON ingest route and required headers without actual credentials', () => {
    const code = generateIngestionConfig({
      endpoint: 'https://ingest.example.com/base/',
      dataset: 'application_logs',
      source: 'json',
      signal: 'logs',
    });
    expect(code).toContain("'https://ingest.example.com/base/api/v1/ingest'");
    expect(code).toContain("'X-P-Stream: application_logs'");
    expect(code).toContain("'Content-Type: application/json'");
    expect(code).toContain('Bearer ${PARSEABLE_API_KEY:?Set PARSEABLE_API_KEY}');
    expect(code).toContain('"message":"Hello from Parseable"');
    expect(code).not.toContain('X-P-Log-Source');
  });
  it.each(['logs', 'metrics', 'traces'] as const)('uses OTLP JSON route for %s', (signal) => {
    const code = generateIngestionConfig({
      endpoint: 'https://example.com',
      dataset: `otel_${signal}`,
      source: 'otlp',
      signal,
    });
    expect(code).toContain(`/v1/${signal}'`);
    expect(code).toContain('--data-binary @- <<PARSEABLE_OTLP_JSON');
    expect(code).toContain('PARSEABLE_SAMPLE_SECONDS="$(date +%s)"');
    expect(code).toContain('PARSEABLE_SAMPLE_TIME="${PARSEABLE_SAMPLE_SECONDS}000000000"');
    expect(code).not.toContain(`@${signal}.otlp.json`);
    expect(code).not.toContain('/api/v1/ingest');
  });
  it.each(['logs', 'metrics', 'traces'] as const)(
    'builds only the selected collector %s pipeline with protobuf encoding',
    (signal) => {
      const code = generateIngestionConfig({
        endpoint: 'https://example.com/base',
        dataset: `otel_${signal}`,
        source: 'collector',
        signal,
      });
      expect(code).toContain('otlp_http/parseable:');
      expect(code).toContain('encoding: proto');
      expect(code).toContain('compression: none');
      expect(code).toContain('Authorization: "Bearer ${env:PARSEABLE_API_KEY}"');
      expect(code).toContain(`X-P-Stream: "otel_${signal}"`);
      expect(code).toContain(`pipelines:\n    ${signal}:`);
      for (const other of ['logs', 'metrics', 'traces'].filter((item) => item !== signal))
        expect(code).not.toContain(`    ${other}:`);
    },
  );
  it('quotes shell metacharacters as literal endpoint characters', () => {
    expect(quoteShell("a'b`c$(touch /tmp/unsafe)")).toBe("'a'\"'\"'b`c$(touch /tmp/unsafe)'");
    const code = generateIngestionConfig({
      endpoint: "https://example.com/a'b`c",
      dataset: 'logs',
      source: 'json',
      signal: 'logs',
    });
    expect(code).toContain("'https://example.com/a'\"'\"'b`c/api/v1/ingest'");
  });
  it.each(['logs\r\nX-Secret: yes', '${env:SECRET}', 'logs;rm', ''])(
    'rejects injected dataset %s',
    (dataset) => {
      expect(() =>
        generateIngestionConfig({
          endpoint: 'https://example.com',
          dataset,
          source: 'collector',
          signal: 'logs',
        }),
      ).toThrow('valid dataset');
    },
  );
});

describe('ingestion header names', () => {
  it.each(['application_logs', 'otel-metrics', 'dataset2'])(
    'accepts native ASCII header name %s',
    (name) => expect(validateIngestionDataset(name)).toBeUndefined(),
  );
  it.each(['日本語', 'café', 'logs\r\nheader', 'logs value', 'logs/value'])(
    'rejects unusable header name %s',
    (name) => {
      expect(validateIngestionDataset(name)).toContain('ASCII');
      expect(() =>
        generateIngestionConfig({
          endpoint: 'https://example.com',
          dataset: name,
          source: 'otlp',
          signal: 'logs',
        }),
      ).toThrow('ASCII');
    },
  );
});

describe('runnable OTLP sample payloads', () => {
  const timestamp = '1791819000000000000';
  const end = '1791819000001000000';
  it('contains one log with the OTLP resource and severity envelope', () => {
    expect(otlpSamplePayload('logs', timestamp)).toMatchObject({
      resourceLogs: [
        {
          resource: {
            attributes: [{ key: 'service.name', value: { stringValue: 'parseable-setup' } }],
          },
          scopeLogs: [
            {
              logRecords: [
                {
                  timeUnixNano: timestamp,
                  severityNumber: 9,
                  body: { stringValue: 'Hello from Parseable' },
                },
              ],
            },
          ],
        },
      ],
    });
  });
  it('contains a gauge point with its OTLP metric type', () => {
    expect(otlpSamplePayload('metrics', timestamp)).toMatchObject({
      resourceMetrics: [
        {
          scopeMetrics: [
            {
              metrics: [
                {
                  name: 'parseable.setup.events',
                  gauge: { dataPoints: [{ timeUnixNano: timestamp, asDouble: 1 }] },
                },
              ],
            },
          ],
        },
      ],
    });
  });
  it('uses hex identifiers, integer enum values, and ordered timestamps for spans', () => {
    expect(otlpSamplePayload('traces', timestamp, end)).toMatchObject({
      resourceSpans: [
        {
          scopeSpans: [
            {
              spans: [
                {
                  traceId: expect.stringMatching(/^[0-9a-f]{32}$/),
                  spanId: expect.stringMatching(/^[0-9a-f]{16}$/),
                  kind: 2,
                  startTimeUnixNano: timestamp,
                  endTimeUnixNano: end,
                  status: { code: 1 },
                },
              ],
            },
          ],
        },
      ],
    });
    expect(BigInt(end)).toBeGreaterThan(BigInt(timestamp));
  });
  it.each(['logs', 'metrics', 'traces'] as const)(
    'embeds the complete %s payload with only known timestamp substitutions',
    (signal) => {
      const code = generateIngestionConfig({
        endpoint: 'https://example.com',
        dataset: `otel_${signal}`,
        source: 'otlp',
        signal,
      });
      const body = code.split('<<PARSEABLE_OTLP_JSON\n')[1]?.split('\nPARSEABLE_OTLP_JSON')[0];
      expect(body).toBeDefined();
      const resolved = body!
        .replaceAll('${PARSEABLE_SAMPLE_TIME}', timestamp)
        .replaceAll('${PARSEABLE_SAMPLE_END}', end);
      expect(JSON.parse(resolved)).toEqual(otlpSamplePayload(signal, timestamp, end));
    },
  );
  it.each(['logs', 'metrics', 'traces'] as const)(
    'expands the %s heredoc into valid JSON in a POSIX shell',
    (signal) => {
      const directory = mkdtempSync(join(tmpdir(), 'parseable-ingestion-'));
      try {
        const fakeCurl = join(directory, 'curl');
        writeFileSync(fakeCurl, '#!/bin/sh\ncat\n');
        chmodSync(fakeCurl, 0o700);
        const code = generateIngestionConfig({
          endpoint: 'https://example.com',
          dataset: `otel_${signal}`,
          source: 'otlp',
          signal,
        });
        const output = execFileSync('/bin/sh', ['-c', code], {
          encoding: 'utf8',
          env: {
            ...process.env,
            PATH: `${directory}:${process.env.PATH}`,
            PARSEABLE_API_KEY: 'unit-test-placeholder',
          },
        });
        const payload: unknown = JSON.parse(output);
        const generatedTime = JSON.stringify(payload).match(
          /(?:timeUnixNano|startTimeUnixNano)":"(\d+)"/,
        )?.[1];
        expect(generatedTime).toMatch(/^\d+000000000$/);
        expect(payload).toEqual(
          otlpSamplePayload(signal, generatedTime!, String(BigInt(generatedTime!) + 1_000_000n)),
        );
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    },
  );
});

describe('ingestion baseline observations', () => {
  const stats = (current: number, lifetime: number) =>
    ({ ingestion: { count: current, lifetime_count: lifetime } }) as DatasetStats;
  it('establishes a first baseline without claiming new arrivals', () =>
    expect(ingestionProgress(stats(100, 300))).toEqual({ lifetime: 300, newEvents: undefined }));
  it('uses lifetime arrivals even when retention lowers current events', () =>
    expect(ingestionProgress(stats(80, 305), 300)).toEqual({ lifetime: 305, newEvents: 5 }));
  it('reports zero when no arrivals occurred', () =>
    expect(ingestionProgress(stats(50, 300), 300).newEvents).toBe(0));
  it('resets the baseline when lifetime counters decrease', () =>
    expect(ingestionProgress(stats(1, 1), 300)).toEqual({ lifetime: 1, newEvents: undefined }));
});

describe('dataset compatibility', () => {
  const info = (type: string, formats: string[]) => ({
    name: 'test',
    telemetryType: type,
    logSourceFormats: formats,
  });
  it('does not trust list dataset type for OTLP metrics or traces', () => {
    expect(ingestionCompatibility(info('metrics', ['json']), 'otlp', 'metrics')).toContain(
      'otel-metrics',
    );
    expect(
      ingestionCompatibility(info('traces', ['otel-traces']), 'collector', 'traces'),
    ).toBeUndefined();
  });
  it('permits JSON logs to upgrade to OTLP logs and prevents reverse use', () => {
    expect(ingestionCompatibility(info('logs', ['json']), 'otlp', 'logs')).toBeUndefined();
    expect(ingestionCompatibility(info('logs', ['otel-logs']), 'json', 'logs')).toContain(
      'uses OTLP',
    );
  });
  it('rejects conflicting telemetry and source metadata', () => {
    expect(ingestionCompatibility(info('metrics', ['otel-metrics']), 'otlp', 'logs')).toContain(
      'metrics dataset',
    );
    expect(ingestionCompatibility(info('logs', ['otel-traces']), 'otlp', 'logs')).toContain(
      'different OTLP signal',
    );
  });
  it.each([
    ['json', 'logs', 'json', 'tenant'],
    ['json', 'logs', 'json', 'tenant, region'],
    ['otlp', 'logs', 'otel-logs', 'tenant'],
    ['otlp', 'metrics', 'otel-metrics', 'tenant, region'],
    ['otlp', 'traces', 'otel-traces', 'tenant'],
    ['collector', 'logs', 'otel-logs', 'tenant'],
    ['collector', 'metrics', 'otel-metrics', 'tenant, region'],
    ['collector', 'traces', 'otel-traces', 'tenant'],
  ] as const)(
    'rejects custom partitions for %s %s setup (%s, %s)',
    (source, signal, format, customPartition) => {
      const error = ingestionCompatibility(
        { ...info(signal, [format]), customPartition },
        source,
        signal,
      );
      expect(error).toContain('custom partition');
      for (const field of customPartition.split(',').map((field) => field.trim()))
        expect(error).toContain(field);
      expect(error).toContain('create');
    },
  );
  it.each([undefined, ''])(
    'permits destinations without custom partitions (%s)',
    (customPartition) => {
      expect(
        ingestionCompatibility({ ...info('logs', ['json']), customPartition }, 'json', 'logs'),
      ).toBeUndefined();
    },
  );
});
