import type { Dataset, DatasetInfo, DatasetStats } from './types';

export type IngestionSource = 'json' | 'otlp' | 'collector';
export type IngestionSignal = Dataset['type'];
export type IngestionOptions = {
  endpoint: string;
  dataset: string;
  source: IngestionSource;
  signal: IngestionSignal;
};

/** Public URLs become both shell arguments and collector values. Never accept credentials
 * or collector environment interpolation in an endpoint supplied by the UI. */
export function validateIngestionEndpoint(value: string): string | undefined {
  if (!value.trim()) return 'Enter the public URL of your Parseable ingestion server.';
  if (/[\s$\\]/.test(value)) return 'Use a URL without whitespace, backslashes or variables.';
  if (
    [...value].some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127)
  )
    return 'Use a URL without control characters.';
  if (!value.toLowerCase().startsWith('http://') && !value.toLowerCase().startsWith('https://'))
    return 'Enter a complete HTTP or HTTPS URL.';
  try {
    const url = new URL(value);
    if (!['http:', 'https:'].includes(url.protocol)) return 'Use an HTTP or HTTPS URL.';
    if (url.username || url.password) return 'Keep credentials out of the URL.';
    if (url.search || url.hash) return 'Use a base URL without a query string or fragment.';
    if (/\/(?:api\/v1\/ingest|v1\/(?:logs|metrics|traces))\/?$/.test(url.pathname))
      return 'Enter the base URL, without an ingestion route.';
    return undefined;
  } catch {
    return 'Enter a complete URL, such as https://ingest.example.com.';
  }
}

export function ingestionLogSource(source: IngestionSource, signal: IngestionSignal) {
  return source === 'json' ? 'json' : (`otel-${signal}` as const);
}

/** The ingestion handlers read X-P-Stream with HeaderValue::to_str(), which only
 * accepts ASCII. Dataset creation permits more names than ingestion can accept. */
export function validateIngestionDataset(value: string): string | undefined {
  if (!value) return 'Choose a valid dataset name before generating configuration.';
  if (!/^[A-Za-z0-9_-]+$/.test(value))
    return 'Ingestion requires a valid dataset name with ASCII letters, numbers, hyphens or underscores.';
  return undefined;
}

export function ingestionProgress(stats: DatasetStats, baseline?: number) {
  const lifetime = stats.ingestion.lifetime_count;
  // Retention can lower the current count. Lifetime counts measure arrivals;
  // a counter decrease establishes a fresh baseline instead of inventing a delta.
  return {
    lifetime,
    newEvents: baseline !== undefined && lifetime >= baseline ? lifetime - baseline : undefined,
  };
}

export function otlpSamplePayload(
  signal: IngestionSignal,
  timeUnixNano: string,
  endTimeUnixNano = timeUnixNano,
) {
  const resource = {
    attributes: [{ key: 'service.name', value: { stringValue: 'parseable-setup' } }],
  };
  const scope = { name: 'parseable-ingestion-setup' };
  if (signal === 'logs')
    return {
      resourceLogs: [
        {
          resource,
          scopeLogs: [
            {
              scope,
              logRecords: [
                {
                  timeUnixNano,
                  observedTimeUnixNano: timeUnixNano,
                  severityNumber: 9,
                  severityText: 'INFO',
                  body: { stringValue: 'Hello from Parseable' },
                  attributes: [],
                },
              ],
            },
          ],
        },
      ],
    };
  if (signal === 'metrics')
    return {
      resourceMetrics: [
        {
          resource,
          scopeMetrics: [
            {
              scope,
              metrics: [
                {
                  name: 'parseable.setup.events',
                  description: 'Parseable ingestion setup sample',
                  unit: '1',
                  gauge: { dataPoints: [{ timeUnixNano, asDouble: 1, attributes: [] }] },
                },
              ],
            },
          ],
        },
      ],
    };
  return {
    resourceSpans: [
      {
        resource,
        scopeSpans: [
          {
            scope,
            spans: [
              {
                traceId: '0123456789abcdef0123456789abcdef',
                spanId: '0123456789abcdef',
                name: 'parseable.setup',
                kind: 2,
                startTimeUnixNano: timeUnixNano,
                endTimeUnixNano,
                attributes: [],
                status: { code: 1 },
              },
            ],
          },
        ],
      },
    ],
  };
}

export function ingestionCompatibility(
  info: DatasetInfo,
  source: IngestionSource,
  signal: IngestionSignal,
): string | undefined {
  const type = source === 'json' ? 'logs' : signal;
  if (info.telemetryType && info.telemetryType.toLowerCase() !== type)
    return `This is a ${info.telemetryType} dataset. Select a ${type} dataset or create one.`;
  const formats = info.logSourceFormats;
  if (source === 'json' && formats.some((item) => item.startsWith('otel-')))
    return 'This dataset uses OTLP. Select OTLP HTTP or create a JSON dataset.';
  if (source !== 'json' && signal !== 'logs' && !formats.includes(`otel-${signal}`))
    return `This dataset does not use otel-${signal}. Create a dataset for this signal.`;
  if (
    source !== 'json' &&
    signal === 'logs' &&
    formats.some((item) => ['otel-metrics', 'otel-traces'].includes(item))
  )
    return 'This dataset uses a different OTLP signal. Select a logs dataset.';
  if (info.customPartition) {
    const fields = info.customPartition
      .split(',')
      .map((field) => field.trim())
      .join(', ');
    return `This dataset requires custom partition fields: ${fields}. Guided setup does not support custom partitions. Select or create a dataset without custom partitions, or configure your sender manually with values for every required field.`;
  }
  return undefined;
}

export function quoteShell(value: string): string {
  return `'${value.replaceAll("'", "'\"'\"'")}'`;
}

export function generateIngestionConfig(options: IngestionOptions): string {
  const error = validateIngestionEndpoint(options.endpoint);
  if (error) throw new Error(error);
  const datasetError = validateIngestionDataset(options.dataset);
  if (datasetError) throw new Error(datasetError);
  const endpoint = options.endpoint.replace(/\/+$/, '');
  const signal = options.source === 'json' ? 'logs' : options.signal;
  if (options.source === 'collector') {
    // Current collector exporter name. Protobuf is the exporter's default and
    // costs the server less than JSON; the server this UI ships with accepts it.
    // Disable default gzip: this backend does not implement body decompression.
    return `receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "127.0.0.1:4317"
      http:
        endpoint: "127.0.0.1:4318"

processors:
  batch: {}

exporters:
  otlp_http/parseable:
    endpoint: ${JSON.stringify(endpoint)}
    encoding: proto
    compression: none
    headers:
      Authorization: "Bearer \${env:PARSEABLE_API_KEY}"
      X-P-Stream: ${JSON.stringify(options.dataset)}

service:
  pipelines:
    ${signal}:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp_http/parseable]`;
  }
  const route = options.source === 'json' ? '/api/v1/ingest' : `/v1/${signal}`;
  const payload =
    options.source === 'json'
      ? `  --data-binary '{"message":"Hello from Parseable","service":"my-service","level":"info"}'`
      : `  --data-binary @- <<PARSEABLE_OTLP_JSON\n${JSON.stringify(otlpSamplePayload(signal, '${PARSEABLE_SAMPLE_TIME}', '${PARSEABLE_SAMPLE_END}'), null, 2)}\nPARSEABLE_OTLP_JSON`;
  const timestamp =
    options.source === 'otlp'
      ? `PARSEABLE_SAMPLE_SECONDS="$(date +%s)"\nPARSEABLE_SAMPLE_TIME="\${PARSEABLE_SAMPLE_SECONDS}000000000"\nPARSEABLE_SAMPLE_END="\${PARSEABLE_SAMPLE_SECONDS}001000000"\n`
      : '';
  return `# Set PARSEABLE_API_KEY in your shell to an API key with ingest access.
${timestamp}curl --fail-with-body --request POST ${quoteShell(endpoint + route)} \\
  --header "Authorization: Bearer \${PARSEABLE_API_KEY:?Set PARSEABLE_API_KEY}" \\
  --header ${quoteShell(`X-P-Stream: ${options.dataset}`)} \\
  --header 'Content-Type: application/json' \\
${payload}`;
}
