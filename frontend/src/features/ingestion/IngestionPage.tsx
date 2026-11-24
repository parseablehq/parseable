import { useCallback, useState, type ReactNode } from 'react';
import { Database, Radio, Terminal, CheckCircle2 } from 'lucide-react';
import { Badge, Card } from '../../components/ui';
import { PageHeader } from '../../components/explorer/PageHeader';
import { useApp } from '../../app/AppProvider';
import { useAsync } from '../../hooks/useAsync';
import { useMutation } from '../../hooks/useMutation';
import {
  ingestionCompatibility,
  ingestionLogSource,
  validateIngestionEndpoint,
  validateIngestionDataset,
  type IngestionSignal,
  type IngestionSource,
} from '../../lib/ingestion';
import { datasetError } from '../datasets/helpers';
import { SourcePicker } from './SourcePicker';
import { DatasetStep } from './DatasetStep';
import { ConnectionConfig } from './ConnectionConfig';
import { IngestionVerification } from './IngestionVerification';
import './ingestion.css';

function SetupStep({
  number,
  title,
  description,
  icon,
  children,
}: {
  number: number;
  title: string;
  description: string;
  icon: ReactNode;
  children: ReactNode;
}) {
  return (
    <Card className="ingestion-step">
      <div className="ingestion-step-heading">
        <span className="ingestion-step-number">{number}</span>
        <div>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
        <span className="ingestion-step-icon">{icon}</span>
      </div>
      <div className="ingestion-step-body">{children}</div>
    </Card>
  );
}

function IngestionWorkspace() {
  const { client, mode } = useApp();
  const datasets = useAsync(useCallback((abort) => client.listDatasets(abort), [client]));
  const [source, setSource] = useState<IngestionSource>('json');
  const [signal, setSignal] = useState<IngestionSignal>('logs');
  const [dataset, setDataset] = useState('');
  const [endpoint, setEndpoint] = useState(
    mode === 'demo' ? 'https://ingest.example.com' : window.location.origin,
  );
  const mutation = useMutation();
  const effectiveSignal = source === 'json' ? 'logs' : signal;
  const metadata = useAsync(
    useCallback(
      (abort) => (dataset ? client.datasetInfo(dataset, abort) : Promise.resolve(undefined)),
      [client, dataset],
    ),
  );
  const compatibilityError =
    validateIngestionDataset(dataset) ||
    (metadata.data ? ingestionCompatibility(metadata.data, source, effectiveSignal) : undefined);
  const ready = Boolean(
    dataset && metadata.data && !metadata.loading && !metadata.error && !compatibilityError,
  );
  async function createDataset(name: string): Promise<boolean> {
    let created = false;
    await mutation.run(async () => {
      try {
        await client.createDataset(name, {
          telemetryType: effectiveSignal,
          logSource: ingestionLogSource(source, effectiveSignal),
        });
      } catch (failure) {
        throw new Error(datasetError(failure, 'create datasets'));
      }
      if (!mutation.isActive()) return;
      setDataset(name);
      datasets.reload();
      created = true;
    });
    return created;
  }
  return (
    <div className="page ingestion-page">
      <PageHeader
        title="Add data"
        description="Connect your applications and bring logs, metrics, and traces into Parseable."
        actions={<Badge>{mode === 'demo' ? 'Demo setup' : 'Live workspace'}</Badge>}
      />
      <div className="ingestion-intro">
        <span className="ingestion-intro-icon">
          <Radio size={24} aria-hidden="true" />
        </span>
        <div>
          <h2>From source to insight</h2>
          <p>
            Choose your source, select a destination, and configure your sender. Your credentials
            stay in your deployment environment.
          </p>
        </div>
        <Badge>4 steps</Badge>
      </div>
      {mode === 'demo' && (
        <div className="ingestion-demo-notice" role="status">
          Demo mode builds example configuration and creates demo datasets. Use your live workspace
          to check real ingestion.
        </div>
      )}
      <div className="ingestion-layout">
        <div className="ingestion-steps">
          <SetupStep
            number={1}
            title="Choose your source"
            description="Start with the format your application produces."
            icon={<Radio size={18} aria-hidden="true" />}
          >
            <SourcePicker
              source={source}
              signal={effectiveSignal}
              onSourceChange={(value) => {
                setSource(value);
                mutation.reset();
              }}
              onSignalChange={(value) => {
                setSignal(value);
                mutation.reset();
              }}
              disabled={mutation.pending}
            />
          </SetupStep>
          <SetupStep
            number={2}
            title="Select a destination"
            description="Keep each telemetry signal in its own dataset."
            icon={<Database size={18} aria-hidden="true" />}
          >
            <DatasetStep
              datasets={datasets.data ?? []}
              dataset={dataset}
              onSelect={setDataset}
              onCreate={createDataset}
              loading={datasets.loading}
              error={datasets.error ? datasetError(datasets.error, 'list datasets') : undefined}
              reload={datasets.reload}
              pending={mutation.pending}
              createError={mutation.error}
              info={metadata.data}
              infoLoading={metadata.loading}
              infoError={
                metadata.error
                  ? datasetError(metadata.error, 'read dataset information')
                  : undefined
              }
              compatibilityError={compatibilityError}
              retryInfo={metadata.reload}
              resetCreateError={mutation.reset}
            />
          </SetupStep>
          <SetupStep
            number={3}
            title="Configure your sender"
            description="Copy the generated setup into your application environment."
            icon={<Terminal size={18} aria-hidden="true" />}
          >
            <ConnectionConfig
              options={{ source, signal: effectiveSignal, dataset, endpoint }}
              onEndpointChange={setEndpoint}
              ready={ready}
              demo={mode === 'demo'}
            />
          </SetupStep>
          <SetupStep
            number={4}
            title="Check for data"
            description="Inspect the destination after your sender has started."
            icon={<CheckCircle2 size={18} aria-hidden="true" />}
          >
            <IngestionVerification
              key={`${source}:${effectiveSignal}:${dataset}`}
              client={client}
              dataset={dataset}
              signal={effectiveSignal}
              enabled={ready && !validateIngestionEndpoint(endpoint)}
              demo={mode === 'demo'}
            />
          </SetupStep>
        </div>
        <aside className="ingestion-guide" aria-label="Setup guide">
          <h2>One dataset, one signal</h2>
          <p>
            Logs explain events. Metrics reveal trends. Traces connect a request across services.
          </p>
          <div className="ingestion-guide-divider" />
          <h3>Before you start</h3>
          <ul>
            <li>A reachable ingestion server</li>
            <li>An API key with ingest access</li>
            <li>A dataset that matches your format</li>
          </ul>
          <div className="ingestion-guide-divider" />
          <h3>Sending OpenTelemetry?</h3>
          <p>
            Use OTLP over HTTP with JSON or protobuf encoding. A collector can receive your SDK's
            OTLP traffic and forward it to Parseable.
          </p>
          <a
            className="text-link"
            href="https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter"
            target="_blank"
            rel="noreferrer"
          >
            Collector exporter reference ↗
          </a>
        </aside>
      </div>
    </div>
  );
}

export function IngestionPage() {
  const { mode, session } = useApp();
  return <IngestionWorkspace key={`${mode}:${session}`} />;
}
