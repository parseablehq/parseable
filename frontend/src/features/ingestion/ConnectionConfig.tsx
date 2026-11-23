import { Badge, InlineError, Input } from '../../components/ui';
import { Link } from 'react-router-dom';
import {
  generateIngestionConfig,
  validateIngestionEndpoint,
  validateIngestionDataset,
  type IngestionOptions,
} from '../../lib/ingestion';
import { CommandCodeBlock } from './CommandCodeBlock';

export function ConnectionConfig({
  options,
  onEndpointChange,
  ready,
  demo,
}: {
  options: IngestionOptions;
  onEndpointChange: (value: string) => void;
  ready: boolean;
  demo: boolean;
}) {
  const endpointError = validateIngestionEndpoint(options.endpoint);
  const datasetError = options.dataset ? validateIngestionDataset(options.dataset) : undefined;
  const enabled = ready && !endpointError && !datasetError;
  const collector = options.source === 'collector';
  return (
    <div className="stack">
      <Input
        label="Public ingestion endpoint"
        value={options.endpoint}
        onChange={(event) => onEndpointChange(event.target.value)}
        error={endpointError}
        autoComplete="off"
        spellCheck={false}
        hint="Use the server URL reachable from your application or collector. Include any reverse proxy base path."
      />
      <InlineError error={ready ? datasetError : undefined} />
      {enabled ? (
        <>
          <div className="ingestion-config-status">
            <Badge>{demo ? 'Example configuration' : 'Configuration ready'}</Badge>
            <span>Run this on the machine sending your data.</span>
          </div>
          <CommandCodeBlock
            code={generateIngestionConfig(options)}
            title={collector ? 'collector.yaml' : 'Ingestion command'}
            language={collector ? 'YAML' : 'Shell'}
          />
          <div className="ingestion-instructions">
            <p>
              Set <code>PARSEABLE_API_KEY</code> in that environment to an API key with ingest
              permission for <strong>{options.dataset}</strong>. Manage keys in{' '}
              <Link to="/team">Team</Link>.
            </p>
            {collector ? (
              <p>
                Save as <code>collector.yaml</code> and start your collector with{' '}
                <code>otelcol --config=collector.yaml</code>. Point your application's OTLP exporter
                to the collector on port 4317 or 4318. This receiver binds to localhost; adjust it
                for your deployment.
              </p>
            ) : options.source === 'otlp' ? (
              <p>
                The command sends one sample{' '}
                {options.signal === 'traces'
                  ? 'span'
                  : options.signal === 'metrics'
                    ? 'gauge point'
                    : 'log'}{' '}
                with the current timestamp. Replace the sample OpenTelemetry envelope with your
                application's telemetry. The trace sample uses fixed identifiers for setup.
              </p>
            ) : (
              <p>
                The command sends one sample log. Replace the JSON body with your application's
                structured events.
              </p>
            )}
            {options.source !== 'json' && (
              <p>
                Parseable OSS accepts OTLP over HTTP with JSON or protobuf encoding. This
                configuration uses JSON and sends only {options.signal} to this dataset.
              </p>
            )}
          </div>
        </>
      ) : (
        <p className="ingestion-prompt">
          Choose a compatible destination dataset and enter a valid endpoint to generate your setup.
        </p>
      )}
    </div>
  );
}
