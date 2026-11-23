"""One typed OTLP request per fixture, serialized into both wire formats."""
import json

CONTENT_TYPES = {'json': 'application/json', 'protobuf': 'application/x-protobuf'}
SIGNALS = ('logs', 'metrics', 'traces')
HTTP_LIMIT = 10 * 2**20
TIMESTAMP = 1_792_780_200_000_000_000


def fixtures(signal, rows, width, batch=0, *, timestamp_ns=TIMESTAMP, padding=0):
    # Keep the generated bindings optional for the other harnesses and their tests.
    from google.protobuf.json_format import MessageToDict
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

    def attributes(target, values):
        fields = {str: 'string_value', int: 'int_value', bool: 'bool_value', float: 'double_value'}
        for key, value in values:
            attribute = target.attributes.add(key=key)
            setattr(attribute.value, fields[type(value)], value)

    if signal == 'logs':
        message = ExportLogsServiceRequest()
        resource = message.resource_logs.add()
        scope = resource.scope_logs.add()
        records = scope.log_records
    elif signal == 'metrics':
        message = ExportMetricsServiceRequest()
        resource = message.resource_metrics.add()
        scope = resource.scope_metrics.add()
        metric = scope.metrics.add(name='system.cpu.utilization', unit='1',
                                   description='Fraction of time the CPU was busy')
        records = metric.gauge.data_points
    elif signal == 'traces':
        message = ExportTraceServiceRequest()
        resource = message.resource_spans.add()
        scope = resource.scope_spans.add()
        records = scope.spans
    else:
        raise ValueError(signal)

    # The resource describes a host and has no service identity, so Parseable
    # emits one row per data point without a synthetic target_info sample.
    attributes(resource.resource, [('host.name', 'test-edge-01'),
                                   ('deployment.environment.name', 'staging')])
    scope.scope.name = 'checkout.telemetry'
    scope.scope.version = '1.2.3'
    for index in range(rows):
        record = records.add()
        timestamp = timestamp_ns + index * 1_000_000
        trace_id = (batch * rows + index + 1).to_bytes(16, 'big')
        span_id = (index + 1).to_bytes(8, 'big')
        if signal == 'logs':
            record.time_unix_nano = timestamp
            record.observed_time_unix_nano = timestamp + 1000
            record.severity_number = 9
            record.severity_text = 'INFO'
            record.body.string_value = f'Processed checkout request for order {10000 + index}'
            record.trace_id, record.span_id = trace_id, span_id
        elif signal == 'metrics':
            record.time_unix_nano = timestamp
            record.start_time_unix_nano = timestamp - 1_000_000
            record.as_double = (index % 64) / 64
        else:
            record.trace_id, record.span_id = trace_id, span_id
            record.parent_span_id = b'\x16' * 8
            record.name = 'POST /checkout'
            record.kind = 2
            record.start_time_unix_nano = timestamp
            record.end_time_unix_nano = timestamp + 12_000_000
            record.status.code = 1

        values = [('bench_batch', f'{batch:08d}'), ('bench_row', f'{index:06d}'),
                  ('http.response.status_code', 200), ('region', 'us-central')]
        for field in range(width - len(values)):
            value = (f'checkout-{index % 29}', index + field, index % 2 == 0, 12.5)[field % 4]
            values.append((f'attr_{field:02d}', value))
        if padding:
            values.append(('bench_padding', 'x' * padding))
        attributes(record, values)

    payload = MessageToDict(message, use_integers_for_enums=True)
    # OTLP JSON uses hex for IDs, unlike protobuf's generic JSON mapping.
    if signal == 'logs':
        json_records = payload['resourceLogs'][0]['scopeLogs'][0]['logRecords']
        id_fields = ('trace_id', 'span_id')
    elif signal == 'traces':
        json_records = payload['resourceSpans'][0]['scopeSpans'][0]['spans']
        id_fields = ('trace_id', 'span_id', 'parent_span_id')
    else:
        json_records, id_fields = (), ()
    for source, target in zip(records, json_records):
        for field in id_fields:
            head, *tail = field.split('_')
            target[head + "".join(part.title() for part in tail)] = getattr(source, field).hex()
    return {'json': json.dumps(payload, separators=(',', ':')).encode(),
            'protobuf': message.SerializeToString()}
