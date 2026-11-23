"""Server-free checks for OTLP fixture, counter and readback oracles."""
import base64
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import otlp
from otlp_fixtures import HTTP_LIMIT, SIGNALS, fixtures


class OtlpHarnessTests(unittest.TestCase):
    def test_abba_order_and_paired_ratio(self):
        self.assertEqual([otlp.pair_order(i) for i in range(4)],
                         [('json', 'protobuf'), ('protobuf', 'json')] * 2)
        sample = lambda n: otlp.per_row({'instructions': n, 'cpu_s': n / 100}, 10)
        pairs = [{'json': sample(60), 'protobuf': sample(30)},
                 {'json': sample(600), 'protobuf': sample(150)}]
        self.assertEqual(otlp.summarize(pairs)['protobuf_over_json']['instructions_per_row'],
                         {'median': 0.375, 'min': 0.25, 'max': 0.5})

    def test_perf_counters_reject_missing_multiplexed_and_uncounted_events(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'perf'
            task = '700.25;msec;task-clock;123;100.00;;\n'
            instructions = '13000;;instructions:u;123;100.00;;\n'
            path.write_text('# perf output\n' + instructions + task)
            self.assertEqual(otlp.counters(path), {'instructions': 13000, 'cpu_s': 0.70025})
            for event in ('', instructions.replace('100.00', '38.42'),
                          instructions.replace('13000', '<not counted>'),
                          instructions * 2):
                with self.subTest(event=event):
                    path.write_text(event + task)
                    with self.assertRaises(RuntimeError):
                        otlp.counters(path)

    def test_readback_ignores_timestamp_and_order_only(self):
        rows = [dict(bench_batch='00000000', bench_row=f'{i:06d}', p_timestamp='a', v=i)
                for i in range(2)]
        expected = otlp.canonical_rows(rows, 0, 1, 2)
        other = [dict(row, p_timestamp='b') for row in reversed(rows)]
        self.assertEqual(otlp.canonical_rows(other, 0, 1, 2), expected)
        other[0]['v'] = True
        self.assertNotEqual(otlp.canonical_rows(other, 0, 1, 2), expected)
        with self.assertRaisesRegex(AssertionError, 'identity mismatch'):
            otlp.canonical_rows([rows[0], rows[0]], 0, 1, 2)

    def test_perf_control_accepts_nul_terminated_ack(self):
        cmd_read, cmd_write = os.pipe()
        ack_read, ack_write = os.pipe()
        try:
            os.write(ack_write, b'ack\n\0')
            otlp.perf_control(cmd_write, ack_read, 'disable')
            self.assertEqual(os.read(cmd_read, 50), b'disable\n')
        finally:
            for fd in (cmd_read, cmd_write, ack_read, ack_write):
                os.close(fd)

    def test_bad_status_and_nonempty_success_fail(self):
        with self.assertRaises(AssertionError):
            otlp.expect_response((503, None, b''), 200, b'')
        with self.assertRaises(AssertionError):
            otlp.expect_response((200, 'text/plain', b'error'), 200, b'')

    def test_chunked_probe_sends_every_body_byte(self):
        connection = Mock()
        connection.getresponse.return_value.status = 413
        payload = b'x' * 65536 + b'y'
        self.assertEqual(otlp.post(connection, 'logs', 'probe', 'protobuf', payload,
                                   chunked=True)[0], 413)
        call = connection.request.call_args
        self.assertTrue(call.kwargs['encode_chunked'])
        self.assertNotIn('Content-Length', call.args[3])
        self.assertEqual([bytes(chunk) for chunk in call.args[2]], [b'x' * 65536, b'y'])

    @unittest.skipUnless(importlib.util.find_spec('opentelemetry'), 'install opentelemetry-proto')
    def test_fixture_wire_encodings_match(self):
        from google.protobuf.json_format import ParseDict
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

        types = dict(logs=ExportLogsServiceRequest, metrics=ExportMetricsServiceRequest,
                     traces=ExportTraceServiceRequest)
        for signal in SIGNALS:
            with self.subTest(signal=signal):
                bodies = fixtures(signal, 2, 32, batch=5)
                payload = json.loads(bodies['json'])
                generic = copy.deepcopy(payload)
                if signal == 'logs':
                    records = generic['resourceLogs'][0]['scopeLogs'][0]['logRecords']
                elif signal == 'traces':
                    records = generic['resourceSpans'][0]['scopeSpans'][0]['spans']
                else:
                    records = generic['resourceMetrics'][0]['scopeMetrics'][0]['metrics'][0]['gauge']['dataPoints']
                self.assertEqual(len(records), 2)
                for i, record in enumerate(records):
                    attrs = {attr['key']: attr['value'] for attr in record['attributes']}
                    self.assertEqual(len(attrs), 32)
                    self.assertEqual(attrs['bench_batch'], {'stringValue': '00000005'})
                    self.assertEqual(attrs['bench_row'], {'stringValue': f'{i:06d}'})
                    for key in {'traceId', 'spanId', 'parentSpanId'} & record.keys():
                        record[key] = base64.b64encode(bytes.fromhex(record[key])).decode()
                parsed = ParseDict(generic, types[signal]())
                self.assertEqual(parsed, types[signal].FromString(bodies['protobuf']))
                self.assertTrue(all(len(body) <= HTTP_LIMIT
                                    for body in fixtures(signal, 5000, 32).values()))


if __name__ == '__main__':
    unittest.main()
