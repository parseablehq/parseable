#!/usr/bin/env python3
"""Compare OTLP protobuf and JSON on one release build, with paired perf counters."""
import argparse
from contextlib import closing, contextmanager
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import select
import signal as signals
import statistics
import subprocess
import time

from otlp_fixtures import CONTENT_TYPES, HTTP_LIMIT, SIGNALS, fixtures
from run import AUTH, request, run_arm

PERF = ['perf', 'stat', '-x', ';', '--no-big-num', '-e', 'instructions:u,task-clock']


def pair_order(pair):
    return ('json', 'protobuf') if pair % 2 == 0 else ('protobuf', 'json')


def counters(path):
    events = {}
    for line in path.read_text().splitlines():
        fields = line.split(';')
        if len(fields) < 5 or fields[2] not in ('instructions:u', 'task-clock'):
            continue
        value, unit, event, _, running = fields[:5]
        if event in events or value.startswith('<') or not 99 <= float(running) <= 100:
            raise RuntimeError(f'unusable perf counter in {path}: {line}')
        events[event] = (value, unit)
    if set(events) != {'instructions:u', 'task-clock'}:
        raise RuntimeError(f'missing perf counters in {path}')
    instructions = int(events['instructions:u'][0])
    cpu_ms, unit = events['task-clock']
    if instructions <= 0 or unit != 'msec' or float(cpu_ms) <= 0:
        raise RuntimeError(f'invalid perf counter values in {path}: {events}')
    return {'instructions': instructions, 'cpu_s': float(cpu_ms) / 1000}


def perf_control(command_fd, ack_fd, command):
    os.write(command_fd, (command + '\n').encode())
    ready, _, _ = select.select([ack_fd], [], [], 10)
    # Some perf versions include the string terminator in the FIFO write.
    if not ready or os.read(ack_fd, 4096).rstrip(b'\0') != b'ack\n':
        raise RuntimeError(f'perf did not acknowledge {command}')


@contextmanager
def measure_process(pid, path):
    # Start disabled, attach to the already-warmed process and all of its threads.
    ctl = path.with_suffix('.ctl')
    ack = path.with_suffix('.ack')
    for fifo in (ctl, ack):
        os.mkfifo(fifo, 0o600)
    ctl_fd = os.open(ctl, os.O_RDWR)
    ack_fd = os.open(ack, os.O_RDWR)
    try:
        with path.with_suffix('.log').open('w') as log:
            # Obtain the privileged child's PID. Signalling the sudo wrapper
            # can leave perf waiting for the attached server to exit.
            perf = subprocess.Popen(
                ['sudo', '-n', 'sh', '-c', 'echo $$; exec "$@"', 'sh'] + PERF
                + ['-D', '-1', '--control', f'fifo:{ctl},{ack}', '-o', str(path), '-p', str(pid)],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log,
            )
            perf_pid = None
            try:
                if not select.select([perf.stdout], [], [], 10)[0]:
                    raise RuntimeError(f'perf startup timed out, see {log.name}')
                pid_line = perf.stdout.readline().strip()
                if not pid_line.isdigit():
                    raise RuntimeError(f'perf did not start, see {log.name}')
                perf_pid = int(pid_line)
                perf_control(ctl_fd, ack_fd, 'enable')
                yield
                perf_control(ctl_fd, ack_fd, 'disable')
            finally:
                try:
                    if perf.poll() is None:
                        if perf_pid is not None:
                            subprocess.run(['sudo', '-n', 'kill', '-INT', str(perf_pid)],
                                           check=True, timeout=10)
                        else:
                            perf.terminate()
                    status = perf.wait(timeout=10)
                finally:
                    perf.stdout.close()
            if status not in (0, -signals.SIGINT, 128 + signals.SIGINT):
                raise RuntimeError(f'perf failed with status {status}, see {log.name}')
    finally:
        os.close(ack_fd)
        os.close(ctl_fd)
        ctl.unlink()
        ack.unlink()


def per_row(measurement, rows):
    return dict(measurement, rows=rows, instructions_per_row=measurement['instructions'] / rows,
                cpu_s_per_row=measurement['cpu_s'] / rows)


def spread(values):
    return {'median': statistics.median(values), 'min': min(values), 'max': max(values)}


def summarize(pairs):
    keys = ('instructions_per_row', 'cpu_s_per_row')
    return {
        'pairs': len(pairs),
        'arms': {arm: {key: spread([pair[arm][key] for pair in pairs]) for key in keys}
                 for arm in CONTENT_TYPES},
        'protobuf_over_json': {
            key: spread([pair['protobuf'][key] / pair['json'][key] for pair in pairs])
            for key in keys},
    }


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + '\n')


def measure_stage(command, capture):
    def wait_for(pipe, message, timeout):
        ready, _, _ = select.select([pipe], [], [], timeout)
        if not ready or pipe.readline().strip() != message:
            raise RuntimeError(f'stage binary did not report {message}')

    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          text=True) as process:
        try:
            wait_for(process.stdout, 'ready', 10)
            with measure_process(process.pid, capture):
                process.stdin.write('start\n')
                process.stdin.flush()
                wait_for(process.stdout, 'done', 180)
            process.stdin.close()
            if process.wait(timeout=10) != 0:
                raise RuntimeError('stage binary failed')
        finally:
            if process.poll() is None:
                process.kill()
    return counters(capture)


def stage(args):
    results = []
    for signal in SIGNALS:
        for rows in (1, 100, 5000):
            for width in (4, 32):
                directory = args.output / f'{signal}-{rows}-{width}'
                directory.mkdir()
                paths = {}
                bodies = fixtures(signal, rows, width)
                for encoding, body in bodies.items():
                    paths[encoding] = directory / encoding
                    paths[encoding].write_bytes(body)
                subprocess.run([args.example, signal, 'check', paths['json'], paths['protobuf'],
                                str(rows)], check=True)
                iterations = (args.records + rows - 1) // rows
                pairs = []
                for pair in range(args.pairs):
                    result = {}
                    for encoding in pair_order(pair):
                        capture = directory / f'{pair}-{encoding}.perf'
                        affinity = ['taskset', '-c', args.cpus] if args.cpus else []
                        command = affinity + [args.example, signal, encoding, paths[encoding],
                                              str(iterations)]
                        result[encoding] = per_row(measure_stage(command, capture), rows * iterations)
                    pairs.append(result)
                sizes = {encoding: len(body) / rows for encoding, body in bodies.items()}
                summary = dict(signal=signal, rows_per_request=rows, attributes_per_row=width,
                               iterations=iterations, bytes_per_row=sizes,
                               protobuf_over_json_bytes=sizes['protobuf'] / sizes['json'],
                               **summarize(pairs))
                write_json(directory / 'pairs.json', pairs)
                results.append(summary)
                write_json(args.output / 'summary.json', results)
                print(json.dumps(summary), flush=True)


def post(conn, signal, stream, encoding, body, headers=None, *, chunked=False):
    headers = {'Authorization': AUTH, 'Content-Type': CONTENT_TYPES[encoding],
               'X-P-Stream': stream, **(headers or {})}
    if chunked:
        # A declared oversized Content-Length can close the connection while
        # sendall is still writing. Chunked delivery exercises the byte limit
        # with the complete fixture on the wire.
        view = memoryview(body)
        body = (view[offset:offset + 65536] for offset in range(0, len(view), 65536))
    conn.request('POST', f'/v1/{signal}', body, headers, encode_chunked=chunked)
    response = conn.getresponse()
    return response.status, response.getheader('Content-Type'), response.read()


def expect_response(response, status, body=None):
    if response[0] != status or (body is not None and response[2] != body):
        raise AssertionError(f'expected HTTP {status} and body {body!r}; got {response[:2]} '
                             f'and {response[2][:500]!r}')


def query(port, sql):
    # The query engine truncates time bounds to whole minutes.
    payload = dict(query=sql, startTime='1h', endTime='now+2m', sendNull=True)
    with closing(http.client.HTTPConnection('127.0.0.1', port, timeout=90)) as conn:
        result = request(port, 'POST', '/api/v1/query', json.dumps(payload).encode(),
                         connection=conn)
    return json.loads(result)


def query_count(port, stream, expected):
    # Two aggregates force SQL readback instead of the single-count stats shortcut.
    result = query(port, f'SELECT COUNT(*) AS n, COUNT(bench_batch) AS identified FROM {stream}')
    if result != [{'n': expected, 'identified': expected}]:
        raise AssertionError((stream, 'unexpected count', result, expected))


def canonical_rows(rows, first, stop, rows_per_request):
    # Count all rows separately, then confirm every expected identity exactly once.
    expected = [(f'{batch:08d}', f'{row:06d}') for batch in range(first, stop)
                for row in range(rows_per_request)]
    actual = sorted((r['bench_batch'], r['bench_row']) for r in rows)
    if actual != expected:
        raise AssertionError(f'identity mismatch for batches [{first}, {stop}): '
                             f'{len(actual)} actual rows, {len(expected)} expected')
    normalized = [{key: value for key, value in row.items() if key != 'p_timestamp'} for row in rows]
    return sorted(json.dumps(row, sort_keys=True, separators=(',', ':')) for row in normalized)


def readback(port, stream, requests, rows):
    query_count(port, stream, requests * rows)
    digest = hashlib.sha256()
    chunk = max(1, 5000 // rows)
    for first in range(0, requests, chunk):
        stop = min(first + chunk, requests)
        result = query(port, f"SELECT * FROM {stream} WHERE bench_batch >= '{first:08d}' "
                       f"AND bench_batch < '{stop:08d}'")
        for canonical in canonical_rows(result, first, stop, rows):
            digest.update(canonical.encode() + b'\n')
    return {'count': requests * rows, 'logical_rows_sha256': digest.hexdigest()}


def http_benchmark(args):
    timestamp_ns = time.time_ns() - 300_000_000_000
    captures = {signal: [] for signal in SIGNALS}
    for pair in range(args.pairs):
        measured = {}
        for encoding in pair_order(pair):
            def action(port, pid):
                result = {}
                arm_dir = args.output / f'pair{pair}-{encoding}'
                for signal in SIGNALS:
                    # Generate outside the sample window. Requests have unique IDs;
                    # batch 0 warms the same stream and connection (included in readback).
                    bodies = []
                    payload_bytes = 0
                    for batch in range(args.requests + 1):
                        body = fixtures(signal, args.rows, args.attributes, batch,
                                        timestamp_ns=timestamp_ns)[encoding]
                        payload_bytes += len(body)
                        if len(body) > HTTP_LIMIT or payload_bytes > 256 * 2**20:
                            raise ValueError(f'{signal}/{encoding}: exceeds body or fixture budget')
                        bodies.append(body)
                    stream = f'bench{signal}'
                    path = arm_dir / f'{signal}.perf'
                    with closing(http.client.HTTPConnection('127.0.0.1', port, timeout=90)) as conn:
                        expect_response(post(conn, signal, stream, encoding, bodies[0]), 200, b'')
                        with measure_process(pid, path):
                            started = time.monotonic()
                            for body in bodies[1:]:
                                expect_response(post(conn, signal, stream, encoding, body), 200, b'')
                            elapsed = time.monotonic() - started
                    result[signal] = dict(per_row(counters(path), args.rows * args.requests),
                                          elapsed_s=elapsed, timed_body_bytes=payload_bytes-len(bodies[0]),
                                          **readback(port, stream, len(bodies), args.rows))
                return result

            measured[encoding] = run_arm(args.binary, args.output, encoding, pair, [], args.cpus, action)
        for signal in SIGNALS:
            result = {encoding: arm[signal] for encoding, arm in measured.items()}
            if result['json']['logical_rows_sha256'] != result['protobuf']['logical_rows_sha256']:
                raise AssertionError((signal, pair, 'encodings returned different logical rows', result))
            captures[signal].append(result)
            print(f'{signal} pair {pair + 1}: {result["json"]["count"]} identical logical rows '
                  f'(including warmup)', flush=True)

    summary = {signal: summarize(pairs) for signal, pairs in captures.items()}
    for signal, item in summary.items():
        item.update(rows_per_request=args.rows, attributes_per_row=args.attributes,
                    fixture_timestamp_ns=timestamp_ns,
                    requests_per_arm=args.requests, verified_rows_per_arm=captures[signal][0]['json']['count'],
                    bytes_per_row={arm: captures[signal][0][arm]['timed_body_bytes'] / (args.rows * args.requests)
                                   for arm in CONTENT_TYPES})
    write_json(args.output / 'summary.json', summary)
    print(json.dumps(summary), flush=True)


def verify(args):
    timestamp_ns = time.time_ns() - 300_000_000_000

    def action(port, _pid):
        results = []
        for signal in SIGNALS:
            stream = f'verify{signal}'
            count = 0
            with closing(http.client.HTTPConnection('127.0.0.1', port, timeout=90)) as conn:
                def fixture(padding=0):
                    return fixtures(signal, 1, 4, count, timestamp_ns=timestamp_ns, padding=padding)

                def probe(name, encoding, payload, status=200, headers=None, *, chunked=False):
                    nonlocal count
                    response = post(conn, signal, stream, encoding, payload, headers, chunked=chunked)
                    expect_response(response, status, b'' if status == 200 else None)
                    if status == 200:
                        count += 1
                    query_count(port, stream, count)
                    result = dict(signal=signal, case=name, encoding=encoding,
                                  sent_bytes=len(payload), status=response[0],
                                  response_content_type=response[1],
                                  response_body=response[2].decode(errors='replace'), count=count)
                    results.append(result)
                    print(json.dumps(result), flush=True)

                for encoding in ('protobuf', 'json'):
                    probe('standard', encoding, fixture()[encoding])
                for name, header in [('parameter', 'application/x-protobuf; charset=utf-8'),
                                     ('mixed_case', 'Application/X-Protobuf')]:
                    probe(name, 'protobuf', fixture()['protobuf'],
                          headers={'Content-Type': header})
                for encoding in CONTENT_TYPES:
                    probe('gzip', encoding, gzip.compress(fixture()[encoding]),
                          headers={'Content-Encoding': 'gzip'})

                valid = fixture()
                for encoding in CONTENT_TYPES:
                    body = valid[encoding]
                    probe('malformed', encoding, body + b'\0', 400)
                    truncated = body[:-1]
                    if encoding == 'protobuf':
                        truncated = body + truncated  # complete resource followed by a cut one
                    probe('truncated', encoding, truncated, 400)
                    oversized = fixture(padding=HTTP_LIMIT)[encoding]
                    probe('oversized_chunked', encoding, oversized, 413, chunked=True)
            readback(port, stream, count, 1)
        return results

    results = run_arm(args.binary, args.output, 'verification', 0, [], args.cpus, action)
    write_json(args.output / 'summary.json', results)


def bounded(low, high):
    def parse(value):
        number = int(value)
        if not low <= number <= high:
            raise argparse.ArgumentTypeError(f'expected a number in [{low}, {high}]')
        return number
    return parse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest='mode', required=True)
    for mode, handler in [('stage', stage), ('http', http_benchmark), ('verify', verify)]:
        sub = subparsers.add_parser(mode)
        sub.set_defaults(handler=handler)
        sub.add_argument('--output', type=Path, required=True)
        sub.add_argument('--cpus', default='')
        if mode == 'stage':
            sub.add_argument('--example', type=Path, required=True)
            sub.add_argument('--records', type=bounded(1, 10_000_000), default=50_000)
        else:
            sub.add_argument('--binary', type=Path, required=True)
        if mode != 'verify':
            sub.add_argument('--pairs', type=bounded(1, 32), default=4)
        if mode == 'http':
            sub.add_argument('--rows', type=bounded(1, 5000), default=100)
            sub.add_argument('--attributes', type=bounded(4, 32), default=4)
            sub.add_argument('--requests', type=bounded(1, 50_000), default=256)
    args = parser.parse_args()
    args.output = args.output.resolve()
    if args.mode == 'stage':
        args.example = args.example.resolve(strict=True)
    else:
        args.binary = args.binary.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    args.handler(args)


if __name__ == '__main__':
    main()
