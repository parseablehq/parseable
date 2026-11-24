# Traces, APM and ingestion validation

The editable frontend exposes `/traces`, `/traces/explore/:dataset`, `/apm`, `/apm/:dataset`, `/apm/:dataset/:service` and `/add-data`. Observe navigation includes Traces and APM; Data includes Add data. Dataset and overview links choose the explorer for that dataset's telemetry source. The server remains authoritative for authorization, and live failures retain their messages instead of substituting demo records.

## Original reference and comparison limits

The original Prism reference was captured on 2026-10-12 against a disposable native Parseable server with synthetic OTLP spans, expanded events/links, errors, orphan spans and a repeated batch. The machine-local research cache is `/tmp/missing-screens-reference`; its [findings](/tmp/missing-screens-reference/FINDINGS.md) and `evidence/` responses describe the contracts used here. These absolute paths are local evidence, not portable repository artifacts.

Relevant original screenshots are the [span list](parity-screenshots/observability/original/01-traces-explore-spans-list.png), [trace list](parity-screenshots/observability/original/02-traces-explore-traces-list-3records-4rows-dup-bug.png), [waterfall](parity-screenshots/observability/original/04-trace-detail-waterfall-a1.png), [span errors](parity-screenshots/observability/original/05-span-detail-charge-errors-tab.png), [events](parity-screenshots/observability/original/06-span-detail-charge-events-tab.png), [fields](parity-screenshots/observability/original/07-span-detail-charge-fields-tab.png), [APM OSS screen](parity-screenshots/observability/original/08-apm-oss-enterprise-cta-no-data.png), [Add data entry](parity-screenshots/observability/original/09-add-data-entry.png), [collector configuration](parity-screenshots/observability/original/14-otel-collector-setup-step4-config.png) and [waiting confirmation](parity-screenshots/observability/original/15-otel-collector-setup-step5-confirm-waiting.png).

These references establish layout and server behavior, not pixel equality. Original OSS APM displayed an enterprise call to action rather than service data. This implementation supplies a working APM view using the existing SQL API; it does not reproduce that upgrade flow. Ingestion provides sender configuration and measured server evidence rather than claiming that an existing dataset proves a new source connection.

## New live screen captures

Screenshots from the new app against the patched native server: [trace list](parity-screenshots/observability/traces-live-list.png), [waterfall and error span](parity-screenshots/observability/traces-live-waterfall.png), [service inventory](parity-screenshots/observability/apm-live-inventory.png), [dependencies](parity-screenshots/observability/apm-live-dependencies.png), [service insights](parity-screenshots/observability/apm-live-insights.png), [mobile inventory](parity-screenshots/observability/apm-live-mobile.png), [ingestion setup](parity-screenshots/observability/ingestion-live-setup.png), and [Collector configuration](parity-screenshots/observability/ingestion-live-collector.png). These use synthetic data and a placeholder API key. The separate 16 Storybook baselines cover deterministic light/dark and desktop/narrow component layouts.

## Traces contract

Discovery reads the dataset inventory and information and requires the `otel-traces` source. List requests use `POST /api/prism/v1/services/traces/list` with `dataset`, captured `startTime`/`endTime`, optional exact `serviceName`, `options` (`traces`, `spans`, `errorSpans`), `sortBy`, `limit` and `offset`. Root-trace views differ from individual error-span views: a root with a nonzero descendant error count is not itself necessarily an explicit error span. Pagination uses 25 records. A service handoff defaults to all spans so child-service participation remains visible.

Lookup uses `POST /api/prism/v1/services/traces/detail` with dataset, trace ID and the selected bounds. Detail responses carry span summaries, parent IDs, errors and event counts. The waterfall subtracts timestamp strings at nanosecond precision before converting relative timing to milliseconds; numeric epoch fields can exceed JavaScript integer precision. Parent relationships drive indentation. Orphans remain visible with a parent-unavailable label, and malformed cycles do not cause recursive traversal failure. Search matches operation, service and span ID; arrow keys, Home/End and Enter support keyboard selection.

Stored fields and event payloads come from a bounded `/api/v1/query` request for the selected trace/span identity. The native summary's event count does not supply an event payload. The UI deduplicates repeated stored records/events, exposes multiple distinct records, and retains the summary when reading stored fields is forbidden. At most 100 stored records are requested, with a visible limit notice when reached. Unknown service labels in child detail records are normalized for display; other malformed response fields remain errors.

Trace lists, counts and lookup discovery use the exact half-open selected window (`start <= time < end`), including fractional seconds. Native trace queries widen only the planner's transport envelope to whole minutes; SQL retains the requested bounds. Once discovered, detail retains its existing expansion to the minutes containing the trace's observed minimum/maximum timestamps, so nearby spans of that trace remain visible. The backend stores OTLP timestamps without an explicit UTC suffix. A trace whose root or spans fall outside the discovered bounds may be incomplete or unavailable. Empty or reversed request windows, missing required fields, permission errors and not-found responses are explicit; the UI does not infer missing span/event data.

## APM contract and limits

APM uses the existing `POST /api/v1/query` endpoint against eligible OTLP trace datasets. Queries quote dataset identifiers and service values, inspect schema first, and deduplicate by service, trace ID and span ID before aggregation. Event/link expansion and repeated ingestion do not count as additional spans. SQL enforces the exact captured ingestion-time interval while the transport envelope expands to minute boundaries so the native query planner does not truncate that interval.

Inventory and insights report unique span count, distinct traces within a service, explicit error spans, span error rate, average latency, approximate P50/P95/P99 and maximum duration. Counts include all observed span kinds; span rates are not request throughput, and duration is span latency rather than inferred end-to-end transaction latency. Missing durations stay unavailable. Missing status columns explain that zero explicit errors does not establish healthy spans. Required missing schema fields block aggregation rather than inventing values.

Service insights contain rate/error and latency charts, operations ranked by volume, and explicit errors followed by long spans for investigation. Chart buckets use the captured ingestion-time range. Trace links preserve dataset, exact service, start/end bounds and the selected trace ID. URL path segments encode full service names, including names with slashes.

Dependencies require a child span and its parent in the same dataset/time window. The query joins observed identities and excludes ambiguous parent identities. Missing parent columns disable dependencies with an explanation; missing parents do not become inferred links. The diagram uses deterministic placement, supports cycles and scrolling, and exposes accessible full-name service links. It draws at most 24 services and 40 connections, ranked by child span count; the accompanying list retains all returned connections. Query results cap inventory at 200 services, operations at 100, investigation spans at 50 and topology at 200 connections. Truncation notices explain that local search/sort applies to the returned set.

## Ingestion contract and limits

JSON HTTP targets `/api/v1/ingest`; OTLP HTTP targets `/v1/logs`, `/v1/metrics` or `/v1/traces`. Dataset creation supplies the matching telemetry type and source (`json`, `otel-logs`, `otel-metrics`, `otel-traces`). Existing metadata must match the selected source/signal before configuration becomes available. Destinations with custom partitions show the required field names and instructions to select/create an unpartitioned dataset or configure a sender manually. Generated configuration, copying and guided verification stay unavailable for those destinations because setup cannot infer valid partition values. JSON input uses logs; other sources choose one signal per dataset.

Generated collector configuration uses `otlp_http/parseable`, HTTP protobuf encoding, `compression: none`, `X-P-Stream`, and an environment variable for the ingest API key. Receivers bind to localhost on ports 4317/4318 and must be adjusted for other deployment arrangements. The native OSS server accepts OTLP HTTP as protobuf (`application/x-protobuf`) or JSON (`application/json`); the generated curl sample sends JSON. Collector configuration converts compatible sender traffic. Setup does not deploy or start a sender.

Endpoint validation permits HTTP/HTTPS base URLs and reverse proxy base paths, and rejects embedded credentials, query/fragment strings, variables, whitespace, backslashes and appended ingestion routes. Commands quote shell values and dataset names are validated. Credentials are entered into the sender's environment rather than persisted by this setup. Copy success and failure are visible, with manual copying available when the browser denies clipboard access.

Check ingestion reads dataset statistics, schema and metadata. The first check establishes a lifetime-count baseline and distinguishes existing/current data from an empty destination. Later checks report new events only when lifetime ingestion increases; unchanged counters report no new events. Statistics can lag, retention can remove current rows, and arrivals may belong to another sender. None of these checks proves this particular source connection. Verification is disabled in demo mode; demo configuration and datasets remain labeled examples.

## Storybook and test workflow

Fixtures use deterministic transports rather than a network service. Core story families and representative IDs:

| Family               | Story IDs                                                                                                                                                                                                 |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Trace explorer       | `traces-explorer--default`, `--detail`, `--service-handoff`, `--permission-denied`, `--empty`, `--loading`, `--no-datasets`, `--missing-fields`, `--error`, `--lookup-not-found`, `--metadata-permission` |
| Service inventory    | `apm-serviceinventory--services`, `--empty`, `--no-matches`, `--long-service-names`                                                                                                                       |
| Dependency list      | `apm-servicetopology--observed-dependencies`, `--focused-service`, `--no-dependencies`                                                                                                                    |
| Dependency map       | `apm-servicemap--directed-services`, `--focused-service`, `--cyclic-dependencies`, `--long-names`                                                                                                         |
| Service insights     | `apm-serviceinsights--overview`, `--empty`, `--limited-results`                                                                                                                                           |
| Full ingestion setup | `ingestion-setup--default`, `--empty-workspace`, `--permission-denied`, `--verification-error`, `--demo-mode`, `--loading`, `--new-arrivals`                                                              |
| Ingestion components | `ingestion-components--json-command`, `--otlp-command`, `--collector-config`, `--invalid-endpoint`, `--missing-dataset`, `--copyable-code`                                                                |

Suffix-only entries in the table share their row's prefix. Open a story with `/iframe.html?id=<full-story-id>&viewMode=story` on port 6006. `story-tests/observability-components.spec.ts` covers filtering/sorting/paging, keyboard controls, field/event deduplication, errors and retry, service handoffs, configuration validation, clipboard success/failure, verification and WCAG checks in light/dark themes. `story-tests/observability-visuals.spec.ts` covers waterfall selection, inventory, focused dependency map and collector setup at desktop and narrow widths, with fixed UTC/en-US settings and reduced motion. Narrow layouts also require that the page itself does not overflow horizontally.

From `frontend/`, run:

```sh
npm run build
npm test
npm run test:ui
npm run build-storybook
npx playwright test -c playwright.storybook.config.ts observability-components.spec.ts
```

Run visual comparisons in the pinned CI image; host browser/fonts can differ. From the repository root, after building Storybook:

```sh
flock /home/ajs/.cache/parseable-playwright.lock docker run --rm --ipc=host \
  --user "$(id -u):$(id -g)" -e CI=1 \
  -v "$PWD:/work" -w /work/frontend \
  mcr.microsoft.com/playwright:v1.63.0-noble \
  npx playwright test -c playwright.storybook.config.ts --workers=2
```

Use an appropriate shared lock path on other machines. Serialize suites using shared Playwright output directories, even when the suites use different ports; concurrent runs can remove each other's trace artifacts. Missing baselines fail by default. To create or review an intentional visual change, explicitly restrict `--update-snapshots` to `observability-visuals.spec.ts`, inspect every changed image, then rerun without updating. Do not update the existing chart baselines to accommodate host rendering differences.

Application suites in `e2e/traces.spec.ts`, `e2e/apm.spec.ts`, `e2e/ingestion.spec.ts` and `e2e/observability-navigation.spec.ts` cover routes and mocked server behavior. Opt-in live suites require a disposable server and are separate evidence from Storybook or mocked browser checks; use the existing [live validation runbook](validation-review.md) and suite-specific environment variables. Final results are recorded below. The optional fixture SQL test requires the captured reference dataset; it is skipped in the default unit run.

## PR #27 regression fixes — 2026-10-12

The review follow-up used test-driven development for exact trace windows and custom-partition setup. Three compiled Rust regressions initially failed on fractional bounds, list/count filtering and detail discovery. Eight new ingestion unit cases initially failed because partitioned destinations were accepted. The new app and Storybook cases also failed against an isolated copy of the original implementation, then passed with the compatibility guard. A demo boundary test failed before aligning demo discovery with the native half-open interval.

The new opt-in `e2e-live/apm-traces.spec.ts` creates an OTLP fixture, derives fractional bounds from its persisted ingestion timestamp, follows the actual APM investigation link, and checks native list/detail responses and the rendered waterfall without API mocks. It failed against the previous backend because the native list omitted the span, then passed against the freshly compiled fixed backend. Fixture datasets are removed on completion or failure.

Verified results for the fixes:

- 13 compiled Rust trace tests passed, including custom time-column boundaries and existing retry deduplication.
- 2,144 frontend unit tests passed; one optional fixture test skipped.
- 39 focused app browser cases passed across traces, APM, ingestion and observability navigation.
- 9 ingestion Storybook cases passed, including the new partitioned-destination story and existing accessibility checks.
- 5 live browser cases passed: the new APM handoff regression and all four existing JSON/OTLP ingestion cases.
- 18 compiled native trace-handler cases passed against a fixture with 11 raw rows and 9 unique spans.
- TypeScript, production frontend build, server build, Rust formatting, affected frontend formatting and whitespace checks passed.

GPT-6.1 Sol workstreams covered backend TDD, ingestion TDD and browser integration. Opus 5.5 supplied implementation direction and independently reviewed the resulting changes, finding no blocking defects. Full browser/Storybook suites and visual baselines were not rerun for these scoped fixes. The temporary test server and datasets were cleaned up.

## Original feature validation — 2026-10-12

Branch: `feat/missing-observability-screens`. Eight Sol 6.1 workstreams and six Opus 5.5 review/research rounds covered contracts, implementation, integration, visual quality and original runtime comparison. The closing Opus review found no high- or medium-severity defects. Its remaining discovery-error and unknown-type explanations were also addressed. Local chart tabs, operation search and column sorting can reset when their data panels reload; page-level APM search, latency metric and Services/Dependencies selection persist.

| Check                                                     | Result                                                                                                  |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| TypeScript, production build, formatting, diff whitespace | Pass                                                                                                    |
| Full unit suite                                           | 2,133 passed; 1 opt-in live fixture test skipped                                                        |
| App browser coverage                                      | 462 passing cases across full run and focused reruns; 2 pre-existing opt-in skips (464 discovered)      |
| Production mounts and missing lazy chunks                 | 22 passed, including all three new routes at `/` and `/next/`                                           |
| Storybook                                                 | All 172 cases covered: 103 existing plus 69 new; final affected suite 69 passed                         |
| New deterministic visual baselines                        | 16 reviewed, then passed without updating, using Playwright 1.63.0 Noble container                      |
| Live ingestion browser suite                              | 4 passed: JSON, OTLP logs, OTLP metrics and OTLP traces; created datasets cleaned up                    |
| APM unit + opt-in generated live SQL                      | 18 passed against the original fixture server, including operations, empty summary and focused topology |
| Native trace regression                                   | 8 compiled Rust tests and 18 actual HTTP handler cases passed on the patched server                     |

The native fixture has 11 raw rows and 9 unique spans. Counts and returned records agree: 3 root traces, 9 spans and 2 error spans. APM reports the same 9 unique spans. The backend fix covers identical retries with differing ingestion timestamps; conflicting span metadata is not canonicalized. A metric OTLP envelope can produce multiple stored events, so verification tests match the actual lifetime counter rather than assuming one event per envelope.

The first host browser passes were disrupted by active HMR, overlapping artifact output, and Chromium `ERR_INSUFFICIENT_RESOURCES`/renderer crashes. Final affected runs used the shared lock and pinned container; every failed case was rerun successfully after applicable source/test fixes. This is combined coverage, not a claim that the initial full run passed without retries. Storybook found and verified the correction of invalid definition-list children in APM statistic cards.

Original and new captures, progress updates, and completion evidence are published in the [Mayfly implementation wiki](https://mayfly-celld.tail835e0d.ts.net/w/46ncdGypknzAj7dnRkJ8bQ?page=999c03ee-fbfc-453f-a442-4895bf1d7837#J63UryiLZKvb1G5vnY7RVQhXPDhXy-GpIhjXiCB_TzM).
