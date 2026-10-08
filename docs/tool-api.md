# Parseable tool REST API

Parseable owns the canonical tool catalog and execution semantics. External MCP
servers adapt these REST endpoints to MCP `tools/list` and `tools/call`.

Both endpoints require normal Parseable authentication. Tool execution also
applies the RBAC action required by the selected tool and any referenced
dataset or resource.

## List tools

```http
GET /api/prism/v1/llm/tools/list
```

Response:

```json
{
  "tools": [
    {
      "name": "get_dataset_schema",
      "title": "Get dataset schema",
      "description": "Get columns and data types for a dataset.",
      "inputSchema": {
        "type": "object",
        "properties": { "dataset": { "type": "string", "minLength": 1 } },
        "required": ["dataset"],
        "additionalProperties": false
      }
    }
  ]
}
```

The response includes an `ETag` and
`Cache-Control: private, max-age=300, must-revalidate`. A request with a
matching `If-None-Match` receives `304 Not Modified`.

The catalog is edition-specific:

- OSS returns OSS-supported tools.
- Enterprise returns OSS tools plus Enterprise tools.
- Parseable Cloud uses the Enterprise server catalog and exposes the tools
  enabled by that deployment.

## Call a tool

```http
POST /api/prism/v1/llm/tools/call
Content-Type: application/json
```

Request:

```json
{
  "name": "get_dataset_schema",
  "arguments": { "dataset": "application-logs" }
}
```

`arguments` must satisfy the tool's `inputSchema`. For mutation and
external-side-effect tools, the MCP host or another trusted caller is
responsible for applying its confirmation policy before calling this endpoint.

Successful and failed executions use the MCP `CallToolResult` shape:

```json
{
  "content": [
    { "type": "text", "text": "{\"fields\":[...]}" }
  ],
  "structuredContent": {
    "result": { "fields": [] }
  },
  "isError": false
}
```

Tool failures set `isError` to `true`, include a text error content item, and
put the message under `structuredContent.error.message`. A completed tool call
returns HTTP 200 even when the tool reports `isError: true`, matching MCP
`tools/call` semantics. REST status codes distinguish failures that prevent a
call from starting, such as malformed input, authentication, or an unknown
tool.

The REST API is not itself an MCP transport. An MCP adapter maps the registry
response to `tools/list`, forwards calls to this endpoint, and returns the
`CallToolResult` from Parseable through MCP `tools/call`.
