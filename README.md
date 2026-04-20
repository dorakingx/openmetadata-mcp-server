# OpenMetadata MCP Server

Minimal MCP server for hackathon use-cases that lets LLM agents discover data assets, inspect table metadata, and traverse lineage in OpenMetadata.

## Features

- `search_data_assets`: Search table/data assets by natural language or keyword.
- `get_table_details`: Fetch table metadata including schema, tags, and owner.
- `get_table_lineage`: Fetch upstream/downstream dependencies for a table.
- `analyze_table_for_pii`: Provide governance-focused schema context for LLM PII audit reasoning.
- `apply_pii_tag_to_column`: Apply a governance tag to a table column for automated remediation.
- `update_column_description`: Add or update undocumented column descriptions inferred by the LLM.

## Autonomous Agentic Governance Workflow

- Step 1: Search & Discover (`search_data_assets`, `get_table_details`)
- Step 2: Audit & Reason (`analyze_table_for_pii`)
- Step 2.5: Verify Rules (`get_available_governance_tags`)
- Step 3: Document (`update_column_description`)
- Step 4: Act & Remediate (`apply_pii_tag_to_column`)

Audit, Document & Remediate: Automatically suggest descriptions for undocumented columns and apply governance tags.

## Prerequisites

- Python `3.11+`
- Access to an OpenMetadata instance (default expected: `http://localhost:8585`)
- OpenMetadata JWT token with read permissions for search, table metadata, and lineage

## Setup

1. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:

   ```bash
   cp .env.example .env
   ```

4. Edit `.env`:

   ```env
   OPENMETADATA_HOST=http://localhost:8585
   OPENMETADATA_JWT_TOKEN=your-token-here
   ```

## Run the MCP Server

```bash
mcp run server.py
```

## Tool Intents

- `search_data_assets(query, limit=10)`
  - Example intent: "Find customer or user profile tables."
- `get_table_details(identifier)`
  - Example intent: "Show schema and owner for `service.db.schema.table`."
- `get_table_lineage(identifier)`
  - Example intent: "Show what feeds and depends on this table."

## Error Handling Notes

The server returns structured error payloads (`ok: false`) with:
- an `error` message
- a `hint` to help the LLM self-correct (invalid token, bad identifier, connectivity issue)
- optional `status_code` when available

Common remediation:
- `401`: refresh/check `OPENMETADATA_JWT_TOKEN`
- `404`: verify table FQN or UUID
- connection errors: verify `OPENMETADATA_HOST` and that OpenMetadata is running
