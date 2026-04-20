"""MCP server exposing OpenMetadata discovery tools."""

from __future__ import annotations

import os
from typing import Any, Dict

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from ometa_client import OpenMetadataClient, OpenMetadataClientError


load_dotenv()

OPENMETADATA_HOST = os.getenv("OPENMETADATA_HOST", "http://localhost:8585")
OPENMETADATA_JWT_TOKEN = os.getenv("OPENMETADATA_JWT_TOKEN", "")

mcp = FastMCP("openmetadata-mcp-server")

_client: OpenMetadataClient | None = None


def _get_client() -> OpenMetadataClient:
    global _client
    if _client is None:
        if not OPENMETADATA_JWT_TOKEN:
            raise OpenMetadataClientError(
                message="OPENMETADATA_JWT_TOKEN is not set.",
                hint="Create a `.env` file and provide a valid OpenMetadata JWT token.",
            )
        _client = OpenMetadataClient(host=OPENMETADATA_HOST, jwt_token=OPENMETADATA_JWT_TOKEN)
    return _client


def _error_payload(err: OpenMetadataClientError) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": err.message,
        "hint": err.hint,
        "status_code": err.status_code,
    }


@mcp.tool()
def search_data_assets(query: str, limit: int = 10) -> Dict[str, Any]:
    """
    Search OpenMetadata for table assets using keyword or natural language query.

    Args:
        query: User search phrase, e.g., "customer churn tables".
        limit: Max number of assets to return (default: 10).
    """
    if not query.strip():
        return {
            "ok": False,
            "error": "query must not be empty.",
            "hint": "Provide a keyword or phrase like 'orders fact table'.",
            "status_code": None,
        }

    try:
        result = _get_client().search_data_assets(query=query.strip(), limit=max(1, min(limit, 50)))
        return {"ok": True, **result}
    except OpenMetadataClientError as err:
        return _error_payload(err)


@mcp.tool()
def get_table_details(identifier: str) -> Dict[str, Any]:
    """
    Retrieve rich metadata for one table by FQN or UUID.

    Args:
        identifier: Table fully qualified name (preferred) or table UUID.
    """
    if not identifier.strip():
        return {
            "ok": False,
            "error": "identifier must not be empty.",
            "hint": "Provide a table FQN (recommended) or UUID.",
            "status_code": None,
        }

    try:
        result = _get_client().get_table_details(identifier=identifier.strip())
        return {"ok": True, "table": result}
    except OpenMetadataClientError as err:
        return _error_payload(err)


@mcp.tool()
def get_table_lineage(identifier: str) -> Dict[str, Any]:
    """
    Retrieve upstream and downstream lineage for one table by FQN or UUID.

    Args:
        identifier: Table fully qualified name (preferred) or table UUID.
    """
    if not identifier.strip():
        return {
            "ok": False,
            "error": "identifier must not be empty.",
            "hint": "Provide a table FQN (recommended) or UUID.",
            "status_code": None,
        }

    try:
        result = _get_client().get_table_lineage(identifier=identifier.strip())
        return {"ok": True, **result}
    except OpenMetadataClientError as err:
        return _error_payload(err)


@mcp.tool()
def analyze_table_for_pii(identifier: str) -> Dict[str, Any]:
    """
    Fetch focused governance context so the LLM can audit untagged PII.

    Args:
        identifier: Table fully qualified name (preferred) or table UUID.
    """
    if not identifier.strip():
        return {
            "ok": False,
            "error": "identifier must not be empty.",
            "hint": "Provide a table FQN (recommended) or UUID.",
            "status_code": None,
        }

    try:
        context = _get_client().get_table_governance_context(identifier=identifier.strip())
        return {
            "ok": True,
            "instruction": (
                "Review the following columns. If any column likely contains PII "
                "(like emails, phone numbers, or addresses) but lacks a 'PII' tag, "
                "please suggest adding it."
            ),
            "governance_context": context,
        }
    except OpenMetadataClientError as err:
        return _error_payload(err)


@mcp.tool()
def apply_pii_tag_to_column(
    identifier: str, column_name: str, tag_fqn: str = "PII.Sensitive"
) -> Dict[str, Any]:
    """
    Apply a governance tag to a top-level column for remediation workflows.

    Args:
        identifier: Table fully qualified name (preferred) or table UUID.
        column_name: Exact top-level column name to update.
        tag_fqn: Tag fully qualified name to apply (default: PII.Sensitive).
    """
    if not identifier.strip():
        return {
            "ok": False,
            "error": "identifier must not be empty.",
            "hint": "Provide a table FQN (recommended) or UUID.",
            "status_code": None,
        }
    if not column_name.strip():
        return {
            "ok": False,
            "error": "column_name must not be empty.",
            "hint": "Provide an exact top-level column name.",
            "status_code": None,
        }

    try:
        client = _get_client()
        table = client._resolve_table(identifier.strip())
        table_id = table.get("id")
        if not table_id:
            return {
                "ok": False,
                "error": "Could not resolve table ID from identifier.",
                "hint": "Verify the table FQN/UUID and try again.",
                "status_code": None,
            }

        action_result = client.apply_column_tag(
            table_id=table_id,
            column_name=column_name.strip(),
            tag_fqn=tag_fqn.strip() or "PII.Sensitive",
        )
        return {
            "ok": True,
            "message": action_result.get("message", "Tag operation completed."),
            "table": {
                "id": table_id,
                "fqn": table.get("fullyQualifiedName"),
                "name": table.get("name"),
            },
            "column_name": column_name.strip(),
            "tag_fqn": tag_fqn.strip() or "PII.Sensitive",
            "result": action_result,
        }
    except OpenMetadataClientError as err:
        return _error_payload(err)


if __name__ == "__main__":
    mcp.run()
