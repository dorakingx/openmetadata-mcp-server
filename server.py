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


if __name__ == "__main__":
    mcp.run()
