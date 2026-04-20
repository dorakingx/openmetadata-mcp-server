"""OpenMetadata REST client wrapper for MCP tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass
class OpenMetadataClientError(Exception):
    """Typed client error with optional remediation hint."""

    message: str
    hint: Optional[str] = None
    status_code: Optional[int] = None

    def __str__(self) -> str:
        if self.hint:
            return f"{self.message} Hint: {self.hint}"
        return self.message


class OpenMetadataClient:
    """Thin OpenMetadata API wrapper focused on table discovery."""

    def __init__(self, host: str, jwt_token: str, timeout_seconds: int = 20) -> None:
        self.host = host.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {jwt_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.host}{path}"

        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        except requests.exceptions.ConnectionError as exc:
            raise OpenMetadataClientError(
                message=f"Could not connect to OpenMetadata at '{self.host}'.",
                hint="Ensure OPENMETADATA_HOST is correct and the server is running.",
            ) from exc
        except requests.exceptions.Timeout as exc:
            raise OpenMetadataClientError(
                message=f"Request to OpenMetadata timed out after {self.timeout_seconds}s.",
                hint="Try again, reduce query scope, or increase timeout in client config.",
            ) from exc
        except requests.RequestException as exc:
            raise OpenMetadataClientError(
                message=f"Unexpected request error while calling '{url}': {exc}",
                hint="Check network connectivity and OpenMetadata availability.",
            ) from exc

        if response.status_code >= 400:
            hint = self._status_hint(response.status_code)
            detail = self._extract_error_detail(response)
            raise OpenMetadataClientError(
                message=f"OpenMetadata API returned HTTP {response.status_code}: {detail}",
                hint=hint,
                status_code=response.status_code,
            )

        try:
            payload: Dict[str, Any] = response.json()
        except ValueError as exc:
            raise OpenMetadataClientError(
                message=f"OpenMetadata API returned non-JSON response for '{path}'.",
                hint="Verify the API endpoint and OpenMetadata server health.",
            ) from exc

        return payload

    def _patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        url = f"{self.host}{path}"
        headers = {"Content-Type": "application/json-patch+json"}

        try:
            response = self.session.patch(
                url,
                json=payload,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except requests.exceptions.ConnectionError as exc:
            raise OpenMetadataClientError(
                message=f"Could not connect to OpenMetadata at '{self.host}'.",
                hint="Ensure OPENMETADATA_HOST is correct and the server is running.",
            ) from exc
        except requests.exceptions.Timeout as exc:
            raise OpenMetadataClientError(
                message=f"Patch request to OpenMetadata timed out after {self.timeout_seconds}s.",
                hint="Try again or verify OpenMetadata server health.",
            ) from exc
        except requests.RequestException as exc:
            raise OpenMetadataClientError(
                message=f"Unexpected patch error while calling '{url}': {exc}",
                hint="Check network connectivity and OpenMetadata availability.",
            ) from exc

        if response.status_code >= 400:
            hint = self._status_hint(response.status_code)
            detail = self._extract_error_detail(response)
            if response.status_code == 400:
                hint = (
                    "Validate column/tag payload. The tag may not exist; verify tag FQN "
                    f"'{payload[0].get('value', {}).get('tagFQN', '')}'."
                )
            raise OpenMetadataClientError(
                message=f"OpenMetadata API returned HTTP {response.status_code}: {detail}",
                hint=hint,
                status_code=response.status_code,
            )

        try:
            return response.json()
        except ValueError:
            # Some patch responses can be empty; return a minimal success payload.
            return {"status": "success"}

    @staticmethod
    def _extract_error_detail(response: requests.Response) -> str:
        try:
            data = response.json()
            return (
                data.get("message")
                or data.get("detail")
                or data.get("error")
                or response.text
                or "Unknown error."
            )
        except ValueError:
            return response.text or "Unknown error."

    @staticmethod
    def _status_hint(status_code: int) -> str:
        if status_code == 401:
            return "Check OPENMETADATA_JWT_TOKEN and ensure it is valid."
        if status_code == 403:
            return "Token is valid but lacks required permissions for this resource."
        if status_code == 404:
            return "The table or endpoint was not found. Verify table FQN/ID."
        if status_code >= 500:
            return "OpenMetadata server error. Retry or inspect server logs."
        return "Check request parameters and OpenMetadata API compatibility."

    @staticmethod
    def _name_from_entity_ref(entity_ref: Optional[Dict[str, Any]]) -> Optional[str]:
        if not entity_ref:
            return None
        return entity_ref.get("name") or entity_ref.get("displayName") or entity_ref.get("id")

    @staticmethod
    def _format_tags(columns: Optional[List[Dict[str, Any]]]) -> List[str]:
        tags: List[str] = []
        if not columns:
            return tags
        for column in columns:
            for tag in column.get("tags", []) or []:
                tag_name = tag.get("tagFQN") or tag.get("name")
                if tag_name and tag_name not in tags:
                    tags.append(tag_name)
        return tags

    def _resolve_table(self, identifier: str) -> Dict[str, Any]:
        # First attempt: treat identifier as FQN.
        try:
            return self._get(f"/api/v1/tables/name/{identifier}")
        except OpenMetadataClientError as exc:
            if exc.status_code not in (400, 404):
                raise

        # Fallback: treat identifier as UUID table id.
        return self._get(f"/api/v1/tables/{identifier}")

    def search_data_assets(self, query: str, limit: int = 10) -> Dict[str, Any]:
        payload = self._get(
            "/api/v1/search/query",
            params={
                "q": query,
                "index": "table_search_index",
                "from": 0,
                "size": limit,
            },
        )

        hits = (
            payload.get("hits", {})
            .get("hits", [])
            if isinstance(payload.get("hits"), dict)
            else payload.get("hits", [])
        )
        assets: List[Dict[str, Any]] = []

        for hit in hits:
            source = hit.get("_source", {})
            table_name = source.get("name") or source.get("displayName")
            database_name = (
                source.get("database")
                or source.get("databaseName")
                or source.get("service")
                or "unknown"
            )
            assets.append(
                {
                    "table_name": table_name,
                    "database": database_name,
                    "description": source.get("description") or "",
                    "fqn": source.get("fullyQualifiedName"),
                    "id": source.get("id"),
                }
            )

        return {
            "query": query,
            "returned_count": len(assets),
            "assets": assets,
        }

    def get_table_details(self, identifier: str) -> Dict[str, Any]:
        table = self._resolve_table(identifier)

        columns = table.get("columns", []) or []
        formatted_columns = [
            {
                "name": col.get("name"),
                "data_type": col.get("dataType"),
                "description": col.get("description") or "",
                "tags": [
                    tag.get("tagFQN") or tag.get("name")
                    for tag in (col.get("tags", []) or [])
                    if tag.get("tagFQN") or tag.get("name")
                ],
            }
            for col in columns
        ]

        owner = table.get("owner", {}) or {}
        owner_value = owner.get("name") or owner.get("displayName") or owner.get("id")

        return {
            "id": table.get("id"),
            "fqn": table.get("fullyQualifiedName"),
            "name": table.get("name"),
            "description": table.get("description") or "",
            "database": self._name_from_entity_ref(table.get("database")),
            "schema": self._name_from_entity_ref(table.get("databaseSchema")),
            "owner": owner_value,
            "columns": formatted_columns,
            "table_tags": [t.get("tagFQN") for t in (table.get("tags", []) or []) if t.get("tagFQN")],
            "column_tags": self._format_tags(columns),
        }

    def get_table_lineage(self, identifier: str) -> Dict[str, Any]:
        table = self._resolve_table(identifier)
        table_id = table.get("id")
        if not table_id:
            raise OpenMetadataClientError(
                message="Could not resolve table ID required for lineage query.",
                hint="Ensure the table identifier is a valid FQN or UUID.",
            )

        lineage = self._get(f"/api/v1/lineage/table/{table_id}")
        nodes = lineage.get("nodes", []) or []
        edges = lineage.get("upstreamEdges", []) or []

        node_map = {node.get("id"): node for node in nodes if node.get("id")}

        upstream: List[Dict[str, Any]] = []
        downstream: List[Dict[str, Any]] = []

        for edge in edges:
            from_id = edge.get("fromEntity")
            to_id = edge.get("toEntity")
            from_node = node_map.get(from_id, {})
            to_node = node_map.get(to_id, {})

            edge_payload = {
                "from": from_node.get("fullyQualifiedName") or from_node.get("name") or from_id,
                "to": to_node.get("fullyQualifiedName") or to_node.get("name") or to_id,
                "lineage_type": edge.get("lineageDetails", {}).get("sqlQuery", "table_dependency"),
            }

            if to_id == table_id:
                upstream.append(edge_payload)
            elif from_id == table_id:
                downstream.append(edge_payload)

        return {
            "table": {
                "id": table_id,
                "fqn": table.get("fullyQualifiedName"),
                "name": table.get("name"),
            },
            "upstream": upstream,
            "downstream": downstream,
            "upstream_count": len(upstream),
            "downstream_count": len(downstream),
        }

    def get_table_governance_context(self, identifier: str) -> Dict[str, Any]:
        """Return minimal schema context for LLM governance checks."""
        table = self._resolve_table(identifier)
        columns = table.get("columns", []) or []

        governance_columns = [
            {
                "name": col.get("name"),
                "data_type": col.get("dataType"),
                "description": col.get("description") or "",
                "tags": [
                    tag.get("tagFQN") or tag.get("name")
                    for tag in (col.get("tags", []) or [])
                    if tag.get("tagFQN") or tag.get("name")
                ],
            }
            for col in columns
        ]

        return {
            "table": {
                "id": table.get("id"),
                "fqn": table.get("fullyQualifiedName"),
                "name": table.get("name"),
            },
            "columns": governance_columns,
            "column_count": len(governance_columns),
        }

    def apply_column_tag(
        self, table_id: str, column_name: str, tag_fqn: str = "PII.Sensitive"
    ) -> Dict[str, Any]:
        """Apply a governance tag to a top-level table column via JSON Patch."""
        table = self._get(f"/api/v1/tables/{table_id}")
        columns = table.get("columns", []) or []

        target_index: Optional[int] = None
        target_column: Optional[Dict[str, Any]] = None
        for idx, column in enumerate(columns):
            if column.get("name") == column_name:
                target_index = idx
                target_column = column
                break

        if target_index is None or target_column is None:
            raise OpenMetadataClientError(
                message=f"Column '{column_name}' was not found in table '{table_id}'.",
                hint="Use an exact top-level column name from get_table_details output.",
            )

        existing_tags = target_column.get("tags", []) or []
        existing_tag_fqns = {
            tag.get("tagFQN") or tag.get("name")
            for tag in existing_tags
            if tag.get("tagFQN") or tag.get("name")
        }
        if tag_fqn in existing_tag_fqns:
            return {
                "status": "no_op",
                "message": f"Tag '{tag_fqn}' is already present on column '{column_name}'.",
                "table_id": table_id,
                "column_name": column_name,
                "tag_fqn": tag_fqn,
            }

        if isinstance(target_column.get("tags"), list):
            patch_payload = [
                {
                    "op": "add",
                    "path": f"/columns/{target_index}/tags/-",
                    "value": {"tagFQN": tag_fqn},
                }
            ]
        else:
            patch_payload = [
                {
                    "op": "add",
                    "path": f"/columns/{target_index}/tags",
                    "value": [{"tagFQN": tag_fqn}],
                }
            ]

        self._patch(f"/api/v1/tables/{table_id}", patch_payload)
        return {
            "status": "updated",
            "message": f"Applied tag '{tag_fqn}' to column '{column_name}'.",
            "table_id": table_id,
            "column_name": column_name,
            "tag_fqn": tag_fqn,
        }
