"""Client for DataHub's own MCP server.

Foreshock deliberately does not reimplement lineage traversal or metadata
writes. DataHub ships an MCP server exposing both, and an agent that reacts to
change should use the same tool surface a human-driven agent would.

The split is the point: Foreshock supplies the substrate DataHub lacks (a typed
event stream), and DataHub's MCP server supplies the hands. Everything this
module does is a call into tooling the sponsor already maintains.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# max_hops=3 is documented by the server as "unlimited" for practical graphs.
UNLIMITED_HOPS = 3


@dataclass(frozen=True)
class ToolsConfig:
    """How to launch and authenticate the DataHub MCP server."""

    gms_url: str = "http://127.0.0.1:8080"
    gms_token: str | None = None
    # Mutations are off unless explicitly enabled, so a dry run cannot write.
    enable_mutations: bool = False
    # A lineage walk on a large or overloaded estate can hang indefinitely.
    # A subscriber that blocks forever on one event stops seeing every later
    # one, which is worse than giving up on the event in hand.
    call_timeout_seconds: float = 45.0
    extra_env: dict[str, str] = field(default_factory=dict)

    def server_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["DATAHUB_GMS_URL"] = self.gms_url
        if self.gms_token:
            env["DATAHUB_GMS_TOKEN"] = self.gms_token
        env["TOOLS_IS_MUTATION_ENABLED"] = "true" if self.enable_mutations else "false"
        # Keep the server's own logging off stdio, which carries the protocol.
        env.setdefault("DATAHUB_TELEMETRY_ENABLED", "false")
        env.update(self.extra_env)
        return env

    def server_parameters(self) -> StdioServerParameters:
        return StdioServerParameters(
            command=sys.executable,
            args=["-m", "mcp_server_datahub", "--transport", "stdio"],
            env=self.server_env(),
        )


class DataHubTools:
    """Typed wrapper over the DataHub MCP tools Foreshock actually uses."""

    def __init__(self, session: ClientSession, timeout_seconds: float = 45.0) -> None:
        self._session = session
        self._timeout = timeout_seconds

    async def call(self, name: str, arguments: dict[str, Any]) -> Any:
        """Invoke a tool and decode its payload.

        MCP returns content blocks; the DataHub server puts a JSON document in
        the first text block. Anything that is not JSON is handed back as text
        so a caller can still surface it in an error.

        Raises ``TimeoutError`` rather than waiting forever, so one slow walk
        cannot silently stall the whole subscription.
        """
        try:
            result = await asyncio.wait_for(
                self._session.call_tool(name, arguments), timeout=self._timeout
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"{name} did not return within {self._timeout}s"
            ) from exc
        if result.isError:
            detail = "; ".join(
                block.text for block in result.content if hasattr(block, "text")
            )
            raise RuntimeError(f"{name} failed: {detail}")
        for block in result.content:
            text = getattr(block, "text", None)
            if text is None:
                continue
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        return None

    async def downstream_lineage(
        self,
        urn: str,
        *,
        column: str | None = None,
        max_hops: int = UNLIMITED_HOPS,
        max_results: int = 50,
    ) -> Any:
        """Everything that depends on ``urn``, transitively.

        Passing ``column`` narrows the walk to that column's own lineage, and
        each result then carries ``lineageColumns`` naming the downstream
        columns actually derived from it.
        """
        arguments: dict[str, Any] = {
            "urn": urn,
            "upstream": False,
            "max_hops": max_hops,
            "max_results": max_results,
        }
        if column is not None:
            arguments["column"] = column
        return await self.call("get_lineage", arguments)

    async def lineage_path(self, source_urn: str, target_urn: str) -> Any:
        """The concrete hop chain from a change to one thing it endangers."""
        return await self.call(
            "get_lineage_paths_between",
            {
                "source_urn": source_urn,
                "target_urn": target_urn,
                "direction": "downstream",
            },
        )

    async def get_entities(self, urns: list[str]) -> Any:
        return await self.call("get_entities", {"urns": urns})

    async def add_tags(self, tag_urns: list[str], entity_urns: list[str]) -> Any:
        return await self.call(
            "add_tags", {"tag_urns": tag_urns, "entity_urns": entity_urns}
        )

    async def append_description(self, entity_urn: str, description: str) -> Any:
        """Append rather than replace, so the agent never destroys human text."""
        return await self.call(
            "update_description",
            {
                "entity_urn": entity_urn,
                "operation": "append",
                "description": description,
            },
        )


@asynccontextmanager
async def open_tools(config: ToolsConfig | None = None) -> AsyncIterator[DataHubTools]:
    """Start the DataHub MCP server and yield a client bound to it."""
    settings = config or ToolsConfig()
    async with stdio_client(settings.server_parameters()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield DataHubTools(session, settings.call_timeout_seconds)
