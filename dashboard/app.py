"""The demo dashboard: one page and a small API in front of the nodes. Run: python -m dashboard.app"""

import asyncio
import os

import aiohttp
from aiohttp import web

NODE_TIMEOUT = aiohttp.ClientTimeout(total=0.3)  # /state and /admin/partition: slower counts as down
WRITE_TIMEOUT = aiohttp.ClientTimeout(total=6)  # a node answers a write within 5 s


class Dashboard:
    def __init__(self, nodes: dict[str, str]) -> None:
        self.nodes = nodes  # id -> "host:port"

    async def call(self, method: str, node: str, path: str, body: dict | None = None,
                   timeout: aiohttp.ClientTimeout = NODE_TIMEOUT) -> tuple[int, dict | None]:
        """One request to one node, giving (status, JSON body). Status 0 means it didn't answer."""
        url = f"http://{self.nodes[node]}{path}"
        try:  # no shared session: a connection per request is plenty for five nodes
            async with aiohttp.request(method, url, json=body, timeout=timeout) as resp:
                return resp.status, await resp.json(content_type=None)  # None for an empty body
        except (aiohttp.ClientError, TimeoutError):
            return 0, None

    async def cluster(self) -> list[dict]:
        async def state(node: str) -> dict:
            status, body = await self.call("GET", node, "/state")
            return {**body, "up": True} if status == 200 else {"id": node, "up": False}
        return await asyncio.gather(*(state(n) for n in self.nodes))

    async def partition(self, groups: list[list[str]]) -> web.Response:
        """Each node blocks everyone outside its own group. A node that is down misses this."""
        blocked = {n: [m for m in self.nodes if m not in group] for group in groups for n in group}
        posts = [self.call("POST", n, "/admin/partition", {"blocked": b}) for n, b in blocked.items()]
        await asyncio.gather(*posts)
        return web.Response(status=204)

    async def on_page(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))

    async def on_cluster(self, request: web.Request) -> web.Response:
        return web.json_response(await self.cluster())

    async def on_write(self, request: web.Request) -> web.Response:
        write = await request.json()
        leaders = [s for s in await self.cluster() if s.get("role") == "leader"]
        if not leaders:
            return web.json_response({"status": 503})  # our own: nodes never send 503
        # In a partition two nodes can both say "leader". The higher term is the real one.
        node = max(leaders, key=lambda s: s["term"])["id"]
        status, body = await self.call("POST", node, "/client/write", write, WRITE_TIMEOUT)
        if status == 421 and body["leader"] in self.nodes:  # try once at the leader it names
            node = body["leader"]
            status, body = await self.call("POST", node, "/client/write", write, WRITE_TIMEOUT)
        return web.json_response({"node": node, "status": status, "body": body})

    async def on_partition(self, request: web.Request) -> web.Response:
        return await self.partition((await request.json())["groups"])

    async def on_heal(self, request: web.Request) -> web.Response:
        return await self.partition([list(self.nodes)])  # one group: nobody blocks anybody


def main() -> None:
    dashboard = Dashboard(dict(p.split("=") for p in os.environ["NODES"].split(",")))
    app = web.Application()
    app.add_routes([
        web.get("/", dashboard.on_page),
        web.get("/api/cluster", dashboard.on_cluster),
        web.post("/api/write", dashboard.on_write),
        web.post("/api/partition", dashboard.on_partition),
        web.post("/api/heal", dashboard.on_heal),
    ])
    web.run_app(app, port=int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
