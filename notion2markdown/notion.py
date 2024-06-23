from datetime import datetime
from pathlib import Path
import json
from .utils import normalize_id

from notion_client import AsyncClient
from notion_client.helpers import async_iterate_paginated_api


class NotionDownloader:
    def __init__(self, token: str, filter: str | None = None):
        self.transformer = LastEditedToDateTime()
        self.notion = NotionClient(
            token=token, transformer=self.transformer, filter=filter
        )
        self.io = NotionIO(self.transformer)

    async def download_url(self, url: str, out_dir: str | Path = "./json"):
        """Download the notion page or database."""
        out_dir = Path(out_dir)
        slug = url.split("/")[-1].split("?")[0]
        if "-" in slug:
            page_id = slug.split("-")[-1]
            await self.download_page(page_id, out_dir / f"{page_id}.json")
        else:
            raise NotImplementedError
            # self.download_database(slug, out_dir)

    async def download_page(
        self, page_id: str, out_path: str | Path = "./json", fetch_metadata: bool = True
    ):
        """Download the notion page."""
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        blocks = await self.notion.get_blocks(page_id)
        self.io.save(blocks, out_path)

        if fetch_metadata:
            metadata = await self.notion.get_metadata(page_id)
            self.io.save([metadata], out_path.parent / "database.json")

    # def download_database(self, database_id: str, out_dir: str | Path = "./json"):
    #     """Download the notion database and associated pages."""
    #     out_dir = Path(out_dir)
    #     out_dir.mkdir(parents=True, exist_ok=True)
    #     path = out_dir / "database.json"
    #     prev = {pg["id"]: pg["last_edited_time"] for pg in self.io.load(path)}
    #     pages = self.notion.get_database(database_id)  # download database
    #     self.io.save(pages, path)
    #
    #     for cur in pages:  # download individual pages in database IF updated
    #         if prev.get(cur["id"], datetime(1, 1, 1)) < cur["last_edited_time"]:
    #             self.download_page(cur["id"], out_dir / f"{cur['id']}.json", False)
    #             logger.info(f"Downloaded {cur['url']}")


class LastEditedToDateTime:
    def forward(self, blocks, key: str = "last_edited_time") -> list:
        return [
            {
                **block,
                "last_edited_time": datetime.fromisoformat(
                    block["last_edited_time"][:-1]
                ),
                "id": normalize_id(block["id"]),
            }
            for block in blocks
        ]

    def reverse(self, o) -> None | str:
        if isinstance(o, datetime):
            return o.isoformat() + "Z"


class NotionIO:
    def __init__(self, transformer):
        self.transformer = transformer

    def load(self, path: str | Path) -> list[dict]:
        """Load blocks from json file."""
        if Path(path).exists():
            with open(path) as f:
                return self.transformer.forward(json.load(f))
        return []

    def save(self, blocks: list[dict], path: str):
        """Dump blocks to json file."""
        with open(path, "w") as f:
            json.dump(blocks, f, default=self.transformer.reverse, indent=4)


class NotionClient:
    def __init__(self, token: str, transformer, filter: dict | None = None):
        self.client = AsyncClient(auth=token)
        self.transformer = transformer
        self.filter = filter

    async def get_metadata(self, page_id: str) -> dict:
        """Get page metadata as json."""
        return self.transformer.forward(
            [await self.client.pages.retrieve(page_id=page_id)]
        )[0]

    async def get_blocks(self, block_id: int) -> list:
        """Get all page blocks as json. Recursively fetches descendants."""
        blocks = []

        async for block_list in async_iterate_paginated_api(
            self.client.blocks.children.list, block_id=block_id
        ):
            for block in block_list:
                block["children"] = (
                    list(await self.get_blocks(block["id"]))
                    if block["has_children"]
                    else []
                )
                blocks.append(block)

        return list(self.transformer.forward(blocks))

    # def get_database(self, database_id: str) -> list:
    #     """Fetch pages in database as json."""
    #     if self.filter:
    #         results = paginate(
    #             self.client.databases.query,
    #             # database_id=database_id,
    #             filter=self.filter,
    #         )
    #     else:
    #         results = paginate(
    #             self.client.databases.query,
    #             database_id=database_id,
    #         )
    #     return list(self.transformer.forward(chain(*results)))
