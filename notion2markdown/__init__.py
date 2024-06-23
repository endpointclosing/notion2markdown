from pathlib import Path
from .notion import NotionDownloader
from .json2md import JsonToMdConverter


class NotionExporter:
    def __init__(
        self,
        token: str,
        strip_meta_chars: str | None = None,
        extension: str = "md",
        filter: dict | None = None,
    ):
        self.downloader = NotionDownloader(token, filter)
        self.converter = JsonToMdConverter(
            strip_meta_chars=strip_meta_chars, extension=extension
        )

    async def export_url(
        self, url: str, json_dir: str | Path = "./json", md_dir: str | Path = "./md"
    ):
        """Export the notion page or database."""
        await self.downloader.download_url(url, json_dir)
        return self.converter.convert(json_dir, md_dir)

    def export_database(
        self,
        database_id: str,
        json_dir: str | Path = "./json",
        md_dir: str | Path = "./md",
    ):
        """Export the notion database and associated pages."""
        self.downloader.download_database(database_id, json_dir)
        self.converter.convert(json_dir, md_dir)

    def export_page(
        self, page_id: str, json_dir: str | Path = "./json", md_dir: str | Path = "./md"
    ):
        """Export the notion page."""
        self.downloader.download_page(page_id, json_dir)
        self.converter.convert(json_dir, md_dir)
