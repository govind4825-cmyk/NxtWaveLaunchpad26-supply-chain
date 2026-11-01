"""Minimal RSS 2.0 / Atom reader for CAP index feeds."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime

from defusedxml import ElementTree as ET


@dataclass
class FeedItem:
    title: str | None
    link: str | None
    guid: str | None
    published: datetime | None
    author: str | None

    @property
    def key(self) -> str:
        return self.guid or self.link or (self.title or "")


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find(el, name):
    for c in el.iter():
        if c is not el and _local(c.tag) == name:
            return c
    return None


def _txt(el, name):
    c = _find(el, name)
    return c.text.strip() if c is not None and c.text else None


def _date(v: str | None) -> datetime | None:
    if not v:
        return None
    try:
        return parsedate_to_datetime(v)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None


def parse_feed(xml: bytes | str) -> list[FeedItem]:
    root = ET.fromstring(xml)
    items: list[FeedItem] = []
    nodes = [e for e in root.iter() if _local(e.tag) in ("item", "entry")]
    for n in nodes:
        link = _txt(n, "link")
        if link is None:
            # Atom puts the URL in an attribute.
            le = _find(n, "link")
            if le is not None:
                link = le.attrib.get("href")
        items.append(
            FeedItem(
                title=_txt(n, "title"),
                link=link,
                guid=_txt(n, "guid") or _txt(n, "id"),
                published=_date(_txt(n, "pubDate") or _txt(n, "updated") or _txt(n, "published")),
                author=_txt(n, "author"),
            )
        )
    return items
