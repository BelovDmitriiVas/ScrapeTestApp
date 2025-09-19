from __future__ import annotations

import json
import re
import sys
from typing import List
from urllib.parse import urlparse

import trafilatura
from langchain_text_splitters import RecursiveCharacterTextSplitter

SUPPORTED = {
    'ur','ru','pl','ar','kk','zh','hy','en','it','hi','my',
    'de','am','fa','es','bg','ja','sk','el','nl','mr','da','fr'
}

def split_sentences(text: str, lang: str) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    if lang in SUPPORTED:
        import pysbd
        seg = pysbd.Segmenter(language=lang, clean=True)
        sents = seg.segment(text)
        return [s.strip() for s in sents if s and s.strip()]
    sents = re.split(r"(?<=[.!?…])\s+", text)
    return [s.strip() for s in sents if s and s.strip()]

def chunk_sentences(sentences: List[str], chunk_size: int, chunk_overlap: int) -> List[str]:
    joined = "\n".join(sentences)
    splitter = RecursiveCharacterTextSplitter(
        separators=["\n\n", "\n"],
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        keep_separator=False,
    )
    docs = splitter.create_documents([joined])
    return [d.page_content.strip() for d in docs if d and d.page_content.strip()]

def extract_main_text(html: str, base_url: str | None = None) -> str:
    return trafilatura.extract(html, url=base_url, include_comments=False, include_tables=False) or ""

import scrapy
from scrapy.crawler import CrawlerProcess

class BaseSpider(scrapy.Spider):
    name = "stream-spider"

    def __init__(self, start_url: str, max_pages: int, allow_offsite: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.max_pages = int(max_pages)
        self.seen = 0
        parsed = urlparse(start_url)
        self.allowed_domains = [] if allow_offsite else [parsed.netloc]

class StreamSpider(BaseSpider):
    def parse(self, response: scrapy.http.Response):
        if self.seen >= self.max_pages:
            return
        self.seen += 1
        raw = extract_main_text(response.text, base_url=response.url)
        if raw:
            lang = self.settings.get("PIPE_LANG", "ru")
            chunk_size = self.settings.getint("PIPE_CHUNK_SIZE", 1200)
            chunk_overlap = self.settings.getint("PIPE_CHUNK_OVERLAP", 150)
            sents = split_sentences(raw, lang)
            chunks = chunk_sentences(sents, chunk_size, chunk_overlap) if sents else []
            for i, ch in enumerate(chunks, start=1):
                rec = {"url": response.url, "chunk_index": i, "chars": len(ch), "chunk": ch}
                sys.stdout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                sys.stdout.flush()
        if self.seen >= self.max_pages:
            return
        for href in response.css("a::attr(href)").getall():
            if self.seen >= self.max_pages:
                break
            yield response.follow(href, callback=self.parse)