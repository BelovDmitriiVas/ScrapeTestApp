import sys
from scrapy.crawler import CrawlerProcess

from scrape_to_chunks import StreamSpider  

URL = ""

PAGES = 5
DEPTH = 3
LANG = "kk"
CHUNK_SIZE = 4000
CHUNK_OVERLAP = 50
RESPECT_ROBOTS = False
ALLOWS_OFFSITE = False

process = CrawlerProcess(settings={
    "LOG_LEVEL": "ERROR",
    "ROBOTSTXT_OBEY": False,
    "ROBOTSTXT_ENABLED": False,
    "DOWNLOADER_MIDDLEWARES": {
        "scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware": None,
    },
    "USER_AGENT": "MinimalScrapyBot/0.3 (+https://example.org)",
    "DEPTH_LIMIT": DEPTH,
    "DOWNLOAD_TIMEOUT": 30,
    "REDIRECT_ENABLED": True,

    "PIPE_LANG": LANG,
    "PIPE_CHUNK_SIZE": CHUNK_SIZE,
    "PIPE_CHUNK_OVERLAP": CHUNK_OVERLAP,
})

process.crawl(
    StreamSpider,
    start_url=URL,
    max_pages=max(1, PAGES),
    allow_offsite=ALLOWS_OFFSITE,
)
process.start()
