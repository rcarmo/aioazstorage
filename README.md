# aioazstorage

A thin `asyncio` wrapper for Azure Storage

## Why?

I wanted a low-latency `asyncio` library for accessing Azure Storage via persistent connections (essentially tables and queues) and the existing Python SDK (which is auto-generated from API specs) was both too high-level and not async-ready.

This is _an intentionally low-level wrapper_, and meant largely for my own consumption. However, pull requests are welcome.

## Features

* [ ] queue metadata
* [ ] advanced message semantics (including queueing status codes)
* [ ] message peek/clear/update
* [x] message queueing/retrieval/deletion
* [x] queue creation/deletion
* [x] table entry creation/updating/deletion/querying (with EDM annotation of supported types)
* [x] table creation/deletion/querying

## Requirements

* Python 3.6
* `aiohttp`
* `ujson` (optional)
* `uvloop` (optional)