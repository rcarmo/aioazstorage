# aioazstorage

A thin `asyncio` wrapper for Azure Storage

## Why?

I wanted an `asyncio` wrapper able to access Azure Storage (essentially tables and queues) and the existing Python SDK (which is auto-generated from API specs) was both too high-level and not async-ready.

This is _an intentionally low-level wrapper_, and meant largely for my own consumption. 

However, pull requests are welcome.

## Features

* [ ] queue item creation/handling
* [ ] queue creation/deletion/querying
* [ ] table entry creation/deletion/querying
* [x] table creation/deletion/querying

## Requirements

* Python 3.6
* `aiohttp`
* `ujson` (optional)