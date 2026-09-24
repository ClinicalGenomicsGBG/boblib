<h1 align="center">
<img src="images/boblib.svg" height="200"/>
<br/>boblib

[![Ruff](https://shieldcn.dev/badge/ruff.svg?size=xs&logo=ruff&color=D7FF64)](https://github.com/astral-sh/ruff)
[![ty](https://shieldcn.dev/badge/ty.svg?size=xs&logo=ty&color=46EBE1)](https://github.com/astral-sh/ty)
[![uv](https://shieldcn.dev/badge/uv.svg?size=xs&logo=uv&color=DE5FE9)](https://github.com/astral-sh/uv)
[![python](https://shieldcn.dev/badge/python-3.10.svg?size=xs&split=true&logo=python&labelColor=306998&color=FFD43B)](https://www.python.org/downloads/release/python-3100/)

</h1>

Common utilities for interacting with data and metadata created by Bob. Interacts with SLIMS and long-term storage (S3), while caching resources and responses as far as possible for efficiency. Aims to be as general and reusable as possible, to support any downstream pipelines that needs to integrate with Bob's data and metadata.

## Key features
- Create protocol runs for a given test (pipeline) in a given workflow.
- Upload data to long-term storage (S3).
- Fetch new protocol runs for a given test in a given workflow and parse them into usable data structures.
- Ensure local files exist for a given sample, or fetch them from long-term storage (S3).
