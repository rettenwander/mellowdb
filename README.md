# mellowdb

A tiny, educational embedded key–value store in Go.
It uses a B-Tree on top of a simple paged storage engine, with binary node serialization, configurable fill factor, and a clean, testable design.

> Status: experimental / alpha — API will change. Great for learning and small projects.

## Features
- B-Tree index
- Paged storage engine
- Binary serialization of nodes/items
- Configurable MaxNodeSize and MaxFillPercent
- Thorough tests

## Design Overview

- Index: Classic B-Tree:
    - Inserts descend to a leaf; if a node exceeds the fill threshold, split bottom-up and (if needed) create a new root.
    - Keys are kept in sorted order; children pointers partition key ranges.
- Storage: A simple page-oriented engine implementing ReadNode/WriteNode/GetNewNode:
    - File-backed engine persists pages; in-memory mock enables fast tests.
- Serialization: Nodes and items are written to a compact binary buffer and read back safely.
- Config: MaxNodeSize and MaxFillPercent control split frequency and tree height.

This repo aims to be approachable while still modeling real storage concepts.

## Roadmap
- [ ] Delete / Update operations
- [ ] Concurrency story (single writer vs. multiple readers)
- [ ] WAL / crash-safety and basic transactions

## Contributing

PRs and issues are welcome! If you spot a bug, include a failing test or a small repro (key sequence + expected behavior).

## Why this project

I built mellowdb to learn systems design in Go: trees, binary formats, and paging.

## License

MIT © Niklas Rettenwander
