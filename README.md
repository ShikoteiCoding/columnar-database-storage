# Columnar Database Storage Exercise

This repository contains a guided Python exercise inspired by DuckDB's table storage hierarchy.

The goal is to help a candidate rebuild a simplified low-level storage layer with these ideas:

- catalog -> schema -> table entry
- table -> row groups -> column data -> column segments
- segment trees for row-range lookup
- data blocks and partial block packing
- checkpoint metadata and version info
- end-to-end append, scan, checkpoint, and reload flow

## What is included

- a staged curriculum in `docs/curriculum.md`
- code skeletons with docstrings and `NotImplementedError`
- unit tests grouped by exercise question
- a `main.py` demo script that should work once the exercise is completed

## Suggested workflow

1. Install `uv`.
2. Run `uv sync`.
3. Read `docs/curriculum.md`.
4. Implement one question at a time.
5. Run the matching question target from the `Makefile`.
6. Move to the next question.
7. Finish by running `make demo`.

## Python version

This project targets Python 3.13.

## Run the tests

```bash
make test
```

## Run a single question

```bash
make q1
make q2
make q3
make q4
make q5
make q6
make q7
make q8
make q9
make q10
```

## Run the demo

```bash
make demo
```

## Design notes

This exercise intentionally keeps the implementation smaller than DuckDB while preserving the same learning shape:

- a four-level storage hierarchy
- row-range segment indexes
- block pointers and metadata pointers
- checkpoint state objects that pass pointers upward
- version information for deleted rows

The implementation is expected to stay pure Python and dependency free.
