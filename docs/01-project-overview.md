# 1. Project Overview

## Why this project exists

Spark performance problems are easy to describe incorrectly.

A job can be slow because of skew, excessive shuffle, spilling, garbage collection, small files, a poor join strategy, or several effects interacting at the same time.

This lab is designed to practice **forensic diagnosis**:

> observe the execution evidence first, then form the explanation.

## Core question

When a Spark stage is slow:

1. Which tasks are slow?
2. How different are they from the normal task population?
3. How much data does each task process?
4. Is the data distribution uneven?
5. Is the stage spilling?
6. Is JVM garbage collection significant?
7. Which explanation is supported by the evidence?

## Current implementation

The repository contains an event-log parser and a stage diagnosis layer.

The parser reads Spark JSON-lines event logs and extracts successful task attempts. It handles rolling event-log directories and compressed `.gz` logs. It records task duration, executor run time, JVM GC time, shuffle read/write, spill, input bytes/records, and shuffle records.

The diagnosis layer currently looks for three main signals:

- `DATA_SKEW`
- `SPILL_PRESSURE`
- `GC_PRESSURE`

The thresholds are configurable rather than hard-coded into a single interpretation.

## What this project is not

This is not a generic Spark tutorial and it is not a claim that every slow job has one root cause.

The analyzer produces **evidence-based signals**, not a magical root-cause oracle.
