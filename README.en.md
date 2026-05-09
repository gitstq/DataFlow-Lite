<p align="center">
  <a href="README.md">简体中文</a> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <a href="README.en.md">English</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8+-blue.svg" alt="Python 3.8+">
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License">
  <img src="https://img.shields.io/badge/Zero_Dependencies-✓-success.svg" alt="Zero Dependencies">
  <img src="https://img.shields.io/badge/Tests-40%20Passed-brightgreen.svg" alt="Tests">
</p>

<h1 align="center">⚡ DataFlow-Lite</h1>

<p align="center">
  <strong>A Lightweight Incremental Data Pipeline Engine</strong><br>
  Zero Dependencies · Pure Python · YAML Declarative Config · Incremental Processing · Data Lineage Tracking
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Zero_Dependencies-Pure_Python-critical.svg">
  <img src="https://img.shields.io/badge/YAML_Declarative_Config-Out_of_the_Box-9cf.svg">
  <img src="https://img.shields.io/badge/Incremental_Processing-Second_Level_Updates-orange.svg">
  <img src="https://img.shields.io/badge/Data_Lineage_Tracking-Visualizable-blue.svg">
</p>

---

## 🎉 About

**DataFlow-Lite** is a lightweight incremental data pipeline engine built for local developers. Inspired by the incremental data processing philosophy of [CocoIndex](https://github.com/cocoindex-io/cocoindex) — a project once trending on GitHub — we built this pipeline tool from the ground up with zero external dependencies, implemented entirely in pure Python.

### 🔥 Problems We Solve

- **Traditional ETL tools are too heavy**: Tools like Airflow and dbt come with complex dependency chains and verbose configurations, making them overkill for lightweight local use
- **Full recomputation is wasteful**: When only a small portion of source data changes, reprocessing everything wastes time and resources
- **Data lineage is opaque**: There's often no clear way to trace where data comes from, what transformations it goes through, and where it ends up
- **No lightweight pipeline tools exist**: Developers need a local data processing tool that just works — no Docker, no package managers, no hassle

### ✨ What Sets Us Apart

| Feature | DataFlow-Lite | Traditional ETL Tools |
|---------|:---:|:---:|
| External Dependencies | **Zero** | Multiple |
| Installation | Copy and run | pip install / Docker |
| Incremental Processing | ✅ Built-in | Requires extra setup |
| Data Lineage | ✅ Built-in | Requires plugins |
| YAML Config | ✅ Built-in parser | Depends on PyYAML |
| Learning Curve | **Minimal** | Steep |

---

## ✨ Core Features

### 🔗 Pipeline Definition
- **YAML declarative configuration**: Define complete data pipelines in clean YAML — no code required
- **Built-in YAML parser**: Zero external dependencies thanks to our custom lightweight YAML parser
- **Environment variable support**: Reference environment variables with `${ENV_VAR}` syntax in your configs

### 🔄 Smart Incremental Detection
- **Three detection strategies**: Hash comparison / MTime (modification time) / File size
- **Sub-second change pinpointing**: Automatically identifies new, modified, and deleted files
- **Skip unchanged data**: Automatically skips processing when source data hasn't changed, saving compute resources

### 📊 Data Lineage Tracking
- **Automatic data flow recording**: Full traceability from source → transform → sink
- **Upstream/downstream queries**: Instantly look up where any node's data comes from and where it goes
- **JSON export**: One-click export of the complete lineage graph for analysis and documentation

### 🧰 Rich Data Sources & Sinks
- **5 data sources**: File, CSV, JSON, Markdown, Directory
- **5 output sinks**: File, JSON, CSV, SQLite, Directory
- **10 transform operations**: Filter, Map, Rename, Sort, Deduplicate, Flatten, Extract, Aggregate, Merge, Template

### 🖥️ Developer-Friendly CLI
- **6 core commands**: `run`, `init`, `status`, `lineage`, `reset`, `validate`
- **Colorized terminal output**: Clear execution status and progress display
- **5 built-in templates**: Quickly bootstrap common pipeline configurations

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.8+** (no other dependencies required)

### Installation

```bash
# Clone the repository
git clone https://github.com/gitstq/DataFlow-Lite.git
cd DataFlow-Lite

# No dependencies to install — just run it
python -m src.dataflow_lite.cli --help
```

### Get Started in Seconds

```bash
# 1. Initialize a pipeline config (5 templates available)
python -m src.dataflow_lite.cli init -t basic -o my_pipeline.yaml

# 2. Validate your configuration
python -m src.dataflow_lite.cli validate my_pipeline.yaml

# 3. Run the pipeline
python -m src.dataflow_lite.cli run my_pipeline.yaml

# 4. Check engine status
python -m src.dataflow_lite.cli status

# 5. View data lineage
python -m src.dataflow_lite.cli lineage

# 6. Export the lineage graph
python -m src.dataflow_lite.cli lineage -e lineage.json
```

### One-Minute Example: CSV Data Processing

```bash
# Initialize a CSV processing pipeline
python -m src.dataflow_lite.cli init -t csv_process -o csv_pipeline.yaml

# Edit the config to point to your CSV file
# Then run it
python -m src.dataflow_lite.cli run csv_pipeline.yaml
```

---

## 📖 Detailed Guide

### Pipeline Configuration Syntax

```yaml
# Basic pipeline definition
name: my_pipeline
description: Pipeline description
version: "1.0.0"

# Global settings
settings:
  change_detection: hash  # hash | mtime | size

# Pipeline steps (executed in order)
steps:
  # Step 1: Data source
  - name: read_data
    type: source
    source: csv          # file | csv | json | markdown | directory
    path: ./data.csv
    has_header: true

  # Step 2: Data transformation
  - name: filter_active
    type: transform
    operation: filter    # filter | map | rename | sort | deduplicate | ...
    field: status
    operator: equals
    value: active

  # Step 3: Data output
  - name: save_output
    type: sink
    sink: json           # file | json | csv | sqlite | directory
    path: ./output/result.json
```

### Data Sources Reference

| Source | type Value | Key Parameters | Description |
|--------|-----------|----------------|-------------|
| 📄 File | `file` | `path`, `encoding` | Read the entire contents of a single file |
| 📊 CSV | `csv` | `path`, `delimiter`, `has_header` | Read CSV row by row as dictionaries |
| 📋 JSON | `json` | `path`, `json_path` | Read JSON files or arrays |
| 📝 Markdown | `markdown` | `path` | Parse Markdown with frontmatter support |
| 📁 Directory | `directory` | `path`, `extensions`, `recursive` | Recursively scan files in a directory |

### Transform Operations Reference

| Operation | operation Value | Description | Example Use Case |
|-----------|----------------|-------------|------------------|
| 🔍 Filter | `filter` | Filter records by condition | Keep only rows where status=active |
| 🔄 Map | `map` | Transform or compute field values | Case conversion, type casting |
| ✏️ Rename | `rename` | Batch-rename fields | old_name → new_name |
| 📊 Sort | `sort` | Sort records by field | Sort by price in ascending order |
| 🔁 Deduplicate | `deduplicate` | Remove duplicates by field | Remove duplicate email addresses |
| 📐 Flatten | `flatten` | Flatten nested structures | user.address.city → user_address_city |
| 🧩 Extract | `extract` | Extract data via regex | Pull phone numbers from text |
| 📈 Aggregate | `aggregate` | Grouped statistical computation | Sum sales by category |
| ➕ Merge | `merge` | Add or merge fields | Add a processed flag |
| 📝 Template | `template` | Render content from templates | Generate formatted output |

### Incremental Processing Strategies

```bash
# Hash mode (default) — most accurate, based on file content hashing
python -m src.dataflow_lite.cli run pipeline.yaml -d hash

# MTime mode — fastest, based on file modification time
python -m src.dataflow_lite.cli run pipeline.yaml -d mtime

# Size mode — lightest, based on file size
python -m src.dataflow_lite.cli run pipeline.yaml -d size

# Force a full rebuild (ignore incremental state)
python -m src.dataflow_lite.cli run pipeline.yaml --force
```

### CLI Command Reference

```bash
# Run a pipeline
dataflow run <config.yaml> [options]
  -s, --state-dir DIR     State storage directory (default: .dataflow)
  -f, --force             Force a full rebuild
  -d, --detection STR     Change detection strategy (hash|mtime|size)
  -v, --verbose           Verbose output
  -q, --quiet             Quiet mode

# Initialize a pipeline config
dataflow init [options]
  -t, --template NAME     Template name (basic|csv_process|markdown_index|json_transform|directory_scan)
  -o, --output PATH       Output file path

# Check engine status
dataflow status [-s DIR]

# View / export data lineage
dataflow lineage [-p PIPELINE] [-e EXPORT_PATH] [-s DIR]

# Reset engine state
dataflow reset [-y] [-s DIR]

# Validate a pipeline config
dataflow validate <config.yaml>
```

---

## 💡 Design Philosophy & Roadmap

### Design Principles

1. **Zero-dependency philosophy**: No external libraries — built entirely on the Python standard library to minimize friction
2. **Declarative first**: YAML configuration over code, so even non-programmers can define data pipelines
3. **Incremental by design**: Incremental processing isn't an add-on — it's the core architectural principle of the engine
4. **Observability out of the box**: Built-in data lineage tracking keeps data flows transparent and auditable

### Technology Choices

| Technology | Why We Chose It |
|-----------|-----------------|
| Python | The most popular language for data processing with the richest ecosystem |
| SQLite | Built-in database engine, zero configuration, ideal for local state storage |
| YAML | Human-readable config format, more approachable than JSON |
| Hash detection | The most accurate method for detecting file changes |

### Roadmap

- [ ] **v1.1** — File watch mode for automatic incremental processing triggers
- [ ] **v1.2** — Web UI dashboard for visualizing pipeline status and lineage graphs
- [ ] **v1.3** — Plugin system for custom data sources, transforms, and sinks
- [ ] **v2.0** — Distributed processing support for multi-machine pipeline execution

---

## 📦 Packaging & Deployment

### Using as a Library

```python
from src.dataflow_lite.engine import DataFlowEngine
from src.dataflow_lite.config import EngineConfig
from src.dataflow_lite.pipeline import Pipeline

# Create an engine instance
config = EngineConfig(state_dir=".dataflow", change_detection="hash")
engine = DataFlowEngine(config)

# Run from a YAML file
result = engine.run_from_file("my_pipeline.yaml")
print(result.summary())

# View engine statistics
stats = engine.get_stats()
print(stats)
```

### Using as a CLI Tool

```bash
# Run directly (no installation needed)
python -m src.dataflow_lite.cli run pipeline.yaml

# Or add to PATH and use the shorthand
dataflow run pipeline.yaml
``### Compatible Environments

| Environment | Minimum Version | Recommended Version |
|------------|----------------|---------------------|
| Python | 3.8 | 3.10+ |
| Operating System | Windows 10 / macOS 10.15 / Ubuntu 20.04 | Latest |
| Architecture | x86_64 | x86_64 / ARM64 |

---

## 🤝 Contributing

Contributions, bug reports, and feature requests are welcome!

### How to Contribute

1. **Fork** this repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit your changes: `git commit -m 'feat: add my feature'`
4. Push to your branch: `git push origin feature/my-feature`
5. Submit a **Pull Request**

### Commit Convention

We follow the Angular commit convention:
- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation updates
- `refactor:` Code refactoring
- `test:` Test-related changes
- `chore:` Build/tooling changes

### Reporting Issues

Please use [GitHub Issues](https://github.com/gitstq/DataFlow-Lite/issues) to submit bug reports or feature requests. Include:
- Issue description
- Steps to reproduce
- Expected behavior
- Actual behavior
- Environment info (Python version, OS)

---

## 📄 License

This project is open-sourced under the [MIT License](LICENSE).

```
MIT License

Copyright (c) 2026 DataFlow-Lite Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction...
```

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/gitstq">DataFlow-Lite Contributors</a>
</p>
