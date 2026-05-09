"""
CLI命令行接口 — 提供友好的终端交互体验
CLI interface — provides a friendly terminal interaction experience
"""

import argparse
import json
import os
import sys
import time
from typing import List, Optional

# 确保可以以模块方式运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.dataflow_lite.config import EngineConfig
from src.dataflow_lite.engine import DataFlowEngine
from src.dataflow_lite.pipeline import load_pipeline_config, Pipeline
from src.dataflow_lite.detector import IncrementalDetector
from src.dataflow_lite.lineage import LineageTracker


# ─── 终端颜色工具 ─────────────────────────────────────────────

class Colors:
    """终端颜色常量"""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"

    @classmethod
    def enabled(cls) -> bool:
        """检测终端是否支持颜色"""
        if os.environ.get("NO_COLOR"):
            return False
        if not hasattr(sys.stdout, "isatty"):
            return False
        return sys.stdout.isatty()


def c(text: str, color: str) -> str:
    """着色文本 Color text"""
    if Colors.enabled():
        return f"{color}{text}{Colors.RESET}"
    return text


def print_banner():
    """打印启动横幅"""
    banner = f"""
{c('╔══════════════════════════════════════════════════════╗', Colors.CYAN)}
{c('║', Colors.CYAN)}  {c('⚡ DataFlow-Lite', Colors.BOLD + Colors.WHITE)}  {c('v1.0.0', Colors.DIM)}                          {c('║', Colors.CYAN)}
{c('║', Colors.CYAN)}  {c('Lightweight Incremental Data Pipeline Engine', Colors.DIM)}    {c('║', Colors.CYAN)}
{c('╚══════════════════════════════════════════════════════╝', Colors.CYAN)}
"""
    print(banner)


def print_step_result(step_result):
    """打印单步执行结果"""
    status_icons = {
        "success": c("✓", Colors.GREEN),
        "skipped": c("○", Colors.YELLOW),
        "error": c("✗", Colors.RED),
    }
    icon = status_icons.get(step_result.status, "?")
    name = c(step_result.step_name, Colors.WHITE)
    stype = c(f"[{step_result.step_type}]", Colors.DIM)

    info_parts = []
    if step_result.output_count:
        info_parts.append(f"→ {step_result.output_count} records")
    if step_result.duration > 0:
        info_parts.append(f"{step_result.duration:.2f}s")
    info = c(" | ".join(info_parts), Colors.DIM) if info_parts else ""

    error = ""
    if step_result.error_message:
        error = f"  {c(step_result.error_message, Colors.RED)}"

    print(f"  {icon} {name} {stype} {info}{error}")


def print_pipeline_result(result):
    """打印管线执行结果摘要"""
    status_colors = {
        "success": Colors.GREEN,
        "skipped": Colors.YELLOW,
        "error": Colors.RED,
    }
    color = status_colors.get(result.status, Colors.WHITE)
    status_text = c(result.status.upper(), color + Colors.BOLD)

    print()
    print(f"  {c('─' * 50, Colors.DIM)}")
    print(f"  {c('Pipeline:', Colors.WHITE)} {result.pipeline_name}  {status_text}")
    print(f"  {c('Duration:', Colors.DIM)} {result.duration:.2f}s")
    print(f"  {c('Records:', Colors.DIM)} {result.total_records_in} in → {result.total_records_out} out")

    if result.change_result:
        cr = result.change_result
        if cr.has_changes:
            changes = []
            if cr.added:
                changes.append(f"{c('+' + str(len(cr.added)), Colors.GREEN)} new")
            if cr.modified:
                changes.append(f"{c('~' + str(len(cr.modified)), Colors.YELLOW)} changed")
            if cr.deleted:
                changes.append(f"{c('-' + str(len(cr.deleted)), Colors.RED)} deleted")
            print(f"  {c('Changes:', Colors.DIM)} {' | '.join(changes)}")
        else:
            print(f"  {c('Changes:', Colors.DIM)} {c('No changes detected', Colors.GREEN)}")

    print(f"  {c('─' * 50, Colors.DIM)}")
    print()


# ─── CLI命令处理 ──────────────────────────────────────────────

def cmd_run(args):
    """执行管线命令"""
    config_path = args.pipeline

    if not os.path.exists(config_path):
        print(f"  {c('Error:', Colors.RED)} Pipeline config not found: {config_path}")
        sys.exit(1)

    # 构建引擎配置
    engine_config = EngineConfig(
        state_dir=args.state_dir,
        verbose=args.verbose,
        force_full=args.force,
        change_detection=args.detection,
    )

    engine = DataFlowEngine(engine_config)

    if args.verbose:
        print(f"  {c('Loading pipeline:', Colors.DIM)} {config_path}")

    try:
        result = engine.run_from_file(config_path)

        if not args.quiet:
            for sr in result.step_results:
                print_step_result(sr)
            print_pipeline_result(result)

        if result.status == "error":
            sys.exit(1)

    except Exception as e:
        print(f"  {c('Error:', Colors.RED)} {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def cmd_init(args):
    """初始化管线配置文件"""
    template = args.template or "basic"
    output_path = args.output or "dataflow.yaml"

    templates = {
        "basic": BASIC_TEMPLATE,
        "csv_process": CSV_PROCESS_TEMPLATE,
        "markdown_index": MARKDOWN_INDEX_TEMPLATE,
        "json_transform": JSON_TRANSFORM_TEMPLATE,
        "directory_scan": DIRECTORY_SCAN_TEMPLATE,
    }

    template_content = templates.get(template)
    if not template_content:
        print(f"  {c('Error:', Colors.RED)} Unknown template: {template}")
        print(f"  Available: {', '.join(templates.keys())}")
        sys.exit(1)

    if os.path.exists(output_path):
        print(f"  {c('Warning:', Colors.YELLOW)} File already exists: {output_path}")
        print(f"  Use --output to specify a different path")
        sys.exit(1)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(template_content)

    print(f"  {c('✓', Colors.GREEN)} Pipeline config created: {output_path}")
    print(f"  {c('Template:', Colors.DIM)} {template}")
    print(f"  {c('Next:', Colors.DIM)} dataflow run {output_path}")


def cmd_status(args):
    """查看引擎状态"""
    engine_config = EngineConfig(state_dir=args.state_dir)
    engine = DataFlowEngine(engine_config)
    stats = engine.get_stats()

    print(f"  {c('DataFlow-Lite Status', Colors.BOLD + Colors.WHITE)}")
    print(f"  {c('─' * 40, Colors.DIM)}")
    print(f"  {c('State dir:', Colors.DIM)} {stats['state_dir']}")
    print(f"  {c('Files tracked:', Colors.DIM)} {stats['detector']['total_files_tracked']}")
    print(f"  {c('Detection:', Colors.DIM)} {stats['detector']['detection_strategy']}")
    print(f"  {c('Pipelines:', Colors.DIM)} {len(stats['pipelines'])}")

    if stats['pipelines']:
        print()
        for pid in stats['pipelines']:
            lineage = engine.lineage.get_pipeline_lineage(pid)
            print(f"  {c('  ▸', Colors.CYAN)} {pid}")
            print(f"    {c('Nodes:', Colors.DIM)} {lineage['stats']['total_nodes']}  "
                  f"{c('Edges:', Colors.DIM)} {lineage['stats']['total_edges']}")

    print()


def cmd_lineage(args):
    """查看/导出数据血缘"""
    engine_config = EngineConfig(state_dir=args.state_dir)
    engine = DataFlowEngine(engine_config)

    if args.export:
        engine.lineage.export_lineage_json(args.export)
        print(f"  {c('✓', Colors.GREEN)} Lineage exported to: {args.export}")
    elif args.pipeline:
        lineage = engine.lineage.get_pipeline_lineage(args.pipeline)
        print(f"  {c('Pipeline Lineage:', Colors.BOLD + Colors.WHITE)} {args.pipeline}")
        print(f"  {c('─' * 40, Colors.DIM)}")
        print(f"  {c('Nodes:', Colors.DIM)} {lineage['stats']['total_nodes']}")
        print(f"  {c('Edges:', Colors.DIM)} {lineage['stats']['total_edges']}")
        print()
        for node in lineage['nodes']:
            type_icons = {"source": "📥", "transform": "⚙️", "sink": "📤"}
            icon = type_icons.get(node['type'], "○")
            print(f"  {icon} {c(node['name'], Colors.WHITE)} [{node['type']}]")
        print()
        for edge in lineage['edges']:
            print(f"  {c('→', Colors.DIM)} {edge['from']} → {edge['to']}")
        print()
    else:
        pipelines = engine.lineage.get_all_pipelines()
        if not pipelines:
            print(f"  {c('No pipeline lineage data found.', Colors.YELLOW)}")
            print(f"  Run a pipeline first to generate lineage data.")
        else:
            print(f"  {c('Registered Pipelines:', Colors.BOLD + Colors.WHITE)}")
            for pid in pipelines:
                print(f"  {c('  ▸', Colors.CYAN)} {pid}")
            print()


def cmd_reset(args):
    """重置引擎状态"""
    if not args.yes:
        print(f"  {c('Warning:', Colors.YELLOW)} This will clear all fingerprints and lineage data.")
        confirm = input(f"  {c('Continue?', Colors.WHITE)} [y/N] ").strip().lower()
        if confirm != "y":
            print(f"  {c('Cancelled.', Colors.DIM)}")
            return

    engine_config = EngineConfig(state_dir=args.state_dir)
    engine = DataFlowEngine(engine_config)
    result = engine.reset_state()
    print(f"  {c('✓', Colors.GREEN)} State reset complete. {result['fingerprints_cleared']} fingerprints cleared.")


def cmd_validate(args):
    """验证管线配置"""
    config_path = args.pipeline

    if not os.path.exists(config_path):
        print(f"  {c('Error:', Colors.RED)} Pipeline config not found: {config_path}")
        sys.exit(1)

    try:
        config_data = load_pipeline_config(config_path)
        pipeline = Pipeline.from_dict(config_data)
        errors = pipeline.validate()

        if errors:
            print(f"  {c('✗', Colors.RED)} Validation failed:")
            for err in errors:
                print(f"    {c('•', Colors.RED)} {err}")
            sys.exit(1)
        else:
            print(f"  {c('✓', Colors.GREEN)} Pipeline config is valid")
            print(f"  {c('Name:', Colors.DIM)} {pipeline.name}")
            print(f"  {c('Steps:', Colors.DIM)} {len(pipeline.steps)}")
            for step in pipeline.steps:
                print(f"    {c('▸', Colors.CYAN)} {step.name} [{step.step_type}]")
    except Exception as e:
        print(f"  {c('✗', Colors.RED)} Parse error: {e}")
        sys.exit(1)


# ─── 管线配置模板 ─────────────────────────────────────────────

BASIC_TEMPLATE = """# DataFlow-Lite Pipeline Configuration
# 数据流管线配置

name: my_pipeline
description: A basic data pipeline example
version: "1.0.0"

settings:
  change_detection: hash

steps:
  # 数据源步骤
  - name: read_input
    type: source
    source: file
    path: ./input.txt

  # 转换步骤
  - name: process_data
    type: transform
    operation: map
    mappings:
      processed_content:
        from: content
        operation: upper

  # 输出步骤
  - name: write_output
    type: sink
    sink: file
    path: ./output.txt
    template: "{{processed_content}}"
"""

CSV_PROCESS_TEMPLATE = """# CSV Data Processing Pipeline
# CSV数据处理管线

name: csv_processor
description: Process CSV files with filtering and transformation
version: "1.0.0"

settings:
  change_detection: hash

steps:
  # 读取CSV文件
  - name: read_csv
    type: source
    source: csv
    path: ./data.csv
    has_header: true

  # 过滤数据
  - name: filter_rows
    type: transform
    operation: filter
    field: status
    operator: equals
    value: active

  # 字段映射
  - name: transform_fields
    type: transform
    operation: map
    mappings:
      name_upper:
        from: name
        operation: upper
      email_lower:
        from: email
        operation: lower

  # 输出为JSON
  - name: export_json
    type: sink
    sink: json
    path: ./output/result.json
    indent: 2
"""

MARKDOWN_INDEX_TEMPLATE = """# Markdown Index Pipeline
# Markdown文档索引管线

name: markdown_indexer
description: Scan markdown files and generate an index
version: "1.0.0"

settings:
  change_detection: hash

steps:
  # 扫描Markdown文件目录
  - name: scan_docs
    type: source
    source: directory
    path: ./docs
    extensions:
      - .md
      - .markdown
    recursive: true

  # 输出文件清单
  - name: export_index
    type: sink
    sink: json
    path: ./output/doc_index.json
"""

JSON_TRANSFORM_TEMPLATE = """# JSON Transform Pipeline
# JSON数据转换管线

name: json_transformer
description: Transform and reshape JSON data
version: "1.0.0"

settings:
  change_detection: hash

steps:
  # 读取JSON数据
  - name: read_json
    type: source
    source: json
    path: ./data.json

  # 重命名字段
  - name: rename_fields
    type: transform
    operation: rename
    fields:
      old_name: new_name
      foo: bar

  # 添加计算字段
  - name: add_fields
    type: transform
    operation: merge
    fields:
      processed: true
      timestamp: "2026-01-01"

  # 排序
  - name: sort_data
    type: transform
    operation: sort
    field: name
    reverse: false

  # 输出
  - name: write_output
    type: sink
    sink: json
    path: ./output/transformed.json
"""

DIRECTORY_SCAN_TEMPLATE = """# Directory Scan Pipeline
# 目录扫描管线

name: dir_scanner
description: Scan directory and generate file inventory
version: "1.0.0"

settings:
  change_detection: mtime

steps:
  # 扫描目录
  - name: scan_directory
    type: source
    source: directory
    path: ./src
    recursive: true

  # 按扩展名聚合统计
  - name: aggregate_stats
    type: transform
    operation: aggregate
    group_by: extension
    operations:
      - field: size
        operation: sum
        as: total_size
      - field: size
        operation: avg
        as: avg_size
      - field: filename
        operation: count
        as: file_count

  # 输出统计结果
  - name: export_stats
    type: sink
    sink: csv
    path: ./output/file_stats.csv
"""


# ─── 主入口 ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="dataflow",
        description="⚡ DataFlow-Lite — Lightweight Incremental Data Pipeline Engine",
    )
    parser.add_argument("-v", "--version", action="version", version="DataFlow-Lite v1.0.0")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run 命令
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("pipeline", help="Path to pipeline config file (YAML)")
    run_parser.add_argument("-s", "--state-dir", default=".dataflow", help="State directory")
    run_parser.add_argument("-f", "--force", action="store_true", help="Force full rebuild")
    run_parser.add_argument("-d", "--detection", default="hash",
                            choices=["hash", "mtime", "size"], help="Change detection strategy")
    run_parser.add_argument("--verbose", action="store_true", help="Verbose output")
    run_parser.add_argument("-q", "--quiet", action="store_true", help="Quiet mode")
    run_parser.set_defaults(func=cmd_run)

    # init 命令
    init_parser = subparsers.add_parser("init", help="Initialize a pipeline config")
    init_parser.add_argument("-t", "--template", default="basic",
                             choices=["basic", "csv_process", "markdown_index",
                                      "json_transform", "directory_scan"],
                             help="Template to use")
    init_parser.add_argument("-o", "--output", default="dataflow.yaml", help="Output file path")
    init_parser.set_defaults(func=cmd_init)

    # status 命令
    status_parser = subparsers.add_parser("status", help="Show engine status")
    status_parser.add_argument("-s", "--state-dir", default=".dataflow", help="State directory")
    status_parser.set_defaults(func=cmd_status)

    # lineage 命令
    lineage_parser = subparsers.add_parser("lineage", help="View/export data lineage")
    lineage_parser.add_argument("-p", "--pipeline", help="Show lineage for specific pipeline")
    lineage_parser.add_argument("-e", "--export", help="Export lineage to JSON file")
    lineage_parser.add_argument("-s", "--state-dir", default=".dataflow", help="State directory")
    lineage_parser.set_defaults(func=cmd_lineage)

    # reset 命令
    reset_parser = subparsers.add_parser("reset", help="Reset engine state")
    reset_parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    reset_parser.add_argument("-s", "--state-dir", default=".dataflow", help="State directory")
    reset_parser.set_defaults(func=cmd_reset)

    # validate 命令
    validate_parser = subparsers.add_parser("validate", help="Validate pipeline config")
    validate_parser.add_argument("pipeline", help="Path to pipeline config file")
    validate_parser.set_defaults(func=cmd_validate)

    args = parser.parse_args()

    if not args.command:
        print_banner()
        parser.print_help()
        return

    if args.command != "init":
        print_banner()

    args.func(args)


if __name__ == "__main__":
    main()
