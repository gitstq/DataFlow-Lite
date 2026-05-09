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
  <strong>轻量级增量数据流转换引擎</strong><br>
  Lightweight Incremental Data Pipeline Engine
</p>

<p align="center">
  <img src="https://img.shields.io/badge/零外部依赖-纯Python实现-critical.svg">
  <img src="https://img.shields.io/badge/YAML声明式配置-开箱即用-9cf.svg">
  <img src="https://img.shields.io/badge/增量处理-秒级更新-orange.svg">
  <img src="https://img.shields.io/badge/数据血缘追踪-可视化-blue.svg">
</p>

---

## 🎉 项目介绍

**DataFlow-Lite** 是一款面向本地开发者的轻量级增量数据流转换引擎。灵感来源于 GitHub Trending 热门项目 [CocoIndex](https://github.com/cocoindex-io/cocoindex) 的增量数据处理理念，我们独立自研了这款零外部依赖、纯 Python 实现的数据管线工具。

### 🔥 解决的核心痛点

- **传统 ETL 工具太重**：Airflow、dbt 等工具依赖复杂，配置繁琐，不适合本地轻量使用
- **全量重算太慢**：源数据仅变更一小部分，却要全量重新处理，浪费时间
- **数据血缘不透明**：数据从哪来、经过什么转换、输出到哪去，缺乏清晰的追踪
- **缺乏轻量管线工具**：开发者需要一个 `pip install` 即可使用的本地数据处理工具

### ✨ 自研差异化亮点

| 特性 | DataFlow-Lite | 传统 ETL 工具 |
|------|:---:|:---:|
| 外部依赖 | **零** | 多个 |
| 安装方式 | 复制即用 | pip install / Docker |
| 增量处理 | ✅ 内置 | 需额外配置 |
| 数据血缘 | ✅ 内置 | 需额外插件 |
| YAML 配置 | ✅ 内置解析器 | 依赖 PyYAML |
| 学习曲线 | **极低** | 较高 |

---

## ✨ 核心特性

### 🔗 数据流定义
- **YAML 声明式配置**：用简洁的 YAML 定义完整的数据管线，无需编写代码
- **内置 YAML 解析器**：零外部依赖，自研轻量级 YAML 解析器
- **环境变量支持**：配置中支持 `${ENV_VAR}` 环境变量引用

### 🔄 智能增量检测
- **三种检测策略**：Hash 哈希比对 / MTime 修改时间 / Size 文件大小
- **秒级变更定位**：自动识别新增、修改、删除的文件
- **跳过无变更处理**：源数据未变更时自动跳过，节省计算资源

### 📊 数据血缘追踪
- **自动记录数据流向**：源 → 转换 → 目标的完整链路
- **上下游查询**：快速查看任意节点的数据来源和去向
- **JSON 导出**：一键导出完整血缘图，便于分析和文档化

### 🧰 丰富的数据源与输出
- **5 种数据源**：文件、CSV、JSON、Markdown、目录
- **5 种输出目标**：文件、JSON、CSV、SQLite、目录
- **10 种转换操作**：过滤、映射、重命名、排序、去重、扁平化、提取、聚合、合并、模板

### 🖥️ 友好的 CLI 体验
- **6 大命令**：`run`、`init`、`status`、`lineage`、`reset`、`validate`
- **彩色终端输出**：清晰的执行状态和进度展示
- **5 种内置模板**：快速初始化常见管线配置

---

## 🚀 快速开始

### 环境要求

- **Python 3.8+**（无其他依赖）

### 安装

```bash
# 克隆仓库
git clone https://github.com/gitstq/DataFlow-Lite.git
cd DataFlow-Lite

# 无需安装依赖，直接使用
python -m src.dataflow_lite.cli --help
```

### 快速体验

```bash
# 1. 初始化一个管线配置（5种模板可选）
python -m src.dataflow_lite.cli init -t basic -o my_pipeline.yaml

# 2. 验证配置是否正确
python -m src.dataflow_lite.cli validate my_pipeline.yaml

# 3. 执行管线
python -m src.dataflow_lite.cli run my_pipeline.yaml

# 4. 查看引擎状态
python -m src.dataflow_lite.cli status

# 5. 查看数据血缘
python -m src.dataflow_lite.cli lineage

# 6. 导出数据血缘图
python -m src.dataflow_lite.cli lineage -e lineage.json
```

### 一分钟示例：CSV 数据处理

```bash
# 初始化 CSV 处理管线
python -m src.dataflow_lite.cli init -t csv_process -o csv_pipeline.yaml

# 编辑配置，指定你的 CSV 文件路径
# 然后执行
python -m src.dataflow_lite.cli run csv_pipeline.yaml
```

---

## 📖 详细使用指南

### 管线配置语法

```yaml
# 管线基本定义
name: my_pipeline
description: 管线描述
version: "1.0.0"

# 全局设置
settings:
  change_detection: hash  # hash | mtime | size

# 管线步骤（按顺序执行）
steps:
  # 步骤1：数据源
  - name: read_data
    type: source
    source: csv          # file | csv | json | markdown | directory
    path: ./data.csv
    has_header: true

  # 步骤2：数据转换
  - name: filter_active
    type: transform
    operation: filter    # filter | map | rename | sort | deduplicate | ...
    field: status
    operator: equals
    value: active

  # 步骤3：数据输出
  - name: save_output
    type: sink
    sink: json           # file | json | csv | sqlite | directory
    path: ./output/result.json
```

### 数据源配置详解

| 数据源 | type 值 | 关键参数 | 说明 |
|--------|---------|----------|------|
| 📄 文件 | `file` | `path`, `encoding` | 读取单个文件全部内容 |
| 📊 CSV | `csv` | `path`, `delimiter`, `has_header` | 逐行读取 CSV 为字典 |
| 📋 JSON | `json` | `path`, `json_path` | 读取 JSON 文件/数组 |
| 📝 Markdown | `markdown` | `path` | 解析 Markdown 含 frontmatter |
| 📁 目录 | `directory` | `path`, `extensions`, `recursive` | 递归扫描目录文件 |

### 转换操作详解

| 操作 | operation | 说明 | 示例场景 |
|------|-----------|------|----------|
| 🔍 过滤 | `filter` | 按条件筛选记录 | 只保留 status=active 的行 |
| 🔄 映射 | `map` | 字段值转换/计算 | 大小写转换、类型转换 |
| ✏️ 重命名 | `rename` | 批量重命名字段 | old_name → new_name |
| 📊 排序 | `sort` | 按字段排序 | 按价格升序排列 |
| 🔁 去重 | `deduplicate` | 按字段去重 | 去除重复邮箱 |
| 📐 扁平化 | `flatten` | 嵌套结构扁平化 | user.address.city → user_address_city |
| 🧩 提取 | `extract` | 正则提取信息 | 从文本中提取电话号码 |
| 📈 聚合 | `aggregate` | 分组统计计算 | 按类别统计销售额 |
| ➕ 合并 | `merge` | 添加/合并字段 | 添加 processed 标记 |
| 📝 模板 | `template` | 模板渲染生成内容 | 生成格式化输出 |

### 增量处理策略

```bash
# Hash 模式（默认）— 最精确，基于文件内容哈希
python -m src.dataflow_lite.cli run pipeline.yaml -d hash

# MTime 模式 — 最快速，基于文件修改时间
python -m src.dataflow_lite.cli run pipeline.yaml -d mtime

# Size 模式 — 最轻量，基于文件大小
python -m src.dataflow_lite.cli run pipeline.yaml -d size

# 强制全量重建（忽略增量）
python -m src.dataflow_lite.cli run pipeline.yaml --force
```

### CLI 命令参考

```bash
# 执行管线
dataflow run <config.yaml> [选项]
  -s, --state-dir DIR     状态存储目录 (默认: .dataflow)
  -f, --force             强制全量重建
  -d, --detection STR     变更检测策略 (hash|mtime|size)
  -v, --verbose           详细输出
  -q, --quiet             静默模式

# 初始化管线配置
dataflow init [选项]
  -t, --template NAME     模板名称 (basic|csv_process|markdown_index|json_transform|directory_scan)
  -o, --output PATH       输出文件路径

# 查看引擎状态
dataflow status [-s DIR]

# 查看/导出数据血缘
dataflow lineage [-p PIPELINE] [-e EXPORT_PATH] [-s DIR]

# 重置引擎状态
dataflow reset [-y] [-s DIR]

# 验证管线配置
dataflow validate <config.yaml>
```

---

## 💡 设计思路与迭代规划

### 设计理念

1. **零依赖哲学**：不引入任何外部库，纯 Python 标准库实现，降低使用门槛
2. **声明式优先**：YAML 配置代替代码编写，让非程序员也能定义数据管线
3. **增量为核心**：增量处理不是附加功能，而是引擎的核心设计理念
4. **可观测性**：内置数据血缘追踪，让数据流向清晰透明

### 技术选型原因

| 技术 | 选型原因 |
|------|----------|
| Python | 生态最丰富、开发者最熟悉的数据处理语言 |
| SQLite | 内置数据库，零配置，适合本地状态存储 |
| YAML | 人类可读的配置格式，比 JSON 更友好 |
| Hash 检测 | 最精确的文件变更检测方式 |

### 后续迭代计划

- [ ] **v1.1** — 添加文件监听模式（watch），自动触发增量处理
- [ ] **v1.2** — Web UI 可视化面板，展示管线状态和血缘图
- [ ] **v1.3** — 插件系统，支持自定义数据源/转换/输出
- [ ] **v2.0** — 分布式处理支持，多机协作执行管线

---

## 📦 打包与部署

### 作为库使用

```python
from src.dataflow_lite.engine import DataFlowEngine
from src.dataflow_lite.config import EngineConfig
from src.dataflow_lite.pipeline import Pipeline

# 创建引擎
config = EngineConfig(state_dir=".dataflow", change_detection="hash")
engine = DataFlowEngine(config)

# 从 YAML 文件执行
result = engine.run_from_file("my_pipeline.yaml")
print(result.summary())

# 查看引擎统计
stats = engine.get_stats()
print(stats)
```

### 作为 CLI 工具使用

```bash
# 直接运行（无需安装）
python -m src.dataflow_lite.cli run pipeline.yaml

# 或添加到 PATH 后
dataflow run pipeline.yaml
```

### 兼容环境

| 环境 | 最低版本 | 推荐版本 |
|------|---------|---------|
| Python | 3.8 | 3.10+ |
| 操作系统 | Windows 10 / macOS 10.15 / Ubuntu 20.04 | 最新版 |
| 架构 | x86_64 | x86_64 / ARM64 |

---

## 🤝 贡献指南

欢迎贡献代码、报告 Bug 或提出新功能建议！

### 贡献流程

1. **Fork** 本仓库
2. 创建特性分支：`git checkout -b feature/my-feature`
3. 提交变更：`git commit -m 'feat: add my feature'`
4. 推送分支：`git push origin feature/my-feature`
5. 提交 **Pull Request**

### 提交规范

遵循 Angular 提交规范：
- `feat:` 新增功能
- `fix:` 修复问题
- `docs:` 文档更新
- `refactor:` 代码重构
- `test:` 测试相关
- `chore:` 构建/工具变更

### Issue 反馈

请使用 [GitHub Issues](https://github.com/gitstq/DataFlow-Lite/issues) 提交 Bug 报告或功能建议，包含：
- 问题描述
- 复现步骤
- 期望行为
- 实际行为
- 环境信息（Python 版本、操作系统）

---

## 📄 开源协议

本项目基于 [MIT License](LICENSE) 开源。

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
