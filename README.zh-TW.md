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
  <strong>輕量級增量資料流轉換引擎</strong><br>
  Lightweight Incremental Data Pipeline Engine
</p>

<p align="center">
  <img src="https://img.shields.io/badge/零外部依賴-純Python實作-critical.svg">
  <img src="https://img.shields.io/badge/YAML宣告式設定-開箱即用-9cf.svg">
  <img src="https://img.shields.io/badge/增量處理-秒級更新-orange.svg">
  <img src="https://img.shields.io/badge/資料血緣追蹤-視覺化-blue.svg">
</p>

---

## 🎉 專案介紹

**DataFlow-Lite** 是一款面向本地開發者的輕量級增量資料流轉換引擎。靈感來源於 GitHub Trending 熱門專案 [CocoIndex](https://github.com/cocoindex-io/cocoindex) 的增量資料處理理念，我們獨立自研了這款零外部依賴、純 Python 實作的資料管線工具。

### 🔥 解決的核心痛點

- **傳統 ETL 工具太重**：Airflow、dbt 等工具依賴複雜，設定繁瑣，不適合本地輕量使用
- **全量重算太慢**：來源資料僅變更一小部分，卻要全量重新處理，浪費時間
- **資料血緣不透明**：資料從哪來、經過什麼轉換、輸出到哪去，缺乏清晰的追蹤
- **缺乏輕量管線工具**：開發者需要一個 `pip install` 即可使用的本地資料處理工具

### ✨ 自研差異化亮點

| 特性 | DataFlow-Lite | 傳統 ETL 工具 |
|------|:---:|:---:|
| 外部依賴 | **零** | 多個 |
| 安裝方式 | 複製即用 | pip install / Docker |
| 增量處理 | ✅ 內建 | 需額外設定 |
| 資料血緣 | ✅ 內建 | 需額外外掛 |
| YAML 設定 | ✅ 內建解析器 | 依賴 PyYAML |
| 學習曲線 | **極低** | 較高 |

---

## ✨ 核心特性

### 🔗 資料流定義
- **YAML 宣告式設定**：用簡潔的 YAML 定義完整的資料管線，無需撰寫程式碼
- **內建 YAML 解析器**：零外部依賴，自研輕量級 YAML 解析器
- **環境變數支援**：設定中支援 `${ENV_VAR}` 環境變數引用

### 🔄 智慧增量偵測
- **三種偵測策略**：Hash 雜湊比對 / MTime 修改時間 / Size 檔案大小
- **秒級變更定位**：自動識別新增、修改、刪除的檔案
- **跳過無變更處理**：來源資料未變更時自動跳過，節省運算資源

### 📊 資料血緣追蹤
- **自動記錄資料流向**：來源 → 轉換 → 目標的完整鏈路
- **上下游查詢**：快速查看任意節點的資料來源與去向
- **JSON 匯出**：一鍵匯出完整血緣圖，便於分析與文件化

### 🧰 豐富的資料來源與輸出
- **5 種資料來源**：檔案、CSV、JSON、Markdown、目錄
- **5 種輸出目標**：檔案、JSON、CSV、SQLite、目錄
- **10 種轉換操作**：過濾、映射、重新命名、排序、去重、扁平化、擷取、聚合、合併、範本

### 🖥️ 友善的 CLI 體驗
- **6 大命令**：`run`、`init`、`status`、`lineage`、`reset`、`validate`
- **彩色終端輸出**：清晰的執行狀態與進度展示
- **5 種內建範本**：快速初始化常見管線設定

---

## 🚀 快速開始

### 環境需求

- **Python 3.8+**（無其他依賴）

### 安裝

```bash
# 克隆儲存庫
git clone https://github.com/gitstq/DataFlow-Lite.git
cd DataFlow-Lite

# 無需安裝依賴，直接使用
python -m src.dataflow_lite.cli --help
```

### 快速體驗

```bash
# 1. 初始化一個管線設定（5種範本可選）
python -m src.dataflow_lite.cli init -t basic -o my_pipeline.yaml

# 2. 驗證設定是否正確
python -m src.dataflow_lite.cli validate my_pipeline.yaml

# 3. 執行管線
python -m src.dataflow_lite.cli run my_pipeline.yaml

# 4. 查看引擎狀態
python -m src.dataflow_lite.cli status

# 5. 查看資料血緣
python -m src.dataflow_lite.cli lineage

# 6. 匯出資料血緣圖
python -m src.dataflow_lite.cli lineage -e lineage.json
```

### 一分鐘範例：CSV 資料處理

```bash
# 初始化 CSV 處理管線
python -m src.dataflow_lite.cli init -t csv_process -o csv_pipeline.yaml

# 編輯設定，指定你的 CSV 檔案路徑
# 然後執行
python -m src.dataflow_lite.cli run csv_pipeline.yaml
```

---

## 📖 詳細使用指南

### 管線設定語法

```yaml
# 管線基本定義
name: my_pipeline
description: 管線描述
version: "1.0.0"

# 全域設定
settings:
  change_detection: hash  # hash | mtime | size

# 管線步驟（依序執行）
steps:
  # 步驟1：資料來源
  - name: read_data
    type: source
    source: csv          # file | csv | json | markdown | directory
    path: ./data.csv
    has_header: true

  # 步驟2：資料轉換
  - name: filter_active
    type: transform
    operation: filter    # filter | map | rename | sort | deduplicate | ...
    field: status
    operator: equals
    value: active

  # 步驟3：資料輸出
  - name: save_output
    type: sink
    sink: json           # file | json | csv | sqlite | directory
    path: ./output/result.json
```

### 資料來源設定詳解

| 資料來源 | type 值 | 關鍵參數 | 說明 |
|--------|---------|----------|------|
| 📄 檔案 | `file` | `path`, `encoding` | 讀取單個檔案全部內容 |
| 📊 CSV | `csv` | `path`, `delimiter`, `has_header` | 逐行讀取 CSV 為字典 |
| 📋 JSON | `json` | `path`, `json_path` | 讀取 JSON 檔案/陣列 |
| 📝 Markdown | `markdown` | `path` | 解析 Markdown 含 frontmatter |
| 📁 目錄 | `directory` | `path`, `extensions`, `recursive` | 遞迴掃描目錄檔案 |

### 轉換操作詳解

| 操作 | operation | 說明 | 範例場景 |
|------|-----------|------|----------|
| 🔍 過濾 | `filter` | 按條件篩選記錄 | 只保留 status=active 的列 |
| 🔄 映射 | `map` | 欄位值轉換/計算 | 大小寫轉換、型別轉換 |
| ✏️ 重新命名 | `rename` | 批量重新命名欄位 | old_name → new_name |
| 📊 排序 | `sort` | 按欄位排序 | 按價格升冪排列 |
| 🔁 去重 | `deduplicate` | 按欄位去重 | 移除重複的電子郵件 |
| 📐 扁平化 | `flatten` | 巢狀結構扁平化 | user.address.city → user_address_city |
| 🧩 擷取 | `extract` | 正規表示式擷取資訊 | 從文字中擷取電話號碼 |
| 📈 聚合 | `aggregate` | 分組統計計算 | 按類別統計銷售額 |
| ➕ 合併 | `merge` | 新增/合併欄位 | 新增 processed 標記 |
| 📝 範本 | `template` | 範本渲染產生內容 | 產生格式化輸出 |

### 增量處理策略

```bash
# Hash 模式（預設）— 最精確，基於檔案內容雜湊
python -m src.dataflow_lite.cli run pipeline.yaml -d hash

# MTime 模式 — 最快速，基於檔案修改時間
python -m src.dataflow_lite.cli run pipeline.yaml -d mtime

# Size 模式 — 最輕量，基於檔案大小
python -m src.dataflow_lite.cli run pipeline.yaml -d size

# 強制全量重建（忽略增量）
python -m src.dataflow_lite.cli run pipeline.yaml --force
```

### CLI 命令參考

```bash
# 執行管線
dataflow run <config.yaml> [選項]
  -s, --state-dir DIR     狀態儲存目錄 (預設: .dataflow)
  -f, --force             強制全量重建
  -d, --detection STR     變更偵測策略 (hash|mtime|size)
  -v, --verbose           詳細輸出
  -q, --quiet             靜默模式

# 初始化管線設定
dataflow init [選項]
  -t, --template NAME     範本名稱 (basic|csv_process|markdown_index|json_transform|directory_scan)
  -o, --output PATH       輸出檔案路徑

# 查看引擎狀態
dataflow status [-s DIR]

# 查看/匯出資料血緣
dataflow lineage [-p PIPELINE] [-e EXPORT_PATH] [-s DIR]

# 重設引擎狀態
dataflow reset [-y] [-s DIR]

# 驗證管線設定
dataflow validate <config.yaml>
```

---

## 💡 設計思路與迭代規劃

### 設計理念

1. **零依賴哲學**：不引入任何外部函式庫，純 Python 標準函式庫實作，降低使用門檻
2. **宣告式優先**：YAML 設定代替程式碼撰寫，讓非程式設計師也能定義資料管線
3. **增量為核心**：增量處理不是附加功能，而是引擎的核心設計理念
4. **可觀測性**：內建資料血緣追蹤，讓資料流向清晰透明

### 技術選型原因

| 技術 | 選型原因 |
|------|----------|
| Python | 生態最豐富、開發者最熟悉的資料處理語言 |
| SQLite | 內建資料庫，零設定，適合本地狀態儲存 |
| YAML | 人類可讀的設定格式，比 JSON 更友善 |
| Hash 偵測 | 最精確的檔案變更偵測方式 |

### 後續迭代計畫

- [ ] **v1.1** — 新增檔案監聽模式（watch），自動觸發增量處理
- [ ] **v1.2** — Web UI 視覺化面板，展示管線狀態與血緣圖
- [ ] **v1.3** — 外掛系統，支援自訂資料來源/轉換/輸出
- [ ] **v2.0** — 分散式處理支援，多機協作執行管線

---

## 📦 打包與部署

### 作為函式庫使用

```python
from src.dataflow_lite.engine import DataFlowEngine
from src.dataflow_lite.config import EngineConfig
from src.dataflow_lite.pipeline import Pipeline

# 建立引擎
config = EngineConfig(state_dir=".dataflow", change_detection="hash")
engine = DataFlowEngine(config)

# 從 YAML 檔案執行
result = engine.run_from_file("my_pipeline.yaml")
print(result.summary())

# 查看引擎統計
stats = engine.get_stats()
print(stats)
```

### 作為 CLI 工具使用

```bash
# 直接執行（無需安裝）
python -m src.dataflow_lite.cli run pipeline.yaml

# 或加入 PATH 後
dataflow run pipeline.yaml
```

### 相容環境

| 環境 | 最低版本 | 推薦版本 |
|------|---------|---------|
| Python | 3.8 | 3.10+ |
| 作業系統 | Windows 10 / macOS 10.15 / Ubuntu 20.04 | 最新版 |
| 架構 | x86_64 | x86_64 / ARM64 |

---

## 🤝 貢獻指南

歡迎貢獻程式碼、回報 Bug 或提出新功能建議！

### 貢獻流程

1. **Fork** 本儲存庫
2. 建立特性分支：`git checkout -b feature/my-feature`
3. 提交變更：`git commit -m 'feat: add my feature'`
4. 推送分支：`git push origin feature/my-feature`
5. 提交 **Pull Request**

### 提交規範

遵循 Angular 提交規範：
- `feat:` 新增功能
- `fix:` 修復問題
- `docs:` 文件更新
- `refactor:` 程式碼重構
- `test:` 測試相關
- `chore:` 建置/工具變更

### Issue 回饋

請使用 [GitHub Issues](https://github.com/gitstq/DataFlow-Lite/issues) 提交 Bug 回報或功能建議，包含：
- 問題描述
- 重現步驟
- 預期行為
- 實際行為
- 環境資訊（Python 版本、作業系統）

---

## 📄 開源授權

本專案基於 [MIT License](LICENSE) 開源。

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
