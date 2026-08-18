# ETF Database Importer

此工具用於自動化解析 ETF 報告 JSON 檔案，並將成分股的「納入 (addition)」與「剔除 (deletion)」歷史同步至 MS SQL 資料庫。

## 目錄結構
```
~/etf/etf_calculator/etfDbImporter/
├── etfDbImporter.py  # 主程式 (使用 lowerCamelCase 命名)
├── README.md         # 說明文件
└── logs/             # 執行日誌資料夾
```

## 資料庫規格
*   **資料庫名稱**：`ETF_機率`
*   **資料表名稱**：`etf_additions_history`
*   **主要欄位**：
    *   `etf_code`: ETF 代號 (如 0050, 0056)
    *   `report_date`: 報告日期
    *   `stock_code`: 股票代碼
    *   `stock_name`: 股票名稱
    *   `action`: 動作 (addition / deletion)
    *   `weight`: 權重
    *   `dividend_rate`: 股利率
    *   `estimated_amount`: 投信預估買賣金額
    *   `estimated_shares`: 投信預估買賣張數
    *   `reason`: 調整原因 (JSON 格式)

## 使用方式

需先切換至對應的 Python 虛擬環境：
```bash
source ~/etf/venv/bin/activate
```

### 1. 自動匯入昨天 (T-1) 的資料
```bash
python3 etfDbImporter.py
```

### 2. 指定日期匯入
```bash
python3 etfDbImporter.py --date 2026-05-07
```

## 注意事項
*   **過濾機制**：目前程式僅處理 `0050`, `0056`, `00878`, `00919` 系列資料。
*   **重複處理**：使用 `MERGE` 指令，若相同日期、相同股票已有記錄，會自動更新內容而不會造成重複。
*   **日誌**：每次執行的詳細狀況都會記錄在 `logs/import_YYYY-MM-DD.log` 中。
