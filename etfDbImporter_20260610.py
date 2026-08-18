"""
ETF 成分股變動資料匯入工具
=====================================
讀取 report_generator 輸出的 JSON（或交易員提供的 showdown CSV），
將「新增/刪除成分股」資料寫入 SQL Server 的 etf_additions_history_dev 資料表。

【兩種資料來源的切換邏輯】
  ┌─────────────────────────────────────────────────────────────────┐
  │ 一般期間（today 不在調整期內）                                     │
  │   → 讀取 report_generator/tmp_<etfCode>/<etfCode>_<date>_prod.json │
  │     這份 JSON 是系統根據歷史規則估算出的成分股調整名單               │
  ├─────────────────────────────────────────────────────────────────┤
  │ 調整期間（adjust_begin < today <= adjust_end）                   │
  │   → 優先讀取 showdown_csv/<etfCode>.csv                          │
  │     這份 CSV 是交易員人工確認後的最終名單，比 JSON 更準確            │
  │     若 CSV 不存在，fallback 回 JSON                              │
  └─────────────────────────────────────────────────────────────────┘
"""
import os
import json
import csv
import sys
import pyodbc
import argparse
import logging
from datetime import datetime, timedelta, date

# DAO 模組放在非標準路徑，手動加入 sys.path 才能 import
DAO_SRC_DIR = '/home/webuser/etf/etf_calculator/dateData/src'
if DAO_SRC_DIR not in sys.path:
    sys.path.insert(0, DAO_SRC_DIR)
import DAO

# ══════════════════════════════════════════════════════════════════
# 設定區：資料庫、路徑、ETF 清單
# ══════════════════════════════════════════════════════════════════

dbConfig = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': '172.16.8.91',
    'database': 'ETF_機率',
    'user': 'lethe',
    'pass': 'lethe891'
}

# report_generator 的輸出目錄，內含 tmp_<etfCode>/ 子資料夾
reportDir = '/home/webuser/etf/etf_calculator/report_generator'

# 本腳本自身的根目錄，用來放 log 檔
baseDir = '/home/webuser/etf/etf_calculator/etfDbImporter'

# 交易員手動上傳的 showdown CSV 所在目錄，檔名格式：<etfCode>.csv
showdownCsvDir = '/home/webuser/etf/showdown_csv'

# 目標資料表名稱
TABLE_NAME = 'etf_additions_history_dev'

# 需要匯入的 ETF 代碼清單
targetEtfList = ['0050', '0051', '0056', '00713', '00878', '00900', '00918', '00919_May', '00919_Dec', '00929']

# 「調整期間」設定：僅列在此的 ETF 才有調整期邏輯
# Key   = ETF 代碼
# Value = 從生效日起算，往後數幾個「交易日」為調整期結束日
#         例如 '0056': 5 代表生效日後第 5 個交易日當天為最後一天（含）
# 沒有列在此 dict 的 ETF（如 0050、0051、00900）：
#   → 永遠使用 JSON，adjust_begin / adjust_end 存 NULL
ADJUST_DAYS = {
    '0056':      5,
    '00713':     15,
    '00878':     12,
    '00918':     3,
    '00919_May': 8,
    '00919_Dec': 8,
    '00929':     8,
}


# ══════════════════════════════════════════════════════════════════
# 初始化
# ══════════════════════════════════════════════════════════════════

def setupLogging(targetDate):
    """初始化 logging：同時輸出到檔案（logs/import_<date>.log）和 stdout。"""
    logDir = os.path.join(baseDir, 'logs')
    if not os.path.exists(logDir):
        os.makedirs(logDir)
    logFile = os.path.join(logDir, f'import_{targetDate}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(logFile, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def getDbConnection():
    """建立並回傳 SQL Server 連線。TrustServerCertificate=yes 跳過 SSL 憑證驗證。"""
    connStr = (
        f"DRIVER={{{dbConfig['driver']}}};"
        f"SERVER={dbConfig['server']};"
        f"DATABASE={dbConfig['database']};"
        f"UID={dbConfig['user']};"
        f"PWD={dbConfig['pass']};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connStr)


# ══════════════════════════════════════════════════════════════════
# 資料來源解析
# ══════════════════════════════════════════════════════════════════

def parseJsonFile(filePath):
    """
    解析 report_generator 輸出的 JSON 檔。

    JSON 結構（相關欄位）：
      {
        "date": "YYYY-MM-DD",          ← 報告日期
        "baseline": {
          "baselineNew":     [...],    ← 新增成分股清單
          "baselineRemoved": [...],    ← 刪除成分股清單
          "baseWeights":     {...}     ← 股票名稱 → 權重 的對照表
        }
      }

    回傳：(reportDate, newList, removedList, weightsMap)
    """
    with open(filePath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    reportDate   = data.get('date')
    baseline     = data.get('baseline', {})
    newList      = baseline.get('baselineNew', [])      # 新增成分股
    removedList  = baseline.get('baselineRemoved', [])  # 刪除成分股
    weightsMap   = baseline.get('baseWeights', {})      # 補充權重對照表（部分股票放在這裡）

    return reportDate, newList, removedList, weightsMap


def parseCsvFile(filePath):
    """
    解析交易員手動提供的 showdown CSV 檔。

    CSV 欄位格式：
      etf_code, stock_code, stock_name, action
      （action 值為 'addition' 或 'deletion'）

    注意：
      - 使用 utf-8-sig 避免 Excel 存檔產生的 BOM 符號造成欄位名稱誤讀
      - CSV 不含 report_date 欄位，日期由呼叫端傳入（T-1 的 targetDate）
      - CSV 沒有權重、除息率等估算欄位，一律回傳空 dict {}

    回傳：(newList, removedList, {})
    """
    newList, removedList = [], []

    with open(filePath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 將代碼與名稱合併成 "代碼 名稱" 的格式，與 JSON 的 item['name'] 保持一致
            rawEstShares = (row.get('estimated_shares') or '').strip()
            item = {
                'name': f"{row['stock_code'].strip()} {row['stock_name'].strip()}",
                'estimated_shares': rawEstShares,
            }

            action = row['action'].strip().lower()
            if action == 'addition':
                newList.append(item)
            elif action == 'deletion':
                removedList.append(item)

    return newList, removedList, {}


# ══════════════════════════════════════════════════════════════════
# 交易日 / 生效日計算
# ══════════════════════════════════════════════════════════════════

def getEffectiveDateForEtf(etfCode, refDate):
    """
    查詢指定 ETF 在 refDate 之後最近一次的生效日（effective date）。

    【為什麼要傳 refDate 而不是直接用 today？】
      DAO 函式的內部條件是「只回傳 today < effective_date 的日期」，
      也就是說，一旦今天已經過了生效日、進入調整期，直接傳 today 會查不到任何結果。
      解法：呼叫端傳入「today - 90天」倒退作為基準，確保能查到當前週期的生效日。

    【00878 為何要特別處理？】
      其他 ETF 的 DAO 函式直接回傳 date 物件或可轉換的字串，
      但 getDayEtf00878 回傳的 EFFECTIVE_DATE 欄位是帶 HTML 標籤的字串，
      格式為 "Effective Date: <br>YYYY-MM-DD"，需要先 split 再取用。

    回傳：date 物件，若查不到則回傳 None
    """
    # 各 ETF 對應的 DAO 查詢函式
    dayFuncMap = {
        '0050':      DAO.getDayEtf005051,
        '0051':      DAO.getDayEtf005051,
        '0056':      DAO.getDayEtf0056,
        '00713':     DAO.getDayEtf00713,
        '00900':     DAO.getDayEtf00900,
        '00918':     DAO.getDayEtf00918,
        '00919_May': DAO.getDayEtf00919_May,
        '00919_Dec': DAO.getDayEtf00919_Dec,
        '00929':     DAO.getDayEtf00929,
    }
    try:
        if etfCode == '00878':
            # 00878 的 EFFECTIVE_DATE 格式特殊：'Effective Date: <br>YYYY-MM-DD'
            # 取 <br> 後半段並 strip 空白即可得到日期字串
            days = DAO.getDayEtf00878(refDate)
            if not days:
                return None
            dateStr = days[DAO.EFFECTIVE_DATE].split('<br>')[-1].strip()
            return DAO.toDate(dateStr)

        func = dayFuncMap.get(etfCode)
        if not func:
            return None  # 不認識的 ETF 代碼，略過

        days = func(refDate)
        if not days:
            return None  # DAO 查無資料（可能是 refDate 太遠未來）

        effectiveDate = days[DAO.EFFECTIVE_DATE]

        # DAO 有時回傳 date 物件，有時回傳字串，統一用 toDate 轉換
        if isinstance(effectiveDate, date):
            return effectiveDate
        return DAO.toDate(str(effectiveDate))

    except Exception as e:
        logging.warning(f"Could not get effective date for {etfCode}: {e}")
        return None


def getTradingDaysAfterDate(effectiveDate, count):
    """
    取得生效日之後的前 count 個交易日（嚴格大於 effectiveDate，不含當天）。

    做法：
      1. 從 effectiveDate 所在的年月開始，向 DAO 查詢當月所有交易日
      2. 篩選出日期 > effectiveDate 的交易日，依序加入結果
      3. 若該月交易日不足，自動往後移到下一個月繼續查詢
      4. 直到湊到 count 天為止

    回傳：list of date，長度可能小於 count（若 DAO 查詢失敗）
    """
    results = []
    year, month = effectiveDate.year, effectiveDate.month

    while len(results) < count:
        try:
            # DAO.loadDataFromDB 回傳 (交易日列表, 其他資料)，只需要交易日列表
            tradingDays, _ = DAO.loadDataFromDB(year, month, DAO.KEY_FRIDAY)
        except Exception as e:
            logging.error(f"Failed to load trading days for {year}-{month}: {e}")
            break

        for td in tradingDays:
            d = td[DAO.KEY_DAY]
            if d > effectiveDate:          # 只取嚴格大於生效日的交易日
                results.append(d)
                if len(results) >= count:  # 提早達標，不必等迴圈跑完
                    return results

        # 當月處理完，移到下一個月
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1

    return results


# ══════════════════════════════════════════════════════════════════
# 資料庫結構維護
# ══════════════════════════════════════════════════════════════════

'''
def ensureAdjustColumns(conn):
    """
    確保 TABLE_NAME 存在 adjust_begin 與 adjust_end 兩個 DATE 欄位。
    若欄位已存在則略過，可重複執行（idempotent）。
    這兩個欄位是後來新增的需求，舊資料表可能尚未有此欄位。
    """
    cursor = conn.cursor()

    # 查詢資料表現有欄位
    cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{TABLE_NAME}' AND COLUMN_NAME IN ('adjust_begin', 'adjust_end')
    """)
    existing = {row[0] for row in cursor.fetchall()}

    if 'adjust_begin' not in existing:
        cursor.execute(f"ALTER TABLE [dbo].[{TABLE_NAME}] ADD adjust_begin DATE NULL")
        logging.info("Added column: adjust_begin")

    if 'adjust_end' not in existing:
        cursor.execute(f"ALTER TABLE [dbo].[{TABLE_NAME}] ADD adjust_end DATE NULL")
        logging.info("Added column: adjust_end")

    conn.commit()
    cursor.close()
'''

# ══════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════

def importDataForDate(targetDate):
    """
    主要匯入函式，執行以下步驟：
      1. 建立 DB 連線，確保欄位存在
      2. 為每個 ETF 計算調整期間（adjust_begin ~ adjust_end）及是否啟用 CSV
      3. 掃描 reportDir 下的 tmp_<etfCode> 資料夾
      4. 依調整期判斷讀取 CSV 或 JSON
      5. 刪除舊資料後寫入新資料（先刪後插保證冪等）
    """
    today = date.today()
    logging.info(f"Starting import process for date: {targetDate}")

    # ── Step 1：建立 DB 連線 ────────────────────────────────────────
    try:
        conn = getDbConnection()
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        return


    # SQL：刪除同一 etf_code + report_date 的所有舊記錄，避免重跑時資料重複
    deleteSql = f"""
    DELETE FROM [dbo].[{TABLE_NAME}]
    WHERE etf_code = ? AND report_date = ?
    """

    # SQL：寫入一筆成分股變動記錄
    insertSql = f"""
    INSERT INTO [dbo].[{TABLE_NAME}]
        (etf_code, report_date, stock_code, stock_name, action,
         weight, dividend_rate, reason, estimated_amount, estimated_shares,
         adjust_begin, adjust_end)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # ── Step 2：預先計算各 ETF 的調整期間 ──────────────────────────
    #
    # 問題：DAO 函式只回傳「未來」的生效日（條件：today < effective_date）。
    #       如果今天已經進入調整期，直接用 today 查不到當前週期的生效日。
    # 解法：用 today - 90 天作為查詢基準（lookbackDate），
    #       90 天足以回溯到任何 ETF 當前週期的生效日。
    adjustInfo = {}
    lookbackDate = today - timedelta(days=30)

    for etfCode in targetEtfList:
        adjustDaysCount = ADJUST_DAYS.get(etfCode)

        if not adjustDaysCount:
            # 未設定調整天數的 ETF（如 0050、0051、00900）→ 不啟用調整期邏輯
            adjustInfo[etfCode] = {'begin': None, 'end': None, 'useCSV': False}
            continue

        # 查詢該 ETF 當前週期的生效日
        effectiveDate = getEffectiveDateForEtf(etfCode, lookbackDate)
        if not effectiveDate:
            adjustInfo[etfCode] = {'begin': None, 'end': None, 'useCSV': False}
            logging.warning(f"Could not determine effective date for {etfCode}")
            continue

        # 取得生效日之後的前 adjustDaysCount 個交易日，末尾即為調整期結束日
        tradingDays = getTradingDaysAfterDate(effectiveDate, adjustDaysCount)
        if len(tradingDays) < adjustDaysCount:
            # 交易日不足（例如 DAO 資料不完整），不啟用調整期
            adjustInfo[etfCode] = {'begin': None, 'end': None, 'useCSV': False}
            logging.warning(f"Insufficient trading days after {effectiveDate} for {etfCode}")
            continue

        adjustBegin = effectiveDate       # 調整期開始日 = 生效日當天
        adjustEnd   = tradingDays[-1]     # 調整期結束日 = 生效日後第 N 個交易日

        # 判斷今天是否落在調整期內
        # 條件：adjustBegin < today <= adjustEnd
        #   - 生效日當天（adjustBegin）不含：ETF 剛公告，名單可能尚未確認，先用 JSON
        #   - 結束日當天（adjustEnd）含：最後一天仍需 CSV
        useCSV = adjustBegin < today <= adjustEnd

        adjustInfo[etfCode] = {'begin': adjustBegin, 'end': adjustEnd, 'useCSV': useCSV}
        logging.info(f"{etfCode}: adjust_begin={adjustBegin}, adjust_end={adjustEnd}, useCSV={useCSV}")

    # 將各 ETF 的調整期起訖日寫入 JSON，供 app.py 判斷是否為調整期
    adjustPeriodsPath = os.path.join(baseDir, 'adjust_periods.json')
    adjustPeriodsData = {
        code: {
            'adjust_begin': info['begin'].isoformat(),
            'adjust_end':   info['end'].isoformat(),
        }
        for code, info in adjustInfo.items()
        if info['begin'] and info['end']
    }
    try:
        with open(adjustPeriodsPath, 'w', encoding='utf-8') as f:
            json.dump(adjustPeriodsData, f, ensure_ascii=False, indent=2)
        logging.info(f"Adjust periods written to {adjustPeriodsPath}")
    except Exception as e:
        logging.warning(f"Failed to write adjust periods JSON: {e}")

    # ── Step 3 & 4：掃描資料夾，讀取資料 ───────────────────────────
    if not os.path.exists(reportDir):
        logging.error(f"Report directory does not exist: {reportDir}")
        conn.close()
        return

    foundAnyFile = False  # 用來偵測「完全沒找到任何檔案」的異常情況

    for folderName in os.listdir(reportDir):
        # 只處理 tmp_<etfCode> 格式的資料夾，跳過其他目錄
        if not folderName.startswith('tmp_'):
            continue

        etfCode = folderName.replace('tmp_', '')
        if etfCode not in targetEtfList:
            continue  # 不在目標清單內的 ETF，略過

        # 取出該 ETF 的調整期資訊
        info       = adjustInfo.get(etfCode, {'begin': None, 'end': None, 'useCSV': False})
        adjustBegin = info['begin']
        adjustEnd   = info['end']
        useCSV      = info['useCSV']

        # 初始化本次讀取的資料
        reportDate, newList, removedList, weightsMap = None, [], [], {}
        usedCSV = False  # 標記實際上是否成功讀取了 CSV（False = 最終使用 JSON）

        # ── 優先嘗試 CSV（調整期間才會進入此區塊）──
        if useCSV:
            csvPath = os.path.join(showdownCsvDir, f"{etfCode}.csv")
            if os.path.exists(csvPath):
                logging.info(f"[Adjustment Period] Using CSV for {etfCode}: {csvPath}")
                try:
                    newList, removedList, weightsMap = parseCsvFile(csvPath)
                    # CSV 不含 report_date，統一使用 targetDate（T-1）
                    reportDate = targetDate
                    usedCSV = True  # 成功讀取 CSV，後續跳過 JSON
                except Exception as e:
                    logging.error(f"Error reading CSV for {etfCode}: {e}, falling back to JSON")
                    # usedCSV 保持 False → 下方會繼續嘗試 JSON
            else:
                logging.warning(f"CSV not found for {etfCode} at {csvPath}, falling back to JSON")

        # ── Fallback：讀取 JSON（未啟用 CSV 或 CSV 讀取失敗時）──
        if not usedCSV:
            fileName = f"{etfCode}_{targetDate}_prod.json"
            filePath = os.path.join(reportDir, folderName, fileName)
            if not os.path.exists(filePath):
                logging.warning(f"Expected file not found: {filePath}")
                continue  # 此 ETF 本次跳過
            logging.info(f"Found JSON file: {filePath}")
            try:
                reportDate, newList, removedList, weightsMap = parseJsonFile(filePath)
            except Exception as e:
                logging.error(f"Error reading JSON for {etfCode}: {e}")
                continue

        if not reportDate:
            logging.warning(f"No report date for {etfCode}, skipping.")
            continue

        foundAnyFile = True

        # ── Step 5：刪除舊資料，寫入新資料 ──────────────────────────
        try:
            # CSV 不含 report_date，統一使用 targetDate（T-1）作為單一日期 DELETE
            # JSON 則使用 JSON 內的 reportDate
            cursor.execute(deleteSql, (etfCode, reportDate))
            logging.info(f"Deleted existing rows for {etfCode} / {reportDate}")

            # 逐筆寫入新增（addition）與刪除（deletion）的成分股
            for action, itemList in [('addition', newList), ('deletion', removedList)]:
                for item in itemList:

                    # 清除股票名稱中的 @ / * 特殊標記
                    # 這些標記在 HTML 報表中代表「除權息警示」等附加資訊，入庫前需移除
                    cleanName = item['name'].replace('@', '').replace('*', '').strip()

                    # item['name'] 格式為「股票代碼 股票名稱」（空白分隔）
                    # 用 split(' ', 1) 只切第一個空白，避免名稱中含空白被誤切
                    nameParts = cleanName.split(' ', 1)
                    stockCode = nameParts[0]
                    stockName = nameParts[1] if len(nameParts) > 1 else ""

                    if usedCSV:
                        # CSV 為人工確認的最終名單，weight/divRate/reason/estAmount 存 NULL
                        weight, divRate, reason, estAmount = None, None, None, None
                        # estimated_shares：使用者有填則寫入，空白則 NULL
                        rawEstShares = item.get('estimated_shares', '')
                        try:
                            estShares = int(float(rawEstShares)) if rawEstShares else None
                        except (ValueError, TypeError):
                            estShares = None
                        # CSV 不含 report_date，統一使用 targetDate（T-1）
                        rowReportDate = reportDate
                    else:
                        # JSON 來源：嘗試取得各估算欄位
                        weight = item.get('weight')

                        # 部分股票的權重不在 item 本身，而是集中放在 baseWeights dict
                        # key 為股票名稱（非代碼），找不到 item.weight 時從這裡補
                        if weight is None and stockName in weightsMap:
                            weight = weightsMap[stockName]

                        divRate   = item.get('dividendRate', '')
                        reason    = json.dumps(item.get('reason', []), ensure_ascii=False)
                        estAmount = item.get('estimatedAmount')
                        estShares = item.get('estimatedShares')
                        rowReportDate = reportDate

                    # 組合 INSERT 參數（順序需與 insertSql 的 VALUES 對應）
                    params = (
                        etfCode, rowReportDate, stockCode, stockName, action,
                        weight, divRate, reason, estAmount, estShares,
                        adjustBegin, adjustEnd   # 若不在調整期，這兩個值為 None（存 NULL）
                    )
                    cursor.execute(insertSql, params)

            conn.commit()
            logging.info(f"Successfully processed {etfCode} for {targetDate}")

        except Exception as e:
            logging.error(f"Error processing {etfCode}: {e}")
            conn.rollback()  # 任何一筆失敗，整個 ETF 的寫入全部回滾，避免部分寫入

    if not foundAnyFile:
        logging.warning(f"No files found for target date: {targetDate}")

    cursor.close()
    conn.close()
    logging.info(f"Import process completed for date: {targetDate}")


# ══════════════════════════════════════════════════════════════════
# 程式入口
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETF Database Importer')
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format (Default: T-1)')
    args = parser.parse_args()

    # 未指定日期時，預設匯入昨天的資料（T-1）
    if args.date:
        targetDate = args.date
    else:
        targetDate = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    setupLogging(targetDate)
    importDataForDate(targetDate)

# # 切換至虛擬環境
# source ~/etf/venv/bin/activate
# # 模式 A: 匯入昨天資料
# python3 ~/etf/etf_calculator/etfDbImporter/etfDbImporter.py
# # 模式 B: 匯入指定日期資料
# python3 ~/etf/etf_calculator/etfDbImporter/etfDbImporter.py --date 2026-05-07

# /home/webuser/etf/venv/bin/python /home/webuser/etf/etf_calculator/etfDbImporter/etfDbImporter.py
