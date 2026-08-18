"""
ETF 成分股變動資料匯入工具
=====================================
讀取 report_generator 輸出的 JSON（或交易員提供的 showdown CSV），
將「新增/刪除成分股」資料寫入 SQL Server 的 etf_additions_history_dev 資料表。

【資料來源選擇邏輯】
  ┌──────────────────────────────────────────────────────────────────────┐
  │ targetDate 非交易日 → 直接略過                                         │
  ├──────────────────────────────────────────────────────────────────────┤
  │ 非調整期（targetDate < adjustBegin 或 adjustEnd < targetDate）          │
  │   → 刪除 showdown_csv 中的 {etfCode}.csv（若存在，清理用）               │
  │   → 讀取 JSON，不扣 DIFSHARES                                          │
  ├──────────────────────────────────────────────────────────────────────┤
  │ 調整期間（adjustBegin <= targetDate <= adjustEnd）                      │
  │   → 先查 DB90 [ETF成分每日增減明細] 取得當日 DIFSHARES（table90）         │
  │                                                                      │
  │   Case 1：showdown_csv/{etfCode}.csv 存在                             │
  │     → CSV estimated_shares - DIFSHARES → 寫 DB → 刪 CSV               │
  │                                                                      │
  │   Case 2：無 CSV，且 upload_times.json 顯示本期內有上傳過                │
  │           （effectiveDate <= upload_time <= adjustEnd）                │
  │     → 取前一交易日的 DB estimated_shares - DIFSHARES → 寫 DB           │
  │     → 若前一交易日無 DB 資料，fallback 到 JSON                          │
  │                                                                      │
  │   Case 3：其餘情況（無 CSV、無有效 upload 紀錄）                         │
  │     → 讀 JSON，不扣 DIFSHARES                                          │
  └──────────────────────────────────────────────────────────────────────┘

  特殊規則：
  - 成分股在 CSV/前日 DB 有紀錄、但 table90 無此股票 → DIFSHARES 視為 0
  - table90 出現 CSV/前日 DB 完全沒有的新股票 → 不填（DB 存 NULL）
  - JSON 來源不扣 DIFSHARES（保留 JSON 本身的估算欄位）
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
import DAO_dev as DAO

# ══════════════════════════════════════════════════════════════════
# 設定區：資料庫、路徑、ETF 清單
# ══════════════════════════════════════════════════════════════════

DB_90_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': '172.16.8.90',
    'database': 'TEJ',
    'user': 'danny',
    'pass': 'danny890'
}

DB_91_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': '172.16.8.91',
    'database': 'ETF_機率',
    'user': 'lethe',
    'pass': 'lethe891'
}

# report_generator 的輸出目錄，內含 tmp_<etfCode>/ 子資料夾
reportDir = '/home/webuser/etf/etf_calculator/report_generator'

# 本腳本自身的根目錄，用來放 log 檔
baseDir = '/home/webuser/etf/etf_calculator/etfDbImporter/dev'

# 交易員手動上傳的 showdown CSV 所在目錄，檔名格式：<etfCode>.csv
showdownCsvDir = '/home/webuser/etf/showdown_csv/dev'

# CSV 上傳時間紀錄檔（由前端寫入），用來判斷本調整期是否曾上傳過 CSV
UPLOAD_TIMES_PATH = '/var/www/html/web/etf/home/dev/upload_times.json'

# 目標資料表名稱
ETF_ADDITIONS_HISTORY_TABLE = 'etf_additions_history_dev'
DAILY_ETF_PORTFOLIO_ADJUSTMENTS = 'ETF成分每日增減明細'

# 需要匯入的 ETF 代碼清單
targetEtfList = ['0050', '0051', '0056', '00713', '00878', '00900', '00918', '00919_May', '00919_Dec', '00929']

# 「調整期間」設定：僅列在此的 ETF 才有調整期邏輯
# Key   = ETF 代碼
# Value = 從生效日起算，往後數幾個「交易日」為調整期結束日
# 沒有列在此 dict 的 ETF（如 0050、0051、00900）：
#   → 永遠使用 JSON，adjust_begin / adjust_end 存 NULL
ADJUST_DAYS = {
    '0050':      1,
    '0051':      1,
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


def getDbConnection(dbConfig):
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
        "date": "YYYY-MM-DD",
        "baseline": {
          "baselineNew":     [...],
          "baselineRemoved": [...],
          "baseWeights":     {...}
        }
      }

    回傳：(reportDate, newList, removedList, weightsMap)
    """
    with open(filePath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    reportDate   = data.get('date')
    baseline     = data.get('baseline', {})
    newList      = baseline.get('baselineNew', [])
    removedList  = baseline.get('baselineRemoved', [])
    weightsMap   = baseline.get('baseWeights', {})

    return reportDate, newList, removedList, weightsMap


def parseCsvFile(filePath):
    """
    解析交易員手動提供的 showdown CSV 檔。

    CSV 欄位格式：
      etf_code, stock_code, stock_name, action, estimated_shares
      （action 值為 'addition' 或 'deletion'）

    回傳：(newList, removedList, {})
    """
    newList, removedList = [], []

    with open(filePath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rawEstShares = (row.get('estimated_shares') or '').strip()
            item = {
                'stock_code':       row['stock_code'].strip(),
                'stock_name':       row['stock_name'].strip(),
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

    【為何傳入 refDate 而非 today？】
      DAO 的條件是「只回傳 refDate < effective_date 的日期」。
      若今天已進入調整期，直接傳 today 會查不到結果。
      解法：呼叫端傳入 targetDate - 30 天作為 refDate，確保查得到當前週期。

    【00878 為何特別處理？】
      其 EFFECTIVE_DATE 格式為 "Effective Date: <br>YYYY-MM-DD"，
      需先 split 取後半段才能得到純日期字串。

    回傳：date 物件，若查不到則回傳 None
    """
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
            days = DAO.getDayEtf00878(refDate)
            if not days:
                return None
            # 格式："Effective Date: <br>YYYY-MM-DD"，取 <br> 後段
            dateStr = days[DAO.EFFECTIVE_DATE].split('<br>')[-1].strip()
            return DAO.toDate(dateStr)

        func = dayFuncMap.get(etfCode)
        if not func:
            return None  # 不認識的 ETF 代碼，略過

        days = func(refDate)
        if not days:
            return None  # DAO 查無資料

        effectiveDate = days[DAO.EFFECTIVE_DATE]

        # DAO 有時回傳 date 物件，有時回傳字串，統一轉換
        if isinstance(effectiveDate, date):
            return effectiveDate
        return DAO.toDate(str(effectiveDate))

    except Exception as e:
        logging.warning(f"Could not get effective date for {etfCode}: {e}")
        return None


def getTradingDaysAfterDate(etfCode, effectiveDate, count):
    """
    取得從 effectiveDate 起（含當天）的前 count 個交易日。

    做法：從 effectiveDate 所在月份開始逐月查詢，直到累積 count 個交易日。
    回傳：list of date，長度可能小於 count（若 DAO 查詢失敗）
    """
    results = []
    year, month = effectiveDate.year, effectiveDate.month

    while len(results) < count:
        try:
            tradingDays, _ = DAO.loadDataFromDB(year, month, DAO.KEY_FRIDAY)
        except Exception as e:
            logging.error(f"Failed to load trading days for {year}-{month}: {e}")
            break

        if etfCode in ['0050', '0051', '0056']:
            for td in tradingDays:
                d = td[DAO.KEY_DAY]
                if d > effectiveDate:
                    results.append(d)
                    if len(results) >= count:
                        return results
        else:
            for td in tradingDays:
                d = td[DAO.KEY_DAY]
                if d >= effectiveDate:
                    results.append(d)
                    if len(results) >= count:
                        return results

        # 當月處理完，移至下一個月
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1

    return results


def isTradingDay(targetDateObj):
    """使用 DAO 確認 targetDateObj 是否為交易日。"""
    year, month = targetDateObj.year, targetDateObj.month
    try:
        tradingDays, _ = DAO.loadDataFromDB(year, month, DAO.KEY_FRIDAY)
        return targetDateObj in {td[DAO.KEY_DAY] for td in tradingDays}
    except Exception as e:
        logging.error(f"isTradingDay check failed for {targetDateObj}: {e}")
        return False


def getPreviousTradingDay(targetDateObj):
    """
    回傳 targetDateObj 前一個交易日，找不到則回傳 None。
    先查當月，若 targetDateObj 為當月首個交易日則往前一個月繼續查。
    """
    year, month = targetDateObj.year, targetDateObj.month
    for _ in range(2):  # 最多往前查兩個月
        try:
            tradingDays, _ = DAO.loadDataFromDB(year, month, DAO.KEY_FRIDAY)
            # 取所有嚴格早於 targetDateObj 的交易日，取最新的一天
            candidates = sorted(
                [td[DAO.KEY_DAY] for td in tradingDays if td[DAO.KEY_DAY] < targetDateObj],
                reverse=True
            )
            if candidates:
                return candidates[0]
        except Exception as e:
            logging.error(f"getPreviousTradingDay failed for {year}-{month}: {e}")
            return None
        # 當月無結果，往前一個月
        if month == 1:
            year, month = year - 1, 12
        else:
            month -= 1
    return None


# ══════════════════════════════════════════════════════════════════
# DB90 查詢
# ══════════════════════════════════════════════════════════════════

def queryTable90(conn90, targetDateStr, fundId):
    """
    查詢 DB90 [TEJ].[dbo].[ETF成分每日增減明細]，以 targetDate 和 FUNDID 篩選。

    注意事項：
    - DATADATE 格式為 'YYYYMMDD'（無連字符），需先轉換
    - SYMBOL 格式為 '1210.TSE'，取 '.' 前段作為 stock_code 與系統對應
    - FUNDID 為 etfCode 去掉 '_' 後綴（例：'00919_May' → '00919'）

    回傳：dict { stock_code: (DIFSHARES / 1000) }
    """
    dateStr = targetDateStr.replace('-', '')  # '2026-06-08' → '20260608'
    result  = {}
    cursor  = conn90.cursor()
    try:
        cursor.execute(f"""
            SELECT SYMBOL, DIFSHARES
            FROM [TEJ].[dbo].[{DAILY_ETF_PORTFOLIO_ADJUSTMENTS}]
            WHERE DATADATE = ? AND FUNDID = ?
        """, (dateStr, fundId))
        for row in cursor.fetchall():
            # 取 SYMBOL '.' 前段作為純股票代碼（去除交易所後綴）
            stockCode = (row[0] or '').split('.')[0]
            result[stockCode] = float(row[1]) / 1000 if row[1] is not None else 0.0
    except Exception as e:
        logging.error(f"queryTable90 failed ({fundId}, {dateStr}): {e}")
    finally:
        cursor.close()
    return result


# ══════════════════════════════════════════════════════════════════
# upload_times.json 讀取
# ══════════════════════════════════════════════════════════════════

def getUploadDateForEtf(etfCode):
    """
    從 UPLOAD_TIMES_PATH 讀取 etfCode 的最後 CSV 上傳時間，以 date 物件回傳。
    檔案不存在或無對應 key 時回傳 None。

    JSON 格式範例：
      { "today": "2026-06-12", "0056": "2026-06-10 16:48:01", ... }
    """
    try:
        with open(UPLOAD_TIMES_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        timeStr = data.get(etfCode)
        if timeStr:
            return datetime.strptime(timeStr, '%Y-%m-%d %H:%M:%S').date()
    except Exception as e:
        logging.warning(f"Failed to read upload_times.json: {e}")
    return None


def getTodayFromUploadTimes():
    """
    從 UPLOAD_TIMES_PATH 讀取 today 欄位，以 date 物件回傳。
    檔案不存在或無 today key 時回傳 None。
    """
    try:
        with open(UPLOAD_TIMES_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        todayStr = data.get('today')
        if not todayStr:
            return None
        for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(todayStr, fmt).date()
            except ValueError:
                continue
    except Exception as e:
        print(f"Failed to read today from upload_times.json: {e}")
    return None


# ══════════════════════════════════════════════════════════════════
# DB91 補助查詢
# ══════════════════════════════════════════════════════════════════

def fetchPrevDayRecords(cursor, etfCode, prevDay):
    """
    從 ETF_ADDITIONS_HISTORY_TABLE 取得 etfCode × prevDay 的全部記錄。
    回傳：list of dict { stock_code, stock_name, action, estimated_shares }
    """
    sql = f"""
    SELECT stock_code, stock_name, action, estimated_shares
    FROM [dbo].[{ETF_ADDITIONS_HISTORY_TABLE}]
    WHERE etf_code = ? AND report_date = ?
    """
    try:
        cursor.execute(sql, (etfCode, prevDay))
        return [
            {
                'stock_code':       row[0],
                'stock_name':       row[1],
                'action':           row[2],
                'estimated_shares': float(row[3]) if row[3] is not None else None,
            }
            for row in cursor.fetchall()
        ]
    except Exception as e:
        logging.error(f"fetchPrevDayRecords failed for {etfCode} on {prevDay}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# 資料列建構（調整期 DIFSHARES 扣減）
# ══════════════════════════════════════════════════════════════════

def _parseStockNameParts(rawName):
    """
    rawName 格式為 '@1210 大成*'。
    移除 @ / * 後以第一個空白切割，回傳 (stockCode, stockName)。
    @ 和 * 是 HTML 報表中的除權息警示標記，入庫前需移除。
    """
    cleanName = rawName.replace('@', '').replace('*', '').strip()
    parts     = cleanName.split(' ', 1)
    stockCode = parts[0]
    stockName = parts[1] if len(parts) > 1 else ''
    return stockCode, stockName


def buildAdjustRows(etfCode, targetDate, newList, removedList, table90, adjustBegin, adjustEnd):
    """
    Case 1（CSV 來源）用。
    對 CSV 中每檔成分股計算 estimated_shares - DIFSHARES，
    回傳 INSERT 用的 tuple 清單。
    weight / divRate / reason / estAmount 一律存 NULL（CSV 無此欄位）。
    """
    rows = []
    for action, itemList in [('addition', newList), ('deletion', removedList)]:
        for item in itemList:
            # CSV 來源：stock_code / stock_name 已是獨立欄位，不需要拆分
            stockCode = item['stock_code']
            stockName = item['stock_name']

            rawEstShares = item.get('estimated_shares', '')
            try:
                estShares = float(rawEstShares) if rawEstShares else None
            except (ValueError, TypeError):
                estShares = None

            # 扣減當日實際成交量；若 table90 無此股則 DIFSHARES 視為 0
            if estShares is not None:
                estShares = estShares - table90.get(stockCode, 0)

            rows.append((
                etfCode, targetDate, stockCode, stockName, action,
                None, None, None, None, estShares,
                adjustBegin, adjustEnd
            ))
    return rows


def buildAdjustRowsFromPrevDb(etfCode, targetDate, prevRecords, table90, adjustBegin, adjustEnd):
    """
    Case 2b（前一交易日 DB 來源）用。
    對前日每筆記錄計算 estimated_shares - DIFSHARES，
    回傳 INSERT 用的 tuple 清單。
    weight / divRate / reason / estAmount 一律存 NULL。
    """
    rows = []
    for record in prevRecords:
        stockCode  = record['stock_code']
        stockName  = record['stock_name']
        action     = record['action']
        estShares  = record['estimated_shares']

        # 扣減當日實際成交量；若 table90 無此股則 DIFSHARES 視為 0
        if estShares is not None:
            estShares = estShares - table90.get(stockCode, 0)

        rows.append((
            etfCode, targetDate, stockCode, stockName, action,
            None, None, None, None, estShares,
            adjustBegin, adjustEnd
        ))
    return rows


# ══════════════════════════════════════════════════════════════════
# DB 寫入
# ══════════════════════════════════════════════════════════════════

def executeInserts(cursor, conn, deleteSql, insertSql, etfCode, targetDate, rows):
    """刪除 etfCode × targetDate 的舊記錄後，批次 INSERT rows（冪等操作）。"""
    try:
        cursor.execute(deleteSql, (etfCode, targetDate))
        for row in rows:
            cursor.execute(insertSql, row)
        conn.commit()
        logging.info(f"Inserted {len(rows)} rows for {etfCode} / {targetDate}")
    except Exception as e:
        logging.error(f"Error inserting records for {etfCode}: {e}")
        conn.rollback()


def writeJsonRecords(cursor, conn, deleteSql, insertSql,
                     etfCode, reportDate, newList, removedList, weightsMap,
                     adjustBegin, adjustEnd):
    """
    JSON 來源的原本邏輯，不扣 DIFSHARES。
    weight / divRate / reason / estAmount 從 JSON 欄位取得。
    """
    try:
        cursor.execute(deleteSql, (etfCode, reportDate))

        for action, itemList in [('addition', newList), ('deletion', removedList)]:
            for item in itemList:
                stockCode, stockName = _parseStockNameParts(item['name'])

                weight = item.get('weight')
                # 部分股票的權重不在 item 本身，而集中放在 baseWeights dict
                if weight is None and stockName in weightsMap:
                    weight = weightsMap[stockName]

                divRate   = item.get('dividendRate', '')
                reason    = json.dumps(item.get('reason', []), ensure_ascii=False)
                estAmount = item.get('estimatedAmount')
                estShares = item.get('estimatedShares')

                cursor.execute(insertSql, (
                    etfCode, reportDate, stockCode, stockName, action,
                    weight, divRate, reason, estAmount, estShares,
                    adjustBegin, adjustEnd
                ))

        conn.commit()
        logging.info(f"Inserted JSON records for {etfCode} / {reportDate}")
    except Exception as e:
        logging.error(f"Error inserting JSON records for {etfCode}: {e}")
        conn.rollback()


# ══════════════════════════════════════════════════════════════════
# JSON 檔案路徑解析
# ══════════════════════════════════════════════════════════════════

def loadJsonForEtf(etfCode, folderName, targetDate):
    """
    讀取 tmp_<etfCode>/<etfCode>_<targetDate>_prod.json。
    成功時回傳 (reportDate, newList, removedList, weightsMap)，失敗時回傳 None。
    """
    fileName = f"{etfCode}_{targetDate}_prod.json"
    filePath = os.path.join(reportDir, folderName, fileName)
    if not os.path.exists(filePath):
        logging.warning(f"Expected JSON not found: {filePath}")
        return None
    logging.info(f"Found JSON file: {filePath}")
    try:
        return parseJsonFile(filePath)
    except Exception as e:
        logging.error(f"Error reading JSON for {etfCode}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════

def importDataForDate(targetDate):
    """
    主要匯入函式。targetDate 非交易日則直接 return。
    流程：非交易日略過 → 建立 DB 連線 → 計算調整期 → 逐 ETF 判斷資料來源 → 寫入 DB。
    """
    targetDateObj = datetime.strptime(targetDate, '%Y-%m-%d').date()
    logging.info(f"Starting import for date: {targetDate}")

    # ── Step 1：非交易日直接略過 ────────────────────────────────────
    if not isTradingDay(targetDateObj):
        logging.info(f"{targetDate} is not a trading day, skipping.")
        return

    # ── Step 2：建立 DB 連線 ────────────────────────────────────────
    try:
        conn91   = getDbConnection(DB_91_CONFIG)
        cursor91 = conn91.cursor()
    except Exception as e:
        logging.error(f"Failed to connect to DB91: {e}")
        return

    try:
        conn90 = getDbConnection(DB_90_CONFIG)
    except Exception as e:
        logging.error(f"Failed to connect to DB90: {e}")
        cursor91.close()
        conn91.close()
        return

    # 先刪後插保證同一 etf_code + report_date 的資料冪等
    deleteSql = f"""
    DELETE FROM [dbo].[{ETF_ADDITIONS_HISTORY_TABLE}]
    WHERE etf_code = ? AND report_date = ?
    """

    insertSql = f"""
    INSERT INTO [dbo].[{ETF_ADDITIONS_HISTORY_TABLE}]
        (etf_code, report_date, stock_code, stock_name, action,
         weight, dividend_rate, reason, estimated_amount, estimated_shares,
         adjust_begin, adjust_end)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # ── Step 3：計算各 ETF 調整期（基準日 = targetDateObj）────────────
    #
    # lookbackDate 往前 30 天：確保即使 targetDate 已進入調整期，
    # DAO 仍能查到當前週期的生效日
    adjustInfo   = {}
    lookbackDate = targetDateObj - timedelta(days=30)

    for etfCode in targetEtfList:
        adjustDaysCount = ADJUST_DAYS.get(etfCode)

        if not adjustDaysCount:
            # 未設定調整天數的 ETF（如 00900）→ 不啟用調整期邏輯
            adjustInfo[etfCode] = {'effectiveDate': None, 'begin': None, 'end': None, 'inAdjust': False}
            continue

        effectiveDate = getEffectiveDateForEtf(etfCode, lookbackDate)
        #print(etfCode, effectiveDate)
        if not effectiveDate:
            adjustInfo[etfCode] = {'effectiveDate': None, 'begin': None, 'end': None, 'inAdjust': False}
            logging.warning(f"Could not determine effective date for {etfCode}")
            continue

        tradingDays = getTradingDaysAfterDate(etfCode, effectiveDate, adjustDaysCount)
        if len(tradingDays) < adjustDaysCount:
            # 交易日資料不足（DAO 資料不完整），不啟用調整期
            adjustInfo[etfCode] = {'effectiveDate': None, 'begin': None, 'end': None, 'inAdjust': False}
            logging.warning(f"Insufficient trading days after {effectiveDate} for {etfCode}")
            continue

        # adjustBegin = 生效日後第一個交易日（非生效日本身）
        # 生效日當天不進調整期邏輯，僅用 JSON；CSV 可在 effectiveDate ~ adjustEnd 期間上傳
        adjustBegin = tradingDays[0]
        adjustEnd   = tradingDays[-1]
        inAdjust    = adjustBegin <= targetDateObj <= adjustEnd

        adjustInfo[etfCode] = {
            'effectiveDate': effectiveDate,
            'begin':         adjustBegin,
            'end':           adjustEnd,
            'inAdjust':      inAdjust,
        }
        logging.info(
            f"{etfCode}: effective_date={effectiveDate}, adjust_begin={adjustBegin}, "
            f"adjust_end={adjustEnd}, inAdjust={inAdjust}"
        )

    # 更新 adjust_periods.json（供 app.py 等外部服務讀取）
    adjustPeriodsPath = os.path.join(baseDir, 'adjust_periods.json')
    adjustPeriodsData = {
        code: {
            'effective_date': info['effectiveDate'].isoformat(),
            'adjust_begin':   info['begin'].isoformat(),
            'adjust_end':     info['end'].isoformat(),
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

    # ── Step 4：逐一處理各 ETF 資料夾 ───────────────────────────────
    if not os.path.exists(reportDir):
        logging.error(f"Report directory does not exist: {reportDir}")
        conn90.close()
        cursor91.close()
        conn91.close()
        return

    foundAnyFile = False  # 偵測「完全沒找到任何檔案」的異常情況

    for folderName in os.listdir(reportDir):
        # 只處理 tmp_<etfCode> 格式的資料夾
        if not folderName.startswith('tmp_'):
            continue

        etfCode = folderName.replace('tmp_', '')
        if etfCode not in targetEtfList:
            continue

        info          = adjustInfo.get(etfCode, {'effectiveDate': None, 'begin': None, 'end': None, 'inAdjust': False})
        etfEffDate    = info['effectiveDate']  # 生效日（CSV 上傳窗口起點）
        adjustBegin   = info['begin']           # 調整期首日（生效日後第一個交易日）
        adjustEnd     = info['end']
        inAdjust      = info['inAdjust']
        csvPath       = os.path.join(showdownCsvDir, f"{etfCode}.csv")

        # ── 非調整期 ────────────────────────────────────────────────
        if not inAdjust:
            # effectiveDate 當天：保留 CSV（供 adjustBegin 使用），直接走 JSON
            # 上傳窗口（effectiveDate ~ adjustEnd）之外才清理 stale CSV
            isInUploadWindow = (etfEffDate is not None and
                                etfEffDate <= targetDateObj <= adjustEnd)
            if os.path.exists(csvPath) and not isInUploadWindow:
                try:
                    os.remove(csvPath)
                    logging.info(f"Deleted stale CSV for {etfCode} (outside upload window)")
                except Exception as e:
                    logging.warning(f"Failed to delete stale CSV for {etfCode}: {e}")

            # 非調整期一律走 JSON，不扣 DIFSHARES
            result = loadJsonForEtf(etfCode, folderName, targetDate)
            if result is None:
                continue
            reportDate, newList, removedList, weightsMap = result
            if not reportDate:
                logging.warning(f"No report date in JSON for {etfCode}, skipping.")
                continue

            foundAnyFile = True
            writeJsonRecords(cursor91, conn91, deleteSql, insertSql,
                             etfCode, reportDate, newList, removedList, weightsMap,
                             adjustBegin, adjustEnd)
            continue

        # ── 調整期內 ────────────────────────────────────────────────
        # 不論走哪個 Case，都需要 table90 的當日實際成交量
        fundId  = etfCode.split('_')[0]  # '00919_May' → '00919'
        table90 = queryTable90(conn90, targetDate, fundId)
        logging.info(f"{etfCode}: table90 has {len(table90)} entries for {targetDate}")

        if os.path.exists(csvPath):
            # ── Case 1：有 CSV → estimated - DIFSHARES ──────────
            logging.info(f"[AdjPeriod] Case1 - Using CSV for {etfCode}")
            try:
                newList, removedList, _ = parseCsvFile(csvPath)
            except Exception as e:
                logging.error(f"Error reading CSV for {etfCode}: {e}")
                # CSV 讀取失敗仍刪除，避免損壞的檔案反覆觸發錯誤
                try:
                    os.remove(csvPath)
                except Exception:
                    pass
                continue

            rows = buildAdjustRows(etfCode, targetDate, newList, removedList,
                                   table90, adjustBegin, adjustEnd)
            foundAnyFile = True
            executeInserts(cursor91, conn91, deleteSql, insertSql,
                           etfCode, targetDate, rows)

            # CSV 用完即刪，下一個交易日自動進 Case 2b 流程
            try:
                os.remove(csvPath)
                logging.info(f"Deleted used CSV for {etfCode}")
            except Exception as e:
                logging.warning(f"Failed to delete CSV for {etfCode}: {e}")

        else:
            # ── 無 CSV：依情況決定資料來源 ──────────────────────────
            useJson = False

            # 確認 upload_times.json 是否在上傳窗口（effectiveDate ~ adjustEnd）內曾上傳過 CSV
            # 有上傳紀錄 → DB 有有效資料 → 可用前日 estimated 繼續扣減
            uploadDate     = getUploadDateForEtf(etfCode)
            hasValidUpload = (uploadDate is not None and
                              etfEffDate is not None and
                              etfEffDate <= uploadDate <= adjustEnd)

            if hasValidUpload:
                # ── Case 2：前一交易日 DB estimated - DIFSHARES ──
                prevDay     = getPreviousTradingDay(targetDateObj)
                prevRecords = fetchPrevDayRecords(cursor91, etfCode, prevDay) if prevDay else []

                if prevRecords:
                    logging.info(f"[AdjPeriod] Case2 - prev DB ({prevDay}) for {etfCode}")
                    rows = buildAdjustRowsFromPrevDb(
                        etfCode, targetDate, prevRecords, table90, adjustBegin, adjustEnd
                    )
                    foundAnyFile = True
                    executeInserts(cursor91, conn91, deleteSql, insertSql,
                                   etfCode, targetDate, rows)
                else:
                    # 前日 DB 無資料（例如那天腳本未執行）→ fallback JSON
                    logging.warning(
                        f"[AdjPeriod] No prev DB records for {etfCode} ({prevDay}), fallback to JSON"
                    )
                    useJson = True
            else:
                # ── Case 3：無有效 upload 紀錄 → 走 JSON ──────────────
                logging.info(f"[AdjPeriod] Case3 - no valid upload record, using JSON for {etfCode}")
                useJson = True

            if useJson:
                result = loadJsonForEtf(etfCode, folderName, targetDate)
                if result is None:
                    continue
                reportDate, newList, removedList, weightsMap = result
                if not reportDate:
                    logging.warning(f"No report date in JSON for {etfCode}, skipping.")
                    continue
                foundAnyFile = True
                writeJsonRecords(cursor91, conn91, deleteSql, insertSql,
                                 etfCode, reportDate, newList, removedList, weightsMap,
                                 adjustBegin, adjustEnd)

    if not foundAnyFile:
        logging.warning(f"No files found for target date: {targetDate}")

    conn90.close()
    cursor91.close()
    conn91.close()
    logging.info(f"Import process completed for date: {targetDate}")


# ══════════════════════════════════════════════════════════════════
# 程式入口
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETF Database Importer')
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format (Default: T-1)')
    parser.add_argument('--from-upload-times', action='store_true',
                        help='Derive target date (T-1) from today field in upload_times.json')
    args = parser.parse_args()

    if args.date:
        targetDate = args.date
    elif args.from_upload_times:
        uploadToday = getTodayFromUploadTimes()
        if uploadToday is None:
            print("Error: could not read 'today' from upload_times.json")
            sys.exit(1)
        targetDate = (uploadToday - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # 未指定日期時，預設匯入昨天的資料（T-1）
        targetDate = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(targetDate)

    setupLogging(targetDate)
    importDataForDate(targetDate)

# # 切換至虛擬環境
# source ~/etf/venv/bin/activate
# # 模式 A: 匯入昨天資料
# python3 ~/etf/etf_calculator/etfDbImporter/dev/etfDbImporter.py
# # 模式 B: 匯入指定日期資料
# python3 ~/etf/etf_calculator/etfDbImporter/dev/etfDbImporter.py --date 2026-05-07

# /home/webuser/etf/venv/bin/python /home/webuser/etf/etf_calculator/etfDbImporter/dev/etfDbImporter.py
