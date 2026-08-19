"""
ETF 成分股變動資料匯入工具
=====================================
讀取 report_generator 輸出的 JSON（或 sitcRebalanceTracker 產出的反單策略 CSV），
將「新增/刪除成分股」資料寫入 SQL Server 的 etf_additions_history_dev 資料表。

【資料來源選擇邏輯】
  ┌──────────────────────────────────────────────────────────────────────┐
  │ targetDate 非交易日 → 直接略過                                         │
  ├──────────────────────────────────────────────────────────────────────┤
  │ CASE 1：調整期（adjustBegin <= targetDate <= adjustEnd）                │
  │   → 讀 sitcRebalanceTracker/dev/output_csv/{etfCode}_{targetDate}.csv │
  │     （estimated_shares 為剩餘待調整量，tracker 已扣完 DIFSHARES，       │
  │       importer 不再自行查減；weight/divRate/reason/estAmount 存 NULL） │
  │   → 檔案不存在 → 同步呼叫 tracker --etf --date 產生一次                 │
  │   → 仍不存在（非調整期/無 showdown CSV/執行失敗）→ log error，不寫 DB   │
  ├──────────────────────────────────────────────────────────────────────┤
  │ CASE 2：暫停期（deadline < targetDate < adjustBegin）                   │
  │   → 讀取 {etfCode}_{deadline}_prod.json（整段暫停期沿用 deadline 結果） │
  │   → buildJsonRows(...)，report_date 填 targetDate，dao.upsertRows()    │
  ├──────────────────────────────────────────────────────────────────────┤
  │ ELSE：正常期（其餘情況）                                                │
  │   → Stale CSV 清理（upload window adjust_effective ~ adjustEnd 外才刪） │
  │   → 讀取 {etfCode}_{targetDate}_prod.json，不扣 DIFSHARES              │
  └──────────────────────────────────────────────────────────────────────┘

  特殊規則：
  - tracker CSV 的 estimated_shares 空值或非數字 → DB 存 NULL
  - JSON 來源不扣 DIFSHARES（保留 JSON 本身的估算欄位）
  - importer 只負責填 report_date / adjust_begin / adjust_end，
    調整期的數值計算一律以 tracker 產出為準
"""
import os
import re
import json
import csv
import sys
import argparse
import logging
import subprocess
from datetime import datetime, timedelta, date

# DAO 模組放在非標準路徑，手動加入 sys.path 才能 import
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAO_SRC_DIR = os.path.join(BASE_DIR, 'src')
if DAO_SRC_DIR not in sys.path:
    sys.path.insert(0, DAO_SRC_DIR)
import DAO as DAO

# ══════════════════════════════════════════════════════════════════
# 設定區：路徑、ETF 清單
# ══════════════════════════════════════════════════════════════════

# report_generator 的輸出目錄，內含 tmp_<etfCode>/ 子資料夾
reportDir = '/home/webuser/etf/etf_calculator/report_generator'

# 交易員手動上傳的 showdown CSV 所在目錄，檔名格式：<etfCode>.csv
showdownCsvDir = '/home/webuser/etf/showdown_csv/dev'

# sitcRebalanceTracker 產出的反單策略 CSV（調整期的資料來源）
TRACKER_OUTPUT_DIR = '/home/webuser/etf/etf_calculator/sitcRebalanceTracker/dev/output_csv'
TRACKER_PY         = '/home/webuser/etf/etf_calculator/sitcRebalanceTracker/dev/tracker.py'
ETF_VENV_PY        = '/home/webuser/etf/venv/bin/python'

# CSV 上傳時間紀錄檔（由前端寫入），用來判斷本調整期是否曾上傳過 CSV
UPLOAD_TIMES_PATH = '/var/www/html/web/etf/home/dev/upload_times.json'

# DAO_dev 產生的 ETF 日期資料（含調整期），由此讀取取代原本即時計算
ETF_DATES_PATH = '/home/webuser/etf/etf_calculator/report_generator/dateData/dev/etf_dates.json'

# 需要匯入的 ETF 代碼清單
targetEtfList = ['0050', '0051', '0056', '00713', '00878', '00900', '00918', '00919_May', '00919_Dec', '00929']
#targetEtfList = ['00929']

# tracker CSV 額外欄位 → DB 欄位對照（順序 = insertEtfHistory 後段 15 欄的順序）
TRACKER_EXTRA_COLS = [
    '總調整量', '累計調整張', '累計調整進度(%)', '調整利差(%)',
    '換手量', '調整倍數',
    '外資當日買賣超', '外資當日買賣超占比(%)', '外資累計買賣超', '外資累計買賣超占比(%)',
    '當日借券餘額', '當日借賣餘額', '當日借券賣出', '當日借賣可用額度', '借賣日內占比(%)',
]


# ══════════════════════════════════════════════════════════════════
# 初始化
# ══════════════════════════════════════════════════════════════════

def setupLogging(targetDate):
    """初始化 logging：同時輸出到檔案（logs/import_<date>.log）和 stdout。"""
    logDir = os.path.join(BASE_DIR, 'logs')
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
    newList, removedList, weightList = [], [], []

    with open(filePath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rawEstShares = (row.get('estimated_shares') or '').strip()
            item = {
                'stock_code':       row['stock_code'].strip(),
                'stock_name':       (row.get('stock_name') or '').strip(),
                'estimated_shares': rawEstShares,
            }

            action = row['action'].strip().lower()
            if action == 'addition':
                newList.append(item)
            elif action == 'deletion':
                removedList.append(item)
            elif action == 'weight':
                weightList.append(item)

    return newList, removedList, weightList


def ensureTrackerCsv(etfCode, targetDate):
    """回傳 tracker output CSV 路徑；不存在時同步呼叫 tracker 產生一次。

    產生後仍不存在（非調整期 / 無 showdown CSV / 執行失敗）回傳 None。
    """
    # tracker 輸出在 output_csv/{etfCode}/ 子資料夾（固定檔名版，時間戳檔為歷史備份）
    csvPath = os.path.join(TRACKER_OUTPUT_DIR, etfCode, f'{etfCode}_{targetDate}.csv')
    if os.path.exists(csvPath):
        return csvPath

    logging.info(f'{etfCode} {targetDate} 無 tracker CSV, 呼叫 tracker 產生')
    try:
        result = subprocess.run(
            [ETF_VENV_PY, TRACKER_PY, '--etf', etfCode, '--date', targetDate],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            logging.error(f'{etfCode} {targetDate} tracker 執行失敗: {(result.stderr or "")[-500:]}')
    except subprocess.TimeoutExpired:
        logging.error(f'{etfCode} {targetDate} tracker 執行超時 (>300s)')
        return None
    except Exception as e:
        logging.exception(f'{etfCode} {targetDate} tracker 呼叫失敗: {e}')
        return None

    return csvPath if os.path.exists(csvPath) else None


def _toFloat(raw):
    """字串轉 float；空值或非數字回傳 None（DB 存 NULL）。"""
    raw = (raw or '').strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parseTrackerCsv(filePath):
    """讀 tracker output CSV -> [(stock_code, stock_name, action, estimated_shares, extras), ...]。

    estimated_shares 為剩餘待調整量, tracker 已扣完 DIFSHARES, 這裡直接沿用;
    extras 為 TRACKER_EXTRA_COLS 順序的 15 個 float(缺值 None)。
    """
    rows = []
    with open(filePath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = (row.get('estimated_shares') or '').strip()
            try:
                estShares = int(float(raw)) if raw else None
            except ValueError:
                estShares = None
            extras = tuple(_toFloat(row.get(col)) for col in TRACKER_EXTRA_COLS)
            rows.append((
                (row.get('stock_code') or '').strip(),
                (row.get('stock_name') or '').strip(),
                (row.get('action') or '').strip(),
                estShares,
                extras,
            ))
    return rows


def buildTrackerRows(etfCode, targetDate, trackerRows, adjustBegin, adjustEnd):
    """把 tracker CSV 列轉成 INSERT tuple（12 舊欄 + 15 個 tracker 分析欄）。
    weight / divRate / reason / estAmount 一律 NULL。"""
    return [
        (etfCode, targetDate, stockCode, stockName, action,
         None, None, None, None, estShares,
         adjustBegin, adjustEnd) + extras
        for stockCode, stockName, action, estShares, extras in trackerRows
    ]


# ══════════════════════════════════════════════════════════════════
# 交易日計算
# ══════════════════════════════════════════════════════════════════

def isTradingDay(targetDateObj):
    """使用 DAO 確認 targetDateObj 是否為交易日。"""
    year, month = targetDateObj.year, targetDateObj.month
    try:
        tradingDays, _ = DAO.loadDataFromDB(year, month, DAO.KEY_FRIDAY)
        return targetDateObj in {td[DAO.KEY_DAY] for td in tradingDays}
    except Exception as e:
        logging.exception(f"isTradingDay check failed for {targetDateObj}: {e}")
        return False


# ══════════════════════════════════════════════════════════════════
# upload_times.json 讀取
# ══════════════════════════════════════════════════════════════════

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
        logging.warning(f"Failed to read today from upload_times.json: {e}")
    return None


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


def buildAdjustRows(etfCode, targetDate, newList, removedList, weightList, difShares, adjustBegin, adjustEnd, stockNamesMap):
    """
    Case 1（CSV 來源）用。
    對 CSV 中每檔成分股計算 estimated_shares - DIFSHARES，
    回傳 INSERT 用的 tuple 清單。
    stock_name 優先從 stockNamesMap（v新日股價）查找，查無則以 CSV 欄位值為備用。
    weight / divRate / reason / estAmount 一律存 NULL（CSV 無此欄位）。
    """
    rows = []
    for action, itemList in [('addition', newList), ('deletion', removedList), ('weight', weightList)]:
        for item in itemList:
            stockCode = item['stock_code']
            stockName = stockNamesMap.get(stockCode) or item.get('stock_name', '')

            rawEstShares = item.get('estimated_shares', '')
            try:
                estShares = float(rawEstShares) if rawEstShares else None
            except (ValueError, TypeError):
                estShares = None

            # 扣減當日實際成交量；若 difShares 無此股則 DIFSHARES 視為 0
            if estShares is not None:
                estShares = int(estShares - difShares.get(stockCode, 0))

            rows.append((
                etfCode, targetDate, stockCode, stockName, action,
                None, None, None, None, estShares,
                adjustBegin, adjustEnd
            ))
    return rows


# ══════════════════════════════════════════════════════════════════
# JSON 資料列建構
# ══════════════════════════════════════════════════════════════════

def buildJsonRows(etfCode, reportDate, newList, removedList, weightsMap, adjustBegin, adjustEnd):
    """
    JSON 來源（不扣 DIFSHARES）。
    將 newList / removedList 轉換成 INSERT 用的 tuple 清單並回傳。
    """
    rows = []
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
            if estShares is not None:
                estShares = int(estShares * 1000)

            rows.append((
                etfCode, reportDate, stockCode, stockName, action,
                weight, divRate, reason, estAmount, estShares,
                adjustBegin, adjustEnd
            ) + (None,) * len(TRACKER_EXTRA_COLS))
    return rows


# ══════════════════════════════════════════════════════════════════
# JSON 檔案路徑解析
# ══════════════════════════════════════════════════════════════════

def loadJsonForEtf(etfCode, folderName, targetDate, useTestJson=False):
    """
    讀取 tmp_<etfCode>/<etfCode>_<targetDate>_prod.json（或 _test.json）。
    成功時回傳 (reportDate, newList, removedList, weightsMap)，失敗時回傳 None。
    """
    suffix = '_test.json' if useTestJson else '_prod.json'
    fileName = f"{etfCode}_{targetDate}{suffix}"
    filePath = os.path.join(reportDir, folderName, fileName)
    if not os.path.exists(filePath):
        logging.warning(f"Expected JSON not found: {filePath}")
        return None
    logging.info(f"Found JSON file: {filePath}")
    try:
        return parseJsonFile(filePath)
    except Exception as e:
        logging.exception(f"Error reading JSON for {etfCode}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# deadline 解析
# ══════════════════════════════════════════════════════════════════

def parseDeadlineDate(deadlineStr, etfCode):
    """
    解析 etf_dates.json 的 deadline 欄位，回傳 date 物件或 None。

    支援兩種格式：
    - 純日期字串：'2026-05-25'
    - 含多個日期的描述字串：'市值資料截止日: 2026-11-18<br>股利資料截止日: 2026-11-10'
      → 提取所有 YYYY-MM-DD，取最晚者
    """
    if not deadlineStr:
        return None
    try:
        return datetime.strptime(deadlineStr, '%Y-%m-%d').date()
    except ValueError:
        pass
    matches = re.findall(r'\d{4}-\d{2}-\d{2}', deadlineStr)
    dates = []
    for m in matches:
        try:
            dates.append(datetime.strptime(m, '%Y-%m-%d').date())
        except ValueError:
            pass
    if not dates:
        logging.warning(f"No valid date found in deadline for {etfCode}: {deadlineStr!r}")
        return None
    result = max(dates)
    logging.info(f"Parsed deadline for {etfCode}: {result} (from {deadlineStr!r})")
    return result


# ══════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════

def importDataForDate(targetDate, useTestJson=False):
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
    dao = DAO.EtfImporterDAO()
    try:
        dao.connect()
    except Exception as e:
        logging.exception(f"Failed to connect to DB: {e}")
        return

    try:
        # ── Step 3：從 etf_dates.json 讀取調整期資料 ────────────────────
        adjustInfo = {}

        try:
            with open(ETF_DATES_PATH, 'r', encoding='utf-8') as f:
                etfDates = json.load(f)
        except Exception as e:
            logging.exception(f"Failed to read etf_dates.json: {e}")
            return

        for etfCode in targetEtfList:
            entry = etfDates.get(etfCode)
            if not entry:
                adjustInfo[etfCode] = {'effectiveDate': None, 'deadline': None, 'begin': None, 'end': None, 'inAdjust': False}
                logging.warning(f"No entry in etf_dates.json for {etfCode}")
                continue

            adjEffectiveStr = entry.get('adjust_effective')
            adjustBeginStr  = entry.get('adjust_begin')
            adjustEndStr    = entry.get('adjust_end')
            deadlineStr     = entry.get('adjust_deadline')

            if not adjEffectiveStr or not adjustBeginStr or not adjustEndStr:
                adjustInfo[etfCode] = {'effectiveDate': None, 'deadline': None, 'begin': None, 'end': None, 'inAdjust': False}
                continue

            try:
                effectiveDate = datetime.strptime(adjEffectiveStr, '%Y-%m-%d').date()
                adjustBegin   = datetime.strptime(adjustBeginStr,  '%Y-%m-%d').date()
                adjustEnd     = datetime.strptime(adjustEndStr,    '%Y-%m-%d').date()
            except Exception as e:
                adjustInfo[etfCode] = {'effectiveDate': None, 'deadline': None, 'begin': None, 'end': None, 'inAdjust': False}
                logging.warning(f"Failed to parse dates for {etfCode} in etf_dates.json: {e}")
                continue

            deadline = parseDeadlineDate(deadlineStr, etfCode)

            inAdjust = adjustBegin <= targetDateObj <= adjustEnd
            adjustInfo[etfCode] = {
                'effectiveDate': effectiveDate,
                'deadline':      deadline,
                'begin':         adjustBegin,
                'end':           adjustEnd,
                'inAdjust':      inAdjust,
            }
            logging.info(
                f"{etfCode}: adjust_effective={effectiveDate}, deadline={deadline}, "
                f"adjust_begin={adjustBegin}, adjust_end={adjustEnd}, inAdjust={inAdjust}"
            )

        # ── Step 4：逐一處理各 ETF 資料夾 ───────────────────────────────
        if not os.path.exists(reportDir):
            logging.error(f"Report directory does not exist: {reportDir}")
            return

        foundAnyFile = False  # 偵測「完全沒找到任何檔案」的異常情況

        for folderName in os.listdir(reportDir):
            # 只處理 tmp_<etfCode> 格式的資料夾
            if not folderName.startswith('tmp_'):
                continue

            etfCode = folderName.replace('tmp_', '')
            if etfCode not in targetEtfList:
                continue

            info          = adjustInfo.get(etfCode, {'effectiveDate': None, 'deadline': None, 'begin': None, 'end': None, 'inAdjust': False})
            etfEffDate    = info['effectiveDate']
            deadline      = info['deadline']
            adjustBegin   = info['begin']
            adjustEnd     = info['end']
            inAdjust      = info['inAdjust']
            csvPath       = os.path.join(showdownCsvDir, f"{etfCode}.csv")

            # ── CASE 1：調整期 → 以 sitcRebalanceTracker 的 output CSV 為準 ──
            if inAdjust:
                trackerCsv = ensureTrackerCsv(etfCode, targetDate)
                if trackerCsv is None:
                    logging.error(f'[AdjPeriod] {etfCode} {targetDate} 無 tracker CSV, 跳過寫入 DB')
                    continue

                logging.info(f'[AdjPeriod] Using tracker CSV for {etfCode}: {trackerCsv}')
                try:
                    trackerRows = parseTrackerCsv(trackerCsv)
                except Exception as e:
                    logging.exception(f'{etfCode} 讀 tracker CSV 失敗: {e}')
                    continue
                if not trackerRows:
                    logging.error(f'[AdjPeriod] {etfCode} tracker CSV 無資料列, 跳過')
                    continue

                rows = buildTrackerRows(etfCode, targetDate, trackerRows, adjustBegin, adjustEnd)
                foundAnyFile = True
                dao.upsertRows(etfCode, targetDate, rows)
                continue
            
            # ── CASE 2：暫停期 ───────────────────────────────────────────
            if deadline is not None and adjustBegin is not None and deadline < targetDateObj < adjustBegin:
                # Hard-coded: 00929 / 00918 讀 _tmp.csv，不讀截止日 JSON，不扣 DIFSHARES，不刪 CSV
                #if etfCode in ('00929', '00918'):
                #    tmpCsvPath = os.path.join(showdownCsvDir, f"{etfCode}_tmp.csv")
                #    if not os.path.exists(tmpCsvPath):
                #        logging.error(f"[SuspendPeriod][{etfCode}] _tmp.csv not found: {tmpCsvPath}, skipping")
                #        continue
                #    logging.info(f"[SuspendPeriod][{etfCode}] Using _tmp.csv: {tmpCsvPath}")
                #    try:
                #        newList, removedList, weightList = parseCsvFile(tmpCsvPath)
                #    except Exception as e:
                #        logging.exception(f"[SuspendPeriod][{etfCode}] Error reading _tmp.csv: {e}")
                #        continue
                #    stockNamesMap = dao.queryStockNames(targetDate)
                #    rows = buildAdjustRows(etfCode, targetDate, newList, removedList, weightList,
                #                           {}, adjustBegin, adjustEnd, stockNamesMap)
                #    foundAnyFile = True
                #    dao.upsertRows(etfCode, targetDate, rows)
                #    continue

                deadlineDateStr = deadline.strftime('%Y-%m-%d')
                result = loadJsonForEtf(etfCode, folderName, deadlineDateStr, useTestJson)
                if result is None:
                    continue
                _, newList, removedList, weightsMap = result
                foundAnyFile = True
                rows = buildJsonRows(etfCode, targetDate, newList, removedList, weightsMap,
                                     adjustBegin, adjustEnd)
                dao.upsertRows(etfCode, targetDate, rows)
                continue

            # ── ELSE：正常期 ─────────────────────────────────────────────
            # 上傳窗口（effectiveDate ~ adjustEnd）之外才清理 stale CSV
            isInUploadWindow = (etfEffDate is not None and
                                etfEffDate <= targetDateObj <= adjustEnd)
            if os.path.exists(csvPath) and not isInUploadWindow:
                try:
                    os.remove(csvPath)
                    logging.info(f"Deleted stale CSV for {etfCode} (outside upload window)")
                except Exception as e:
                    logging.warning(f"Failed to delete stale CSV for {etfCode}: {e}")

            result = loadJsonForEtf(etfCode, folderName, targetDate, useTestJson)
            if result is None:
                continue
            reportDate, newList, removedList, weightsMap = result
            if not reportDate:
                logging.warning(f"No report date in JSON for {etfCode}, skipping.")
                continue

            foundAnyFile = True
            rows = buildJsonRows(etfCode, reportDate, newList, removedList, weightsMap,
                                 adjustBegin, adjustEnd)
            dao.upsertRows(etfCode, reportDate, rows)

        if not foundAnyFile:
            logging.warning(f"No files found for target date: {targetDate}")

    except Exception as e:
        logging.exception(f"Unhandled error during import for {targetDate}: {e}")
        raise
    finally:
        dao.close()

    logging.info(f"Import process completed for date: {targetDate}")


# ══════════════════════════════════════════════════════════════════
# 程式入口
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETF Database Importer')
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format (Default: T-1)')
    parser.add_argument('--from-upload-times', action='store_true',
                        help='Derive target date (T-1) from today field in upload_times.json')
    parser.add_argument('--test', action='store_true',
                        help='Load *_test.json instead of *_prod.json')
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

    #targetDate = '2026-06-26'
    setupLogging(targetDate)
    try:
        importDataForDate(targetDate, useTestJson=args.test)
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)

# # 切換至虛擬環境
# source ~/etf/venv/bin/activate
# # 模式 A: 匯入昨天資料
# python3 ~/etf/etf_calculator/etfDbImporter/dev/etfDbImporter.py
# # 模式 B: 匯入指定日期資料
# python3 ~/etf/etf_calculator/etfDbImporter/dev/etfDbImporter.py --date 2026-05-07

# /home/webuser/etf/venv/bin/python /home/webuser/etf/etf_calculator/etfDbImporter/dev/etfDbImporter.py --from-upload-times
