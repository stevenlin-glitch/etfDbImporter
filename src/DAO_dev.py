#!/home/webuser/etf/venv/bin/python
import json
import os
import datetime
import sys
import logging
import pyodbc
from QueryLoader import QueryLoader as qloader
from MSSQLLoader import MSSQLDB as mssql
import YamlLoader

from pprint import pprint

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(BASE_DIR)
CONF_DB = os.path.join(BASE_DIR, "conf", "db_settings.yaml")
CONF_QUERY = os.path.join(BASE_DIR, "conf", "query_dev.yaml")

KEY_FRIDAY = 5
KEY_DAY = 'dd'


def _buildConnStr(cfg):
    return (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['user']};"
        f"PWD={cfg['pass']};"
        "TrustServerCertificate=yes;"
    )


class EtfImporterDAO:
    """DB90（TEJ）與 DB91（ETF_機率）的讀寫介面，對外隱藏連線細節。"""

    def __init__(self):
        dbConf        = YamlLoader.load(CONF_DB)
        self._db90Cfg = dbConf['db90']
        self._db91Cfg = dbConf['db91']
        self._sql     = YamlLoader.load(CONF_QUERY)
        self._conn90  = None
        self._conn91  = None
        self._cursor91 = None

    def connect(self):
        """建立 DB90 與 DB91 的 pyodbc 連線。失敗時拋出例外。"""
        self._conn90   = pyodbc.connect(_buildConnStr(self._db90Cfg))
        self._conn91   = pyodbc.connect(_buildConnStr(self._db91Cfg))
        self._cursor91 = self._conn91.cursor()

    def close(self):
        """關閉所有連線。"""
        if self._cursor91:
            self._cursor91.close()
            self._cursor91 = None
        if self._conn91:
            self._conn91.close()
            self._conn91 = None
        if self._conn90:
            self._conn90.close()
            self._conn90 = None

    def queryDifShares(self, targetDateStr, fundId):
        """
        查詢 DB90 [ETF成分每日增減明細]，回傳 { stock_code: DIFSHARES }。
        FUNDID 為 etfCode 去掉 '_' 後綴（例：'00919_May' → '00919'）。
        """
        dateStr = targetDateStr.replace('-', '')
        result  = {}
        cursor  = self._conn90.cursor()
        try:
            cursor.execute(self._sql['queryDifShares'], (dateStr, fundId))
            for row in cursor.fetchall():
                stockCode = (row[0] or '').split('.')[0]
                result[stockCode] = float(row[1]) if row[1] is not None else 0.0
        except Exception as e:
            logging.error(f"queryDifShares failed ({fundId}, {dateStr}): {e}")
        finally:
            cursor.close()
        return result

    def queryTotalDifShares(self, beginDateStr, targetDateStr, fundId):
        """
        查詢 DB90 [ETF成分每日增減明細]，累加 beginDate ~ targetDate 的 DIFSHARES，
        回傳 { stock_code: total_DIFSHARES }。
        """
        beginStr = beginDateStr.replace('-', '')
        endStr   = targetDateStr.replace('-', '')
        result   = {}
        cursor   = self._conn90.cursor()
        try:
            cursor.execute(self._sql['queryTotalDifShares'], (beginStr, endStr, fundId))
            for row in cursor.fetchall():
                stockCode = (row[0] or '').split('.')[0]
                result[stockCode] = float(row[1]) if row[1] is not None else 0.0
        except Exception as e:
            logging.error(f"queryTotalDifShares failed ({fundId}, {beginStr}~{endStr}): {e}")
        finally:
            cursor.close()
        return result

    def queryStockNames(self, targetDateStr):
        """
        查詢 DB90 [TEJ].[dbo].[v新日股價]，回傳 { sid: nn } 即 { stock_code: stock_name }。
        targetDateStr 格式：'YYYY-MM-DD'。
        """
        result = {}
        cursor = self._conn90.cursor()
        try:
            cursor.execute(self._sql['queryStockNames'], (targetDateStr,))
            for row in cursor.fetchall():
                sid = (row[0] or '').strip()
                nn  = (row[1] or '').strip()
                if sid:
                    result[sid] = nn
        except Exception as e:
            logging.error(f"queryStockNames failed ({targetDateStr}): {e}")
        finally:
            cursor.close()
        return result

    def fetchPrevDayRecords(self, etfCode, prevDay):
        """
        從 etf_additions_history_dev 取得 etfCode × prevDay 的全部記錄。
        回傳：list of dict { stock_code, stock_name, action, estimated_shares }
        """
        try:
            self._cursor91.execute(self._sql['fetchEtfHistory'], (etfCode, prevDay))
            return [
                {
                    'stock_code':       row[0],
                    'stock_name':       row[1],
                    'action':           row[2],
                    'estimated_shares': float(row[3]) if row[3] is not None else None,
                }
                for row in self._cursor91.fetchall()
            ]
        except Exception as e:
            logging.error(f"fetchPrevDayRecords failed for {etfCode} on {prevDay}: {e}")
            return []

    def upsertRows(self, etfCode, reportDate, rows):
        """
        刪除 etfCode × reportDate 的舊記錄後，批次 INSERT rows（冪等操作）。
        rows 格式：(etf_code, report_date, stock_code, stock_name, action,
                    weight, dividend_rate, reason, estimated_amount, estimated_shares,
                    adjust_begin, adjust_end)
        """
        try:
            self._cursor91.execute(self._sql['deleteEtfHistory'], (etfCode, reportDate))
            for row in rows:
                self._cursor91.execute(self._sql['insertEtfHistory'], row)
            self._conn91.commit()
            logging.info(f"Upserted {len(rows)} rows for {etfCode} / {reportDate}")
        except Exception as e:
            logging.error(f"upsertRows failed for {etfCode} / {reportDate}: {e}")
            self._conn91.rollback()


def loadDataFromDB(year, month, weekday):
    qHandle = qloader(CONF_QUERY)
    db = mssql(CONF_DB)
    db.open()
    tradingDays = db.exec(qHandle.getTradingDays(year, month))
    tradingWeekday = db.exec(qHandle.getTradingWeekday(year, month, weekday))
    db.close()
    return tradingDays, tradingWeekday

if __name__ == "__main__":
    today = datetime.date.today()
