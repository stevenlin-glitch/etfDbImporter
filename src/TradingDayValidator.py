#!/home/webuser/etf/venv/bin/python

import sys
from MSSQLLoader import MSSQLDB as mssql

CONF_DB = "/home/webuser/etf/etf_calculator/00919_May/conf/db_settings.yaml"
TRADE_DATE = 'tradeDate'

def isTradingDay(date):
    db = mssql(CONF_DB)
    db.open()
    sql = f"""
        SELECT COUNT(*) AS tradeDate
        FROM [TEJ].[dbo].[台灣交易日]
        WHERE 日期 = '{date}' AND 休市 = 0
    """
    result = db.exec(sql)[0][TRADE_DATE]
    db.close()
    return result

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("請輸入日期參數，格式如 2025-05-30")
        sys.exit(-1)  # 參數錯誤

    date = sys.argv[1]
    result = isTradingDay(date)

    # 用 exit code 傳回結果
    if result == 1:
        sys.exit(1)  # 是交易日，exit code 1
    else:
        sys.exit(0)  # 不是交易日，exit code 0
