import os
import json
import pyodbc
import argparse
import logging
from datetime import datetime, timedelta

# Database Configuration
dbConfig = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': '172.16.8.91',
    'database': 'ETF_機率',
    'user': 'lethe',
    'pass': 'lethe891'
}

reportDir = '/home/webuser/etf/etf_calculator/report_generator'
baseDir = '/home/webuser/etf/etf_calculator/etfDbImporter'

def setupLogging(targetDate):
    """Sets up logging to both file and console."""
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
    """Establishes a connection to the MS SQL database."""
    connStr = (
        f"DRIVER={{{dbConfig['driver']}}};"
        f"SERVER={dbConfig['server']};"
        f"DATABASE={dbConfig['database']};"
        f"UID={dbConfig['user']};"
        f"PWD={dbConfig['pass']};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connStr)

def parseJsonFile(filePath):
    """Parses the ETF report JSON file."""
    with open(filePath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    reportDate = data.get('date')
    baseline = data.get('baseline', {})
    newList = baseline.get('baselineNew', [])
    removedList = baseline.get('baselineRemoved', [])
    weightsMap = baseline.get('baseWeights', {})
    
    return reportDate, newList, removedList, weightsMap

def importDataForDate(targetDate):
    """Scans report directories and imports data for the specified date."""
    logging.info(f"Starting import process for date: {targetDate}")
    
    try:
        conn = getDbConnection()
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    mergeSql = """
    MERGE [dbo].[etf_additions_history] AS target
    USING (SELECT ? AS etf_code, ? AS report_date, ? AS stock_code, ? AS stock_name, ? AS action) AS source
    ON (target.etf_code = source.etf_code AND target.report_date = source.report_date AND target.stock_code = source.stock_code AND target.action = source.action)
    WHEN MATCHED THEN
        UPDATE SET weight = ?, dividend_rate = ?, reason = ?, estimated_amount = ?, estimated_shares = ?
    WHEN NOT MATCHED THEN
        INSERT (etf_code, report_date, stock_code, stock_name, action, weight, dividend_rate, reason, estimated_amount, estimated_shares)
        VALUES (source.etf_code, source.report_date, source.stock_code, source.stock_name, source.action, ?, ?, ?, ?, ?);
    """

    foundAnyFile = False
    
    # Iterate through tmp folders in the report directory
    if not os.path.exists(reportDir):
        logging.error(f"Report directory does not exist: {reportDir}")
        return

    targetEtfList = ['0050', '0056', '00878', '00919_May', '00919_Dec']
    
    for folderName in os.listdir(reportDir):
        if folderName.startswith('tmp_'):
            etfCode = folderName.replace('tmp_', '')
            
            # Only process ETFs in the target list
            if etfCode not in targetEtfList:
                continue
                
            fileName = f"{etfCode}_{targetDate}_prod.json"
            filePath = os.path.join(reportDir, folderName, fileName)
            
            if os.path.exists(filePath):
                foundAnyFile = True
                logging.info(f"Found file: {filePath}")
                try:
                    reportDate, newList, removedList, weightsMap = parseJsonFile(filePath)
                    
                    if not reportDate:
                        logging.warning(f"No date field found in {fileName}, skipping.")
                        continue

                    # Process Inclusion
                    for item in newList:
                        cleanName = item['name'].replace('@', '').replace('*', '').strip()
                        nameParts = cleanName.split(' ', 1)
                        stockCode = nameParts[0]
                        stockName = nameParts[1] if len(nameParts) > 1 else ""
                        
                        weight = item.get('weight')
                        if weight is None and stockName in weightsMap:
                            weight = weightsMap[stockName]
                        
                        divRate = item.get('dividendRate', '')
                        reason = json.dumps(item.get('reason', []), ensure_ascii=False)
                        estAmount = item.get('estimatedAmount')
                        estShares = item.get('estimatedShares')
                        
                        params = (etfCode, reportDate, stockCode, stockName, 'addition', 
                                  weight, divRate, reason, estAmount, estShares,
                                  weight, divRate, reason, estAmount, estShares)
                        cursor.execute(mergeSql, params)

                    # Process Deletion
                    for item in removedList:
                        cleanName = item['name'].replace('@', '').replace('*', '').strip()
                        nameParts = cleanName.split(' ', 1)
                        stockCode = nameParts[0]
                        stockName = nameParts[1] if len(nameParts) > 1 else ""
                        
                        weight = item.get('weight')
                        divRate = item.get('dividendRate', '')
                        reason = json.dumps(item.get('reason', []), ensure_ascii=False)
                        estAmount = item.get('estimatedAmount')
                        estShares = item.get('estimatedShares')
                        
                        params = (etfCode, reportDate, stockCode, stockName, 'deletion', 
                                  weight, divRate, reason, estAmount, estShares,
                                  weight, divRate, reason, estAmount, estShares)
                        cursor.execute(mergeSql, params)
                    
                    conn.commit()
                    logging.info(f"Successfully processed {etfCode} for {targetDate}")
                except Exception as e:
                    logging.error(f"Error processing {etfCode} from {fileName}: {e}")
                    conn.rollback()
            else:
                # Only log missing files for common ETF directories to avoid noise
                logging.warning(f"Expected file not found: {filePath}")

    if not foundAnyFile:
        logging.warning(f"No production JSON files found for target date: {targetDate}")

    cursor.close()
    conn.close()
    logging.info(f"Import process completed for date: {targetDate}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETF Database Importer (Lower Camel Case Edition)')
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format (Default: T-1)')
    
    args = parser.parse_args()
    
    # Determine target date: Specified or T-1
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