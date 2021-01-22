# Importing main class of the summarizer
from rymrds import RTMRawDataSummarizer
from config import RTMConfig as config

# Python native modules
import logging
from datetime import datetime, timedelta
import pytz

logging.basicConfig(format='[%(asctime)s] - %(levelname)s - : %(message)s', level=logging.DEBUG)

tz = pytz.timezone('Asia/Manila')
if config.FACILITY == 'MIPT':
    tz = pytz.timezone('Asia/Bangkok')

now = datetime.now(tz)

snipdate = now.strftime("%Y-%m-%d %H:00:00")
startdate = (now - timedelta(hours=2)).strftime("%Y-%m-%d %H:00:00")
enddate = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:59:59")

# snipdate = '2020-07-01 15:00:00'
# startdate = '2020-07-01 13:00:00'
# enddate = '2020-07-01 14:59:59'

logging.info('Starting summary for snip date = %s', snipdate)
RTMRawDataSummarizer({
    "startdate" : startdate
    , "enddate" : enddate
    , "snipdate" : snipdate
}).execute()
logging.info('Summary Complete')