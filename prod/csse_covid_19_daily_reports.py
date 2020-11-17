
from google.cloud import bigquery
from CSSECovid19DailyReports import CSSECovid19DailyReports
import datetime

project_id = 'stanleysfang'
client = bigquery.Client(project=project_id)

daily_reports = CSSECovid19DailyReports(client)

#### Load Data ####
cur = datetime.datetime.today()
d = datetime.datetime(cur.year, cur.month, cur.day-1)

daily_reports.update_us(d)
