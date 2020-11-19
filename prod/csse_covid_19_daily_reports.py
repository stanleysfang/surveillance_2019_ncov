
from google.cloud import bigquery
from CSSECovid19DailyReports import CSSECovid19DailyReports
import datetime

project_id = 'stanleysfang'
client = bigquery.Client(project=project_id)

daily_reports = CSSECovid19DailyReports(client)

#### Load Data ####
yesterday = datetime.date.today() - datetime.timedelta(days=1)

daily_reports.update_us(yesterday)
daily_reports.update_global(yesterday)
