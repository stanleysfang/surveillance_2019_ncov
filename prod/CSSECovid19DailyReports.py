
from google.cloud import bigquery
from BigQueryWrapper import QueryRunner, Loader, Extractor
import pandas as pd
import datetime
import re

class CSSECovid19DailyReports:
    def __init__(self, client=None, run_project="stanleysfang"):
        if client:
            self.client = client
        else:
            self.client = bigquery.Client(project=run_project)
        
        self.run_project = self.client.project
        self.qr = QueryRunner(client=self.client)
        self.loader = Loader(client=self.client)
        self.extractor = Extractor(client=self.client)
    
    def update_us(self, dt, end_dt=None, destination_table='stanleysfang.surveillance_2019_ncov.csse_covid_19_daily_reports_us'):
        dt_list = None
        if isinstance(dt, (str, datetime.datetime)) and isinstance(end_dt, (str, datetime.datetime)):
            dt_list = pd.date_range(start=dt, end=end_dt).tolist()
        elif isinstance(dt, (str, datetime.datetime)):
            dt_list = [dt]
        elif isinstance(dt, list):
            dt_list = dt
        
        assert isinstance(dt_list, list), 'dt must be a str in "YYYY-mm-dd" format or a datetime.datetime object or a list of these'
        
        url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/'
        cols = [
            'province_state',
            'country_region',
            'last_update',
            'latitude',
            'longitude',
            'confirmed',
            'deaths',
            'recovered',
            'active',
            'FIPS',
            'incident_rate',
            'total_test_results',
            'people_hospitalized',
            'case_fatality_ratio',
            'UID',
            'iso3',
            'testing_rate',
            'hospitalization_rate'
        ]
        dtypes = {
            'province_state': 'object',
            'country_region': 'object',
            # 'last_update': 'datetime64', # use parse_dates argument in pd.read_csv
            'latitude': 'float64',
            'longitude': 'float64',
            'confirmed': 'float64',
            'deaths': 'float64',
            'recovered': 'float64',
            'active': 'float64',
            'FIPS': 'float64',
            'incident_rate': 'float64',
            'total_test_results': 'float64',
            'people_hospitalized': 'float64',
            'case_fatality_ratio': 'float64',
            'UID': 'float64',
            'iso3': 'object',
            'testing_rate': 'float64',
            'hospitalization_rate': 'float64',
        }
        schema = [
            ('province_state', 'STRING'),
            ('country_region', 'STRING'),
            ('last_update', 'TIMESTAMP'),
            ('latitude', 'FLOAT64'),
            ('longitude', 'FLOAT64'),
            ('confirmed', 'INT64'),
            ('deaths', 'INT64'),
            ('recovered', 'INT64'),
            ('active', 'INT64'),
            ('FIPS', 'INT64'),
            ('incident_rate', 'FLOAT64'),
            ('total_test_results', 'INT64'),
            ('people_hospitalized', 'INT64'),
            ('case_fatality_ratio', 'FLOAT64'),
            ('UID', 'INT64'),
            ('iso3', 'STRING'),
            ('testing_rate', 'FLOAT64'),
            ('hospitalization_rate', 'FLOAT64'),
        ]
        
        for d in dt_list:
            assert isinstance(d, (str, datetime.datetime)), 'dt must be a str in "YYYY-mm-dd" format or a datetime.datetime object or a list of these'
            
            if isinstance(d, str):
                d = datetime.datetime.strptime(d, '%Y-%m-%d')
            
            print('Updating ' + d.strftime('%Y-%m-%d') + ' ... ', end='', flush=True)
            daily_report_us = pd.read_csv(
                url + d.strftime('%m-%d-%Y') + '.csv',
                header=0, names=cols,
                dtype=dtypes, parse_dates=['last_update']
            )
            load_job = self.loader.load_df(
                daily_report_us,
                '{destination_table}${partition}'.format(destination_table=destination_table, partition=d.strftime('%Y%m%d')),
                schema=schema,
                time_partitioning=True
            )
            load_job.result()
            print('Done')
