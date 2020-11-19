
from google.cloud import bigquery
from BigQueryWrapper import QueryRunner, Loader, Extractor
import pandas as pd
import datetime

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
        
        # US
        self.url_us = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/'
        self.col_mapping_us = {
            'UID': ('UID', 'float64'),
            'FIPS': ('FIPS', 'float64'), # Federal Information Processing Standards code that uniquely identifies counties within the US
            'ISO3': ('ISO3', 'object'),
            'Province_State': ('province_state', 'object'),
            'Country_Region': ('country_region', 'object'),
            'Lat': ('latitude', 'float64'),
            'Long_': ('longitude', 'float64'),
            'Confirmed': ('confirmed', 'float64'),
            'Deaths': ('deaths', 'float64'),
            'Recovered': ('recovered', 'float64'),
            'Active': ('active', 'float64'),
            'Incident_Rate': ('incident_rate', 'float64'),
            'Case_Fatality_Ratio': ('case_fatality_ratio', 'float64'),
            'Mortality_Rate': ('case_fatality_ratio', 'float64'),
            'Total_Test_Results': ('total_test_results', 'float64'),
            'People_Tested': ('total_test_results', 'float64'),
            'Testing_Rate': ('testing_rate', 'float64'),
            'People_Hospitalized': ('people_hospitalized', 'float64'),
            'Hospitalization_Rate': ('hospitalization_rate', 'float64'),
            'Last_Update': ('last_update', 'datetime64'),
        }
        self.col_order_us = [
            'UID',
            'FIPS',
            'ISO3',
            'province_state',
            'country_region',
            'latitude',
            'longitude',
            'confirmed',
            'deaths',
            'recovered',
            'active',
            'incident_rate',
            'case_fatality_ratio',
            'total_test_results',
            'testing_rate',
            'people_hospitalized',
            'hospitalization_rate',
            'last_update',
        ]
        self.schema_us = [
            ('UID', 'INT64'),
            ('FIPS', 'INT64'),
            ('ISO3', 'STRING'),
            ('province_state', 'STRING'),
            ('country_region', 'STRING'),
            ('latitude', 'FLOAT64'),
            ('longitude', 'FLOAT64'),
            ('confirmed', 'INT64'),
            ('deaths', 'INT64'),
            ('recovered', 'INT64'),
            ('active', 'INT64'),
            ('incident_rate', 'FLOAT64'),
            ('case_fatality_ratio', 'FLOAT64'),
            ('total_test_results', 'INT64'),
            ('testing_rate', 'FLOAT64'),
            ('people_hospitalized', 'INT64'),
            ('hospitalization_rate', 'FLOAT64'),
            ('last_update', 'TIMESTAMP'),
        ]
        
        # Global
        self.url_global = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
        self.col_mapping_global = {
            'FIPS': ('FIPS', 'float64'), # Federal Information Processing Standards code that uniquely identifies counties within the US
            'Admin2': ('county', 'object'), # US only
            'Province/State': ('province_state', 'object'),
            'Province_State': ('province_state', 'object'),
            'Country_Region': ('country_region', 'object'),
            'Country/Region': ('country_region', 'object'),
            'Combined_Key': ('combined_key', 'object'),
            'Latitude': ('latitude', 'float64'),
            'Lat': ('latitude', 'float64'),
            'Longitude': ('longitude', 'float64'),
            'Long_': ('longitude', 'float64'),
            'Confirmed': ('confirmed', 'float64'),
            'Deaths': ('deaths', 'float64'),
            'Recovered': ('recovered', 'float64'),
            'Active': ('active', 'float64'),
            'Incident_Rate': ('incident_rate', 'float64'),
            'Incidence_Rate': ('incident_rate', 'float64'),
            'Case_Fatality_Ratio': ('case_fatality_ratio', 'float64'),
            'Case-Fatality_Ratio': ('case_fatality_ratio', 'float64'),
            'Last Update': ('last_update', 'datetime64'),
            'Last_Update': ('last_update', 'datetime64'),
        }
        self.col_order_global = [
            'FIPS',
            'county',
            'province_state',
            'country_region',
            'combined_key',
            'latitude',
            'longitude',
            'confirmed',
            'deaths',
            'recovered',
            'active',
            'incident_rate',
            'case_fatality_ratio',
            'last_update',
        ]
        self.schema_global = [
            ('FIPS', 'INT64'),
            ('county', 'STRING'),
            ('province_state', 'STRING'),
            ('country_region', 'STRING'),
            ('combined_key', 'STRING'),
            ('latitude', 'FLOAT64'),
            ('longitude', 'FLOAT64'),
            ('confirmed', 'INT64'),
            ('deaths', 'INT64'),
            ('recovered', 'INT64'),
            ('active', 'INT64'),
            ('incident_rate', 'FLOAT64'),
            ('case_fatality_ratio', 'FLOAT64'),
            ('last_update', 'TIMESTAMP'),
        ]
    
    def find_all_cols(self, url, start_dt, end_dt=datetime.date.today() - datetime.timedelta(days=1)):
        col_set = set()
        for d in pd.date_range(start_dt, end_dt):
            print(d.strftime('%Y-%m-%d'))
            df = pd.read_csv(url + d.strftime('%m-%d-%Y') + '.csv')
            for col in df.columns:
                if col not in col_set:
                    col_set.add(col)
        print(col_set)
        return col_set
    
    def standardize_daily_reports(self, df, col_mapping, col_order):
        cols = []
        for col in df.columns:
            cols.append(col_mapping[col][0])
            df[col] = df[col].astype(col_mapping[col][1])
        df.columns = cols
        for col, dtype in set(col_mapping.values()):
            if col not in df.columns:
                df[col] = pd.Series(dtype=dtype)
        df = df[col_order]
        return df
    
    def update(self, dt, end_dt, destination_table, url, col_mapping, col_order, schema):
        dt_list = None
        if isinstance(dt, (str, datetime.date)) and isinstance(end_dt, (str, datetime.date)):
            dt_list = pd.date_range(start=dt, end=end_dt).tolist()
        elif isinstance(dt, (str, datetime.date)):
            dt_list = [dt]
        elif isinstance(dt, list):
            dt_list = dt
        
        assert isinstance(dt_list, list), 'dt must be a str in "YYYY-mm-dd" format or a datetime.date object or a list of these'
        
        for d in dt_list:
            assert isinstance(d, (str, datetime.date)), 'dt must be a str in "YYYY-mm-dd" format or a datetime.date object or a list of these'
            
            if isinstance(d, str):
                d = datetime.datetime.strptime(d, '%Y-%m-%d').date()
            
            print('Updating ' + d.strftime('%Y-%m-%d') + ' ... ', end='', flush=True)
            daily_report = pd.read_csv(url + d.strftime('%m-%d-%Y') + '.csv')
            daily_report = self.standardize_daily_reports(daily_report, col_mapping, col_order)
            load_job = self.loader.load_df(
                daily_report,
                '{destination_table}${partition}'.format(destination_table=destination_table, partition=d.strftime('%Y%m%d')),
                schema=schema,
                time_partitioning=True
            )
            load_job.result()
            print('Done')
    
    def update_us(self, dt, end_dt=None, destination_table='stanleysfang.surveillance_2019_ncov.csse_covid_19_daily_reports_us'):
        print('CSSE COVID-19 Daily Reports US:')
        self.update(
            dt, end_dt, destination_table,
            url=self.url_us,
            col_mapping=self.col_mapping_us,
            col_order=self.col_order_us,
            schema=self.schema_us
        )
    
    def update_global(self, dt, end_dt=None, destination_table='stanleysfang.surveillance_2019_ncov.csse_covid_19_daily_reports_global'):
        print('CSSE COVID-19 Daily Reports Global:')
        self.update(
            dt, end_dt, destination_table,
            url=self.url_global,
            col_mapping=self.col_mapping_global,
            col_order=self.col_order_global,
            schema=self.schema_global
        )
