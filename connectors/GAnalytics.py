import time, json, re
import pandas as pd
from apiclient.discovery import build
from google.oauth2 import service_account
import socket
from googleapiclient.errors import HttpError
from my_clickhouse_con import ClickHouse
from google.auth.exceptions import RefreshError


class GAnalytics:
    def __init__(self, path_to_json, view_id, client_name, connect_params):
        self.path_to_json = path_to_json
        self.KEY_FILE_LOCATION = path_to_json
        self.SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
        self.VIEW_ID = view_id
        self.credentials = service_account.Credentials.from_service_account_file(self.KEY_FILE_LOCATION)
        self.scoped_credentials = self.credentials.with_scopes(self.SCOPES)
        self.ch = ClickHouse(connect_params["host"], connect_params["user"], connect_params["password"])
        self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)
        self.data_set_id = f"{client_name}_GAnalytics_ALL"
        self.client_name = client_name

        self.report_dict = {

            "GENERAL": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_users", "type": "INTEGER"},
                    {"name": "ga_newUsers", "type": "INTEGER"},
                    {"name": "ga_sessions", "type": "INTEGER"},
                    {"name": "ga_bounces", "type": "INTEGER"},
                    {"name": "ga_sessionDuration", "type": "FLOAT"},
                    {"name": "ga_pageviews", "type": "INTEGER"},
                    {"name": "ga_hits", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_keyword", "type": "STRING"},
                    {"name": "ga_adContent", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"}
                ]
            },

            "EVENTGrouping": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_uniqueEvents", "type": "INTEGER"},
                    {"name": "ga_totalEvents", "type": "INTEGER"},
                    {"name": "ga_eventValue", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_keyword", "type": "STRING"},
                    {"name": "ga_adContent", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_eventCategory", "type": "STRING"},
                    {"name": "ga_eventAction", "type": "STRING"},
                    {"name": "ga_eventLabel", "type": "STRING"},
                    {"name": "ga_channelGrouping", "type": "STRING"}
                ]
            },

            "Source": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_users", "type": "INTEGER"},
                    {"name": "ga_newUsers", "type": "INTEGER"},
                    {"name": "ga_sessions", "type": "INTEGER"},
                    {"name": "ga_bounces", "type": "INTEGER"},
                    {"name": "ga_sessionDuration", "type": "FLOAT"},
                    {"name": "ga_pageviews", "type": "INTEGER"},
                    {"name": "ga_hits", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_channelGrouping", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"}
                ]
            },

            "Source1to10": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_goal1Completions", "type": "INTEGER"},
                    {"name": "ga_goal2Completions", "type": "INTEGER"},
                    {"name": "ga_goal3Completions", "type": "INTEGER"},
                    {"name": "ga_goal4Completions", "type": "INTEGER"},
                    {"name": "ga_goal5Completions", "type": "INTEGER"},
                    {"name": "ga_goal6Completions", "type": "INTEGER"},
                    {"name": "ga_goal7Completions", "type": "INTEGER"},
                    {"name": "ga_goal8Completions", "type": "INTEGER"},
                    {"name": "ga_goal9Completions", "type": "INTEGER"},
                    {"name": "ga_goal10Completions", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_channelGrouping", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"}
                ]
            },
            "Source11to20": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_goal11Completions", "type": "INTEGER"},
                    {"name": "ga_goal12Completions", "type": "INTEGER"},
                    {"name": "ga_goal13Completions", "type": "INTEGER"},
                    {"name": "ga_goal14Completions", "type": "INTEGER"},
                    {"name": "ga_goal15Completions", "type": "INTEGER"},
                    {"name": "ga_goal16Completions", "type": "INTEGER"},
                    {"name": "ga_goal17Completions", "type": "INTEGER"},
                    {"name": "ga_goal18Completions", "type": "INTEGER"},
                    {"name": "ga_goal19Completions", "type": "INTEGER"},
                    {"name": "ga_goal20Completions", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_channelGrouping", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"}
                ]
            },

            "GeneralByClientID": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_pageviews", "type": "INTEGER"},
                    {"name": "ga_sessionDuration", "type": "FLOAT"},
                    {"name": "ga_hits", "type": "INTEGER"},
                    {"name": "ga_bounces", "type": "INTEGER"},
                    {"name": "ga_goalCompletionsAll", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_keyword", "type": "STRING"},
                    {"name": "ga_adContent", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"},
                    {"name": "ga_userType", "type": "STRING"},
                    {"name": "ga_city", "type": "STRING"},
                    {"name": "ga_ClientId", "type": "STRING"}
                ]
            },
            "Goal1to10ByClientID": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_goal1Completions", "type": "INTEGER"},
                    {"name": "ga_goal2Completions", "type": "INTEGER"},
                    {"name": "ga_goal3Completions", "type": "INTEGER"},
                    {"name": "ga_goal4Completions", "type": "INTEGER"},
                    {"name": "ga_goal5Completions", "type": "INTEGER"},
                    {"name": "ga_goal6Completions", "type": "INTEGER"},
                    {"name": "ga_goal7Completions", "type": "INTEGER"},
                    {"name": "ga_goal8Completions", "type": "INTEGER"},
                    {"name": "ga_goal9Completions", "type": "INTEGER"},
                    {"name": "ga_goal10Completions", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_keyword", "type": "STRING"},
                    {"name": "ga_adContent", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"},
                    {"name": "ga_userType", "type": "STRING"},
                    {"name": "ga_city", "type": "STRING"},
                    {"name": "ga_ClientId", "type": "STRING"}
                ]
            },
            "Goal11to20ByClientID": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_goal11Completions", "type": "INTEGER"},
                    {"name": "ga_goal12Completions", "type": "INTEGER"},
                    {"name": "ga_goal13Completions", "type": "INTEGER"},
                    {"name": "ga_goal14Completions", "type": "INTEGER"},
                    {"name": "ga_goal15Completions", "type": "INTEGER"},
                    {"name": "ga_goal16Completions", "type": "INTEGER"},
                    {"name": "ga_goal17Completions", "type": "INTEGER"},
                    {"name": "ga_goal18Completions", "type": "INTEGER"},
                    {"name": "ga_goal19Completions", "type": "INTEGER"},
                    {"name": "ga_goal20Completions", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_keyword", "type": "STRING"},
                    {"name": "ga_adContent", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"},
                    {"name": "ga_userType", "type": "STRING"},
                    {"name": "ga_city", "type": "STRING"},
                    {"name": "ga_ClientId", "type": "STRING"}
                ]
            },
            "Goal1to10": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_goal1Completions", "type": "INTEGER"},
                    {"name": "ga_goal2Completions", "type": "INTEGER"},
                    {"name": "ga_goal3Completions", "type": "INTEGER"},
                    {"name": "ga_goal4Completions", "type": "INTEGER"},
                    {"name": "ga_goal5Completions", "type": "INTEGER"},
                    {"name": "ga_goal6Completions", "type": "INTEGER"},
                    {"name": "ga_goal7Completions", "type": "INTEGER"},
                    {"name": "ga_goal8Completions", "type": "INTEGER"},
                    {"name": "ga_goal9Completions", "type": "INTEGER"},
                    {"name": "ga_goal10Completions", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_keyword", "type": "STRING"},
                    {"name": "ga_adContent", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"}
                ]
            },
            "Goal11to20": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_goal11Completions", "type": "INTEGER"},
                    {"name": "ga_goal12Completions", "type": "INTEGER"},
                    {"name": "ga_goal13Completions", "type": "INTEGER"},
                    {"name": "ga_goal14Completions", "type": "INTEGER"},
                    {"name": "ga_goal15Completions", "type": "INTEGER"},
                    {"name": "ga_goal16Completions", "type": "INTEGER"},
                    {"name": "ga_goal17Completions", "type": "INTEGER"},
                    {"name": "ga_goal18Completions", "type": "INTEGER"},
                    {"name": "ga_goal19Completions", "type": "INTEGER"},
                    {"name": "ga_goal20Completions", "type": "INTEGER"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_keyword", "type": "STRING"},
                    {"name": "ga_adContent", "type": "STRING"},
                    {"name": "ga_date", "type": "DATE"},
                    {"name": "ga_deviceCategory", "type": "STRING"}
                ]
            },
            "TRANSACTION": {
                "partition_name": "ga_date",
                "metrics": [
                    {"name": "ga_transactions", "type": "INTEGER"},
                    {"name": "ga_itemQuantity", "type": "INTEGER"},
                    {"name": "ga_transactionRevenue", "type": "FLOAT"}
                ],
                "dimensions": [
                    {"name": "ga_campaign", "type": "STRING"},
                    {"name": "ga_sourceMedium", "type": "STRING"},
                    {"name": "ga_date", "type": "STRING"},
                    {"name": "ga_deviceCategory", "type": "STRING"}
                ]
            },
        }

        create_data = self.ch.create_data_set_and_tables(self.data_set_id, self.report_dict, self.client_name,
                                                         "GAnalytics")
        assert create_data == [], "Error"

    def convert_data(self, dimension_list, metric_list, response_data_list):
        columns = dimension_list + metric_list
        total_data_list = []
        for element in response_data_list:
            for one_dict in element:
                total_data_list.append(one_dict['dimensions']+one_dict['metrics'][0]['values'])
        return columns, total_data_list

    def request(self, body):
        try:
            response = self.analytics.reports().batchGet(body=body).execute()
        except socket.timeout:
            time.sleep(2)
            return self.request(body)
        except ConnectionResetError:
            time.sleep(2)
            self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)
            return self.request(body)
        except RefreshError:
            time.sleep(40)
            self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)
            return self.request(body)
        except HttpError as http_error:
            http_error = json.loads(http_error.content.decode("utf8"))
            code = int(http_error['error']['code'])
            message = http_error['error']['message']
            status = http_error['error']['status']
            if code > 500:
                time.sleep(30)
                self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)
                return self.request(body)
            else:
                raise Exception(f"code - {code}. status - {status}.\n {message}")
        return response

    def create_params(self, list_of_params, type_of_metric):
        params_dict = []
        if type_of_metric == "metrics":
            key = 'expression'
        elif type_of_metric == "dimensions":
            key = 'name'
        else:
            raise Exception("Not supported type")
        for param in list_of_params:
            params_dict.append({key: param})
        return params_dict

    def create_body(self, date_from, date_to, metric, dimension, page_token='', dimension_filter=None):
        if dimension_filter is None:
            dimension_filter = []
        body = {
            "reportRequests":
                [{
                    "viewId": self.VIEW_ID,
                    "dateRanges": [{"startDate": date_from, "endDate": date_to}],
                    "metrics": metric,
                    "dimensions": dimension,
                    "dimensionFilterClauses": dimension_filter,
                    "samplingLevel": "LARGE",
                    "pageSize": 50000,
                    "pageToken": page_token
                }]
        }
        return body

    def get_request(self, date_from, date_to, report_name, view_id, dimension_filter=None):
        metric_list = [re.sub('[_]', ':', field) for field in list(self.report_dict[report_name]['metrics'].keys())]
        dimension_list = [re.sub('[_]', ':', field) for field in
                          list(self.report_dict[report_name]['dimensions'].keys())]

        metric = self.create_params(metric_list, 'metrics')
        dimension = self.create_params(dimension_list, 'dimensions')
        response_data_list = []

        body = self.create_body(date_from, date_to, metric, dimension, dimension_filter=dimension_filter)

        response = self.request(body)
        if response['reports'][0]['data'].get("rows", False):
            response_data_list.append(response['reports'][0]['data']['rows'])

            while response['reports'][0].get('nextPageToken') is not None:
                page_token = response['reports'][0]['nextPageToken']
                body = self.create_body(date_from, date_to, metric, dimension, page_token=page_token,
                                        dimension_filter=dimension_filter)
                response = self.request(body)
                # print(response)
                response_data_list.append(response['reports'][0]['data']['rows'])
                time.sleep(2)
            report_data = self.convert_data(dimension_list, metric_list, response_data_list)
            if report_data:
                columns = [re.sub('[:]', '_', field) for field in report_data[0]]

                report_data_df = pd.DataFrame(report_data[1], columns=columns)
                report_data_df['view_id'] = view_id
                result = self.ch.insert_dataframe(report_data_df, self.data_set_id,
                                                  f"{self.client_name}_GAnalytics_{report_name}")
        else:
            return []
