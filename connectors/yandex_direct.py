from my_clickhouse_con import ClickHouse
import requests, json, time, re
from my_log import decorator_maker_with_arguments
from datetime import datetime
import access as access_data
import pandas as pd


class YandexDirectReports:
    def __init__(self, access_token, client_login, client_name, connect_params):
        self.url = "https://api.direct.yandex.com/json/v5/"
        self.client_id = re.sub('[.-]', '_', client_login)
        self.ch = ClickHouse(connect_params["host"], connect_params["user"], connect_params["password"])
        self.data_set_id = f"{client_name}_YaDirect_{self.client_id}"
        self.client_name = client_name
        self.connect_params = connect_params

        self.headers_report = {
            "Authorization": "Bearer " + access_token,
            "Client-Login": client_login,
            "Accept-Language": "ru",
            "processingMode": "auto",
            "returnMoneyInMicros": "false",
            "skipReportHeader": "true",
            "skipReportSummary": "true"}

        self.report_dict = {
            "CAMPAIGN_STAT": {
                "type": "CUSTOM_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "CAMPAIGN_DEVICE_AND_PLACEMENT_STAT": {
                "type": "CUSTOM_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "AdNetworkType", "type": "VARCHAR(255)"},
                    {"name": "Device", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "CAMPAIGN_GEO_STAT": {
                "type": "CAMPAIGN_PERFORMANCE_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "LocationOfPresenceName", "type": "VARCHAR(255)"},
                    {"name": "TargetingLocationId", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "CAMPAIGN_PLACEMENT_STAT": {
                "type": "CAMPAIGN_PERFORMANCE_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "Placement", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "CAMPAIGN_SOCDEM_DEVICE_STAT": {
                "type": "CAMPAIGN_PERFORMANCE_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "Device", "type": "VARCHAR(255)"},
                    {"name": "CarrierType", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "MobilePlatform", "type": "VARCHAR(255)"},
                    {"name": "Age", "type": "VARCHAR(255)"},
                    {"name": "Gender", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "AD_STAT": {
                "type": "AD_PERFORMANCE_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "AdId", "type": "VARCHAR(255)"},
                    {"name": "AdFormat", "type": "VARCHAR(255)"},
                    {"name": "AdNetworkType", "type": "VARCHAR(255)"},
                    {"name": "AdGroupId", "type": "VARCHAR(255)"},
                    {"name": "AdGroupName", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "AD_DEVICE_STAT": {
                "type": "AD_PERFORMANCE_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "AdId", "type": "VARCHAR(255)"},
                    {"name": "AdFormat", "type": "VARCHAR(255)"},
                    {"name": "AdGroupId", "type": "VARCHAR(255)"},
                    {"name": "AdGroupName", "type": "VARCHAR(255)"},
                    {"name": "Device", "type": "VARCHAR(255)"},
                    {"name": "AdNetworkType", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "REACH_AND_FREQUENCY_STAT": {
                "type": "REACH_AND_FREQUENCY_PERFORMANCE_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "AdGroupId", "type": "VARCHAR(255)"},
                    {"name": "AdGroupName", "type": "VARCHAR(255)"},
                    {"name": "AdId", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "ImpressionReach", "type": "Int64"},
                    {"name": "AvgImpressionFrequency", "type": "FLOAT"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "KEYWORD_AD_STAT": {
                "type": "CUSTOM_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "AdGroupId", "type": "VARCHAR(255)"},
                    {"name": "AdGroupName", "type": "VARCHAR(255)"},
                    {"name": "AdId", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Clicks", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "CriterionId", "type": "VARCHAR(255)"},
                    {"name": "Criterion", "type": "VARCHAR(255)"},
                    {"name": "CriteriaType", "type": "VARCHAR(255)"}
                ]
            },
            "KEYWORD_SOCDEM_STAT": {
                "type": "CUSTOM_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "AdGroupId", "type": "VARCHAR(255)"},
                    {"name": "AdGroupName", "type": "VARCHAR(255)"},
                    {"name": "AdId", "type": "VARCHAR(255)"},
                    {"name": "CriterionId", "type": "VARCHAR(255)"},
                    {"name": "Criterion", "type": "VARCHAR(255)"},
                    {"name": "CriteriaType", "type": "VARCHAR(255)"},
                    {"name": "Slot", "type": "VARCHAR(255)"},
                    {"name": "Age", "type": "VARCHAR(255)"},
                    {"name": "Gender", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"}
                ]
            },
            "KEYWORD_DEVICE_STAT": {
                "type": "CRITERIA_PERFORMANCE_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "Device", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"},
                    {"name": "CriterionId", "type": "VARCHAR(255)"},
                    {"name": "Criterion", "type": "VARCHAR(255)"},
                    {"name": "CriteriaType", "type": "VARCHAR(255)"}
                ]
            },
            "KEYWORD_DEVICE_AD_STAT": {
                "type": "CUSTOM_REPORT",
                "partition_name": "Date",
                "fields": [
                    {"name": "Date", "type": "Date"},
                    {"name": "CampaignId", "type": "VARCHAR(255)"},
                    {"name": "CampaignName", "type": "VARCHAR(255)"},
                    {"name": "CampaignType", "type": "VARCHAR(255)"},
                    {"name": "AdGroupId", "type": "VARCHAR(255)"},
                    {"name": "AdGroupName", "type": "VARCHAR(255)"},
                    {"name": "AdId", "type": "VARCHAR(255)"},
                    {"name": "Impressions", "type": "Int64"},
                    {"name": "Cost", "type": "Float64"},
                    {"name": "Clicks", "type": "Int64"},
                    {"name": "CriterionId", "type": "VARCHAR(255)"},
                    {"name": "Criterion", "type": "VARCHAR(255)"},
                    {"name": "CriterionType", "type": "VARCHAR(255)"},
                    {"name": "AdNetworkType", "type": "VARCHAR(255)"},
                    {"name": "Device", "type": "VARCHAR(255)"},
                    {"name": "Slot", "type": "VARCHAR(255)"},
                    {"name": "Placement", "type": "VARCHAR(255)"},
                    {"name": "TargetingLocationId", "type": "VARCHAR(255)"},
                    {"name": "TargetingLocationName", "type": "VARCHAR(255)"}
                ]
            }
        }

        create_data = self.ch.create_data_set_and_tables(self.data_set_id, self.report_dict, self.client_name,
                                                         "YaDirect", self.client_id)
        assert create_data == [], "Error"

    def __create_body(self, selection_criteria, field_names, report_name, report_type):

        body = {
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": field_names,
                "ReportName": (self.body_report_name),
                "ReportType": report_type,
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO"
            }
        }
        jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
        return jsonBody

    def __request(self, selection_criteria, field_names, report_name, report_type, method):
        jsonBody = self.__create_body(selection_criteria, field_names, report_name, report_type)
        try:
            data = requests.post(self.url + method, jsonBody, headers=self.headers_report)

        except requests.exceptions.ConnectionError as error:
            print(error)
            data = requests.post(self.url + method, jsonBody, headers=self.headers_report)

        if data.status_code in [201, 202]:
            time.sleep(60)

            return self.__request(selection_criteria, field_names, report_name, report_type, method)
        return data

    @decorator_maker_with_arguments(access_data.clickhouse_connect_params, "generale_logger", "Yandex Direct")
    def get_report(self, report_name, date_from, date_to):
        """
        report_name - report_type - fields:
         - CAMPAIGN_STAT - CUSTOM_REPORT
         - CAMPAIGN_DEVICE_AND_PLACEMENT_STAT - CUSTOM_REPORT
         - CAMPAIGN_GEO_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - CAMPAIGN_PLACEMENT_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - CAMPAIGN_SOCDEM_DEVICE_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - AD_STAT - AD_PERFORMANCE_REPORT
         - AD_DEVICE_STAT - AD_PERFORMANCE_REPORT
         - REACH_AND_FREQUENCY_STAT - REACH_AND_FREQUENCY_PERFORMANCE_REPORT
         - KEYWORD_AD_STAT - CUSTOM_REPORT
         - KEYWORD_SOCDEM_STAT - CUSTOM_REPORT
         - KEYWORD_DEVICE_STAT - CRITERIA_PERFORMANCE_REPORT
         - KEYWORD_DEVICE_AD_STAT - CUSTOM_REPORT

         date format: "YYYY-MM-DD"

        """

        get_data_params = self.report_dict.get(report_name, False)
        if get_data_params:
            report_type = get_data_params['type']
            selection_criteria = {"DateFrom": date_from, "DateTo": date_to}
            field_names = [i['name'] for i in get_data_params['fields']]

            self.body_report_name = datetime.strftime(datetime.now(), format="%Y-%m-%d %H:%M:%S")
            data = self.__request(selection_criteria, field_names, report_name, report_type, "reports")

            if 'error' in data.text:
                error_code = json.loads(data.text)['error']['error_code']
                error_detail = json.loads(data.text)['error']['error_detail']

                response = [(error_code, error_detail)]

            else:
                report_data_split = data.text.split('\n')
                data_split = [x.split('\t') for x in report_data_split]
                stat = pd.DataFrame(data_split[1:-1], columns=data_split[:1][0])

                result = self.ch.insert_dataframe(stat, self.data_set_id,
                                                  f"{self.client_name}_YaDirect_{self.client_id}_{report_name}")

                response = [(data.status_code, "Success")]

            return response

        else:
            return [(0, "Указан недопустимый тип отчета.")]


