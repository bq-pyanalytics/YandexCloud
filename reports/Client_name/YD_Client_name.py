from access import access_data
from connectors._Utils import slice_date_on_period
from datetime import datetime, timedelta
from connectors.yandex_direct import YandexDirectReports


client_name = "CLIENT_NAME1"
ya_client_params = access_data.clients[client_name]

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

date_range = slice_date_on_period(date_from, date_to, ya_client_params['time_range'])

for login in ya_client_params['yandex']:
    access_token = access_data.yandex_tokens[ya_client_params['yandex_token_account']]

    ya = YandexDirectReports(access_token, login['login'], client_name, access_data.clickhouse_connect_params)

    for report in ya.report_dict:
        if report in login['report_range']:
            for date_from, date_to in date_range:
                ya.get_report(report, date_from, date_to)
