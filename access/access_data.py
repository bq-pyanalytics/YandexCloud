clickhouse_connect_params = {
    'host': '<host>',  # xx.xx.xxx.xxx
    'user': '<user name>',
    'password': '<user password>'
}

yandex_tokens = {
    "yakubovsky@yandex.ru": "<access token>"
}

clients = {
    'CLIENT_NAME1': {
        'yandex_token_account': "yakubovsky@yandex.ru",
        'time_range': 3,
        'yandex':
            [

                {'login': 'login1', 'name': 'login_name1', "report_range": ["AD_STAT"]},
                {'login': 'login2', 'name': 'login_name2', "report_range": ["AD_STAT"]},
                {'login': 'login3', 'name': 'login_name3', "report_range": ["AD_STAT"]}
            ]
    },
    'CLIENT_NAME2': {
        'yandex_token_account': "yakubovsky@yandex.ru",
        'yandex':
            [

                {'login': 'login1', 'name': 'login_name1', "report_range": ["AD_STAT"]},
                {'login': 'login2', 'name': 'login_name2', "report_range": ["AD_STAT"]},
                {'login': 'login3', 'name': 'login_name3', "report_range": ["AD_STAT"]}
            ]
    },
}

