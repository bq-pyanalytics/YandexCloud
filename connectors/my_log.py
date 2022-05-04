import logging
import pandas as pd
from datetime import datetime
from my_clickhouse_con import ClickHouse


class LogDBHandler(logging.Handler):

    def __init__(self, connect_params, db_name, db_table):
        logging.Handler.__init__(self)
        self.ch = ClickHouse(connect_params["host"], connect_params["user"], connect_params["password"])
        self.db_name = db_name
        self.db_table = db_table

    def emit(self, record):
        log_msg = record.msg
        log_msg = log_msg.strip()
        log_msg = log_msg.replace('\'', '\'\'')

        exc_params_list = list(record.exc_info[:2])
        for number, exc_list_element in enumerate(exc_params_list):
            if exc_list_element is not None:
                exc_params_list[number] = str(exc_list_element)

        exc_class, exc_param = exc_params_list

        list_of_json = {"level_no": record.levelno,
                        "line_no": record.lineno,
                        "level_name": record.levelname,
                        "module": record.module,
                        "msg": log_msg,
                        "file_name": record.filename,
                        "func_name": record.funcName,
                        "exc_class": exc_class,
                        "exc_param": exc_param,
                        "name": record.name,
                        "pathname": record.pathname,
                        "process": record.process,
                        "process_name": record.processName,
                        "thread": record.thread,
                        "thread_name": record.threadName,
                        "status_code": record.status_code,
                        "source": record.source,
                        "client_name": record.client_name,
                        "data_date_start": record.data_date_start,
                        "data_date_end": record.data_date_end,
                        "report": record.report,
                        "created": record.created,
                        "date": datetime.strftime(datetime.fromtimestamp(record.created), "%Y-%m-%d"),
                        }
        df = pd.DataFrame([list_of_json])
        result = self.ch.insert_dataframe(df, self.db_name, self.db_table)
        return result


def decorator_maker_with_arguments(connect_params, client_name, placement, db_name="logs", db_table="logs",
                                   log_error_level="DEBUG"):
    print("decorator_maker_with_arguments")
    log_db = LogDBHandler(connect_params, db_name, db_table)
    logging.getLogger(f"{client_name}_{placement}").addHandler(log_db)
    log = logging.getLogger(f"{client_name}_{placement}")
    log.setLevel(log_error_level)

    def my_logger(log_function):
        print("my_logger")

        def my_logger_wrapper(self, report_name, date_from, date_to):
            print("my_logger_wrapper")
            response = log_function(self, report_name, date_from, date_to)

            log.debug(response[0][1], extra={"status_code": response[0][0],
                                             "client_name": client_name,
                                             "source": "Yandex Direct",
                                             "data_date_start": date_from,
                                             "data_date_end": date_to,
                                             "report": report_name,
                                             }, exc_info=True)

            return log_function(self, report_name, date_from, date_to)

        return my_logger_wrapper

    return my_logger


