from datetime import datetime, timedelta


def slice_date_on_period(date_from, date_to, period):
    date_range = []
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
    time_delta = (date_to_dt - date_from_dt).days
    if time_delta > period:
        while date_from_dt <= date_to_dt:
            if (date_to_dt - date_from_dt).days >= period - 1:

                date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                   datetime.strftime(date_from_dt + timedelta(days=period - 1), "%Y-%m-%d")))
                date_from_dt += timedelta(days=period)

            else:
                dif_days = (date_to_dt - date_from_dt).days

                date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                   datetime.strftime(date_from_dt + timedelta(days=dif_days), "%Y-%m-%d")))
                date_from_dt += timedelta(days=dif_days + 1)
        return date_range
    else:
        return [(date_from, date_to)]