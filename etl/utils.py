import datetime as dt


def get_key_prefix_from_timestamp() -> str:
    return f"{dt.datetime.now().strftime('%Y/%m/%d/%H/%M')}"
