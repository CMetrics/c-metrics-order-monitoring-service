import os
from datetime import datetime as dt

import pandas as pd
import sqlalchemy as sql
from dotenv import load_dotenv
from redis import Redis

load_dotenv()


REDIS_CON = Redis(
    host=os.environ.get("REDIS_HOST"),
    port=int(os.environ.get("REDIS_PORT")),
    decode_responses=True,
)


def get_db_connection(local: bool) -> sql.Engine:
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    db_name = os.getenv("LOCAL_DB_NAME" if local else "DB_NAME")
    host = os.getenv("LOCAL_DB_HOST" if local else "DB_HOST")
    port = os.getenv("DB_PORT")
    dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db_name}"
    return sql.create_engine(dsn)


def datetime_unix_conversion(
    df: pd.DataFrame, convert_to: str, cols: list = None
) -> pd.DataFrame:
    cols = cols if cols else df.columns
    for col in cols:
        if col.endswith("tmstmp"):
            if convert_to == "unix":
                df[col] = pd.to_datetime(df[col], utc=True).astype("int64") // 10**9
            else:
                df[col] = df[col].apply(lambda x: dt.utcfromtimestamp(x))
    return df


def get_available_redis_streams() -> list:
    i = 0
    all_streams = list()
    while True:
        i, streams = REDIS_CON.scan(i, _type="STREAM", match="{real-time}-trades-*")
        all_streams += streams
        if i == 0:
            return all_streams
