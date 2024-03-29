import os
from datetime import datetime as dt

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from redis import asyncio as async_redis

load_dotenv()


REDIS_CON = async_redis.Redis(
    host=os.environ.get("REDIS_HOST"),
    port=int(os.environ.get("REDIS_PORT")),
    decode_responses=True,
)


def get_db_connection(local: bool) -> psycopg2.connection:
    return psycopg2.connect(
        database=os.getenv("LOCAL_DB_NAME" if local else "DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("LOCAL_DB_HOST" if local else "DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


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


def execute_query(db: psycopg2.connection, query: str):
    with db.cursor() as cursor:
        cursor.execute(query)
        db.commit()
