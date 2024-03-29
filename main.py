import json
import logging
import os
import uuid
from datetime import datetime as dt

import pandas as pd
import requests
import sqlalchemy as sql
from dotenv import load_dotenv

from utils import helpers

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("order_monitoring_service")
LOG.setLevel(logging.INFO)


class OnStartChecker:
    def __init__(self, open_orders: dict, db: sql.Engine, verbose: bool = True):
        self.verbose = verbose
        self.open_orders = open_orders
        self.data = dict()
        self.filled_orders = dict()
        self.db = db

    def get_url(self) -> str:
        base = f"http://{os.getenv('API_HOST')}:{os.getenv('API_PORT')}/ohlc?exchange=coinbase&pairs="
        pairs = ",".join(order["asset_id"] for order in self.open_orders.values())
        return f"{base}{pairs}&timeframe=1m"

    def get_all_ohlcv(self):
        if self.verbose:
            LOG.info("Retrieving OHLCV data")
        endpoint = self.get_url()
        response = requests.get(endpoint)
        data = response.json()
        for record in data:
            pair = list(record.keys())[0]
            df = pd.DataFrame(
                record[pair],
                columns=["time", "open", "high", "low", "close", "volume"],
            )
            self.data[pair] = df

    def get_execution_tmstmp(self, order: dict) -> dt:
        pair = order["asset_id"]
        ohlcv_df = self.data[pair]
        execution_df = ohlcv_df[
            ohlcv_df["time"] >= dt.timestamp(order["order_creation_tmstmp"]) * 1000
        ]
        if order["order_side"] == "buy":
            execution_df = execution_df[execution_df["low"] < order["order_price"]]
        else:
            execution_df = execution_df[execution_df["high"] > order["order_price"]]
        base_log = f"{order['broker_id']} {order['asset_id']} {order['trading_type']} {order['order_side']}"
        if not execution_df.empty:
            if self.verbose:
                LOG.info(
                    f"{base_log} EXECUTED: {order['order_volume']} @ {order['order_price']}"
                )
            return dt.fromtimestamp(execution_df["time"].min() / 1000)
        elif ohlcv_df.empty:
            LOG.error(f"No OHLCV data for {pair}")
        elif self.verbose:
            last_close = ohlcv_df.iloc[-1]["close"]
            distance_to_exec = (
                last_close / order["order_price"]
                if order["order_side"] == "buy"
                else order["order_price"] / last_close
            ) - 1
            LOG.info(
                f"{base_log} not executed: {distance_to_exec:.2%} away from target price"
            )

    def check_order(self, order_id: str):
        order = self.open_orders[order_id]
        execution_tmstmp = self.get_execution_tmstmp(order)
        if execution_tmstmp:
            self.filled_orders[order_id] = execution_tmstmp

    def cancel_previous_records(self):
        order_id_list = list(self.filled_orders.keys())
        order_id_list = "','".join(order_id_list)
        query = (
            f"update cmetrics_orders set expiration_tmstmp = '{dt.now()}' "
            f"where order_id in ('{order_id_list}') and expiration_tmstmp is null"
        )
        with self.db.connect() as connection:
            connection.execute(sql.text(query))
            connection.commit()

    def check_all_open_orders(self):
        if self.verbose:
            LOG.info(f"Will check {len(self.open_orders)} open orders")
        self.get_all_ohlcv()
        for order_id in self.open_orders:
            self.check_order(order_id)

    def get_updated_order_rows_df(self) -> pd.DataFrame:
        df = pd.DataFrame()
        for order_id in self.filled_orders:
            order_df = pd.DataFrame([self.open_orders[order_id]])
            order_df["order_id"] = order_id
            df = pd.concat([df, order_df])
        df.loc[:, "insert_tmstmp"] = dt.now()
        df.loc[:, "fill_pct"] = 1
        df.loc[:, "order_status"] = "executed"
        df["order_dim_key"] = df.apply(lambda x: str(uuid.uuid4()), axis=1)
        return df

    def add_updated_order_rows(self):
        orders_df = self.get_updated_order_rows_df()
        orders_df["order_dim_key"] = orders_df.apply(
            lambda x: str(uuid.uuid4()), axis=1
        )
        orders_df.to_sql(
            "cmetrics_orders",
            schema="public",
            con=self.db,
            if_exists="append",
            index=False,
        )

    def update_orders(self):
        self.cancel_previous_records()
        self.add_updated_order_rows()

    def get_trades_df(self) -> pd.DataFrame:
        updates_orders_df = self.get_updated_order_rows_df()
        trades_df = updates_orders_df.drop(
            columns=[
                "order_dim_key",
                "order_type",
                "order_creation_tmstmp",
                "order_status",
                "fill_pct",
            ]
        )
        trades_df = trades_df.drop_duplicates()
        for id_col in ("trade_dim_key", "trade_id"):
            trades_df[id_col] = trades_df.apply(lambda x: str(uuid.uuid4()), axis=1)
        for column in ("side", "price", "volume"):
            trades_df = trades_df.rename(columns={f"order_{column}": f"trade_{column}"})
        trades_df.loc[:, "insert_tmstmp"] = dt.now()
        trades_df["execution_tmstmp"] = (
            trades_df["order_id"].apply(lambda x: self.filled_orders[x])
            if updates_orders_df is not None
            else dt.now()
        )
        return trades_df

    def add_trades(self):
        trades_df = self.get_trades_df()
        trades_df.to_sql(
            "cmetrics_trades",
            schema="public",
            con=self.db,
            if_exists="append",
            index=False,
        )

    def update_db(self):
        self.update_orders()
        self.add_trades()

    def run_on_start_checker(self):
        self.check_all_open_orders()
        if self.filled_orders:
            self.update_db()


class OrderExecutionService(OnStartChecker):
    def __init__(self, verbose: bool = True):
        self.users = None
        self.verbose = verbose
        self.db = helpers.get_db_connection(local=False)
        self.orders = self.retrieve_from_db()
        super().__init__(open_orders=self.orders, db=self.db, verbose=verbose)

    def retrieve_from_db(self) -> dict:
        if self.verbose:
            LOG.info("Retrieving open orders from DB")
        query = (
            "select * from public.cmetrics_orders "
            "where order_status = 'open' and expiration_tmstmp is null"
        )
        df = pd.read_sql_query(sql=query, con=self.db)
        df.set_index("order_id", inplace=True)
        return df.to_dict(orient="index")

    def retrieve_from_redis(self, user_id: str) -> pd.DataFrame:
        redis_data = self.redis_client.get(user_id)
        return pd.DataFrame(json.loads(redis_data))

    @staticmethod
    def get_filled_qty(order: pd.Series, trade_data: dict) -> float:
        if (
            order["order_side"] == "buy" and trade_data["price"] < order["order_price"]
        ) or (
            order["order_side"] == "sell" and trade_data["price"] > order["order_price"]
        ):
            return order["order_volume"]
        if trade_data["price"] == order["order_price"]:
            return min(trade_data["amount"], order["order_volume"])
        return 0

    def handle_fills(self, filled_orders: pd.DataFrame):
        if not filled_orders.empty:
            filled_orders["fill_pct"] = filled_orders.apply(
                lambda x: round(x["filled_qty"] / x["order_volume"], 4), axis=1
            )
            filled_orders["order_status"] = filled_orders["fill_pct"].apply(
                lambda x: "executed" if x == 1 else "part_fill"
            )
            filled_orders["insert_tmstmp"] = dt.now()
            filled_orders["expiration_tmstmp"] = None
            filled_orders = helpers.datetime_unix_conversion(
                filled_orders, convert_to="timestamp", cols=["order_creation_tmstmp"]
            )
            filled_orders["order_volume"] = filled_orders["filled_qty"]
            filled_orders.drop(columns="filled_qty", inplace=True)
            trades_df = self.get_trades_df(filled_orders)
            self.update_orders(filled_orders)
            self.add_trades(trades_df)

    def check_fills(self, raw_trade_data: dict):
        trade_data = raw_trade_data
        trade_data = trade_data["trades"]
        orders = self.retrieve_from_redis("thomasbouamoud")
        trade_pair = trade_data["symbol"]
        trade_pair = trade_pair.replace("-", "/")
        orders = orders[orders["asset_id"] == trade_pair]
        orders["filled_qty"] = orders.apply(
            lambda x: self.get_filled_qty(x, trade_data), axis=1
        )
        orders["filled_qty"] = orders["order_volume"]
        filled_orders = orders[orders["filled_qty"] > 0]
        self.handle_fills(filled_orders)

    def run_service(self):
        self.run_on_start_checker()
        open_orders_redis_key = "{order-monitoring}-open-orders"
        helpers.REDIS_CON.xadd(
            open_orders_redis_key,
            {'open-orders': json.dumps(self.open_orders)},
            maxlen=len(self.open_orders),
            approximate=True,
        )
        all_streams = helpers.get_available_redis_streams()
        all_streams.append(open_orders_redis_key)
        streams = {stream: "$" for stream in all_streams}
        while True:
            data = helpers.REDIS_CON.xread(streams=streams, block=0)
            message = data[0][1][0][1]
            print(message)
            # Process(target=self.check_fills, args=(message,)).start()


#

if __name__ == "__main__":
    oes = OrderExecutionService()
    oes.run_service()
