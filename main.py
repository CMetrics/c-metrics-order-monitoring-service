import json
import logging
import os
import uuid
from datetime import datetime as dt
from multiprocessing import Process

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
        tmstmp = dt.strptime(order["order_creation_tmstmp"], "%Y-%m-%d %H:%M:%S.%f%z")
        execution_df = ohlcv_df[ohlcv_df["time"] >= dt.timestamp(tmstmp) * 1000]
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

    def cancel_previous_records(self, filled_orders: list):
        if not filled_orders:
            filled_orders = self.filled_orders
        order_id_list = list(filled_orders.keys())
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

    def get_updated_order_rows_df(self, filled_orders: list = None) -> pd.DataFrame:
        df = pd.DataFrame()
        if not filled_orders:
            filled_orders = self.filled_orders
        for order_id in filled_orders:
            order_df = pd.DataFrame([self.open_orders[order_id]])
            order_df["order_id"] = order_id
            df = pd.concat([df, order_df])
        df.loc[:, "insert_tmstmp"] = dt.now()
        df.loc[:, "fill_pct"] = 1
        df.loc[:, "order_status"] = "executed"
        df["order_dim_key"] = df.apply(lambda x: str(uuid.uuid4()), axis=1)
        return df

    def add_updated_order_rows(self, filled_orders: list = None):
        orders_df = self.get_updated_order_rows_df(filled_orders)
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

    def update_orders(self, filled_orders: list = None):
        self.cancel_previous_records(filled_orders)
        self.add_updated_order_rows(filled_orders)

    def get_trades_df(self, filled_orders: list = None) -> pd.DataFrame:
        updates_orders_df = self.get_updated_order_rows_df(filled_orders)
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

    def add_trades(self, trades_df: pd.DataFrame = None):
        if trades_df is None:
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
        self.open_orders_redis_key = "{order-monitoring}-open-orders"
        super().__init__(open_orders=self.orders, db=self.db, verbose=verbose)

    def retrieve_from_db(self) -> dict:
        if self.verbose:
            LOG.info("Retrieving open orders from DB")
        query = (
            "select * from public.cmetrics_orders "
            "where order_status = 'open' and expiration_tmstmp is null"
        )
        df = pd.read_sql_query(sql=query, con=self.db)
        for tmstmp_col in ("insert_tmstmp", "order_creation_tmstmp"):
            df[tmstmp_col] = df[tmstmp_col].astype(str)
        df.set_index("order_id", inplace=True)
        return df.to_dict(orient="index")

    @staticmethod
    def get_filled_qty(order: pd.Series, trade_data: dict) -> float:
        if (
            order["order_side"] == "buy"
            and float(trade_data["price"]) < order["order_price"]
        ) or (
            order["order_side"] == "sell"
            and float(trade_data["price"]) > order["order_price"]
        ):
            return order["order_volume"]
        if float(trade_data["price"]) == order["order_price"]:
            return min(float(trade_data["amount"]), order["order_volume"])
        return 0

    def handle_fills(self, filled_orders: list):
        if filled_orders:
            for order in filled_orders:
                self.open_orders.pop(order["order_id"])
                order["fill_pct"] = round(
                    order["filled_qty"] / order["order_volume"], 4
                )
                order["order_status"] = (
                    "executed" if order["fill_pct"] == 1 else "part_fill"
                )
                order["insert_tmstmp"] = dt.now()
                order["expiration_tmstmp"] = None
                order = helpers.datetime_unix_conversion(
                    order, convert_to="timestamp", cols=["order_creation_tmstmp"]
                )
                order["order_volume"] = order["filled_qty"]
                order.pop("filled_qty")
        trades_df = self.get_trades_df(filled_orders)
        self.update_orders(filled_orders)
        self.add_trades(trades_df)
        helpers.REDIS_CON.xadd(
            self.open_orders_redis_key,
            {"open-orders": json.dumps(self.open_orders)},
            maxlen=1,
            approximate=True,
        )

    def check_fills(self, raw_trade_data: dict):
        trade_data = raw_trade_data
        trade_pair = trade_data["symbol"]
        trade_pair = trade_pair.replace("-", "/")
        filled_orders = list()
        for order in self.open_orders.values():
            if order["asset_id"] == trade_pair:
                fill_qty = self.get_filled_qty(order, trade_data)
                base_log = f"{order['broker_id']} {order['asset_id']} {order['trading_type']} {order['order_side']} "
                if fill_qty:
                    LOG.info(
                        f"{base_log} EXECUTED: {order['order_volume']} @ {order['order_price']}"
                    )
                    filled_orders.append(order)
                else:
                    last_close = float(trade_data["price"])
                    distance_to_exec = (
                        last_close / order["order_price"]
                        if order["order_side"] == "buy"
                        else order["order_price"] / last_close
                    ) - 1
                    LOG.info(
                        f"{base_log} not executed: {distance_to_exec:.2%} away from target price"
                    )
        if filled_orders:
            self.handle_fills(filled_orders)

    def run_service(self):
        self.run_on_start_checker()
        helpers.REDIS_CON.xadd(
            self.open_orders_redis_key,
            {"open-orders": json.dumps(self.open_orders)},
            maxlen=1,
            approximate=True,
        )
        all_streams = helpers.get_available_redis_streams()
        streams = {stream: "$" for stream in all_streams}
        while True:
            data = helpers.REDIS_CON.xread(streams=streams, block=0)
            self.open_orders = json.loads(
                helpers.REDIS_CON.xrange(self.open_orders_redis_key, "-", "+")[0][1][
                    "open-orders"
                ]
            )
            message = data[0][1][0][1]
            Process(target=self.check_fills, args=(message,)).start()


if __name__ == "__main__":
    oes = OrderExecutionService()
    oes.run_service()
