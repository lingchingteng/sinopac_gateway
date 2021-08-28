"""
Gateway for Sinopac securities.
Author: xxxxcccvvvbbb@gmail.com
"""
import os
import sys
from collections import OrderedDict
from copy import copy
from datetime import datetime, timedelta
from typing import Type, List

from queue import Queue
import time

import shioaji as sj
from shioaji import shioaji
from shioaji.constant import (
    QuoteType,
    QuoteVersion,
    SecurityType,
    DayTrade,
    OptionRight,
    OrderState,
    Action,
    FuturesPriceType,
    FuturesOrderType,
    StockPriceType,
    TFTOrderType,
    TFTStockOrderLot,
    StockFirstSell,
)
from shioaji.account import FutureAccount, StockAccount
from shioaji.order import OrderStatus, Status as SinopacStatus
from shioaji.position import Position, AccountBalance
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OptionType,
    Product,
    Status,
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)


# 用來反查 shioaji 對應的 vnpy exchange 對應。
EXCHANGE_VT2SINOPAC = {
    Exchange.TAITSE: "TAITSE",
    Exchange.TAIOTC: "TAIOTC",
    Exchange.TAIOES: "TAIOES",
    Exchange.TAIFEX: "TAIFEX",
}
EXCHANGE_SINOPAC2VT = {v: k for k, v in EXCHANGE_VT2SINOPAC.items()}

STATUS_SINOPAC2VT = {
    SinopacStatus.Cancelled: Status.CANCELLED,
    SinopacStatus.Failed: Status.REJECTED,
    SinopacStatus.Filled: Status.ALLTRADED,
    SinopacStatus.PartFilled: Status.PARTTRADED,
    SinopacStatus.PreSubmitted: Status.SUBMITTING,
    SinopacStatus.Submitted: Status.NOTTRADED,
    SinopacStatus.PendingSubmit: Status.SUBMITTING,
    SinopacStatus.Inactive: Status.SUBMITTING,
}


def daterange(start_date, end_date):
    cur_date = start_date
    while cur_date <= end_date:
        yield cur_date
        cur_date += timedelta(days=1)


class SinopacGateway(BaseGateway):
    """
    VN Trader Gateway for Sinopac connection
    """

    default_setting = {
        "身份證字號": "",
        "密碼": "",
        "憑證檔案路徑": "",
        "憑證密碼": "",
        "環境": ["正式", "模擬"],
        "預設現貨帳號": "0",  # 帳號是列表，這邊會被轉成數字，是 index ，不是帳號數字。
        "預設期貨帳號": "0",  # 帳號是列表，這邊會被轉成數字，是 index ，不是帳號數字。
    }

    exchanges = list(EXCHANGE_SINOPAC2VT.values())

    def __init__(self, event_engine):
        """Constructor"""
        super(SinopacGateway, self).__init__(event_engine, "Sinopac")

        # cache 資料的變數
        # 紀錄有 subscrib 的商品
        self.subscribed = set()
        # 紀錄最新的 ticks 資料
        self.ticks = {}
        # 紀錄 symbol 對應的 shioaji 型態資料
        self.code2contract = {}
        # trade 好像是成交紀錄
        self.trades = set()
        # 用來記錄送出的 Order
        self.orders = OrderedDict()
        # 用來記錄哪一些 contracts 列表已經下載完成
        self.loaded_contract_lists = set()

        # 定期更新資料機制的參數
        # 用 process_timer_event 註冊 EVENT_TIMER 之後，
        # process_timer_event 每一秒會被呼叫一次，每呼叫 query_interval 次，
        # 會執行 query_funcs 中的第一個 method ，被執行過的 method 會再被放到 query_funcs 的最後面。
        self.query_interval = 20
        self.current_count = 0
        self.query_funcs = Queue()
        self.query_funcs.put(self.query_trade)
        self.query_funcs.put(self.query_position)
        self.query_funcs.put(self.query_account)

        # 永豐金的 Module 主體
        self.api = None

    def connect(self, setting: dict):
        """連線到永豐金證券期貨伺服器

        登入的 instance 會放在 self.api 。
        設定 callback 讓持續更新行情。

        """

        simulation = True if setting["環境"] == "模擬" else False
        self.write_log(f"使用永豐金證券 {setting['環境']} 平台")

        # 如果 api 不是 None ，先登出。
        if self.api:
            self.api.logout()

        # 使用永豐金 api
        self.api = sj.Shioaji(simulation=simulation)

        userid = setting["身份證字號"]
        password = setting["密碼"]
        try:
            # 登入的時候順便設定下載商品檔的 callback ，可以在 contract 表下載完成之後接到通知。
            self.api.login(userid, password, contracts_cb=self.cb_query_contract)
        except Exception as exc:
            self.write_log(f"登入失败. [{exc}]")
            return
        self.write_log(f"登入成功. [{userid}]")

        # 如果同一個帳戶在證券或是期貨帳戶有超過兩個以上的帳號就要設定
        self.select_default_account(setting.get("預設現貨帳號", 0), setting.get("預設期貨帳號", 0))

        if not simulation and setting["憑證檔案路徑"] != "":
            self.api.activate_ca(setting["憑證檔案路徑"], setting["憑證密碼"], setting["身份證字號"])
            self.write_log(f"{setting['身份證字號']} 憑證 已啟用.")

        # 設定訂閱報價的 callback
        self.api.quote.set_callback(self.cb_quote)
        # self.api.quote.set_on_tick_stk_v1_callback(self.cb_tick_quote_callback)
        # self.api.quote.set_on_bidask_stk_v1_callback(self.cb_bidask_quote_callback)

        # 註冊 EVENT_TIMER 事件 callback ，負責定期更新資料
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

        self.write_log("交易行情 - 連線成功")

    def cb_tick_quote_callback(self, exchange: sj.Exchange, tick: sj.TickSTKv1):
        """subscribe tick callback"""
        self.write_log(f"Exchange: {exchange}, Tick: {tick}")

    def cb_bidask_quote_callback(self, exchange: sj.Exchange, bidask: sj.BidAskSTKv1):
        """subscribe bidask callback"""
        self.write_log(f"Exchange: {exchange}, BidAsk: {bidask}")

    def close(self):
        """"""
        self.write_log("關閉 Sinopac gateway")
        if self.api:
            self.api.logout()

    def subscribe(self, req: SubscribeRequest):
        """"""
        vt_symbol = f"{req.symbol}.{req.exchange.value}"
        if vt_symbol in self.subscribed:
            return

        contract = self.code2contract.get(vt_symbol, None)
        if contract:
            # 等待報價進來之前，先抓目前的價位。這樣收盤時段也可以得到基本的報價
            self.get_contract_snapshot(contract)
            self.api.quote.subscribe(
                contract,
                quote_type=QuoteType.Tick,
                intraday_odd=False,
                version=QuoteVersion.v0,
            )
            self.api.quote.subscribe(
                contract,
                quote_type=QuoteType.BidAsk,
                intraday_odd=False,
                version=QuoteVersion.v0,
            )
            msg = f"訂閱 [{vt_symbol}] {contract.name}"
            if req.exchange == Exchange.TAIFEX:
                msg = f"訂閱 [{vt_symbol}] {contract.name}{contract.delivery_month}"
            self.write_log(msg)
            self.subscribed.add(vt_symbol)
        else:
            self.write_log(f"無此訂閱商品[{vt_symbol}]")

    def send_order(self, req: OrderRequest):
        """"""
        if req.exchange == Exchange.TAIFEX:
            action = Action.Buy if req.direction == Direction.LONG else Action.Sell
            price_type = FuturesPriceType.LMT
            order_type = FuturesOrderType.ROD
            order = self.api.Order(
                price=req.price,
                quantity=int(req.volume),
                action=action,
                price_type=price_type,
                order_type=order_type,
            )

        elif req.exchange == Exchange.TAITSE:
            action = Action.Buy if req.direction == Direction.LONG else Action.Sell
            price_type = StockPriceType.LMT
            order_type = TFTOrderType.ROD
            first_sell = (
                StockFirstSell.Yes
                if req.offset == Offset.CLOSETODAY
                else StockFirstSell.No
            )
            order = self.api.Order(
                price=req.price,
                quantity=int(req.volume),
                action=action,
                price_type=price_type,
                order_type=order_type,
                first_sell=first_sell,
            )
        symbol = f"{req.symbol}.{req.exchange.value}"
        trade = self.api.place_order(
            self.code2contract[symbol], order, 0, self.cb_place_order
        )
        orderdata = req.create_order_data(str(id(trade)), self.gateway_name)
        self.orders[orderdata.orderid] = trade
        self.on_order(orderdata)
        return orderdata.vt_orderid

    def cancel_order(self, req: CancelRequest):
        """"""
        self.write_log("***cancel_order")
        self.write_log(str(req))
        self.write_log(str(self.orders[req.orderid]))
        self.api.cancel_order(self.orders[req.orderid])

    def query_account(self):
        """查詢帳戶資金，Sinopac 目前只提供查詢證券帳戶資金"""
        self.api.account_balance(timeout=0, cb=self.cb_query_account)

    def cb_query_account(self, account_balances: List[AccountBalance]):
        """callback function of query_account"""
        for ab in account_balances:
            self.on_account(
                AccountData(
                    gateway_name=self.gateway_name,
                    accountid=self.api.stock_account.account_id,
                    balance=ab.acc_balance,
                    frozen=0,
                )
            )

    def query_position(self):
        """查詢持倉部位，未實現損益，Sinopac 目前只提供股票持倉的查詢"""
        self.api.list_positions(
            account=self.api.stock_account, timeout=0, cb=self.cb_query_position
        )

    def cb_query_position(self, positions: List[Position]):
        """callback function of query_position"""

        for position in positions:

            exchange = self.code2contract.get(f"{position}.{Exchange.TAITSE.value}", None)
            if exchange is None:
                exchange = self.code2contract.get(f"{position}.{Exchange.TAIOTC.value}", None)

            pos = PositionData(
                symbol=position.code,
                exchange=EXCHANGE_SINOPAC2VT.get("TWTSE", Exchange.TAITSE), 
                direction=Direction.LONG if position.quantity >= 0 else Direction.SHORT,
                volume=abs(position.quantity),
                frozen=position.quantity - position.quantity,
                price=position.price,
                pnl=position.pnl,
                yd_volume=position.yd_quantity,
                gateway_name=self.gateway_name,
            )
            self.on_position(pos)

    def query_trade(self):
        """查詢 Order 的結果"""
        self.api.update_status(timeout=0, cb=self.cb_query_trade)

    def cb_query_trade(self, trades: List[OrderStatus]):
        """callback function of query_trade"""
        # 這個 method 直接回傳 self._solace.trades 所以不會卡。
        trades = self.api.list_trades()

        for item in trades:
            # TODO: 這一行是在幹嘛？
            self.orders[str(id(item))] = item
            if item.status.status in [SinopacStatus.Filled]:  # 全部成交
                tradeid = item.status.id
                if tradeid in self.trades:
                    continue
                self.trades.add(tradeid)
                trade = TradeData(
                    symbol=item.contract.code,
                    exchange=EXCHANGE_SINOPAC2VT.get(
                        item.contract.exchange, Exchange.TAITSE
                    ),
                    direction=Direction.LONG
                    if item.order.action == "Buy"
                    else Direction.SHORT,
                    tradeid=tradeid,
                    orderid=str(id(item)),
                    price=float(item.order.price),
                    volume=float(item.order.quantity),
                    datetime=item.status.order_datetime,
                    gateway_name=self.gateway_name,
                )
                self.on_trade(trade)
            else:
                unVol = float(
                    item.order.quantity
                    - (item.status.deal_quantity + item.status.cancel_quantity)
                )
                order = OrderData(
                    symbol=item.contract.code,
                    exchange=EXCHANGE_SINOPAC2VT.get(
                        item.contract.exchange, Exchange.TAITSE
                    ),
                    orderid=str(id(item)),
                    direction=Direction.LONG
                    if item.order.action == "Buy"
                    else Direction.SHORT,
                    price=float(item.order.price),
                    volume=unVol,
                    traded=float(item.status.deal_quantity),
                    status=STATUS_SINOPAC2VT[item.status.status],
                    datetime=item.status.order_datetime,
                    gateway_name=self.gateway_name,
                )
                self.on_order(order)

    def query_history(self, req: HistoryRequest):
        """下載歷史資料"""
        if req.interval != Interval.MINUTE:
            self.write_log("query_history 目前只支援一分 K")
            return []

        vt_symbol = f"{req.symbol}.{req.exchange.value}"
        contract = self.code2contract.get(vt_symbol, None)
        if contract is None:
            self.write_log(f"query_history 無此合約 [{vt_symbol}]")
            return []

        kbars = self.api.kbars(
            contract=contract,
            start=req.start.strftime("%Y-%m-%d"),
            end=req.end.strftime("%Y-%m-%d"),
        )

        data = []
        for ts, open, high, low, close, volume in zip(
            kbars.ts, kbars.Open, kbars.High, kbars.Low, kbars.Close, kbars.Volume
        ):
            data.append(
                BarData(
                    gateway_name=self.gateway_name,
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=datetime.fromtimestamp(ts / 1000000000),
                    interval=req.interval,
                    volume=volume,
                    open_price=open,
                    high_price=high,
                    low_price=low,
                    close_price=close,
                )
            )
        return data

    def cb_query_contract(self, securities_type: Type[SecurityType] = None):
        if securities_type == SecurityType.Future:
            for category in self.api.Contracts.Futures:
                for contract in category:
                    # self.write_log(f"code: {contract.code} symbol: {contract.symbol} name:{contract.name} delivery_month: {contract.delivery_month}")
                    # code 跟 symbol 不一樣：TXFI1 == TXF202109
                    # 結果中文的期貨名稱居然變成 (鴻海期貨) 這樣。

                    data = ContractData(
                        symbol=contract.symbol,
                        exchange=Exchange.TAIFEX,
                        name=contract.name + contract.delivery_month,
                        product=Product.FUTURES,
                        size=200,
                        pricetick=0.01,
                        stop_supported=False,
                        net_position=True,
                        min_volume=1,
                        gateway_name=self.gateway_name,
                        history_data=True,
                    )
                    self.on_contract(data)
                    vt_symbol = f"{contract.symbol}.{Exchange.TAIFEX.value}"
                    self.code2contract[vt_symbol] = contract
            self.loaded_contract_lists.add(securities_type)

        if securities_type == SecurityType.Option:
            for category in self.api.Contracts.Options:
                for contract in category:
                    data = ContractData(
                        symbol=contract.code,
                        exchange=Exchange.TAIFEX,
                        name=f"{contract.name} {contract.delivery_month} {contract.strike_price}{contract.option_right}",
                        product=Product.OPTION,
                        size=50,
                        net_position=True,
                        pricetick=0.01,
                        min_volume=1,
                        gateway_name=self.gateway_name,
                        option_strike=contract.strike_price,
                        option_underlying=contract.underlying_code,
                        option_type=OptionType.CALL
                        if contract.option_right == OptionRight.Call
                        else OptionType.PUT,
                        option_expiry=None,
                    )
                    self.on_contract(data)
                    vt_symbol = f"{contract.code}.{Exchange.TAIFEX.value}"
                    self.code2contract[vt_symbol] = contract
            self.loaded_contract_lists.add(securities_type)

        if securities_type == SecurityType.Stock:
            for category in self.api.Contracts.Stocks:
                for contract in category:
                    pricetick = 5
                    if contract.limit_down < 10:
                        pricetick = 0.01
                    elif contract.limit_down < 50:
                        pricetick = 0.05
                    elif contract.limit_down < 100:
                        pricetick = 0.1
                    elif contract.limit_down < 500:
                        pricetick = 0.5
                    elif contract.limit_down < 1000:
                        pricetick = 1

                    exchange = shioaji_exchange_to_vnpy_exchange(contract.exchange)
                    data = ContractData(
                        symbol=contract.code,
                        exchange=exchange,
                        name=f"{contract.name} (不可當沖)"
                        if contract.day_trade != DayTrade.Yes
                        else contract.name,
                        product=taiwan_stock_code_to_vnpy_product(contract.code),
                        size=1,
                        net_position=False,
                        pricetick=pricetick,
                        min_volume=1,
                        gateway_name=self.gateway_name,
                        history_data=True,
                    )
                    self.on_contract(data)
                    vt_symbol = f"{contract.code}.{exchange.value}"
                    self.code2contract[vt_symbol] = contract
            self.loaded_contract_lists.add(securities_type)

        if securities_type == SecurityType.Index:
            for category in self.api.Contracts.Indexs:
                for contract in category:
                    exchange = shioaji_exchange_to_vnpy_exchange(contract.exchange)
                    data = ContractData(
                        symbol=contract.code,
                        exchange=exchange,
                        name=f"{contract.name} (指數)",
                        product=Product.INDEX,
                        size=0,
                        pricetick=0,
                        gateway_name=self.gateway_name,
                        history_data=True,
                    )
                    self.on_contract(data)
                    vt_symbol = f"{contract.code}.{exchange.value}"
                    self.code2contract[vt_symbol] = contract
            self.loaded_contract_lists.add(securities_type)

        self.write_log(f"下載商品檔 {securities_type} 完成")

    def get_contract_snapshot(self, contract):
        snapshot = self.api.quote.snapshots([contract])[0]
        code = snapshot.code
        exchange = shioaji_exchange_to_vnpy_exchange(snapshot.exchange)
        symbol = f"{code}.{exchange.value}"
        tick = self.ticks.get(symbol, None)
        if tick is None:
            self.code2contract[symbol] = contract
            if exchange == Exchange.TAIFEX:
                name = f"{contract['name']}{contract['delivery_month']}"
            else:
                name = f"{contract['name']}"
            tick = TickData(
                symbol=code,
                exchange=exchange,
                name=name,
                datetime=datetime.fromtimestamp(snapshot.ts / 1000000000 - 8 * 60 * 60),
                gateway_name=self.gateway_name,
            )
        tick.volume = snapshot.total_volume
        tick.last_price = snapshot.close
        tick.limit_up = contract.limit_up
        tick.open_interest = 0
        tick.limit_down = contract.limit_down
        tick.open_price = snapshot.open
        tick.high_price = snapshot.high
        tick.low_price = snapshot.low
        tick.pre_close = contract.reference
        tick.bid_price_1 = snapshot.buy_price
        tick.bid_volume_1 = snapshot.buy_volume
        tick.ask_price_1 = snapshot.sell_price
        tick.ask_volume_1 = snapshot.sell_volume
        self.ticks[symbol] = tick
        self.on_tick(copy(tick))

    def cb_place_order(self, trade: sj.order.Trade):
        self.orders[id(trade)] = trade
        if trade.status.status in [SinopacStatus.Filled]:  # 成交
            tradeid = trade.status.id
            trade = TradeData(
                symbol=trade.contract.code,
                exchange=EXCHANGE_SINOPAC2VT.get(
                    trade.contract.exchange, Exchange.TAITSE
                ),
                direction=Direction.LONG
                if trade.order.action == "Buy"
                else Direction.SHORT,
                tradeid=tradeid,
                orderid=str(id(trade)),
                price=float(trade.order.price),
                volume=float(trade.order.quantity),
                datetime=trade.status.order_datetime,
                gateway_name=self.gateway_name,
            )
            self.on_trade(trade)
        else:
            order = OrderData(
                symbol=trade.contract.code,
                exchange=EXCHANGE_SINOPAC2VT.get(
                    trade.contract.exchange, Exchange.TAITSE
                ),
                orderid=str(id(trade)),
                direction=Direction.LONG
                if trade.order.action == "Buy"
                else Direction.SHORT,
                price=float(trade.order.price),
                volume=float(
                    trade.order.quantity
                    - (trade.status.deal_quantity + trade.status.cancel_quantity)
                ),
                traded=float(trade.status.deal_quantity),
                status=STATUS_SINOPAC2VT[trade.status.status],
                datetime=trade.status.order_datetime,
                gateway_name=self.gateway_name,
            )
            self.on_order(order)

    def cb_quote(self, topic, data):
        """
        # L/TFE/TXFF9
        {'Amount': [21088.0], 'AmountSum': [1028165646.0], 'AvgPrice': [10562.513699263414],
         'Close': [10544.0], 'Code': 'TXFF9', 'Date': '2019/05/16', 'DiffPrice': [-37.0],
         'DiffRate': [-0.34968339476419996], 'DiffType': [4], 'High': [10574.0],
         'Low': [10488.0], 'Open': 10537.0, 'TargetKindPrice': 10548.47, 'TickType': [2],
         'Time': '11:15:11.911000', 'TradeAskVolSum': 52599, 'TradeBidVolSum': 53721,
         'VolSum': [97341], 'Volume': [2]}
        # Q/TFE/TXFF9
        {'AskPrice': [10545.0, 10546.0, 10547.0, 10548.0, 10549.0], 'AskVolSum': 262,
         'AskVolume': [17, 99, 59, 45, 42], 'BidPrice': [10544.0, 10543.0, 10542.0, 10541.0, 10540.0],
         'BidVolSum': 289, 'BidVolume': [16, 41, 32, 123, 77], 'Code': 'TXFF9', 'Date': '2019/05/16',
         'DiffAskVol': [0, 0, 0, -1, 0], 'DiffAskVolSum': -1, 'DiffBidVol': [0, 0, 0, 0, 0], 'DiffBidVolSum': 0,
         'FirstDerivedAskPrice': 10547.0, 'FirstDerivedAskVolume': 1, 'FirstDerivedBidPrice': 10542.0,
         'FirstDerivedBidVolume': 1, 'TargetKindPrice': 10548.47, 'Time': '11:15:11.911000'}
        # QUT/idcdmzpcr01/TSE/2330
        {'AskPrice': [248.0, 248.5, 249.0, 249.5, 250.0], 'AskVolume': [355, 632, 630, 301, 429],
         'BidPrice': [247.5, 247.0, 246.5, 246.0, 245.5], 'BidVolume': [397, 389, 509, 703, 434],
         'Date': '2019/05/17', 'Time': '09:53:00.706928'}
        """
        try:
            topics = topic.split("/")
            realtime_type = topics[0]
            tick = None
            if realtime_type == "L":
                tick = self.quote_futures_L(data)
            elif realtime_type == "Q":
                tick = self.quote_futures_Q(data)
            elif realtime_type == "MKT":
                tick = self.quote_stock_MKT(topics[3], data)
            elif realtime_type == "QUT":
                tick = self.quote_stock_QUT(topics[3], data)
            else:
                self.write_log("not support quote")
                self.write_log(topic)
                self.write_log(data)

            if tick:
                tick.open_interest = 0
                self.on_tick(copy(tick))
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.write_log(
                "except: [{}][{}][{}][{}]".format(
                    exc_type, filename, exc_tb.tb_lineno, str(e)
                )
            )
            self.write_log(data)

    def quote_futures_L(self, data):
        code = data.get("Code", None)
        if code is None:
            return
        symbol = f"{code}.TFE"
        tick = self.ticks.get(symbol, None)
        if tick is None:
            contract = self.code2contract.get(symbol, None)
            self.get_contract_snapshot(contract)
            tick = self.ticks.get(symbol, None)

        tick.datetime = datetime.strptime(
            "{} {}".format(data["Date"], data["Time"]), "%Y/%m/%d %H:%M:%S.%f"
        )
        tick.volume = int(data["VolSum"][0])
        tick.last_price = data["Close"][0]
        tick.limit_up = 0
        tick.open_interest = 0
        tick.limit_down = 0
        tick.open_price = data["Open"]
        tick.high_price = data["High"][0]
        tick.low_price = data["Low"][0]
        tick.pre_close = data["Close"][0] - data["DiffPrice"][0]
        return tick

    def quote_stock_MKT(self, code, data):
        """
        QUT/idcdmzpcr01/TSE/2330
        {'AskPrice': [248.0, 248.5, 249.0, 249.5, 250.0], 'AskVolume': [355, 632, 630, 301, 429],
        'BidPrice': [247.5, 247.0, 246.5, 246.0, 245.5], 'BidVolume': [397, 389, 509, 703, 434],
         'Date': '2019/05/17', 'Time': '09:53:00.706928'}
        MKT/idcdmzpcr01/TSE/2330
        {'Close': [248.0], 'Time': '09:53:00.706928', 'VolSum': [7023], 'Volume': [1]}
        """
        symbol = f"{code}.TSE"
        tick = self.ticks.get(symbol, None)
        if tick is None:
            contract = self.code2contract[symbol]
            self.get_contract_snapshot(contract)
            tick = self.ticks.get(symbol, None)

        tick.datetime = datetime.combine(
            datetime.today(),
            datetime.strptime("{}".format(data["Time"]), "%H:%M:%S.%f").time(),
        )
        tick.volume = int(data["VolSum"][0])
        tick.last_price = data["Close"][0]
        tick.open_price = data["Close"][0] if tick.open_price == 0 else tick.open_price
        tick.high_price = (
            data["Close"][0] if data["Close"][0] > tick.high_price else tick.high_price
        )
        tick.low_price = (
            data["Close"][0] if data["Close"][0] < tick.low_price else tick.low_price
        )
        return tick

    def quote_futures_Q(self, data):
        code = data.get("Code", None)
        if code is None:
            return
        symbol = f"{code}.TFE"
        return self.update_orderbook_tick(data, symbol)

    def quote_stock_QUT(self, code, data):
        symbol = f"{code}.TSE"
        return self.update_orderbook_tick(data, symbol)

    def update_orderbook_tick(self, data, symbol):
        tick = self.ticks.get(symbol, None)
        if tick is None:
            contract = self.code2contract[symbol]
            self.get_contract_snapshot(contract)
            tick = self.ticks.get(symbol, None)
        tick.bid_price_1 = data["BidPrice"][0]
        tick.bid_price_2 = data["BidPrice"][1]
        tick.bid_price_3 = data["BidPrice"][2]
        tick.bid_price_4 = data["BidPrice"][3]
        tick.bid_price_5 = data["BidPrice"][4]
        tick.ask_price_1 = data["AskPrice"][0]
        tick.ask_price_2 = data["AskPrice"][1]
        tick.ask_price_3 = data["AskPrice"][2]
        tick.ask_price_4 = data["AskPrice"][3]
        tick.ask_price_5 = data["AskPrice"][4]
        tick.bid_volume_1 = data["BidVolume"][0]
        tick.bid_volume_2 = data["BidVolume"][1]
        tick.bid_volume_3 = data["BidVolume"][2]
        tick.bid_volume_4 = data["BidVolume"][3]
        tick.bid_volume_5 = data["BidVolume"][4]
        tick.ask_volume_1 = data["AskVolume"][0]
        tick.ask_volume_2 = data["AskVolume"][1]
        tick.ask_volume_3 = data["AskVolume"][2]
        tick.ask_volume_4 = data["AskVolume"][3]
        tick.ask_volume_5 = data["AskVolume"][4]
        return tick

    def select_default_account(self, select_stock_number, select_futures_number):
        stock_account_count = 0
        futures_account_count = 0
        for acc in self.api.list_accounts():
            if isinstance(acc, StockAccount):
                self.write_log(
                    f"股票帳號: [{stock_account_count}] - {acc.broker_id}-{acc.account_id} {acc.username}"
                )
                stock_account_count += 1
            if isinstance(acc, FutureAccount):
                self.write_log(
                    f"期貨帳號: [{futures_account_count}] - {acc.broker_id}-{acc.account_id} {acc.username}"
                )
                futures_account_count += 1

        if stock_account_count >= 2:
            acc = self.api.list_accounts()[int(select_stock_number)]
            self.api.set_default_account(acc)
            self.write_log(
                f"***預設 現貨下單帳號 - [{select_stock_number}] {acc.broker_id}-{acc.account_id} {acc.username}"
            )

        if futures_account_count >= 2:
            acc = self.api.list_accounts()[int(select_futures_number)]
            self.api.set_default_account(acc)
            self.write_log(
                f"***預設 期貨下單帳號 - [{select_futures_number}] {acc.broker_id}-{acc.account_id} {acc.username}"
            )

    def process_timer_event(self, event):
        """定期更新資料"""
        self.current_count += 1
        if self.current_count < self.query_interval:
            return
        self.current_count = 0
        func = self.query_funcs.get()
        func()
        self.query_funcs.put(func)

def shioaji_exchange_to_vnpy_exchange(shioaji_exchange):
    if shioaji_exchange == sj.Exchange.TSE:
        return Exchange.TAITSE
    elif shioaji_exchange == sj.Exchange.OTC:
        return Exchange.TAIOTC
    elif shioaji_exchange == sj.Exchange.OES:
        return Exchange.TAIOES
    elif shioaji_exchange == sj.Exchange.TAIFEX:
        return Exchange.TAIFEX
    else:
        raise ValueError(f"shioaji 沒有 {shioaji_exchange} 交易所")

def taiwan_stock_code_to_vnpy_product(code):
    import re

    # 參考文件
    # https://www.twse.com.tw/downloads/zh/products/stock_cod.pdf

    # 一般股票
    if re.match(r"^[1-9]\d{3}$", code):
        return Product.EQUITY
    # ETF: https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y
    elif re.match(r"00\d{2,3}[KLRUB]?", code):
        return Product.ETF
    # ETN: https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y
    elif re.match(r"02[\dA-Z]{4}", code):
        return Product.EQUITY
    # 轉換公司債:於四位股票代號後，依發行先後次序加一至二位流水碼 1-99(可重複使用)。
    elif re.match(r"^[1-9]\d{3}[1-9][0-9]?$", code):
        return Product.BOND
    # 交換公司債或交換金融債：於四位股票代號後，依發行先後次序加二位流水碼 01-09(可重複使用)。
    elif re.match(r"^[1-9]\d{3}0[1-9]$", code):
        return Product.BOND
    # 可轉換公司債換股權利證書：於四位股票代號後，加一位英文字母 X-Z。
    elif re.match(r"^[1-9]\d{3}[XYZ]$", code):
        return Product.EQUITY
    # 特別股：於四位股票代號後加英文字母 A-W。
    elif re.match(r"^[1-9]\d{3}[A-W]$", code):
        return Product.EQUITY
    # 認股權憑證：於四位股票代號後加英文字母 G，第六位則為 1-9。
    elif re.match(r"^[1-9]\d{3}G[1-9]$", code):
        return Product.EQUITY
    # 附認股權特別股：於四位股票代號後加一位英文字母 G，第六位則為英文字母 A-C。
    elif re.match(r"^[1-9]\d{3}G[A-C]$", code):
        return Product.EQUITY
    # 附認股權公司債：於四位股票代號後加一位英文字母 G，第六位則為英文字母 D-L。
    elif re.match(r"^[1-9]\d{3}G[D-L]$", code):
        return Product.BOND
    # 附認股權公司債履約或分拆後之公司債：於四位股票代號後加英文字母 F，第六碼為 1-9。
    elif re.match(r"^[1-9]\d{3}F[1-9]$", code):
        return Product.BOND
    # 認購（售）權證：
    # 以國內證券或指數為標的之認購權證：前二碼為數字，後加四位流水編號。
    elif re.match(r"^\d{2}\d{4}$", code):
        return Product.WARRANT
    # 以國內證券或指數為標的之認售權證：前二碼為數字，後加三位流水編號，第六碼為英文字母 P。
    elif re.match(r"^\d{2}\d{3}P$", code):
        return Product.WARRANT
    # 以外國證券或指數為標的之認購權證：前二碼為數字，後加三位流水編號，第六碼為英文字母 F。
    elif re.match(r"^\d{2}\d{3}F$", code):
        return Product.WARRANT
    # 以外國證券或指數為標的之認售權證：前二碼為數字，後加三位流水編號，第六碼為英文字母 Q。
    elif re.match(r"^\d{2}\d{3}Q$", code):
        return Product.WARRANT
    # 以國內證券或指數為標的之「下限型認購權證（牛證）」：前二碼為數字，後加三位流水編號，第六碼為英文字母 C。
    elif re.match(r"^\d{2}\d{3}C$", code):
        return Product.WARRANT
    # 以國內證券或指數為標的之「上限型認售權證（熊證）」：前二碼為數字，後加三位流水編號，第六碼為英文字母 B。
    elif re.match(r"^\d{2}\d{3}B$", code):
        return Product.WARRANT
    # 以國內證券或指數為標的之「可展延下限型認購權證（牛證）」：前二碼為數字，後加三位流水編號，第六碼為英文字母 X。
    elif re.match(r"^\d{2}\d{3}X$", code):
        return Product.WARRANT
    # 以國內證券或指數為標的之「可展延上限型認售權證（熊證）」：前二碼為數字，後加三位流水編號，第六碼為英文字母 Y。
    elif re.match(r"^\d{2}\d{3}Y$", code):
        return Product.WARRANT
    # 開放式基金受益憑證：英文字母 T 加五位英文數字(二碼公司別+二碼基金別+一碼類別)。
    elif re.match(r"^T[\dA-Z]{5}$", code):
        return Product.FUND
    # 黃金現貨
    # AU9901:台銀金, AU9902 一銀金
    elif code in ["AU9902", "AU9901"]:
        return Product.SPOT
    # 不動產資產信託之受益證券：前二碼為數字，後加三位流水編號，第六碼為英文字母 P。
    elif re.match(r"^\d{2}\d{3}P$", code):
        return Product.EQUITY
    # 不動產資產信託之受益證券：前二碼為數字，後加三位流水編號，第六碼為英文字母 P。
    elif re.match(r"^\d{2}\d{3}T$", code):
        return Product.EQUITY
    else:
        raise ValueError(f"{code} 無法判斷")
