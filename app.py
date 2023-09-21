import quickfix as fix
import time
import random
from datetime import datetime
import json

SEND_DICT = {
    "8": "BeginString",
    "9": "BodyLength",
    "35": "MsgType",
    "34": "MsgSeqNum",
    "49": "SenderCompID",
    "52": "SendingTime",
    "56": "TargetCompID",
    "1": "Account",
    "11": "ClOrdID",
    "15": "Currency",
    "21": "HandlInst",
    "38": "OrderQty",
    "40": "OrdType",
    "44": "Price",
    "54": "Side",
    "55": "Symbol",
    "58": "Text",
    "59": "TimeInForce",
    "60": "TransactTime",
    "167": "SecurityType",
    "207": "SecurityExchange",
    # additional
    "10": "CheckSum",
    # for cancel
    "41": "OrigClOrdID",
}

EXECTION_DICT = {
    "8": "BeginString",
    "9": "BodyLength",
    "35": "MsgType",
    "34": "MsgSeqNum",
    "49": "SenderCompID",
    "52": "SendingTime",
    "56": "TargetCompID",
    "1": "Account",
    "6": "AvgPx",
    "11": "ClOrdID",
    "14": "CumQty",
    "17": "ExecID",
    "20": "ExecTransType",
    "21": "HandlInst",
    "31": "LastPx",
    "32": "LastShares",
    "37": "OrderID",
    "38": "OrderQty",
    "39": "OrdStatus",
    "40": "OrdType",
    "44": "Price",
    "54": "Side",
    "55": "Symbol",
    "59": "TimeInForce",
    "60": "TransactTime",
    "150": "ExecType",
    "151": "LeavesQty",
    "167": "SecurityType",
    "207": "SecurityExchange",
    # additional
    "10": "CheckSum",
    # for cancel
    "41": "OrigClOrdID",
}


class FixClient(fix.Application):
    """docstring for FixClient."""

    def __init__(self, config_name: str):
        super().__init__()
        self.settings_ = fix.SessionSettings(config_name)
        self.order_ids = set()

    def run(self):
        store_fac = fix.FileStoreFactory(self.settings_)
        log_fac = fix.FileLogFactory(self.settings_)
        session_factory = fix.SocketInitiator(self, store_fac, self.settings_, log_fac)
        session_factory.start()
        # Wait for the session to logon
        while not session_factory.isLoggedOn():
            time.sleep(1)
            print(f"try logon...")
        self.choose_option()

    def choose_option(self):
        while True:
            op = input("o:order, c:cancel:\n").lower()
            if op == "o":
                # self.send_order("000001.SZ", 1, 11.8, 100)
                self.send_order("600000.SS", 1, 7.15, 100)
            elif op == "c":
                self.cancel_all()
            else:
                pass

    def send_order(
        self,
        secucode: str,
        direction: int,
        price: float,
        volume: int,
        price_type: int = 2,
        handle_type: str = "DMA",
    ):
        code, mkt = secucode.split(".")
        direction = fix.Side_BUY if direction == 1 else fix.Side_SELL

        order_fields = fix.Message()
        order_fields.getHeader().setField(fix.MsgType("D"))  # D, New Order Single
        order_fields.setField(fix.Account("122701"))
        rand_id = random.randint(1000000, 2000000)
        order_fields.setField(fix.ClOrdID(str(rand_id)))
        order_fields.setField(fix.Currency("CNY"))
        order_fields.setField(fix.HandlInst("1"))  # # 1: DAM; 3: CARE
        order_fields.setField(fix.OrderQty(volume))
        order_fields.setField(fix.OrdType("2"))
        order_fields.setField(fix.Price(price))
        order_fields.setField(fix.Side(direction))
        order_fields.setField(fix.Symbol(code))
        # order_fields.setField(fix.TimeInForce('0'))
        trstime = fix.TransactTime()
        trstime.setString(datetime.utcnow().strftime("%Y%m%d-%H:%M:%S"))
        order_fields.setField(trstime)
        order_fields.setField(fix.SecurityType("CS"))
        order_fields.setField(fix.SecurityExchange(mkt))  # SS,SZ
        fix.Session.sendToTarget(order_fields, self.session_id)

    def cancel_order(self, origin_order_id: str):
        cancel_fields = fix.Message()
        cancel_fields.getHeader().setField(fix.MsgType("F"))  # F, Order Cancel Request
        cancel_fields.setField(fix.Account("122701"))
        rand_id = random.randint(1000000, 2000000)
        cancel_fields.setField(fix.ClOrdID(str(rand_id)))
        cancel_fields.setField(fix.OrderQty(100))
        cancel_fields.setField(fix.OrdType("2"))
        cancel_fields.setField(fix.OrigClOrdID(origin_order_id))
        cancel_fields.setField(fix.Side(fix.Side_BUY))
        cancel_fields.setField(fix.Symbol("600000"))
        trstime = fix.TransactTime()
        trstime.setString(datetime.utcnow().strftime("%Y%m%d-%H:%M:%S"))
        cancel_fields.setField(trstime)
        cancel_fields.setField(fix.SecurityType("CS"))
        cancel_fields.setField(fix.SecurityExchange("SS"))
        cancel_fields.setField(fix.HandlInst("1"))
        # cancel_fields.setField(fix.TimeInForce('0'))
        cancel_fields.setField(fix.Price(7.15))
        cancel_fields.setField(fix.Currency("CNY"))
        fix.Session.sendToTarget(cancel_fields, self.session_id)

    def cancel_all(self):
        for orgin_order_id in self.order_ids:
            self.cancel_order(orgin_order_id)

    def onCreate(self, sessionID):
        print(f"session  crated: {sessionID}")

    def onLogon(self, sessionID):
        self.session_id = sessionID
        print(f"session  logon: {sessionID}")

    def onLogout(self, sessionID):
        print(f"session  logout: {sessionID}")

    def fromAdmin(self, msg, sessionID):
        # print(f"Admin receive: {msg}")
        pass

    def toAdmin(self, msg, sessionID):
        # print(f"Admin send: {msg}")
        pass

    def fromApp(self, msg: fix.Message, sessionID):
        msg_type = fix.MsgType()
        exec_type = fix.ExecType()
        trans_type = exec_type
        msg.getHeader().getField(msg_type)
        msg.getField(exec_type)
        msg.getField(trans_type)
        if msg_type.getValue() == fix.MsgType_ExecutionReport:
            fields = msg.toString().split(chr(1))
            record = {}
            for item in fields:
                if item:
                    key, value = item.split("=")
                    # name = EXECTION_DICT.get(key, key)
                    name = EXECTION_DICT[key]
                    record[name] = value
            print(f"APP RECEIVE: {record}")
            self.order_ids.add(record["ClOrdID"])

    def toApp(self, msg, sessionID):
        fields = msg.toString().split(chr(1))
        record = {}
        for item in fields:
            if item:
                key, value = item.split("=")
                # name = EXECTION_DICT.get(key, key)
                name = SEND_DICT[key]
                record[name] = value
        print(f"APP SEND: {record}")
