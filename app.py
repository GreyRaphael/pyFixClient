import quickfix as fix
import time
import random
from datetime import datetime


class FixClient(fix.Application):
    """docstring for FixClient."""

    def __init__(self, config_name: str):
        super().__init__()
        self.settings_ = fix.SessionSettings(config_name)

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
            op = input("q:query, o:order, c:cancel:")
            if op == "q":
                pass
            elif op == "o":
                self.send_order("000001.SZ", 1, 11.18, 200)
            elif op == "c":
                pass
            else:
                pass

    def send_order(self, secucode: str, direction: int, price: float, volume: int, price_type: int = 2, handle_type: str = "DMA"):
        code, mkt = secucode.split(".")
        direction = fix.Side_BUY if direction == 1 else fix.Side_SELL
        handle_inst = 3 if handle_type == "CARE" else 1  # 1: DAM; 3: CARE

        order_fields = fix.Message()
        order_fields.getHeader().setField(fix.MsgType("D"))  # D, New Order Single
        order_fields.setField(fix.Account("122701"))
        rand_id = random.randint(1000000, 2000000)
        order_fields.setField(fix.ClOrdID(str(rand_id)))
        order_fields.setField(fix.Currency("CNY"))
        order_fields.setField(fix.HandlInst("1"))
        order_fields.setField(fix.OrderQty(volume))
        order_fields.setField(fix.OrdType("2"))
        order_fields.setField(fix.Price(price))
        order_fields.setField(fix.Side(direction))
        order_fields.setField(fix.Symbol(code))
        # order_fields.setField(fix.TimeInForce('0'))
        trstime = fix.TransactTime()
        trstime.setString(datetime.utcnow().strftime("%Y%m%d-%H:%M:%S"))
        order_fields.setField(trstime)
        # order_fields.setField(fix.SecurityType('CS'))
        # order_fields.setField(fix.SecurityExchange(mkt))
        fix.Session.sendToTarget(order_fields, self.session_id)
        print("order string===>", order_fields.toString())

    def onCreate(self, sessionID):
        print(f"session  crated: {sessionID}")

    def onLogon(self, sessionID):
        self.session_id = sessionID
        print(f"session  logon: {sessionID}")

    def onLogout(self, sessionID):
        print(f"session  logout: {sessionID}")

    def fromAdmin(self, msg, sessionID):
        print(f"Admin receive: {msg}")

    def toAdmin(self, msg, sessionID):
        print(f"Admin send: {msg}")

    def fromApp(self, msg, sessionID):
        print(f"App receive: {msg}")

    def toApp(self, msg, sessionID):
        print(f"App send: {msg}")