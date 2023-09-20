import quickfix as fix


class MyApplication(fix.Application):
    def onCreate(self, sessionID):
        print("Session created:", sessionID)

    def onLogon(self, sessionID):
        print("Logged on:", sessionID)

        # Create and send an order
        order = fix.Message()
        order.getHeader().setField(fix.MsgType("D"))
        order.setField(fix.Symbol("AAPL"))
        order.setField(fix.Side(fix.Side_BUY))
        order.setField(fix.OrderQty(100))
        order.setField(fix.Price(150.0))

        fix.Session.sendToTarget(order, sessionID)

    def onLogout(self, sessionID):
        print("Logged out:", sessionID)

    def toAdmin(self, message, sessionID):
        print("Admin message sent:", message)

    def toApp(self, message, sessionID):
        print("Application message sent:", message)

    def fromAdmin(self, message, sessionID):
        print("Admin message received:", message)

    def fromApp(self, message, sessionID):
        print("Application message received:", message)


if __name__ == "__main__":
    # Create a session settings object
    settings = fix.SessionSettings("client.cfg")

    # Create an application object
    application = MyApplication()

    # Create a message store factory
    storeFactory = fix.FileStoreFactory(settings)

    # Create a log factory
    logFactory = fix.FileLogFactory(settings)

    # Create a session factory
    sessionFactory = fix.SocketInitiator(
        application, storeFactory, settings, logFactory
    )

    # Start the session
    sessionFactory.start()

    # Wait for the session to logon
    while not sessionFactory.isLoggedOn():
        pass

    # Wait for user input to exit
    input("Press Enter to exit...\n")

    # Stop the session
    sessionFactory.stop()
