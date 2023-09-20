import quickfix as fix
from app import FixClient

if __name__ == "__main__":
    client = FixClient("client.cfg")
    client.run()
