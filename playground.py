import zmq
import sys 
from datetime import datetime
import re
import pandas as pd
# import matplotlib.pyplot as plt

class OptChain:
    def __init__(self,ticker) -> None:
        self.price = {}
        self.iv = {}
        self.future = None
        self.future_price = None
        self.contracts = {}
        self.ticker = ticker
        self.strikes = {}
        self.ATM = None
        self.IV_series = pd.DataFrame()


    def update(self,name,price,iv,recv_time=None):
        if "EQ" in name[-2:] or not name[:len(self.ticker)] == self.ticker:
             return
        if "FUT" == name[-3:]:
            self.future = name
            self.future_price = price
        else:
            self.price[name] = price
            self.iv[name] = iv

        if not name in self.contracts.keys() :
            contract_specs = extract_contract_specs(name)
            if contract_specs['type'] != 'FUT':
                self.strikes[contract_specs["strike"]] = name
            self.contracts[name] = contract_specs

        if not self.future_price is None and self.strikes:
            self.ATM = self.strikes[sorted([(strike,abs(strike-self.future_price)) for strike in self.strikes.keys()],
                    key=lambda x:x[1])[0][0]]
            self.IV_series.loc[datetime.today(),'IV'] = self.iv[self.ATM]
          
            print(f"ATM @{self.future_price:<20} {self.ATM}  IV :",round(self.iv[self.ATM],2))

def extract_contract_specs(contract):
    if "FUT" == contract[-3:]:
        strike = 0
        underlying,expiry,type = re.findall("(^[A-Z_]+)(.{5})(FUT$)",contract)[0]
    else:        
        underlying,expiry,strike,type = re.findall("(^[A-Z_]+)(.{5})(\d+\.?\d+)(.*)",contract)[0]
    return {
          'underlying':underlying,
          "expiry":expiry,
          "strike":float(strike),
          "type":type
    }

# if __name__ == "__main__":

#     context = zmq.Context()
#     socket = context.socket(zmq.SUB)
#     socket.connect("tcp://172.16.100.230:12134")

#     clear = lambda: print("\033c", end="", flush=True)
#     # socket.setsockopt(zmq.SUBSCRIBE , b"BCAST")
#     socket.setsockopt(zmq.SUBSCRIBE , b"BCAST/NSE/FO")
#     # socket.setsockopt(zmq.SUBSCRIBE , b"BCAST/MCX/FO")

#     # socket.setsockopt(zmq.SUBSCRIBE , b"AGG")
#     # socket.setsockopt(zmq.SUBSCRIBE , b"")
#     print('Started')
#     # socket.setsockopt(zmq.SUBSCRIBE , b"BCAST")

                
#     chain = OptChain('NATURALGAS24MAY')

#     while True:
#             #msg = socket.recv()
#                     msg = socket.recv()
#             #msg = socket.recv_multipart()
#             # try:
#                     # print(msg.decode('utf-8').split(" "))
#                     header,raw_details = msg.decode('utf-8').split(" ")
#                     symbol = header.split('/')[-1]
#                     details = {k:v for k,v in  [i.split('=') for i in raw_details.split(',')]}
#                     chain.update(symbol,float(details['p'])/100,float(details['iv']))

#                     # if 'BIOCON24MAY' in symbol:
#                     #     print(datetime.today().strftime("%B %d, %Y %I:%M:%S %p"))
#                     #     print(symbol)
#                     #     print(details)
#                     # print(symbol)
#                     # print("Received: %s" % details )
#                     # clear()
#             # except Exception as e:
#             #         print(e)
#             #         print('Can"t decode :' , msg)
#     print(datetime.today().strftime("%B %d, %Y %I:%M:%S %p"))

# def subscribeSolver2():
#     context = zmq.Context()
#     socket = context.socket(zmq.SUB)
#     socket.connect("tcp://192.168.151.8:19925")
#     socket.setsockopt_string(zmq.SUBSCRIBE, "ZUPD/OMM/")
##################################
if __name__ == "__main__":

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://172.20.1.230:12134")

    # clear = lambda: print("\033c", end="", flush=True)
#     # socket.setsockopt(zmq.SUBSCRIBE , b"BCAST")
#     socket.setsockopt(zmq.SUBSCRIBE , b"BCAST/NSE/FO")
    # socket.setsockopt(zmq.SUBSCRIBE , b"BCAST")

#     # socket.setsockopt(zmq.SUBSCRIBE , b"AGG")
    socket.setsockopt(zmq.SUBSCRIBE , b"")
    print('Started')
#     # socket.setsockopt(zmq.SUBSCRIBE , b"BCAST")

                
    # chain = OptChain('NATURALGAS24MAY')

    while True:
            #msg = socket.recv()
        msg = socket.recv()
            #msg = socket.recv_multipart()
        try:
            y = msg.decode('utf-8')
            if "FUT" in y:
                print(y)   
        except Exception as e:
            print(e)
            print('Can"t decode :' , msg)
#     print(datetime.today().strftime("%B %d, %Y %I:%M:%S %p"))
