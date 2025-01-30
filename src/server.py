from perspective import Table, Server,table
from perspective.handlers.tornado import PerspectiveTornadoHandler
import perspective
import pandas as pd
import tornado 
import logging
from threading import Thread
from time import sleep
from iv_calc import get_placeholder,calculate_iv
import zmq
import perspective


def start_listening_bcast(option_quotes_tbl, underlying_prices_tbl,option_price_iv, underlying_prices_df):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://172.20.1.230:12134")
    socket.setsockopt(zmq.SUBSCRIBE , b"BCAST/MCX/FO")
    print('started listening_bcast from tcp://172.20.1.230:12134')
    while True:
        
        
        msg = socket.recv()
        header,raw_details = msg.decode('utf-8').split(" ")
        symbol = header.split('/')[-1]
        details = {k:v for k,v in  [i.split('=') for i in raw_details.split(',')]}
        if 'FUT' in symbol:
            uly_price = float(details['price'])/100
            try:
                underlying_prices_df.loc[symbol]['price'] = uly_price
            except KeyError as e:
                continue

            uly_price_updt_rec =[ {
                'name_fut':symbol,
                'price':uly_price
            }]
            underlying_prices_tbl.update(uly_price_updt_rec)
            # TODO : 
            # option_price_iv['name_fut', 'option_type', 'strike', 'maturity', 'price', 'biv', 'aiv']
            # where name_fut is underlying future
            
            chain_for_uly = option_price_iv.name_fut == symbol
            chain_w_bid_ask  = (option_price_iv.name_fut == symbol) & (option_price_iv.bid > 0)

            if chain_for_uly.any():

                option_price_iv.loc[chain_for_uly,'moneyness'] =( option_price_iv.loc[chain_for_uly,'strike'] - uly_price) * option_price_iv.loc[chain_for_uly,'option_type'].map({'PE':1, 'CE':-1}) # otms will be negative moneyness

                opt_chain_expiry = option_price_iv.loc[chain_for_uly].iloc[0]['maturity']
                tte = int((opt_chain_expiry - pd.Timestamp.now()).total_seconds())/pd.Timedelta(days=365).total_seconds()

                option_price_iv.loc[chain_w_bid_ask,'biv'] = option_price_iv.loc[chain_w_bid_ask,['bid','strike','option_type']].apply(
                    lambda opt: calculate_iv(opt['bid'], uly_price, opt['strike'], tte, 0.08, opt['option_type'][0].lower(),opt.name) 
                ,axis=1) 

                option_price_iv.loc[chain_w_bid_ask,'aiv'] = option_price_iv.loc[chain_w_bid_ask,['ask','strike','option_type']].apply(
                    lambda opt: calculate_iv(opt['ask'], uly_price, opt['strike'], tte, 0.08, opt['option_type'][0].lower(),opt.name) 
                ,axis=1) 

                if option_price_iv.loc[chain_w_bid_ask].shape[0] != 0 :
                    x = option_price_iv.loc[chain_w_bid_ask]
                    x['maturity'] = x['maturity'].astype(str)
                    chain_rec_update = list(x.to_dict('records'))
                    print(chain_rec_update[0],type(chain_rec_update[0]))
                    option_quotes_tbl.update(chain_rec_update)

                # with pd.option_context('display.max_rows', None, 'display.max_columns', None,'display.max_colwidth',None):  
                    # print(f"updated_moneyness {symbol} {uly_price}\n",option_price_iv.loc[chain_w_bid_ask][['name_fut','strike','moneyness','biv','aiv']]) 
        else:

            uly = option_price_iv.loc[symbol]['name_fut']
            uly_price = underlying_prices_df.loc[uly]['price']
            # print('ULY',uly,option_price_iv.loc[symbol]['name_fut'])

            try:
                x = uly_price < 0 == True
            except Exception:
                # print(option_price_iv.loc[uly])
                y = 0 

            if uly_price < 0:
                continue

            opt_type = option_price_iv.loc[symbol]['option_type']
            strike = option_price_iv.loc[symbol]['strike']
            tte = int((option_price_iv.loc[symbol]['maturity'] - pd.Timestamp.now()).total_seconds())/pd.Timedelta(days=365).total_seconds()
            opt_bid, opt_ask = float(details['bidp'])/100 , float(details['askp'])/100

            
            biv = calculate_iv(opt_bid, uly_price, strike, tte, 0.08, opt_type[0].lower(),symbol) 
            aiv = calculate_iv(opt_ask, uly_price, strike, tte, 0.08, opt_type[0].lower(),symbol) 

            option_price_iv.loc[symbol, 'biv'] = biv
            option_price_iv.loc[symbol, 'aiv'] = aiv
            option_price_iv.loc[symbol, 'bid'] = opt_bid
            option_price_iv.loc[symbol, 'ask'] = opt_ask

            if option_price_iv.loc[symbol].shape[0] == 0:
                x = option_price_iv.loc[symbol]
                x['maturity'] = x['maturity'].astype(str)
                chain_rec_update = x.to_dict('records')
                option_quotes_tbl.update(chain_rec_update)

class MainHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")

        self.set_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

    def get(self):
        self.render("index.html")





# try:

option_price_iv, underlying_prices_df = get_placeholder()

##patch for mcx weird syminfo on 9aug 2024
underlying_prices_df = underlying_prices_df[~underlying_prices_df.index.duplicated(keep='first')]


MANAGER = Server()
# option_quotes = table(option_price_iv,index="name")
# option_quotes = perspective.table(option_price_iv,index="name")
# underlying_ltp = table(underlying_prices_df,index='name_fut')

client = MANAGER.new_local_client()
option_quotes = client.table(option_price_iv,index='name',name="option_quotes")
underlying_ltp = client.table(underlying_prices_df,index='name_fut',name="underlying_ltp")

app = tornado.web.Application([
    (r"/websocket", PerspectiveTornadoHandler, {"perspective_server": MANAGER, "check_origin": True}),
    (r"/",MainHandler),
])

thread = Thread(target=start_listening_bcast,args=[option_quotes,underlying_ltp,option_price_iv,underlying_prices_df])
thread.start()


app.listen(28004)
logging.critical("Listening on http://localhost:33002")
loop = tornado.ioloop.IOLoop.current()
loop.start()
thread.join()
# except KeyboardInterrupt:
# kill -9 $(ps -A | grep server.py | awk '{print $1}')

