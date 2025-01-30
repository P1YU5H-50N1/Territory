import warnings
warnings.filterwarnings("ignore")

import zmq
from time import  sleep
from py_vollib.black_scholes.implied_volatility import implied_volatility as iv
from irage_helper.irage import SymserverRequest
import pandas as pd 

def get_placeholder():
    cols = "BasePrice,Bcast,PartitionID,ProductID,Series,dest,dpr_high,dpr_low,exch,expiry,group,id,l2_price_multiplier,lot_size,maturity,name,old_name,old_year_month_day,option_type,orig_name,orig_underlying,price_multiplier,security_type,segment,segment_type,snoop_on_exec,source,stream,strike,tags,tbt_dpr_high,tbt_dpr_low,tick_size,underlying,unit,year_month,year_month_day".split(
        ","
    )
    symserver = SymserverRequest()
    all_contracts = symserver.getSyminfo(exch="MCX", seg="FO", cols=cols)
    derivatives = all_contracts.query('security_type != "ULY"')

    options = {
        i[0]: i[1].sort_values("expiry")
        for i in derivatives.query('security_type == "OPT"').groupby("orig_name")
    }
    futures = {
        i[0]: i[1].sort_values("expiry")
        for i in derivatives.query('security_type == "FUT"').groupby("orig_name")
    }
    futures_raw = derivatives.query('security_type == "FUT"').rename(
        columns={"name": "name_fut"}
    )

    portfolios_raw = []
    for uly in options:
        pfs_for_uly = pd.merge_asof(
            options[uly],
            futures[uly][['id',"expiry", "name"]],
            on="expiry",
            suffixes=("", "_fut"),
            direction="nearest",
        )
        portfolios_raw.append(pfs_for_uly)
    portfolios = pd.concat(portfolios_raw)


    portfolios = pd.concat(portfolios_raw)
    portfolios = pd.merge(
        portfolios,
        futures_raw[['id',"name_fut", "maturity"]],
        left_on='id_fut',
        right_on='id',
        suffixes=("", "_fut"),
    )
    portfolios.drop(columns=['id_fut'],inplace=True)

    underlying_prices_df = futures_raw[["name_fut"]].set_index("name_fut")
    underlying_prices_df["price"] = -1.0

    option_price_iv = portfolios[['name','name_fut','option_type','strike','maturity']].copy()
    option_price_iv['maturity'] = option_price_iv['maturity'] + " 23:59:59"
    option_price_iv['maturity'] = pd.to_datetime(option_price_iv['maturity'])
    option_price_iv['strike'] = option_price_iv['strike'] /100
    option_price_iv['price'] = -1
    option_price_iv['biv'] = -1
    option_price_iv['aiv'] = -1
    option_price_iv['bid'] = -1
    option_price_iv['ask'] = -1
    option_price_iv['moneyness'] = -1
    option_price_iv.set_index('name',inplace=True)
    return option_price_iv, underlying_prices_df


def calculate_iv(opt_prc, uly_price, strike, tte, r, opt_type):
    if opt_prc < 0:
        return None
    try:
        cur_iv = iv(opt_prc, uly_price, strike, tte,r, opt_type) * 100
    except Exception as e:
        print(f"Error calculating IV: {e} {symbol}\n args: {[opt_prc, uly_price, strike, tte, 0.05, opt_type[0].lower()]}")
        cur_iv= None
    return cur_iv


if __name__ == "__main__":
    option_price_iv, underlying_prices_df = get_placeholder()
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://172.20.1.230:12134")
    socket.setsockopt(zmq.SUBSCRIBE , b"BCAST/MCX/FO")
    print('started')
    while True:
                if option_price_iv.index.unique().shape[0] != option_price_iv.index.shape[0]:
                    print('shape mismatch',option_price_iv.index.unique().shape[0],option_price_iv.index.shape[0])
                msg = socket.recv()
            # try:
                header,raw_details = msg.decode('utf-8').split(" ")
                symbol = header.split('/')[-1]
                details = {k:v for k,v in  [i.split('=') for i in raw_details.split(',')]}
                if 'FUT' in symbol:
                    print("FUT",symbol)
                    uly_price = float(details['price'])/100
                    underlying_prices_df.loc[symbol]['price'] = uly_price
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
                            lambda opt: calculate_iv(opt['bid'], uly_price, opt['strike'], tte, 0.08, opt['option_type'][0].lower()) 
                        ,axis=1) 

                        option_price_iv.loc[chain_w_bid_ask,'aiv'] = option_price_iv.loc[chain_w_bid_ask,['ask','strike','option_type']].apply(
                            lambda opt: calculate_iv(opt['ask'], uly_price, opt['strike'], tte, 0.08, opt['option_type'][0].lower()) 
                        ,axis=1) 

                        with pd.option_context('display.max_rows', None, 'display.max_columns', None,'display.max_colwidth',None):  
                            print(f"updated_moneyness {symbol} {uly_price}\n",option_price_iv.loc[chain_w_bid_ask][['name_fut','strike','moneyness','biv','aiv']]) 
                else:
                    # print("OPT",symbol)

                    uly = option_price_iv.loc[symbol]['name_fut']
                    uly_price = underlying_prices_df.loc[uly]['price']
                    
                    print('ULY',uly,option_price_iv.loc[symbol]['name_fut'])

                    if uly_price < 0:
                        continue

                    opt_type = option_price_iv.loc[symbol]['option_type']
                    strike = option_price_iv.loc[symbol]['strike']
                    tte = int((option_price_iv.loc[symbol]['maturity'] - pd.Timestamp.now()).total_seconds())/pd.Timedelta(days=365).total_seconds()
                    opt_bid, opt_ask = float(details['bidp'])/100 , float(details['askp'])/100

                    # print("OPT Calc IV",symbol)
                    
                    biv = calculate_iv(opt_bid, uly_price, strike, tte, 0.08, opt_type[0].lower()) 
                    aiv = calculate_iv(opt_ask, uly_price, strike, tte, 0.08, opt_type[0].lower()) 
                    # print("OPT IV",symbol, biv, aiv)

                    option_price_iv.loc[symbol, 'biv'] = biv
                    option_price_iv.loc[symbol, 'aiv'] = aiv
                    option_price_iv.loc[symbol, 'bid'] = opt_bid
                    option_price_iv.loc[symbol, 'ask'] = opt_ask
                    # print(symbol,biv)

                    # TODO:
                    # update biv and aiv of the option in option_price_iv df
                    # option_price_iv['name_fut', 'option_type', 'strike', 'maturity', 'price', 'biv', 'aiv']
                    # where name_fut is underlying future


            # except Exception as e:
            #     print(e)
            #     print('Can"t decode :' , msg)