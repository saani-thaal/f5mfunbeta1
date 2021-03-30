import os
import pickle #exchange token file format
import pandas as pd #data clean
import pyarrow #data file save format
import datetime



#load instruments (total 500 scrips all common to nse and bse)
def instruments_list():
    with open('nse_exchange_token.pickle', 'rb') as f:
        nse_instruments=pickle.load(f)

    with open('bse_exchange_token.pickle', 'rb') as f:
        bse_instruments=pickle.load(f)

    return nse_instruments, bse_instruments





#data_clean
def data_clean(sockets, exchange): #exchange for file_name
    
    dfm=pd.DataFrame() #market
    dfs=pd.DataFrame() #snapquote

    for x in sockets:
        
        dff=pd.DataFrame(x.live_data[:])
        dff['id']=dff['token'].astype('str') + '-' + dff['exchange_time_stamp'].astype('str')
        dff.drop(['instrument', 'token', 'exchange_time_stamp'], axis=1, inplace=True)
        if x.socket_type == 'nse-m' or x.socket_type == 'bse-m' : #if market
            dfm=dfm.append(dff)
        else:
            dfs=dfs.append(dff)

    

    #picking only the required columns
    market_columns=['id','exchange','yearly_high','yearly_low','open','high','low','close','ltt','ltp','ltq','atp','volume','total_buy_quantity','total_sell_quantity']
    snapquote_columns=['id','buyers','bid_prices','bid_quantities', 'sellers', 'ask_prices', 'ask_quantities']

    dfm=dfm[market_columns]
    dfs=dfs[snapquote_columns]

    df=pd.merge(dfm,dfs, on='id', how='inner')

    file_name=exchange + '-' + datetime.date.today().isoformat() + '.parquet'

    #convert column names to string type coz parquet only supports cloumns in string format
    df.columns.astype('str')

    #save file
    df.to_parquet(file_name)

    return file_name
    
