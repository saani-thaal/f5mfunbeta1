import logging

import azure.functions as func

#sub sripts
from . import alice_obj
from . import data_process
from . import upload

#file acess
import os



#time (UTC is used for prccessing, however All work logs are in IST)
import datetime
from datetime import timedelta






def run_it():

    #init_time
    init_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).isoformat()
    
    #trigger_time variables
    t = datetime.datetime.utcnow()
    socket_trigger_time =  datetime.datetime(t.year, t.month, t.day, 3, 25) #8:55 IST (4min 40s buffer time for sockets creation)
    subscription_trigger_time = datetime.datetime(t.year, t.month, t.day, 3, 29, 20) #8:59:40 IST (20s buffer time for sockets subscription)
    shutdown_trigger_time = datetime.datetime(t.year, t.month, t.day, 3, 45) #9:15 IST

    #sockets

        #socket_trigger
    while datetime.datetime.utcnow() < socket_trigger_time :
        pass

        #sockets created time
    socket_created_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).isoformat()

        #instruments_list
    nse_instruments, bse_instruments = data_process.instruments_list()
    
    
    
        #sockets and quote_types variables
    nse_sockets=[]
    bse_sockets=[]
    nse_quote_type=['nse-m', 'nse-m', 'nse-m', 'nse-m', 'nse-m', 'nse-m', 'nse-s', 'nse-s', 'nse-s', 'nse-s', 'nse-s', 'nse-s',]
    bse_quote_type=['bse-m', 'bse-m', 'bse-m', 'bse-m', 'bse-m', 'bse-m', 'bse-s', 'bse-s', 'bse-s', 'bse-s', 'bse-s', 'bse-s',]

        #socket object creation
    for i in range(0,12):
        nse_sockets.append(alice_obj.socket(nse_quote_type[i]))
        bse_sockets.append(alice_obj.socket(bse_quote_type[i]))



    #subscription

        #subcription trigger
    while datetime.datetime.utcnow() < subscription_trigger_time :
        pass



        #subscription_init_time
    subscription_init_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).isoformat()

    for i in range(0,12):
        j=i
        i=i%6 #instruments are shared between market and snapquote sockets
        if (i+1)*250 > len(nse_instruments) or (i+1)*250 > len(bse_instruments) :
            nse_sockets[j].subscribe(nse_instruments[250*i:len(nse_instruments)])
            bse_sockets[j].subscribe(bse_instruments[250*i:len(bse_instruments)])

        else:
            nse_sockets[j].subscribe(nse_instruments[250*i:250*(i+1)])
            bse_sockets[j].subscribe(bse_instruments[250*i:250*(i+1)])

    
    #data collection
    datacollection_init_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).isoformat()
    
        #shutdown trigger
    while datetime.datetime.utcnow() < shutdown_trigger_time :
        pass
    
    
    
    #data_clean ( exchange variable for file_name)
    
    dataprocess_init_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).isoformat()
    
    nse_file_name=data_process.data_clean(nse_sockets, 'nse')
    bse_file_name=data_process.data_clean(bse_sockets, 'bse')
    
    dataprocess_end_time= (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).isoformat()
    
    #upload
    upload.upload(nse_file_name)
    upload.upload(bse_file_name)

    #delete those files after upload
    os.remove(nse_file_name)
    os.remove(bse_file_name)
    
    #endtime (IST)
    end_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).isoformat()

    #work_logs to be returned to flask's @app.route('/') 
    #work_logs = '  (IST)  ' + ' Main fn initiated at : ' + init_time + ' Sockets were created at :' + socket_created_time + ' Subscription initiated at : ' + subscription_init_time + ' Subscription completed and DataCollection started at  : ' + datacollection_init_time + ' DataProcess started at : ' + dataprocess_init_time + ' DataProcess ended at : ' + dataprocess_end_time + ' uploaded at : ' + end_time
    #return work_logs




def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')


    run_it()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
