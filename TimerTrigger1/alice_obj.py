#alice_blue
import alice_blue
from alice_blue import *

#threading
import threading

#time.sleep()
import time


class socket:

    
    
    #credentials
    username='242661'
    password='&Khyati0'
    api_secret='sGWyHQ7B0PneyCkQ38No7CPKSpJdFkN1uzghvvdD976Pcvju8B9KdPznQKe6GHP5'
    app_id='CetEoSQctj'
    twoFA='khyati'


    #init.py
    def __init__(self, socket_type):

        #object instance type ("nse-m", "nse-s", "bse-m", "bse-s")
        self.socket_type=socket_type

        #instance variables
        self.live_data=[]
        self.socket_opened = False

        
        #websocket callbacks
        def event_handler_quote_update(message):
             self.live_data.append(message)

        def open_callback():
            self.socket_opened = True

        #access token and alice objects
        self.access_token = AliceBlue.login_and_get_access_token(username=self.username, password=self.password, twoFA=self.twoFA, api_secret=self.api_secret, app_id=self.app_id)

        self.alice = AliceBlue(username=self.username, password=self.password, access_token=self.access_token, master_contracts_to_download=['NSE', 'BSE'])

        
        self.alice.start_websocket(subscribe_callback=event_handler_quote_update,
                            socket_open_callback=open_callback,
                            run_in_background=True)
        
        while (self.socket_opened==False):
            pass


            

    #subscription methods

    def subscribe(self, instruments):
        if(self.socket_type == 'nse-m'):
            for x in instruments :
                self.alice.subscribe(self.alice.get_instrument_by_token('NSE', x), LiveFeedType.MARKET_DATA)

        if(self.socket_type == 'nse-s'):
            for x in instruments :
                self.alice.subscribe(self.alice.get_instrument_by_token('NSE', x), LiveFeedType.SNAPQUOTE)

        if(self.socket_type == 'bse-m'):
            for x in instruments :
                self.alice.subscribe(self.alice.get_instrument_by_token('BSE', x), LiveFeedType.MARKET_DATA)

        if(self.socket_type == 'bse-s'):
            for x in instruments :
                self.alice.subscribe(self.alice.get_instrument_by_token('BSE', x), LiveFeedType.SNAPQUOTE)
                

            


 



