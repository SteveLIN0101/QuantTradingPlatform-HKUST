# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""

from multiprocessing import Process, Queue
from marketDataService import MarketDataService
from exchangeSimulator import ExchangeSimulator
from quantTradingPlatform import TradingPlatform
import os
#from datetime import datetime

if __name__ == '__main__':
    # delete existing .txt log files 
    for f in os.listdir():
        if f.endswith('.txt'):
            os.remove(f)

    ###########################################################################
    # Define all components
    ###########################################################################
    
    marketData_2_exchSim_q = Queue()
    marketData_2_platform_q = Queue()
    
    platform_2_exchSim_order_q = Queue()
    exchSim_2_platform_execution_q = Queue()
    
    platform_2_strategy_md_q = Queue()
    strategy_2_platform_order_q = Queue()
    platform_2_strategy_execution_q = Queue()

    Process(name='md', target=MarketDataService, args=(marketData_2_exchSim_q, marketData_2_platform_q, )).start()
    Process(name='sim', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, )).start()
    Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, )).start()

    ''' !!!
    # calculate simulation time diff
    diff = 0
    
    # call when exchange start to send orderbook (yyyy-mm-dd)
    def calculate_diff(date)
        now = datetime.now()
        then = datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), 9, 0, 0)
        diff = now - then

    # return simulation time (call by other component)
    def current_time():
        now = datetime.now()
        return (now + diff).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # !!!
    '''