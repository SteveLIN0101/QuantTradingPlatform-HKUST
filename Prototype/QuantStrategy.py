#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""

import os
import time
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Strategy import Strategy
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution

class QuantStrategy(Strategy):
    
    def __init__(self, stratID, stratName, stratAuthor, ticker, day):
        super(QuantStrategy, self).__init__(stratID, stratName, stratAuthor) #call constructor of parent
        self.ticker = ticker #public field
        self.day = day #public field
        
    def getStratDay(self):
        return self.day
    
    def run(self, marketData, execution):
        if (marketData is None) and (execution is None):
            return None
        elif (marketData is None) and ((execution is not None) and (isinstance(execution, SingleStockExecution))):
            #handle executions
            print('[%d] Strategy.handle_execution' % (os.getpid()))
            print(execution.outputAsArray())
            return None
        elif ((marketData is not None) and (isinstance(marketData, OrderBookSnapshot_FiveLevels))) and (execution is None):
            #handle new market data, then create a new order and send it via quantTradingPlatform.
            # return SingleStockOrder('testTicker','2019-07-05',time.asctime(time.localtime(time.time())))

            # change as wished
            trade_date = '2024-04-01'
            trade_time = '09:00:14.670'
            test_order = SingleStockOrder("0050", trade_date, trade_time)
            test_order.type = "MO"
            test_order.size = 5
            test_order.currStatus = "New"
            self.currStatusTime = time.asctime(time.localtime(time.time()))
            test_order.direction = "Buy"

            return test_order

        else:
            return None
                
        