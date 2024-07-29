# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:12:21 2020

@author: hongsong chou
"""

import threading
import os
import time
from common.SingleStockExecution import SingleStockExecution


class ExchangeSimulator:
    # To store the current quoteSnapshot
    quoteSnapshot = None
    scriptionList = [....]

    # To store bid/ask orders
    bidOrders = dict()
    askOrders = dict()

    # To store available order price levels
    bidOrderLevels = []
    askOrderLevels = []

    # To store marketdata orderbook
    bidQuotaSnapshot = {}
    askQuotaSnapshot = {}
    for code in scriptionList:
        bidQuotaSnapshot[code] = None
        askQuotaSnapshot[code] = None

    MO_remain_BUY = {} #key:code, value: [orderid + restvolume]
    LO_remain_BUY = {} #key:code, value: [orderid + restvolume, price]

    MO_remain_SELL = {} #key:code, value: [orderid + restvolume]
    LO_remain_SELL = {} #key:code, value: [orderid + restvolume, price]

    # To generate executionIDs, start from 1
    execID = 1

    m_QuotaSnapshot = threading.Lock();
    #m_MO_remain = threading.Lock();
    #m_LO_remain = threading.Lock();
    
    def __init__(self, marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call ExchSim.init" % (os.getpid(),))
        
        t_md = threading.Thread(name='exchsim.on_md', target=self.consume_md, args=(marketData_2_exchSim_q,))
        t_md.start()
        
        t_order = threading.Thread(name='exchsim.on_order', target=self.consume_order, args=(platform_2_exchSim_order_q, exchSim_2_platform_execution_q, ))
        t_order.start()

    def consume_md(self, marketData_2_exchSim_q):
        while True:
            res = marketData_2_exchSim_q.get()
            #print('[%d]ExchSim.consume_md' % (os.getpid()))
            #print(res.outputAsDataFrame())
            m_QuotaSnapshot.acquire()
            bidQuotaSnapshot[res.ticker] = [(res.bidPrice1, res.bidSize1),...]
            askQuotaSnapshot[res.ticker] = .....

            #currentlevel = 1

            while len(MO_remain_BUY[res.ticker])>0 and #sum(ask_quota_all_level_size) > 0:
                res = MO_remain_BUY[res.ticker].pop(0)
                #use code below
                #if fullyexection:
                    #    index add to MO_Buy_poplist
            #MO_Buy_poplistpoplist.sort(reverse= highest to lowest),
                        #if partilyexection:
                        #    MO_Buy_list[index].size -= []
            #LO_Buy_poplist = []
            while #sum(ask_quota_all_level_size) > 0:
                for remain_order in LO_remain_BUY:
                    if remain_order.price < #ask_quota[currentlevel-1].price[0]:
                        # use copy below
                        #if fullyexection:
                        #    index add to poplist
            #poplist.sort(reverse= highest to lowest),
                        #if partilyexection:
                        #    list[index].size -= []

            #Sell
            
            m_QuotaSnapshot.release()


        #### exectionRestOrder


    
    def consume_order(self, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        while True:
            res = platform_2_exchSim_order_q.get()
            #print('[%d]ExchSim.on_order' % (os.getpid()))
            #print(res.outputAsArray())
            m_QuotaSnapshot.acquire()
            if res.currStatus == 'NEW':
                if res.type == 'MO':
                    if res.direction == 'BUY':
                        level = 1
                        while res.size > 0 and level <= 5:
                            if res.size > askQuotaSnapshot[res.ticker][level-1][1]:
                                askQuotaSnapshot[res.ticker][level-1] = 0
                                res.size -= askQuotaSnapshot[res.ticker][level-1][1]
                                self.produce_execution(......) # size = askQuotaSnapshot[res.ticker][level-1][1]
                                level += 1
                            else:
                                askQuotaSnapshot[res.ticker][level-1][1] -= res.size
                                res.size = 0
                                self.produce_execution(......) # size = res.size
                        if res.size > 0:
                            if res.ticker in MO_remain:
                                MO_remain[res.ticker].append(order..., res.size)
                            else:
                                MO_remain[res.ticker] = [(order..., res.size)]
                    elif res.direction == 'SELL':
                        ....  
                elif res.type == 'LO':
                    if res.direction == 'BUY':
                        level = 1
                        while res.size > 0 and level <= 5 and askQuotaSnapshot[res.ticker][level-1][0] <= res.price:
                            if res.size > askQuotaSnapshot[res.ticker][level-1][1]:
                                askQuotaSnapshot[res.ticker][level-1] = 0
                                res.size -= askQuotaSnapshot[res.ticker][level-1][1]
                                self.produce_execution(......) # size = askQuotaSnapshot[res.ticker][level-1][1], price = askQuotaSnapshot[res.ticker][level-1][0]
                                level += 1
                            else:
                                askQuotaSnapshot[res.ticker][level-1][1] -= res.size
                                res.size = 0
                                self.produce_execution(......) # size = res.size, price = askQuotaSnapshot[res.ticker][level-1][0]
                        if res.size > 0:
                            if res.ticker in LO_remain:
                                LO_remain[res.ticker].append(order..., res.size, price)
                            else:
                                LO_remain[res.ticker] = [(order..., res.size, price)]
                else:
                    pass
            #elif res.currStatus == 'Cancel':
            #    pass
            else:
                pass
            m_QuotaSnapshot.release()
            
    
    def produce_execution(self, (order.size....), exchSim_2_platform_execution_q):

        execution = SingleStockExecution(time.asctime(time.localtime(time.time())),
                                         ...,
                                         self.execID,
                                         self.quoteSnapshot)

        # Only Traded Successfully the execution will be delivered
            self.execID += 1
            exchSim_2_platform_execution_q.put(execution)
            print('[%d]ExchSim.produce_execution' % (os.getpid()))
            print(execution.outputAsArray())
