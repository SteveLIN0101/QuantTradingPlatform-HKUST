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
    
    def __init__(self, marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call ExchSim.init" % (os.getpid(),))
        
        t_md = threading.Thread(name='exchsim.on_md', target=self.consume_md, args=(marketData_2_exchSim_q,))
        t_md.start()
        
        t_order = threading.Thread(name='exchsim.on_order', target=self.consume_order, args=(platform_2_exchSim_order_q, exchSim_2_platform_execution_q, ))
        t_order.start()

        # a data structure of exchange order book mapping key to object
        # e.g. {"test_stock " : OrderBookSnapshot_FiveLevels.outputAsDataFrame() }
        self.exchange_order_book = {

        }

        # store order IDs
        self.exchange_submitted_orders = {

        }

    def update_order_book(self, order_book_snapshot):
        # get ticker id
        ticker = order_book_snapshot.ticker

        # update latest ticker dict info.
        self.exchange_order_book[ticker] = order_book_snapshot

    def consume_md(self, marketData_2_exchSim_q):
        while True:
            res = marketData_2_exchSim_q.get()
            print('[%d]ExchSim.consume_md' % (os.getpid()))
            print(res.outputAsDataFrame())

            # update Exchange Simulator's order book
            self.update_order_book(res)
    
    def consume_order(self, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        while True:
            res = platform_2_exchSim_order_q.get()
            print('[%d]ExchSim.on_order' % (os.getpid()))
            print(res.outputAsArray())
            self.produce_execution(res, exchSim_2_platform_execution_q)
    
    def produce_execution(self, order, exchSim_2_platform_execution_q):
        execution = SingleStockExecution(order.ticker, order.date, time.asctime(time.localtime(time.time())))

        # set execution to have the original order's ID
        execution.orderID = order.orderID
        # set execution to an easily identifiable ID
        execution.execID = "execution_" + str(order.orderID)

        # get latest order book snapshot
        order_book_snapshot = self.exchange_order_book[order.ticker]
    
        best_ask_price,best_ask_size,best_ask_index = order_book_snapshot.get_best_ask()
        best_bid_price,best_bid_size,best_bid_index = order_book_snapshot.get_best_bid()

        print("Best ask price: ", best_ask_price)
        print("Best bid price: ", best_bid_price)

        if order.direction == "Buy":
            execution.direction = order.direction
    
            if order.type == "MO":
                # get order size
                order_size_to_fill = order.size

                # use to store average fill price if order is partially filled
                total_fill_price = 0
                total_fill_size = 0

                # keep looping and getting best ask price until ORDER IS FILLED OR THERE IS NO MORE ASK OFFERS
                while order_size_to_fill > 0 and best_ask_size > 0:
                    # get order size
                    order_size = min(order_size_to_fill, best_ask_size)
                    order_price = best_ask_price

                    # update order size to fill variable
                    order_size_to_fill -= order_size


                    # update object
                    order_book_snapshot.update_ask_by_index(best_ask_index, order_size)


                    # update average fill price
                    total_fill_price += order_price*order_size
                    total_fill_size += order_size

                    # produce execution object

                    # get next best ask price
                    best_ask_price,best_ask_size,best_ask_index = order_book_snapshot.get_best_ask()

                # calculate average fill price, if no fill, set to None to avoid division by zero
                average_fill_price = total_fill_price/total_fill_size if total_fill_size > 0 else None
                
                # if order is partially filled with size remaining
                if order_size_to_fill > 0:
                    # order is partially filled
                    order.currStatus = "PartiallyFilled"

                elif order_size_to_fill == 0:
                    # order is completely filled
                    order.currStatus = "Filled"
                    
                elif order_size_to_fill == order.size:
                    # order is cancelled
                    order.currStatus = "Cancelled"


                # update execution and order objects with info
                execution.price = average_fill_price
                execution.size = total_fill_size
                execution.timeStamp = time.asctime(time.localtime(time.time()))
                execution.direction = order.direction

                order.price = average_fill_price
                order.size = total_fill_size
                order.currStatusTime = execution.timeStamp


            # HANDLE FOR OTHERS 
            if order.type == "LAZINESS":
                # get order size
                order_size = order.size
                order_price = best_ask_price

                pass

        # return both execution and order object to platform
        exchSim_2_platform_execution_q.put([execution,order])

        print('[%d]ExchSim.produce_execution' % (os.getpid()))
        print(execution.outputAsArray())


