# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:15:48 2020

@author: hongsong chou
"""

import threading
import os
from QuantStrategy import QuantStrategy
from datetime import datetime
# !!! from systemController import current_time

class TradingPlatform:
    quantStrat = []
    subscribeStockTicker = [3035, 3374, 6443, 2392, 5347, 3264, 2618, 2498, 0050, 2610]
    subscribeFutureTicker = [IPF1, QLF1, RLF1, GLF1,ﾠNLF1,ﾠNEF1,ﾠHSF1,ﾠHCF1,ﾠNYF1,ﾠDBF1]
    order_id = 1
    mutex = threading.Lock()
    
    def __init__(self, marketData_2_platform_q, strategy_2_platform_order_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call Platform.init" % (os.getpid(),))

        # !!! logic for subscription
        # !!! From marketDataService import subscribe
        # !!! subscribe(marketData_2_platform_q, subscribeStockTicker + subscribeFutureTicker)
        
        #Instantiate individual strategies
        quantStrat.append(QuantStrategy("qs1","quantStrategy1","Zhou Yufeng",[(3035, IPF1), (3374, QLF1), (6443, RLF1), (2392,ﾠGLF1), (5347,ﾠNLF1), (3264,ﾠNEF1), (2618,ﾠHSF1), (2498,ﾠHCF1), (0050,ﾠNYF1), (2610,ﾠDBF1)],"20240628"))
        quantStrat.append(QuantStrategy("qs2","quantStrategy2","Tao Jie",[(3035, IPF1), (3374, QLF1), (6443, RLF1), (2392,ﾠGLF1), (5347,ﾠNLF1), (3264,ﾠNEF1), (2618,ﾠHSF1), (2498,ﾠHCF1), (0050,ﾠNYF1), (2610,ﾠDBF1)],"20240628"))
        quantStrat.append(QuantStrategy("qs3","quantStrategy3","Harsh",[3035, 3374, 6443, 2392,ﾠ5347,ﾠ3264,ﾠ2618,ﾠ2498,ﾠ0050,ﾠ2610],"20240628"))

        t_md = threading.Thread(name='platform.on_marketData', target=self.consume_marketData, args=(marketData_2_platform_q))
        t_md.start()
        
        t_exec = threading.Thread(name='platform.on_exec', target=self.handle_execution, args=(exchSim_2_platform_execution_q))
        t_exec.start()

        # overall order handling of all strategy (strategy 0)
        t_order = threading.Thread(name='platform.on_order', target=self.handle_order, args=(platform_2_exchSim_order_q, strategy_2_platform_order_q))
        t_order.start()

        # store order status and execution by order id
        # to save order execution info along with updated status
        self.order_status_and_executions = {}

        # store order belonging to which strategy
        # to save integrate order info
        self.order_belonging = {}

        # store current position for overall platform
        self.position = {}       # !!! may add logic to get from previous position

    # overall order handling
    def handle_order(platform_2_exchSim_order_q, strategy_2_platform_order_q):
        # print('[%d]Platform.handle_order' % (os.getpid(),))
        while True:
            order = strategy_2_platform_order_q.get()
            if order is not None:
                current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                order.orderID = order_id
                order_id += 1
                order.date = current_datetime.split(" ")[0]
                order.submissionTime = current_datetime.split(" ")[1]
                order.currStatusTime = current_datetime.split(" ")[1]
                order.currStatus = "New"

                platform_2_exchSim_order_q.put(result)

        '''
        # !!! migrate order (in one second)
        while True:
            while 
        '''

        # !!! cancel order

        platform_2_exchSim_order_q.put(result)

        with open('submitted_orders.txt', 'a') as f:
            f.write(str(result.outputAsArray()) + '\n')


    def consume_marketData(self, marketData_2_platform_q):
        print('[%d]Platform.consume_marketData' % (os.getpid()))

        mid_q_pair = [[None] * 2] * len(subscribeStockTicker)

        while True:
            maketData = marketData_2_platform_q.get()
            #print('[%d] Platform.on_md' % (os.getpid()))
            #print(res.outputAsDataFrame())

            if marketData.ticker in subscribeStockTicker:
                temp_thread_qs3 = threading.Thread(name='QuantStrategy3'+datetime.now(), target=self.quantStrat3.run, args=(marketData, None))
                temp_thread_qs3.start()

                index = subscribeStockTicker.index(marketData.ticker)
                mid_q[index][0] = (maketData.askPrice1 + maketData.bidPrice1) / 2
                if mid_q[index][1] is not None:
                    temp_thread_qs1 = threading.Thread(name='QuantStrategy1'+datetime.now(), target=self.quantStrat1.run, args=((marketData.ticker, mid_q[index][0], mid_q[index][1]), None))
                    temp_thread_qs1.start()
                    temp_thread_qs2 = threading.Thread(name='QuantStrategy2'+datetime.now(), target=self.quantStrat2.run, args=((marketData.ticker, mid_q[index][0], mid_q[index][1]), None))
                    temp_thread_qs2.start()

            elif marketData.ticker in subscribeFutureTicker:
                index = subscribeFutureTicker.index(marketData.ticker)
                mid_q[index][1] = (maketData.askPrice1 + maketData.bidPrice1) / 2                
                '''
                if mid_q[index][0] is not None:
                    temp_thread_qs1 = threading.Thread(name='QuantStrategy1'+datetime.now(), target=self.quantStrat1.run, args=((marketData.ticker, mid_q[index][0], mid_q[index][1]), None))
                    temp_thread_qs1.start()
                '''
    
    def handle_execution(self, exchSim_2_platform_execution_q):
        print('[%d]Platform.handle_execution' % (os.getpid(),))
        while True:
            # get both execution and order
            [execution,order] = exchSim_2_platform_execution_q.get()

            # store execution into memory
            self.order_status_and_executions[order.orderID] = {"order" : order, "execution" : execution}

            print('[%d] Platform.handle_execution' % (os.getpid()))


            print('------ EXECUTION',execution.outputAsArray())
            print('------ ORDER',order.outputAsArray())

            # save exeuction to local logs called executions.txt
            with open('executions.txt', 'a') as f:
                f.write(str(execution.outputAsArray()) + '\n')

            with open('executed_orders.txt', 'a') as f:
                f.write(str(order.outputAsArray()) + '\n')


            self.quantStrat.run(None, execution)