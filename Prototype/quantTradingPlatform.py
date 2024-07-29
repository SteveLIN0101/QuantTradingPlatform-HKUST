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
        quantStrat.append(QuantStrategy(1,"quantStrategy1","Zhou Yufeng",[(3035, IPF1), (3374, QLF1), (6443, RLF1), (2392,ﾠGLF1), (5347,ﾠNLF1), (3264,ﾠNEF1), (2618,ﾠHSF1), (2498,ﾠHCF1), (0050,ﾠNYF1), (2610,ﾠDBF1)],"20240628"))
        quantStrat.append(QuantStrategy(2,"quantStrategy2","Tao Jie",[(3035, IPF1), (3374, QLF1), (6443, RLF1), (2392,ﾠGLF1), (5347,ﾠNLF1), (3264,ﾠNEF1), (2618,ﾠHSF1), (2498,ﾠHCF1), (0050,ﾠNYF1), (2610,ﾠDBF1)],"20240628"))
        quantStrat.append(QuantStrategy(3,"quantStrategy3","Harsh",[3035, 3374, 6443, 2392,ﾠ5347,ﾠ3264,ﾠ2618,ﾠ2498,ﾠ0050,ﾠ2610],"20240628"))

        t_md = threading.Thread(name='platform.on_marketData', target=self.consume_marketData, args=(marketData_2_platform_q))
        t_md.start()
        
        t_exec = threading.Thread(name='platform.on_exec', target=self.handle_execution, args=(exchSim_2_platform_execution_q))
        t_exec.start()

        # overall order handling of all strategy (strategy 0)
        t_order = threading.Thread(name='platform.on_order', target=self.handle_order, args=(platform_2_exchSim_order_q, strategy_2_platform_order_q))
        t_order.start()

        # !!! position logged and pnl calculation

        # store order status and execution by order id
        # to save order execution info along with updated status
        # {order_id: [(ticker, date, sumbmissionTime, direction, price, size, type), (exec_id, date, currentStatusTime, currStats, price, size, filled_size, remained_size)]}
        self.order_status_and_executions = {}

        # store order belonging to which strategy
        # to save integrate order info
        self.order_belonging = {}

        # store imcompleted order
        # self.imcompleted_order = []

        # store current position for overall platform
        self.position = {}       # !!! may add logic to get from previous position
        for i in range(len(quantStrat)):
            position[quantStrat[i].getStratID()] = {}

    # overall order handling
    def handle_order(platform_2_exchSim_order_q, strategy_2_platform_order_q):
        # print('[%d]Platform.handle_order' % (os.getpid(),))

        while True:
            stratID, order = strategy_2_platform_order_q.get()
            if order is not None:
                current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                order.orderID = order_id
                order_id += 1
                order.date = current_datetime.split(" ")[0]
                order.submissionTime = current_datetime.split(" ")[1]
                order.currStatusTime = current_datetime.split(" ")[1]
                order.currStatus = "New"

                mutex.acquire()
                order_status_and_executions[order.orderID] = [(order.ticker, order.date, order.submissionTime, \
                                                               order.direction, order.price, order.size, order.type), \
                                                              (None, order.date, order.currStatusTime, order.currStatus, None, None)]
                #imcompleted_order.append(order.orderID)
                order_belonging[order.orderID] = stratID
                mutex.release()

                platform_2_exchSim_order_q.put(order)
            else:
                pass

            '''
            # !!! migrate order (in one second)
            while True:
                while 
            '''

            # !!! cancel order

            with open('submitted_orders.txt', 'a') as f1:
                f1.write(str(result.outputAsArray()) + '\n')


    def consume_marketData(self, marketData_2_platform_q):
        print('[%d]Platform.consume_marketData' % (os.getpid()))

        mid_q_pair = [[None] * 2] * len(subscribeStockTicker)

        while True:
            maketData = marketData_2_platform_q.get()
            #print('[%d] Platform.on_md' % (os.getpid()))
            #print(res.outputAsDataFrame())

            if marketData.ticker in subscribeStockTicker:
                temp_thread_qs3 = threading.Thread(name='QuantStrategy3'+datetime.now(), target=self.quantStrat[2].run, args=(marketData, None))
                temp_thread_qs3.start()

                index = subscribeStockTicker.index(marketData.ticker)
                mid_q[index][0] = (maketData.askPrice1 + maketData.bidPrice1) / 2
                if mid_q[index][1] is not None:
                    temp_thread_qs1 = threading.Thread(name='QuantStrategy1'+datetime.now(), target=self.quantStrat[0].run, args=((marketData.ticker, mid_q[index][0], mid_q[index][1]), None))
                    temp_thread_qs1.start()
                    temp_thread_qs2 = threading.Thread(name='QuantStrategy2'+datetime.now(), target=self.quantStrat[1].run, args=((marketData.ticker, mid_q[index][0], mid_q[index][1]), None))
                    temp_thread_qs2.start()

            elif marketData.ticker in subscribeFutureTicker:
                index = subscribeFutureTicker.index(marketData.ticker)
                mid_q[index][1] = (maketData.askPrice1 + maketData.bidPrice1) / 2                
                '''
                # !!!
                if mid_q[index][0] is not None:
                    temp_thread_qs1 = threading.Thread(name='QuantStrategy1'+datetime.now(), target=self.quantStrat[0].run, args=((marketData.ticker, mid_q[index][0], mid_q[index][1]), None))
                    temp_thread_qs1.start()
                '''
    
    def handle_execution(self, exchSim_2_platform_execution_q):
        print('[%d]Platform.handle_execution' % (os.getpid(),))
        while True:
            execution = exchSim_2_platform_execution_q.get()
            if execution.direction == 'ACK':
                mutex.acquire()
                order_status_and_executions[execution.orderID].append((execution.id, execution.date, execution.timeStamp, 'ACK', None, None, 0, order_status_and_executions[execution.orderID][0][5]))
                mutex.release()
            elif execution.direction == 'BI' or execution.direction == 'SI'
                    if execution.size == order_status_and_executions[execution.orderID][-1][-1]:
                        mutex.acquire()
                        order_status_and_executions[execution.orderID].append((execution.id, execution.date, execution.timeStamp, 'Filled', execution.price, execution.size, filled_size, 0))
                        if execution.ticker in position[order_belonging[execution.orderID]]:
                            position[order_belonging[execution.orderID]][execution.ticker][0] += execution.price * execution.size
                            position[order_belonging[execution.orderID]][execution.ticker][1] += execution.size
                        else:
                            tempList = []
                            tempList.append(execution.price * execution.size)
                            tempList.append(execution.size)
                            position[order_belonging[execution.orderID]][execution.ticker] =  tempList
                        # imcompleted_order.remove(execution.orderID)
                        mutex.release()
                    else:
                        mutex.acquire()
                        order_status_and_executions[execution.orderID].append((execution.id, execution.date, execution.timeStamp, 'PartiallyFilled', execution.price, execution.size, order_status_and_executions[execution.orderID][-1][-2] + execution.size, order_status_and_executions[execution.orderID][-1][-1] - execution.size))
                        if execution.ticker in position[order_belonging[execution.orderID]]:
                            position[order_belonging[execution.orderID]][execution.ticker][0] += execution.price * execution.size
                            position[order_belonging[execution.orderID]][execution.ticker][1] += execution.size
                        else:
                            tempList = []
                            tempList.append(execution.price * execution.size)
                            tempList.append(execution.size)
                            position[order_belonging[execution.orderID]][execution.ticker] =  tempList
                        # imcompleted_order.remove(execution.orderID)
                        mutex.release()
            # !!! elif execution.direction == 'Cancel':
            # !!! elif execution.direction == 'Cancel Fail':
            else:
                pass

            with open('execution and status.txt', 'a') as f2:
                output = [execution.ticker, order_status_and_executions[execution.orderID][0][3], execution.orderID]
                output.extend(list(order_status_and_executions[execution.orderID][-1]))
                f2.write(str(output) + '\n')
            self.quantStrat[order_belonging[execution.execID]].run(None, execution)