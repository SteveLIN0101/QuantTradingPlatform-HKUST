# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:15:48 2020

@author: hongsong chou
"""

import threading
import os
from QuantStrategy import QuantStrategy

class TradingPlatform:
    quantStrat = None
    
    def __init__(self, marketData_2_platform_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call Platform.init" % (os.getpid(),))
        
        #Instantiate individual strategies
        self.quantStrat = QuantStrategy("tf_1","quantStrategy","hongsongchou","JBF_3443","20230706")

        t_md = threading.Thread(name='platform.on_marketData', target=self.consume_marketData, args=(platform_2_exchSim_order_q, marketData_2_platform_q,))
        t_md.start()
        
        t_exec = threading.Thread(name='platform.on_exec', target=self.handle_execution, args=(exchSim_2_platform_execution_q, ))
        t_exec.start()

        # store submitted order submissions by ID to object mapping
        # to save initial order submission info
        # e.g. {orderID : SingleStockOrder}
        self.order_submissions = {}

        # store order and execution by ID
        # to save order execution info along with updated status
        # e.g. {orderID : {order : SingleStockOrder, execution : SingleStockExecution}}
        self.order_status_and_executions = {}

    def consume_marketData(self, platform_2_exchSim_order_q, marketData_2_platform_q):
        print('[%d]Platform.consume_marketData' % (os.getpid(),))
        while True:
            res = marketData_2_platform_q.get()
            print('[%d] Platform.on_md' % (os.getpid()))
            print(res.outputAsDataFrame())
            result = self.quantStrat.run(res, None)
            if result is None:
                pass
            else:
                # store order into memory 
                self.order_submissions[result.orderID] = result

                # store logs into local logs called submitted_orders.txt
                with open('submitted_orders.txt', 'a') as f:
                    f.write(str(result.outputAsArray()) + '\n')


                #do something with the new order
                platform_2_exchSim_order_q.put(result)
    
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