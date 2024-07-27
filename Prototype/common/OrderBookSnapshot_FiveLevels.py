# -*- coding: utf-8 -*-
"""
Created on Thu Jul  4 10:39:28 2019

@author: hongs
"""

import pandas as pd
from common.OrderBookSnapshot import OrderBookSnapshot

class OrderBookSnapshot_FiveLevels(OrderBookSnapshot):
    bidPrice1, bidPrice2, bidPrice3, bidPrice4, bidPrice5 = None, None, None, None, None
    askPrice1, askPrice2, askPrice3, askPrice4, askPrice5 = None, None, None, None, None
    bidSize1, bidSize2, bidSize3, bidSize4, bidSize5 = None, None, None, None, None
    askSize1, askSize2, askSize3, askSize4, askSize5 = None, None, None, None, None
    
    initializationFlag = False
    
    outputCols = ['ticker','date','time', \
                  'askPrice5','askPrice4','askPrice3','askPrice2','askPrice1', \
                  'bidPrice1','bidPrice2','bidPrice3','bidPrice4','bidPrice5', \
                  'askSize5','askSize4','askSize3','askSize2','askSize1', \
                  'bidSize1','bidSize2','bidSize3','bidSize4','bidSize5']

    def __init__(self, ticker, date, timeStamp, bidPrice, askPrice, bidSize, askSize):
        super().__init__(ticker, date, timeStamp)
                
        if (bidPrice is None) or (askPrice is None) or (bidSize is None) or (askSize is None):
            print("In OrderBookSnapshot_FiveLevels: bidPrice, askPruce, bidSize, askSize empty.")
            return
        elif (len(bidPrice) != 5) or (len(askPrice) != 5) or (len(bidSize) != 5) or (len(askSize) != 5):
            print("In OrderBookSnapshot_FiveLevels: bidPrice, askPruce, bidSize, askSize sizes not match.")
            return
        else:
            self.askPrice1, self.askPrice2, self.askPrice3, self.askPrice4, self.askPrice5 = \
                askPrice[0], askPrice[1], askPrice[2], askPrice[3], askPrice[4],
            self.bidPrice1, self.bidPrice2, self.bidPrice3, self.bidPrice4, self.bidPrice5 = \
                bidPrice[0], bidPrice[1], bidPrice[2], bidPrice[3], bidPrice[4],
            self.askSize1, self.askSize2, self.askSize3, self.askSize4, self.askSize5 = \
                askSize[0], askSize[1], askSize[2], askSize[3], askSize[4],
            self.bidSize1, self.bidSize2, self.bidSize3, self.bidSize4, self.bidSize5 = \
                bidSize[0], bidSize[1], bidSize[2], bidSize[3], bidSize[4],
            self.initializationFlag = True
            return

    # return the best ask price,size,index
    def get_best_ask(self):
        # if statements to get first ask price that is not None
        if self.askSize1 > 0:
            return self.askPrice1, self.askSize1,1
        elif self.askSize2 > 0:
            return self.askPrice2, self.askSize2,2
        elif self.askSize3 > 0:
            return self.askPrice3, self.askSize3,3
        elif self.askSize4 > 0:
            return self.askPrice4, self.askSize4,4
        elif self.askSize5 > 0:
            return self.askPrice5, self.askSize5,5
        else:
            return None, None, None

    # return the best bid price,size,index
    def get_best_bid(self):
        # if statements to get first bid price that is not None
        if self.bidSize1 > 0:
            return self.bidPrice1, self.bidSize1,1
        elif self.bidSize2 > 0:
            return self.bidPrice2, self.bidSize2,2
        elif self.bidSize3 > 0:
            return self.bidPrice3, self.bidSize3,3
        elif self.bidSize4 > 0:
            return self.bidPrice4, self.bidSize4,4
        elif self.bidSize5 > 0:
            return self.bidPrice5, self.bidSize5,5
        else:
            return None, None, None

    # easy methods to update bids,asks from exchange
    def update_bid_by_index(self,index,size):
        if index == 1:
            # remove size from bidSize1
            self.bidSize1 = self.bidSize1 - size

        if index == 2:
            # remove size from bidSize2
            self.bidSize2 = self.bidSize2 - size

        if index == 3:
            # remove size from bidSize3
            self.bidSize3 = self.bidSize3 - size
        
        if index == 4:
            # remove size from bidSize4
            self.bidSize4 = self.bidSize4 - size

        if index == 5:
            # remove size from bidSize5
            self.bidSize5 = self.bidSize5 - size

    def update_ask_by_index(self,index,size):
        if index == 1:
            # remove size from askSize1
            self.askSize1 = self.askSize1 - size

        if index == 2:
            # remove size from askSize2
            self.askSize2 = self.askSize2 - size

        if index == 3:
            # remove size from askSize3
            self.askSize3 = self.askSize3 - size
        
        if index == 4:
            # remove size from askSize4
            self.askSize4 = self.askSize4 - size

        if index == 5:
            # remove size from askSize5
            self.askSize5 = self.askSize5 - size

 
    def outputAsDataFrame(self):
        if self.initializationFlag == False:
            return None
        else:
            outputLine = []
            outputLine.append(self.ticker)
            outputLine.append(self.date)
            outputLine.append(self.timeStamp)
            outputLine.append(self.askPrice5)
            outputLine.append(self.askPrice4)
            outputLine.append(self.askPrice3)
            outputLine.append(self.askPrice2)
            outputLine.append(self.askPrice1)
            outputLine.append(self.bidPrice1)
            outputLine.append(self.bidPrice2)
            outputLine.append(self.bidPrice3)
            outputLine.append(self.bidPrice4)
            outputLine.append(self.bidPrice5)
            outputLine.append(self.askSize5)
            outputLine.append(self.askSize4)
            outputLine.append(self.askSize3)
            outputLine.append(self.askSize2)
            outputLine.append(self.askSize1)
            outputLine.append(self.bidSize1)
            outputLine.append(self.bidSize2)
            outputLine.append(self.bidSize3)
            outputLine.append(self.bidSize4)
            outputLine.append(self.bidSize5)
            oneLine = pd.DataFrame(data = [outputLine], columns = self.outputCols)
            
            return oneLine
            
                
            
                        