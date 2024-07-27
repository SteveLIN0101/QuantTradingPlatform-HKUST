# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:12:21 2020

@author: hongsong chou
"""

import time
import random
import os
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
import pandas as pd
from multiprocessing import Process, Queue



# helper function to convert time format
def convert_time_format(time_value):    
    time_str = str(time_value).zfill(9)
    hours = int(time_str[:2])
    minutes = int(time_str[2:4])
    seconds = int(time_str[4:6])
    milliseconds = int(time_str[6:])
    
    return pd.Timestamp(year=2024, month=4, day=1, hour=hours, minute=minutes, second=seconds, microsecond=milliseconds * 1000)


class MarketDataService:

    def __init__(self, marketData_2_exchSim_q, marketData_2_platform_q):
        print("[%d]<<<<< call MarketDataService.init" % (os.getpid(),))
        # time.sleep(3)
        # self.produce_market_data(marketData_2_exchSim_q, marketData_2_platform_q)

        self.marketData_2_exchSim_q = marketData_2_exchSim_q
        self.marketData_2_platform_q = marketData_2_platform_q

        self.get_market_data_with_delay(
            stock_id="0050",
            start_on_date="202404"
        )

    # Function to generate market data from .csv.gz files
    def market_data_generator(self,stock_id=None,start_on_date=None):
        dataFolder = 'processedData_2024/stocks'

        # Get all .csv.gz files in the data folder according to filters defined
        files = []

        # first get all files
        for file in os.listdir(dataFolder):
            if file.endswith('.csv.gz'):
                files.append(file)

        # if stock_id is provided, filter the files
        if stock_id:
            files = [file for file in files if stock_id in file]

        # if start_on_date is provided, filter the files
        if start_on_date:
            for file in files:
                # get the date from the file name : e.g. 6443_md_202404_202404.csv.gz (FILTER BEFORE BETWEEN _ and .csv.gz)
                date = int(file.split('_')[2])
                files = [file for file in files if date >= int(start_on_date)]
                

        # sort the files
        files.sort()
        print("SORTED ORDER OF FILES: ", files)

        # List to hold all DataFrames
        all_data = []

        i = 0
        
        # Iterate through all files
        for file in files:
            # Read the data
            data = pd.read_csv(
                os.path.join(dataFolder, file),
                compression='gzip',
                index_col=0,
                parse_dates=True
            )

            data['datetime'] = data['time'].apply(convert_time_format)
            
            # Calculate differences between consecutive times
            data['time_diff'] = data['datetime'].diff()
            
            
            # Append DataFrame to the list
            all_data.append(data)

            i = i + 1

            # for testing just use 3 months
            if i > 3:
                break

        # Concatenate all DataFrames row by row
        combined_data = pd.concat(all_data, ignore_index=True)
        
        return combined_data

    # Function to get and print the data row by row with a delay
    def get_market_data_with_delay(self,stock_id=None,start_on_date=None):
        combined_data = self.market_data_generator(
            stock_id=stock_id,
            start_on_date=start_on_date
        )
        first_val = True
        for index, row in combined_data.iterrows():

            bidPrice, askPrice, bidSize, askSize = [], [], [], []

            # Row object has following structure , use that to fill the above lists
            # date                         2024-04-01
            # time                           90049771
            # lastPx                          15815.0
            # size                              165.0
            # volume                                0
            # SP5                             15845.0
            # SP4                             15840.0
            # SP3                             15830.0
            # SP2                             15825.0
            # SP1                             15820.0
            # BP1                             15815.0
            # BP2                             15805.0
            # BP3                             15800.0
            # BP4                             15795.0
            # BP5                             15790.0
            # SV5                                   1
            # SV4                                  31
            # SV3                                   1
            # SV2                                   2
            # SV1                                  37
            # BV1                                 244
            # BV2                                 186
            # BV3                                 155
            # BV4                                 152
            # BV5                                  83
            # datetime     2024-04-01 09:00:49.771000
            # time_diff        0 days 00:00:05.014000

            bidPrice = [row['BP5'], row['BP4'], row['BP3'], row['BP2'], row['BP1']]
            askPrice = [row['SP1'], row['SP2'], row['SP3'], row['SP4'], row['SP5']]
            bidSize = [row['BV5'], row['BV4'], row['BV3'], row['BV2'], row['BV1']]
            askSize = [row['SV1'], row['SV2'], row['SV3'], row['SV4'], row['SV5']]
            quoteSnapshot = OrderBookSnapshot_FiveLevels(stock_id, on_date, row['datetime'], 
                                                     bidPrice, askPrice, bidSize, askSize)


            print('[%d]MarketDataService>>>produce_quote' % (os.getpid()))

            # Put the data in the queue
            self.marketData_2_exchSim_q.put(quoteSnapshot)
            self.marketData_2_platform_q.put(quoteSnapshot)

            # save to logs called market_data_output.txt
            with open('market_data_output.txt', 'a') as f:
                # Increase the display width to show more columns
                pd.set_option('display.max_columns', 100)
                f.write(quoteSnapshot.outputAsDataFrame().to_string(index=False) + '\n')

            # no delay for the first row
            if first_val:
                first_val = False
                continue 
                
            time.sleep(row['time_diff'].total_seconds())  # Wait for the specified delay


    # def produce_market_data(self, marketData_2_exchSim_q, marketData_2_platform_q):
    #     for i in range(10):
    #         self.produce_quote(marketData_2_exchSim_q, marketData_2_platform_q)
    #         time.sleep(5)

    # def produce_quote(self, marketData_2_exchSim_q, marketData_2_platform_q):
    #     bidPrice, askPrice, bidSize, askSize = [], [], [], []
    #     bidPrice1 = 20+random.randint(0,100)/100
    #     askPrice1 = bidPrice1 + 0.01
    #     for i in range(5):
    #         bidPrice.append(bidPrice1-i*0.01)
    #         askPrice.append(askPrice1+i*0.01)
    #         bidSize.append(100+random.randint(0,100)*100)
    #         askSize.append(100+random.randint(0,100)*100)
    #     quoteSnapshot = OrderBookSnapshot_FiveLevels('testTicker', '20230706', time.asctime(time.localtime(time.time())), 
    #                                                  bidPrice, askPrice, bidSize, askSize)
    #     print('[%d]MarketDataService>>>produce_quote' % (os.getpid()))
    #     print(quoteSnapshot.outputAsDataFrame())
    #     marketData_2_exchSim_q.put(quoteSnapshot)
    #     marketData_2_platform_q.put(quoteSnapshot)


# To run directly for testing
if __name__ == '__main__':
    # create 2 place holder queues for market data
    sample_marketData_2_exchSim_q = Queue()
    sample_marketData_2_platform_q = Queue()


    marketDataService = MarketDataService(sample_marketData_2_exchSim_q, sample_marketData_2_platform_q)
    marketDataService.get_market_data_with_delay(
    )