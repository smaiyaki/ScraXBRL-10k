
import os
import pickle
import queue
import sys
import threading
import time
# import XMLExtract
import urllib

import pandas as pd
import requests
import wget

import EdgarScrapeMin
import logs
import settings
from datetime import datetime

# import sys
# sys.path.append('/Applications/PyVmMonitor.app/Contents/MacOS/public_api')
# import pyvmmonitor
# @pyvmmonitor.profile_method



class ScrapeAndExtractThreaded:

    def __init__(self, symbol_queue, download_queue):
        self.symbol_queue = symbol_queue
        self.download_queue = download_queue

        #Scrape Section
        self.stock_lists = os.listdir(settings.STOCK_EXCHANGE_LIST_PATH)
        self.symbol_keys = []
        self.scraped_keys = None
        self.finished = False

        #Extract Section
        self.extracted_keys = None
        self.to_extract = queue.Queue()

        self.populate_symbol_keys()
        self.get_all_keys()

    def populate_symbol_keys(self):
        '''Grab all the keys from file'''
        for xlist in self.stock_lists:
            f = pd.read_csv('{0}/{1}'.format(settings.STOCK_EXCHANGE_LIST_PATH, xlist))
            for symbol in f[f.columns[0]]:
                if symbol not in self.symbol_keys:
                    self.symbol_keys.append(symbol)

    def get_all_keys(self):
        scraped_data_log = pickle.load(open(settings.SCRAPE_LOG_FILE_PATH, "rb"))
        self.scraped_keys = list(scraped_data_log.keys())
        extracted_data_log = pickle.load(open(settings.EXTRACT_LOG_FILE_PATH, "rb"))
        self.extracted_keys = list(extracted_data_log.keys())
        sk_set = set(self.scraped_keys)
        ek_set = set(self.extracted_keys)
        to_extract = list(sk_set - ek_set)
        for te in to_extract:
            self.to_extract.put(te)

    @staticmethod
    def scrape_symbol(symbol, download_queue):
        sym_gf = EdgarScrapeMin.GetFilings(symbol, download_queue)
        if sym_gf.filings['errors']['count'] > 0:
            soe_error_data = sym_gf.filings['errors']
            logs.add_scrape_data(symbol, soe_error_data, False)
        if sym_gf.filings['success']['count'] > 0:
            soe_success_data = sym_gf.filings['success']
            logs.add_scrape_data(symbol, soe_success_data, True)
        else:
            logs.add_scrape_data(symbol, None, True)

    # def scrape_list(self):
    #     """Scrape all symbols in list."""
    #     for symbol in self.symbol_keys:
    #         if symbol in self.scraped_keys:
    #             continue
    #         self.scrape_symbol(symbol)
    #         self.scraped_keys.append(symbol)
    #         self.to_extract.put(symbol)
    #     self.finished = True

    def queue_scrape_list(self):
        """Queues up all symbols in list."""
        for symbol in self.symbol_keys:
            if symbol in self.scraped_keys:
                continue
            # self.scrape_symbol(symbol)
            self.symbol_queue.put(symbol)
            print("Putting symbol {}".format(symbol))
            # self.scraped_keys.append(symbol)
            # self.to_extract.put(symbol)
        self.finished = True


# scrape_and_extract = ScrapeAndExtractThreaded()


def run_main_threaded():
    '''Runs the threaded downloader'''
    symbolqueue = queue.Queue()
    downloadqueue = queue.Queue()
    sc = ScrapeAndExtractThreaded(symbolqueue, downloadqueue)


    class FilingsThread(threading.Thread):
        '''Symbol-Scrape EdgarFilings Thread
        Scrapes sec.gov/edgar for 10-k html filings for specific ticker symbol'''
        def __init__(self, name, symbol_queue, download_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.symbol_queue = symbol_queue
            self.download_queue = download_queue

        def run(self):
            while True:
                # Grabs symbol from queue
                symbol = self.symbol_queue.get()
                # Runs the symbol scraper to generate download urls and filepath
                sc.scrape_symbol(symbol, self.download_queue)
                self.symbol_queue.task_done()

    class DownloadThread(threading.Thread):
        '''HTML 10-K Downloader Thread'''
        def __init__(self, name, download_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.download_queue = download_queue

        def run(self):
            while True:
                download_info = self.download_queue.get()
                # print(download_info)
                download_url, download_path = download_info
                wget.download(download_url, download_path)
                self.download_queue.task_done()


    print("Main Thread - Starting Filings Thread Pool")
    for i in range(5):
        filing_thread_name = "FilingThread-[{}]".format(i+1)
        t = FilingsThread(filing_thread_name, symbolqueue, downloadqueue)
        t.setDaemon(True)
        t.start()
        print("Starting Filings thread #{}".format(i))

    print("Main Thread - Download Thread Pool")
    for i in range(5):
        download_thread_name = "DownloadThread-[{}]".format(i+1)
        dt = DownloadThread(download_thread_name, downloadqueue)
        dt.setDaemon(True)
        print("Starting Download thread #{}".format(i))
        dt.start()

    sc.queue_scrape_list()

    timestart = datetime.now()
    print("The game has begun. The time is {}".format(timestart)
    symbolqueue.join()
    time_symbolqueue_close = datetime.now()
    print("The Filings Thread Pool has closed sucessfully I hope.... The time is {}".format(time_symbolqueue_close)
    downloadqueue.join()
    time_downloadqueue_close = datetime.now()
    print("The download queue has completed successfully. I hope. Everything is actually okay")
    print("The time is {}".format(time_downloadqueue_close))
    print("Time elapsed is {}".format(time_downloadqueue_close - timestart))
    print("Happy pipelining you filthy worms")

if __name__ == '__main__':
    run_main_threaded()
