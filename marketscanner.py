import sys
import configparser
import argparse
import time
import logging

import requests
import datetime
import MySQLdb

logging.basicConfig(level=logging.INFO,
                    filename='log/marketpolling.log',
                    format='[%(asctime)s] %(message)s')

logging.info('================ [SCANNER STARTING UP] ================')

parser = argparse.ArgumentParser(description='test config reading')
parser.add_argument('-c','--config',dest='config_file', action='store', default='defaults.cfg')

args = parser.parse_args()
config = configparser.ConfigParser()
config.read(args.config_file)
SQL_ADDR = config['mysql']['address']
SQL_DB   = config['mysql']['database']
SQL_USER = config['mysql']['username']
SQL_PASS = config['mysql']['password']

class DB(object):
    def __init__(self, *args, **kwargs):
        self.c_args = args
        self.c_kwargs = kwargs

    def _connect(self):
        self.conn = MySQLdb.connect(*self.c_args,**self.c_kwargs)

    def query(self, *args, **kwargs):
        try:
          cursor = self.conn.cursor()
          cursor.execute(*args, **kwargs)
        except (AttributeError, MySQLdb.OperationalError):
          self._connect()
          cursor = self.conn.cursor()
          cursor.execute(*args, **kwargs)
        return cursor

    def querymany(self, *args, **kwargs):
        try:
          cursor = self.conn.cursor()
          cursor.executemany(*args, **kwargs)
        except (AttributeError, MySQLdb.OperationalError):
          self._connect()
          cursor = self.conn.cursor()
          cursor.executemany(*args, **kwargs)
        return cursor

    def commit(self):
        self.conn.commit()

market_db = DB(host=SQL_ADDR, user=SQL_USER, passwd=SQL_PASS, db=SQL_DB)
CREST_REGIONS = [int(config['regions'][region]) for region in config['regions']]

def test_db_connection():
    try:
        db = MySQLdb.connect(host=SQL_ADDR, user=SQL_USER, passwd=SQL_PASS, db=SQL_DB)
        return 'PASS'
    except Exception as e:
        return e


def test_crest_online():
    # This doesn't seem like a good way to check if crest is down, oh well.
    href = 'https://crest-tq.eveonline.com/regions/'
    try:
        r = requests.get(href)
        if 'items' in r.json():
            return True
        else:
            raise CRESTOfflineException('Error with EVE-CREST')
    except requests.HTTPError as e:
        raise CRESTOfflineException('Error with EVE-CREST')



def getpages(href):
    '''Recursively get CREST page items.'''
    try:
        json = requests.get(href).json()
        items = json['items']
        if 'next' in json:
            next_page = json['next']['href']
            items += getpages(next_page)
        return items
    except requests.HTTPError as e:
        raise CRESTOfflineException('Error with EVE-CREST')
    
    

def get_market_order_json(regions):
    test_crest_online()
    orders = []
    logging.info('Starting crest market order retrieval.')
    # accumulate all orders across all desired regions
    for region in regions:
        enpoint = 'https://crest-tq.eveonline.com/market/{}/orders/all/'.format(region)
        region_orders = getpages(enpoint)
        logging.info('Got {} orders from region {}.'.format(len(region_orders),region))
        for order in region_orders:
            order['regionID'] = region
        orders += region_orders

    return orders


class CRESTOfflineException(Exception):
    '''Basic Exception in the event a CREST request fails.'''
    def __init__(self, *args, **kwargs):
        super(CRESTOfflineException, self).__init__(*args, **kwargs)

class MarketScanner():
    def __init__(self, poll_freq=300, sql_max_insert=5000):
        self.sql_max_insert = sql_max_insert
        self.poll_freq = poll_freq
    def run(self):
        while True:
            try:
                timestamp = self.order_stats()
                
            except CRESTOfflineException:
                pass # crest is down, wait till next polling cycle to try again.
                logging.info('CREST Error, waiting until next polling cycle to try again.')

            logging.info('Update completed successfully.')
            time_to_sleep = min(timestamp+self.poll_freq-time.time(),self.poll_freq)
            logging.info('Processing orders and history frame took {} seconds.'.format(int(time.time()-timestamp)))
            time_to_sleep = max(time_to_sleep,0) # ensure no negative wait.

            logging.info('Next market query in {} seconds.'.format(int(time_to_sleep)))
            
            time.sleep(time_to_sleep)

    def update_orders(self, regions):
        '''Update order table for a region.'''
        all_orders = get_market_order_json(regions)
        unique_order_ids = set()

        
        
        records = []
        duplicate_orders = 0

        for order in all_orders:
            type_id = order['type']
            region_id = order['regionID']
            station_id = order['stationID']
            order_id = order['id']
            volume_entered = order['volumeEntered']
            volume = order['volume']
            min_volume = order['minVolume']
            buy = 1 if bool(order['buy']) else 0
            price = order['price']
            issued_date = order['issued']
            duration = order['duration']

            if order_id in unique_order_ids:
                duplicate_orders += 1
                continue # skip adding this order if its already logged
            else:
                unique_order_ids.add(order_id)
            
            records.append((type_id,region_id,station_id,order_id,volume_entered,volume,min_volume,buy,price,issued_date,duration))

        for region in regions:
            logging.info('Clearing orders for region {}.'.format(region))
            market_db.query('DELETE FROM orders WHERE region_id=%s;',(region,))
        # batch inserts if we have a limit
        if self.sql_max_insert:
            batches = [records[i:i+self.sql_max_insert] for i in range(0,len(records), self.sql_max_insert)]
            for batch in batches:
                market_db.querymany('INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',batch)
        else:
            market_db.querymany('INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',records)
            
        market_db.commit()

        logging.info('Updated orders table with {} orders across {} region(s). Ignored {} duplicates.'.format(len(unique_order_ids), len(CREST_REGIONS),duplicate_orders))
        return time.time()

    def order_stats(self):
        '''Calculate stats on current orders'''
        time_stamp = self.update_orders(CREST_REGIONS)

        with open('history_frame_insert.sql','r') as query_file:
            query = query_file.read()
            for region in CREST_REGIONS:
                logging.info('Adding history frame for region {}.'.format(region))
                market_db.query(query,(time_stamp,region)) # only add history for regions we are polling

        market_db.commit()


        return time_stamp







if __name__ == '__main__':
    scanner = MarketScanner()
    scanner.run()
