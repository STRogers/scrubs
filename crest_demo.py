import requests
import datetime
import MySQLdb as mysql

import configparser
import argparse
import time

parser = argparse.ArgumentParser(description='test config reading')
parser.add_argument('-c','--config',dest='config_file', action='store', default='defaults.cfg')

args = parser.parse_args()
config = configparser.ConfigParser()
config.read(args.config_file)
SQL_ADDR = config['mysql']['address']
SQL_DB   = config['mysql']['database']
SQL_USER = config['mysql']['username']
SQL_PASS = config['mysql']['password']

def test_db_connection():
    try:
        db = mysql.connect(host=SQL_ADDR, user=SQL_USER, passwd=SQL_PASS, db=SQL_DB)
        return 'PASS'
    except Exception as e:
        return e


def QUERY_ORDERS_BY_ID(orderIDs, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute('CREATE TEMP TABLE orderIDs (id INT);')
        c.executemany('INSERT INTO orderIDs VALUES (?);',orderIDs)
        c.execute("""SELECT *
                    FROM orders AS o
                    WHERE o.id=orderIDs.id
                    """)
        results = c.fetchall()

    return results

def updateOrderTable(regionID=10000002):
    '''Update order table for a region.'''
    # setup the crest url string
    href = 'https://crest-tq.eveonline.com/market/{}/orders/all/'.format(regionID)
    href = href.format(regionID=regionID)

    r = requests.get(href) # get market orders from crest
    rawJson = r.json() # parse json
    orders = rawJson['items'] # orders stored under 'items' key in the json
    page = 1
    while 'next' in rawJson:
        page += 1
        rawJson = requests.get(rawJson['next']['href']).json()
        orders += rawJson['items']
        print('Getting page {}.'.format(page))
    
    # orderIDs = [(order['id'],) for order in orders]
    # matchingOrders = QUERY_ORDERS_BY_ID(orderIDs, 'project.db')

    orderIDs = set()

    db = mysql.connect(host=SQL_ADDR, user=SQL_USER, passwd=SQL_PASS, db=SQL_DB)
    c = db.cursor()
    c.execute('DELETE FROM orders;')

    for order in orders:
        TypeId = order['type']
        RegionId = regionID
        StationID = order['stationID']
        Id = order['id']
        VolumeEntered = order['volumeEntered']
        Volume = order['volume']
        minVolume = order['minVolume']
        buy = 1 if bool(order['buy']) else 0
        Price = order['price']
        IssuedDate = order['issued']
        duration = order['duration']

        if Id in orderIDs:
            continue
        else:
            orderIDs.add(Id)
        
        orderRecord = TypeId,RegionId,StationID,Id,VolumeEntered,Volume,minVolume,buy,Price,IssuedDate,duration
        
        c.execute('INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',orderRecord)
        
    db.commit()
    db.close()
        
    return time.time()
        
def orderStats(regionID):
    '''Calculate stats on current orders'''
    time_stamp = updateOrderTable(regionID)

    db = mysql.connect(host=SQL_ADDR, user=SQL_USER, passwd=SQL_PASS, db=SQL_DB)
    c = db.cursor()
    print('inserting history')
    with open('history_frame_insert.sql','r') as query_file:
        query = query_file.read()
        c.execute(query,(time_stamp,))

    db.commit()
    db.close()


print('DB CONNECTION TEST: {}'.format(test_db_connection()))
orderStats(10000002)