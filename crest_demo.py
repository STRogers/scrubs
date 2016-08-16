import requests
import datetime
import MySQLdb as mysql

import configparser
import argparse

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
        return True
    except:
        return False


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
        
    return datetime.datetime.now()
        
def orderStats(regionID):
    '''Calculate stats on current orders'''
    timeStamp = updateOrderTable(regionID)
    with sqlite3.connect('project.db') as conn:
        c = conn.cursor()
        
        # Volume
        c.execute("""SELECT 
                orders.typeId,
                orders.regionId,
                aggr.vol,
                MAX(price),
                aggr.avg,
                SUM((price-aggr.avg)*(price-aggr.avg))/(aggr.ct-1),
                aggr.ct
            FROM orders
            JOIN (
                SELECT 
                    orders.typeId,
                    SUM(volume) as vol,
                    SUM(price*volume)/SUM(volume) as avg,
                    COUNT() as ct
                FROM orders WHERE buy=1 GROUP BY orders.typeId
            ) AS aggr ON aggr.typeId=orders.typeId
            WHERE buy=1 GROUP BY orders.typeId;""")
        buyData = c.fetchall()
        
        c.execute("""SELECT 
                orders.typeId,
                orders.regionId,
                aggr.vol,
                MAX(price),
                aggr.avg,
                SUM((price-aggr.avg)*(price-aggr.avg))/(aggr.ct-1),
                aggr.ct
            FROM orders
            JOIN (
                SELECT 
                    orders.typeId,
                    SUM(volume) as vol,
                    SUM(price*volume)/SUM(volume) as avg,
                    COUNT() as ct
                FROM orders WHERE buy=0 GROUP BY orders.typeId
            ) AS aggr ON aggr.typeId=orders.typeId
            WHERE buy=0 GROUP BY orders.typeId;""")
        sellData = c.fetchall()
        
        c.execute("""SELECT 
                orders.typeId,
                orders.regionId,
                ? as date,
                aggr.vol,
                aggr.avg,
                0,
                aggr.ct
            FROM orders
            JOIN (
                SELECT 
                    orders.typeId,
                    orders.price as p1,
                    orders.price as p2,
                    SUM(volume) as vol,
                    SUM(price*volume)/SUM(volume) as avg,
                    COUNT() as ct
                FROM orders GROUP BY orders.typeId
            ) AS aggr ON aggr.typeId=orders.typeId
            GROUP BY orders.typeId,orders.regionId;""",(str(timeStamp),))
        allData = c.fetchall()
        
        c.execute('CREATE TEMP TABLE buyData (typeId INT, regionId INT, buyVol INT, buyMax REAL, buyWAvg REAL, buyVar REAL, buyCount INT);')
        c.execute('CREATE TEMP TABLE sellData (typeId INT, regionId INT, sellVol INT, sellMin REAL, sellWAvg REAL, sellVar REAL, sellCount INT);')
        c.execute('CREATE TEMP TABLE allData (typeId INT, regionId INT, date TEXT, allVol INT, allWAvg REAL, allVar REAL, allCount INT);')
        
        c.executemany('INSERT INTO buyData VALUES (?,?,?,?,?,?,?);',buyData)
        c.executemany('INSERT INTO sellData VALUES (?,?,?,?,?,?,?);',sellData)
        c.executemany('INSERT INTO allData VALUES (?,?,?,?,?,?,?);',allData)
        
        c.execute("""INSERT INTO History
            SELECT a.typeId, 
                a.regionId,
                a.date, 
                a.allVol, 
                b.buyVol,
                s.sellVol, 
                b.buyMax, 
                s.sellMin,
                a.allVar,
                b.buyVar,
                s.sellVar,
                a.allWAvg,
                b.buyWAvg, 
                s.sellWAvg,
                a.allCount,
                b.buyCount,
                s.sellCount
            FROM buyData AS b JOIN sellData AS s ON b.typeId=s.typeId AND b.regionId=s.regionId JOIN allData AS a ON b.typeId=a.typeId AND b.regionId=a.regionId;""")
        
test_db_connection()
updateOrderTable(10000002)