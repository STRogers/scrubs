import requests
import sqlite3
import datetime


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

    with sqlite3.connect('project.db') as conn:
        c = conn.cursor()
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
            
            c.execute('INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?);',orderRecord)
            
        conn.commit()
        
    return datetime.datetime.now()
        
def orderStats(regionID):
    '''Calculate stats on current orders'''
    timeStamp = updateOrderTable(regionID)
    with sqlite3.connect('project.db') as conn:
        c = conn.cursor()
        
        # Volume
        c.execute("""SELECT 
                orders.typeId,
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
                regionID,
                ? as date,
                aggr.vol,
                aggr.avg,
                aggr.ct
            FROM orders
            JOIN (
                SELECT 
                    orders.typeId,
                    SUM(volume) as vol,
                    SUM(price*volume)/SUM(volume) as avg,
                    COUNT() as ct
                FROM orders GROUP BY orders.typeId
            ) AS aggr ON aggr.typeId=orders.typeId
            GROUP BY orders.typeId;""",(str(timeStamp),))
        allData = c.fetchall()
        
        c.execute('CREATE TEMP TABLE buyData (typeId INT, buyVol INT, buyMax REAL, buyWAvg REAL, buyVar REAL, buyCount INT);')
        c.execute('CREATE TEMP TABLE sellData (typeId INT, sellVol INT, sellMin REAL, sellWAvg REAL, sellVar REAL, sellCount INT);')
        c.execute('CREATE TEMP TABLE allData (typeId INT, regionId INT, date TEXT, allVol INT, allWAvg REAL, allCount INT);')
        
        c.executemany('INSERT INTO buyData VALUES (?,?,?,?,?,?);',buyData)
        c.executemany('INSERT INTO sellData VALUES (?,?,?,?,?,?);',sellData)
        c.executemany('INSERT INTO allData VALUES (?,?,?,?,?,?);',allData)
        
        c.execute("""INSERT INTO History
            SELECT a.typeId, 
                a.regionId,
                a.date, 
                a.allVol, 
                b.buyVol,
                s.sellVol, 
                b.buyMax, 
                s.sellMin,
                b.buyVar,
                s.sellVar,
                a.allWAvg,
                b.buyWAvg, 
                s.sellWAvg,
                a.allCount,
                b.buyCount,
                s.sellCount
            FROM buyData AS b JOIN sellData AS s ON b.typeId=s.typeId JOIN allData AS a ON b.typeId=a.typeId;""")
        

orderStats(10000002)

# SUM(((price-SUM(price*volume)/SUM(volume))*(price-SUM(price*volume)/SUM(volume))))/(SUM(volume)-1),