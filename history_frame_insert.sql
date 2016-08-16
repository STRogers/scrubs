INSERT INTO history SELECT 
    buys.type_id,
    buys.region_id,
    buys.station_id,
    %s as record_time,
    allOrders.all_volume,
    buys.buy_volume,
    sells.sell_volume,
    buys.max_buy,
    sells.min_sell,
    buys.buy_std_dev,
    sells.sell_std_dev,
    allOrders.all_wavg,
    buys.buy_wavg,
    sells.sell_wavg,
    allOrders.all_count,
    buys.buy_count,
    sells.sell_count
FROM (
    SELECT 
        o.type_id,
        o.region_id,
        o.station_id,
        SUM(o.volume) as buy_volume,
        MAX(o.price) as max_buy,
        SQRT( SUM(o.volume*o.price*o.price)/SUM(o.volume) - (SUM(o.volume*o.price)/SUM(o.volume))*(SUM(o.volume*o.price)/SUM(o.volume))) as buy_std_dev,
        SUM(o.volume*o.price)/SUM(o.volume) as buy_wavg,
        COUNT(*) as buy_count
    FROM orders as o WHERE o.buy=True
    GROUP BY o.type_id,o.region_id,o.station_id
) as buys
JOIN (
    SELECT
        o.type_id,
        o.region_id,
        o.station_id,
        SUM(o.volume) as sell_volume,
        MIN(o.price) as min_sell,
        SQRT( SUM(o.volume*o.price*o.price)/SUM(o.volume) - (SUM(o.volume*o.price)/SUM(o.volume))*(SUM(o.volume*o.price)/SUM(o.volume))) as sell_std_dev,
        SUM(o.volume*o.price)/SUM(o.volume) as sell_wavg,
        COUNT(*) as sell_count
    FROM orders as o WHERE o.buy=False
    GROUP BY o.type_id,o.region_id,o.station_id
) as sells ON sells.type_id=buys.type_id AND sells.region_id=buys.region_id AND sells.station_id=buys.station_id
JOIN (
    SELECT
        o.type_id,
        o.region_id,
        o.station_id,
        SUM(o.volume) as all_volume,
        SUM(o.volume*o.price)/SUM(o.volume) as all_wavg,
        COUNT(*) as all_count
    FROM orders as o
    GROUP BY o.type_id,o.region_id,o.station_id
) as allOrders ON allOrders.type_id=buys.type_id AND allOrders.region_id=buys.region_id AND allOrders.station_id=buys.station_id;