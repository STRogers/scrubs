import sqlite3

def make_project_db():
	with sqlite3.connect('project.db') as conn:
		c = conn.cursor()
		c.execute("""CREATE TABLE history (
					typeId	INTEGER NOT NULL,
					regionId	INTEGER NOT NULL,
					datetime	TEXT NOT NULL,
					allVolume	INTEGER,
					buyVolume	INTEGER,
					sellVolume	INTEGER,
					maxBuy	NUMERIC,
					minSell	NUMERIC,
					buyVariance	NUMERIC,
					sellVariance	NUMERIC,
					allAvg	NUMERIC,
					buyAvg	NUMERIC,
					sellAvg	NUMERIC,
					allCount	INTEGER,
					buyCount	INTEGER,
					sellCount	INTEGER)""")

		c.execute("""CREATE TABLE orders (
					TypeId	INTEGER NOT NULL,
					RegionId	INTEGER NOT NULL,
					StationID	INTEGER NOT NULL,
					Id	INTEGER NOT NULL,
					VolumeEntered	INTEGER NOT NULL,
					Volume	INTEGER NOT NULL,
					minVolume	INTEGER NOT NULL,
					buy	INTEGER NOT NULL,
					Price	NUMERIC NOT NULL,
					IssuedDate	TEXT NOT NULL,
					Duration	INTEGER NOT NULL,
					PRIMARY KEY(Id))""")
		c.commit()
if __name__ == '__main__':
	make_project_db()
