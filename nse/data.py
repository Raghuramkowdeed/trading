import sqlite3;
import csv;
import glob;
from os.path import join, dirname, basename;

schema = '''
symbol TEXT,
series TEXT,
date TEXT,
prev_close REAL,
open REAL,
high REAL,
low REAL,
last_traded_price REAL,
close REAL,
avg_price REAL,
total_traded_qty INTEGER,
turnover_in_lacs REAL,
no_of_trades INTEGER,
deliverable_qty INTEGER,
pct_dly_qt_to_traded_qty REAL
'''
data_dir = join(dirname(__file__), "../data/nse");

def load():
	db = sqlite3.connect(':memory:')
	cur = db.cursor();
	global data_dir;
	data_files = glob.glob(join(data_dir, '*')); # listdir(data_dir);
	for df in data_files:
		with open(df, 'r') as f:
			symbol = basename(df).split('.')[0];
			sql_create = 'CREATE TABLE "{}"({});'.format(symbol, schema); #.replace('\n', ' ')
			cur.execute(sql_create);
			rdr = csv.reader(f);
			sql_insert = 'INSERT INTO "{}" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'.format(symbol);
			cur.executemany(sql_insert, rdr);
			sql_del_hdr = "DELETE FROM '{}' WHERE date='Date';".format(symbol);
			cur.execute(sql_del_hdr);
	db.commit();
	cur.close();
	return db;



if __name__ == '__main__':
	pass;
