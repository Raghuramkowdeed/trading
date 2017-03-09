import sqlite3;
import nse.data as data;
import pandas as pd;

db = data.load();
symbols = db.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall();
symbols = [x[0] for x in symbols];
db.row_factory = sqlite3.Row;
with open('nse_training_data', 'w') as outfile:
	for symbol in symbols:
		df = pd.read_sql_query("SELECT date, open, close, high, low FROM '{}';".format(symbol), db);
		# df['date'] = pd.to_datetime(df['date']);
		# df = df.set_index('date');
		print(df.head());
		n = len(df['close']);
		for i in range(98,n-1):
			inp = df['close'][i-98:i].to_csv(index=False);
			inp = inp.replace('\n', ',');
			inp = inp + str(df.at[i,'open']);
			close_t = df.at[i, 'close'];
			high_tplus1 = df.at[i+1, 'high'];
			low_tplus1 = df.at[i+1, 'low'];
			if close_t < low_tplus1:
				inp = inp + ',BUY';
			elif close_t > high_tplus1:
				inp = inp + ',SELL';
			else:
				inp = inp + ',HOLD';
			outfile.write(inp + '\n');
db.close();
