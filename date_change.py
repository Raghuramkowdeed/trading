from os import listdir;
from os.path import join;
import csv;
from datetime import datetime;

companies = listdir('data.bkp/nse');
companies = [x.split('.')[0] for x in companies];
print(companies);
for company in companies:
	try:
		with open(join('data.bkp/nse', company + '.csv'), 'r') as infile:
			with open(join('data/nse', company + '.csv'), 'w') as outfile:
				rdr = csv.reader(infile);
				wrtr = csv.writer(outfile);
				wrtr.writerow([x.strip() for x in next(rdr)]); # Ignore Header
				for row in rdr:
					dt = datetime.strptime(row[2], '%d-%b-%Y'); # Change Date Format
					row[2] = dt.strftime('%Y-%m-%d');
					wrtr.writerow([x.strip() for x in row]);
	except:
		pass;
