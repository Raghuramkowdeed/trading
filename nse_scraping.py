import nse.historical as hist;

with open('data/nse_companies') as file:
	companies = [x.strip() for x in file.readlines()];
	# companies = listdir('../raw_data');
	# for company in companies:
	# 	remove(join('../raw_data', company));
	hist.scrape_companies(companies);
	hist.clean_companies('raw_data', 'data');
