import requests;
import json;
import csv;
from os.path import join;
from os import listdir, remove;
from lxml import etree;
from io import StringIO, BytesIO;
from datetime import datetime;
# import datetime.strftime as strftime;
# import datetime.strptime as strptime;

def scrape_companies(companies):
  headers = {
    "Connection": "keep-alive",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.100 Safari/537.36",
    "Referer": "https://www.nseindia.com/products/content/equities/equities/eq_security.htm",
    "Accept-Encoding": "gzip, deflate, sdch, br",
    "Accept-Language": "en-GB,en-US;q=0.8,en;q=0.6"
      };
  
  url = "http://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?symbol={0}&segmentLink=3&symbolCount=2&series=ALL&dateRange=24month&fromDate=&toDate=&dataType=PRICEVOLUMEDELIVERABLE"
  for company in companies:
    with open(join('raw_data', company), 'w') as outfile:
      print("Getting " + company);
      try:
        resp = requests.get(url.format(company.replace('&', '%26')), headers=headers);
        outfile.write(resp.text);
        print("Successful " + company);
      except:
        outfile.write(" ");

def clean_companies(raw_data_dir, clean_data_dir):
  companies = listdir(raw_data_dir);
  parser = etree.HTMLParser();
  for company in companies:
    try:
      with open(join(raw_data_dir, company), 'r') as infile:
        html = etree.parse(StringIO(infile.read()), parser);
        # print(html);
        data_div = html.xpath('//div[@id="csvContentDiv"]');
        if len(data_div) > 0:
          data = data_div[0].text;
          csv_data = '\n'.join(data.split(':'));
          with open(join(clean_data_dir, company+'.csv'), 'w') as outfile:
            rdr = csv.reader(StringIO(csv_data));
            wrtr = csv.writer(outfile);
            wrtr.writerow([x.strip() for x in next(rdr)]); # Ignore Header
            for row in rdr:
              dt = datetime.strptime(row[2], '%d-%b-%Y'); # Change Date Format
              row[2] = dt.strftime('%Y-%m-%d');
              wrtr.writerow([x.strip() for x in row]);
              # outfile.write(line + '\n');
        else:
          continue;
      remove(join(raw_data_dir, company));
    except:
      pass;

def clean_update_companies(raw_data_dir, clean_data_dir):
  companies = listdir(raw_data_dir);
  parser = etree.HTMLParser();
  for company in companies:
    try:
      with open(join(raw_data_dir, company), 'r') as infile:
        html = etree.parse(StringIO(infile.read()), parser);
        # print(html);
        data_div = html.xpath('//div[@id="csvContentDiv"]');
        if len(data_div) > 0:
          data = data_div[0].text;
          csv_data = '\n'.join(data.split(':'));
          with open(join(clean_data_dir, company+'.csv'), 'a') as outfile:
            rdr = csv.reader(StringIO(csv_data));
            wrtr = csv.writer(outfile);
            # wrtr.writerow([x.strip() for x in next(rdr)]); # Ignore Header
            next(rdr);
            for row in rdr:
              dt = datetime.strptime(row[2], '%d-%b-%Y'); # Change Date Format
              row[2] = dt.strftime('%Y-%m-%d');
              wrtr.writerow([x.strip() for x in row]);
              # outfile.write(line + '\n');
            else:
              continue;
      remove(join(raw_data_dir, company));
    except:
      pass;

def fetch_company_update(company, start_date, end_date, retries):
  headers = {
    "Connection": "keep-alive",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.100 Safari/537.36",
    "Referer": "https://www.nseindia.com/products/content/equities/equities/eq_security.htm",
    "Accept-Encoding": "gzip, deflate, sdch, br",
    "Accept-Language": "en-GB,en-US;q=0.8,en;q=0.6"
      };

  url = "http://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?symbol={}&segmentLink=3&symbolCount=2&series=ALL&dateRange=+&fromDate={}&toDate={}&dataType=PRICEVOLUMEDELIVERABLE"
  try:
    print("Getting " + company);
    resp = requests.get(url.format(company.replace('&', '%26'), start_date, end_date), headers=headers, timeout=10);
#    outfile.write(resp.text);
    print("Successful " + company);
    return resp.text;
  except requests.exceptions.Timeout:
    if retries == 0:
#      remove(join('raw_data', company));
      with open('failed', 'a') as f:
        f.write(company + "\n");
    else:
      fetch_company_update(company, start_date, end_date, retries-1);
  return "Failure";

def update_companies(companies):
  start_date = input("Start Date in dd-mm-yyyy format: ").strip();
  end_date = input("End Date in dd-mm-yyyy format: ").strip();
  try:
    datetime.strptime(start_date, '%d-%m-%Y');
    datetime.strptime(end_date, '%d-%m-%Y');
  except ValueError:
    raise ValueError("Incorrect date format, should be dd-mm-yyyy")
  for company in companies:
    data = fetch_company_update(company, start_date, end_date, 5);
    if not data == "Failure":
      with open(join('raw_data', company), 'w') as outfile:
        outfile.write(data);


if __name__ == "__main__":
  pass;
