import requests
import os
import json
import codecs
import pandas as pd
from datetime import date, timedelta
import time
from retrying import retry
import random

@retry(stop_max_attempt_number=5, wait_fixed=5000)
def download_twse_csv(download_path,data_date):
	data_date_parsed = data_date.replace('-','')
	json_URL = f'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={data_date_parsed}&type=ALLBUT0999&_=1569302830946'

	with requests.get(json_URL, stream=True) as r:
		if not r:
			print("Requests status is bad: {}. Raising exception.".format(r.status_code))
			r.raise_for_status()
			time.sleep(10)
		
		d = json.loads(r.content.decode('utf-8'))
		status = True

	if 'data9' in d:
		df = pd.DataFrame(d['data9']).iloc[:, [0,1,2,4,5,6,7,8]].rename(columns={0: 'StockNo', 1:'StockName', 2:'vol', 4:'total', 5:'open', 6:'high', 7:'low', 8:'close'})	
		df.to_csv(download_path, index=False)	
	elif 'data8' in d:
		df = pd.DataFrame(d['data8']).iloc[:, [0,1,2,4,5,6,7,8]].rename(columns={0: 'StockNo', 1:'StockName', 2:'vol', 4:'total', 5:'open', 6:'high', 7:'low', 8:'close'})
		df.to_csv(download_path, index=False)
	else:
		status = False

	return status
	


def main():
	d1 = date(2013, 1, 9)  # start date
	d2 = date(2019, 9, 23)  # end date

	delta = d2 - d1         # timedelta
	dates = [d1 + timedelta(days=x) for x in range((d2-d1).days + 1)]
	
	save_path = 'D:\\Strategic_Trading\\Stock_Crawl\\data\\'
	for i, d in enumerate(dates):
		data_date = str(d)
		file_name = 'ALL_STOCK_{datadate}.csv'.format(datadate=data_date)
		complete_file_path = save_path+file_name
		
		if download_twse_csv(download_path=complete_file_path,data_date=data_date):
			print("File: {file_name} download complete, transforming file.".format(file_name=complete_file_path))

		else:
			print("File: {file_name} is empty, removing file.".format(file_name=complete_file_path)) 

		if i % 5 == 0:
			time.sleep(random.randint(10, 20))
		else:
			time.sleep(3)
	
	
if __name__ == "__main__":
	main()