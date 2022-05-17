import shutil
from asyncio.windows_events import NULL
from calendar import c
from cgi import print_environ
import os

import pyodbc as pyodbc
from selenium.webdriver.common.by import By
import requests
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.select import Select
from selenium import webdriver
import time
import datetime
import sqlite3
from csv import writer,reader
from selenium.common.exceptions import TimeoutException
# from datetime import datetime
from os import path
from glob import glob
import logging



app_name = 'wbiwd_gov_in'
# all_file_path = f'D:\python\projects\{app_name}'
all_file_path = os.getcwd()
sqlite_path = f'{all_file_path}\{app_name}.db'
csv_path = f'{all_file_path}\{app_name}.csv'
log_path = f'{all_file_path}\{app_name}.log'
temp_down_path = os.path.expanduser('~') + '\\Documents\\pythonfiles\\' + app_name + '\\temp_files'
download_path = os.path.expanduser('~') + '\\Documents\\pythonfiles\\' + app_name + '\\files'
main_list, data_list = [], []
page_nos, skip_tenders_counts, pos = '', 0, 0
conn = sqlite3.connect(sqlite_path)
cur = conn.cursor()
if os.path.exists(log_path):
    logging.basicConfig(filename=log_path,format='%(asctime)s %(message)s' , filemode='a',level=logging.INFO)
else:
    logging.basicConfig(filename=log_path,format='%(asctime)s %(message)s' , filemode='w',level=logging.INFO)
if os.path.exists(all_file_path):
    pass
else:
    os.makedirs(all_file_path)
if os.path.exists(download_path):
    pass
else:
    os.makedirs(download_path)
if os.path.exists(temp_down_path):
    pass
else:
    os.makedirs(temp_down_path)


search_url = 'https://wbiwd.gov.in/index.php/applications/tenders/'
options = webdriver.ChromeOptions()
prefs = {'download.default_directory' : temp_down_path}
options.add_argument("user-data-dir=C:\\Path")
# options.add_experimental_option('prefs', prefs)
options.add_experimental_option('prefs', {
"download.default_directory": temp_down_path, #Change default directory for downloads
"download.prompt_for_download": False, #To auto download the file
"download.directory_upgrade": True,
"plugins.always_open_pdf_externally": True #It will not show PDF directly in chrome
})
# driver = webdriver.Chrome(options = options,service=Service(executable_path=f"{all_file_path}\chromedriver.exe"))
driver = webdriver.Chrome(options=options)
driver.get(search_url)
logging.info(f'Connection to {search_url}')
driver.maximize_window()
action = ActionChains(driver)
time.sleep(1)


def new_dow(single_link):
    global d_path
    files = ''
    for j in os.listdir(temp_down_path):
        # if j.endswith('.pdf') or j.endswith('.PDF') or j.endswith('.docx') or j.endswith('.doc'):
        files = j

    print(f'-----{files}-----')
    if files != '' and files in single_link:
        if files.endswith('.pdf') or files.endswith('.PDF'):
            name1 = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
            os.rename(temp_down_path + '\\' + files, temp_down_path + '\\' + 'wbiwd_' + name1 + '.pdf')
            time.sleep(1)
            shutil.move(temp_down_path + '\\' + 'wbiwd_' + name1 + '.pdf', download_path)
            time.sleep(1)
            # data_list.append(download_path + '\\' + 'wbiwd_' + name1 + '.pdf')
            d_path = download_path + '\\' + 'wbiwd_' + name1 + '.pdf'
            logging.info(f"File download completed = {d_path}")
        else:
            name1 = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
            os.rename(temp_down_path + '\\' + files, temp_down_path + '\\' + 'wbiwd_' + name1 + '.docx')
            time.sleep(1)
            shutil.move(temp_down_path + '\\' + 'wbiwd_' + name1 + '.docx', download_path)
            time.sleep(1)
            # data_list.append(download_path + '\\' + 'wbiwd_' + name1 + '.pdf')
            d_path = download_path + '\\' + 'wbiwd_' + name1 + '.docx'
            logging.info(f"File download completed = {d_path}")
    else:
        time.sleep(1)
        print(f'reload because file is "{files}"')
        new_dow(single_link)
    print(f'download function = {d_path}', '\n')
    return d_path


def new_down_pdf(link):
    driver.execute_script(f"window.open('{link}');")
    time.sleep(1)
    return new_dow(link)


def sqlite_code(main_li, po):
    cur.executemany('INSERT INTO tenders(Bid_deadline_2,OpeningDate,Tender_Summery,Tender_Notice_No,Documents_2) VALUES(?,?,?,?,?);', main_li)
    conn.commit()
    logging.info(f'Data inserted successfully {tuple(main_li)}')

    cur.execute("SELECT Bid_deadline_2,OpeningDate,Tender_Summery,Tender_Notice_No,Documents_2 FROM tenders WHERE flag = ?", (1,))
    data2 = cur.fetchall()
    print(data2)
    if data2 != []:
        with pyodbc.connect('DRIVER={SQL Server};SERVER=153TESERVER;DATABASE=CrawlingDB;UID=hrithik;PWD=hrithik@123') as conns:
            with conns.cursor() as cursor:
                q = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{app_name}' AND xtype='U') CREATE TABLE {app_name}(Id INTEGER PRIMARY KEY IDENTITY(1,1)\
                                         ,Tender_Notice_No TEXT\
                                         ,Tender_Summery TEXT\
                                         ,Tender_Details TEXT\
                                         ,Bid_deadline_2 TEXT\
                                         ,Documents_2 TEXT\
                                         ,TenderListing_key TEXT\
                                         ,Notice_Type TEXT\
                                         ,Competition TEXT\
                                         ,Purchaser_Name TEXT\
                                         ,Pur_Add TEXT\
                                         ,Pur_State TEXT\
                                         ,Pur_City TEXT\
                                         ,Pur_Country TEXT\
                                         ,Pur_Email TEXT\
                                         ,Pur_URL TEXT\
                                         ,Bid_Deadline_1 TEXT\
                                         ,Financier_Name TEXT\
                                         ,CPV TEXT\
                                         ,scannedImage TEXT\
                                         ,Documents_1 TEXT\
                                         ,Documents_3 TEXT\
                                         ,Documents_4 TEXT\
                                         ,Documents_5 TEXT\
                                         ,currency TEXT\
                                         ,actualvalue TEXT\
                                         ,TenderFor TEXT\
                                         ,TenderType TEXT\
                                         ,SiteName TEXT\
                                         ,createdOn TEXT\
                                         ,updateOn TEXT\
                                         ,Content TEXT\
                                         ,Content1 TEXT\
                                         ,Content2 TEXT\
                                         ,Content3 TEXT\
                                         ,DocFees TEXT\
                                         ,EMD TEXT\
                                         ,OpeningDate TEXT\
                                         ,Tender_No TEXT)"
                cursor.execute(q)
                conns.commit()
                q = f"INSERT INTO {app_name}(Bid_deadline_2,OpeningDate,Tender_Summery,Tender_Notice_No,Documents_2) VALUES(?,?,?,?,?)"
                cursor.executemany(q, data2)
                logging.info(f'Data inserted on server')
                print(f'Data inserted on server')
                po += 1
                print(f'::::::::::::::::::::{po}::::::::::::::::::::', '\n\n\n\n')
        sql1 = f'UPDATE tenders SET flag ={0} WHERE flag = {1};'
        cur.execute(sql1)
        conn.commit()
        logging.info(f'Flag updated')
    else:
        po += 1
        print(f'Data already available in sqlite database')
        logging.info(f'Data already available in sqlite database')
    return po


def scraping_code(skip_tenders_co):
    # data_list = []
    length_of_tr = WebDriverWait(driver, 200).until(EC.presence_of_all_elements_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr')))

    global data_list, i
    for p, i in enumerate(length_of_tr[1:]):
        # page_nos += 1
        td_tags_length = WebDriverWait(i, 200).until(EC.presence_of_all_elements_located((By.XPATH, f'./td')))
        print(f'>>>>>{len(td_tags_length)}<<<<<')
        if len(td_tags_length) == 7:

            da = str(i.text).lower()
            # print(f'*****{da}*****')
            if 'corrigendum' in da or 'corrigemdum' in da:
                logging.info(f'Skip tender because of "corrigendum" = {da}')
                print('skip tender because of "corrigendum"', '\n')
            else:
                cloasing_date = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[4]'))).text
                Bid_deadline_2 = datetime.datetime.strptime(cloasing_date.split(' ')[0], "%Y-%m-%d").strftime('%d-%m-%Y')
                data_list.append(Bid_deadline_2)
                print(Bid_deadline_2)

                Opening_Date = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[5]'))).text
                OpeningDate = datetime.datetime.strptime(Opening_Date.split(' ')[0], "%Y-%m-%d").strftime('%d-%m-%Y')
                data_list.append(OpeningDate)
                print(OpeningDate)

                Tender_Summery = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[6]'))).text
                data_list.append(Tender_Summery)
                print(Tender_Summery)

                Tender_Notice_No = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[2]'))).text
                data_list.append(Tender_Notice_No)
                print(Tender_Notice_No)

                cur.execute("SELECT Tender_Notice_No FROM tenders WHERE OpeningDate = ? and Tender_Summery = ? and Tender_Notice_No = ?",(OpeningDate, Tender_Summery, Tender_Notice_No))
                a = cur.fetchone()

                if a is None:
                    Documents_2 = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[7]/a')))
                    print(Documents_2.get_attribute('href'))
                    data_list.append(new_down_pdf(Documents_2.get_attribute('href')))
                    logging.info(f'Scraped data = {data_list}')
                    main_list.append(data_list)
                    data_list = []
                else:
                    data_list = []
                    logging.info(f'Data already available = {a}')
                    print('Data already available','\n')
        else:
            da = str(i.text).lower()
            # print(f'*****{da}*****')
            if 'corrigendum' in da or 'corrigemdum' in da:
                logging.info(f'Skip tender because of "corrigendum" = {da}')
                print('skip tender because of "corrigendum"', '\n')
            else:
                cloasing_date = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[3]'))).text
                Bid_deadline_2 = datetime.datetime.strptime(cloasing_date.split(' ')[0], "%Y-%m-%d").strftime('%d-%m-%Y')
                data_list.append(Bid_deadline_2)
                print(Bid_deadline_2)

                Opening_Date = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[4]'))).text
                OpeningDate = datetime.datetime.strptime(Opening_Date.split(' ')[0], "%Y-%m-%d").strftime('%d-%m-%Y')
                data_list.append(OpeningDate)
                print(OpeningDate)

                Tender_Summery = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[5]'))).text
                data_list.append(Tender_Summery)
                print(Tender_Summery)

                Tender_Notice_No = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[1]'))).text
                data_list.append(Tender_Notice_No)
                print(Tender_Notice_No)

                cur.execute("SELECT Tender_Notice_No FROM tenders WHERE OpeningDate = ? and Tender_Summery = ? and Tender_Notice_No = ?",(OpeningDate, Tender_Summery, Tender_Notice_No))
                a = cur.fetchone()

                if a is None:
                    Documents_2 = WebDriverWait(i, 200).until(EC.presence_of_element_located((By.XPATH, f'/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[{p + 2}]/td[6]/a')))
                    print(Documents_2.get_attribute('href'))
                    data_list.append(new_down_pdf(Documents_2.get_attribute('href')))
                    logging.info(f'Scraped data = {data_list}')
                    main_list.append(data_list)
                    data_list = []
                else:
                    data_list = []
                    logging.info(f'Data already available = {a}')
                    print('Data already available','\n')


            skip_tenders_co += 1
            logging.info(f'Skip tenders counts which has > 7 length = {skip_tenders_co}')
            print(f'skip tenders counts which has > 7 length {skip_tenders_co}', '\n')
    return skip_tenders_co


v = 1

while True:
    v = 2
    try:
        sql = """  CREATE TABLE IF NOT EXISTS tenders(Id INTEGER PRIMARY KEY AUTOINCREMENT
                                                                 ,Tender_Notice_No TEXT
                                                                 ,Tender_Summery TEXT
                                                                 ,Tender_Details TEXT
                                                                 ,Bid_deadline_2 TEXT
                                                                 ,Documents_2 TEXT
                                                                 ,TenderListing_key TEXT
                                                                 ,Notice_Type TEXT
                                                                 ,Competition TEXT
                                                                 ,Purchaser_Name TEXT
                                                                 ,Pur_Add TEXT
                                                                 ,Pur_State TEXT
                                                                 ,Pur_City TEXT
                                                                 ,Pur_Country TEXT
                                                                 ,Pur_Email TEXT
                                                                 ,Pur_URL TEXT
                                                                 ,Bid_Deadline_1 TEXT
                                                                 ,Financier_Name TEXT
                                                                 ,CPV TEXT
                                                                 ,scannedImage TEXT
                                                                 ,Documents_1 TEXT
                                                                 ,Documents_3 TEXT
                                                                 ,Documents_4 TEXT
                                                                 ,Documents_5 TEXT
                                                                 ,currency TEXT
                                                                 ,actualvalue TEXT
                                                                 ,TenderFor TEXT
                                                                 ,TenderType TEXT
                                                                 ,SiteName TEXT
                                                                 ,createdOn TEXT
                                                                 ,updateOn TEXT
                                                                 ,Content TEXT
                                                                 ,Content1 TEXT
                                                                 ,Content2 TEXT
                                                                 ,Content3 TEXT
                                                                 ,DocFees TEXT
                                                                 ,EMD TEXT
                                                                 ,OpeningDate TEXT
                                                                 ,Tender_No TEXT
                                                                 ,flag INT DEFAULT 1);  """
        cur.execute(sql)
        conn.commit()
        skip_tenders_counts = scraping_code(skip_tenders_counts)

        pos = sqlite_code(main_list, pos)

        main_list = []

        pa = WebDriverWait(driver, 200).until(EC.presence_of_all_elements_located((By.XPATH, "/html/body/div[1]/div[5]/div[1]/div/div[1]/a")))
        size = len(pa)
        pqr = WebDriverWait(driver, 200).until(EC.presence_of_all_elements_located((By.XPATH, "/html/body/div[1]/div[5]/div[1]/div/div[1]/a")))[size - 2: size - 1]
        pqr[0].click()

        # last = WebDriverWait(driver, 200).until(EC.presence_of_all_elements_located((By.XPATH, "/html/body/div[1]/div[5]/div[1]/div/div[1]/a")))


    except Exception as e:
        print(f'Page Loop {e}')
        logging.error(f'{str(e)}')
        driver.close()
        driver.quit()
        break

