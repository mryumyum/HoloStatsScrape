import os
import time
import re
import pickle
import pandas as pd
import numpy as np
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


url = 'https://holo.poi.cat/youtube-stream'
driver = webdriver.Chrome()
driver.get(url)
time.sleep(5)

year = input('Please enter the year you want to collect')
year = str(year)

sel_date_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", "//button[contains(@class, 'mr-2 !rounded-full ng-tns-c37-3 mdc-button mdc-button--outlined mat-mdc-outlined-button mat-unthemed mat-mdc-button-base')]")))
sel_date_button.click()
time.sleep(1)

sel_year_cont_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", "//button[contains(@class, 'mat-calendar-period-button mdc-button mat-mdc-button mat-unthemed mat-mdc-button-base')]")))
sel_year_cont_button.click()
time.sleep(1)


sel_year_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", f"//button[contains(@aria-label, '{year}')]")))
sel_year_button.click()
time.sleep(1)


first_month_year = 'January ' + f'{year}'
sel_month_button =  WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", f"//button[contains(@aria-label, '{first_month_year}')]")))
sel_month_button.click()
time.sleep(1)

first_of_month = 'January 1, ' + f'{year}'
sel_first_button =  WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", f"//button[contains(@aria-label, '{first_of_month}')]")))
sel_first_button.click()
time.sleep(1)

sel_year_cont_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", "//button[contains(@class, 'mat-calendar-period-button mdc-button mat-mdc-button mat-unthemed mat-mdc-button-base')]")))
sel_year_cont_button.click()
time.sleep(1)


sel_year_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", f"//button[contains(@aria-label, '{year}')]")))
sel_year_button.click()
time.sleep(1)

last_month_year = 'December ' + f'{year}'
sel_month_button =  WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", f"//button[contains(@aria-label, '{last_month_year}')]")))
sel_month_button.click()
time.sleep(1)

last_of_month = 'December 31, ' +  f'{year}'
sel_sec_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", f"//button[contains(@aria-label, '{last_of_month}')]")))
ActionChains(driver).key_down(Keys.SHIFT).click(sel_sec_button).key_up(Keys.SHIFT).perform()

time.sleep(5)


every_hundreth = 0
element = driver.find_element('xpath', '//body')

while True:
    
    # Element.send_keys(Keys.PAGE_DOWN)
    driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
    time.sleep(1)
    # Primative optimization, loading every tick would drastically increase collection time
    every_hundreth += 1
    # try:
    #     WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", "//img[contains(@loading, 'lazy')]")))
    # except:
    #     break  # not more posts were loaded - exit the loop
    
    
    if every_hundreth%100 == 0:
        stop_element = driver.find_element("xpath", "//hls-stream-list[contains(@class, 'ng-star-inserted')]").text

        if f'{year}/01/01' in stop_element:
            elems = driver.find_elements("xpath", "//a[@href]")
            filtered_urls = [elem.get_attribute("href") for elem in elems if '/stream' in elem.get_attribute("href")]
            
            #print(filtered_urls)
            print(len(filtered_urls))
            pickle.dump(filtered_urls, open(f'holostats_URL_{year}.pkl','wb'))
            year_col = True
            print(f'YEAR {year} COLLECTED')
            
            # Remove Duplicates
            holo_raw_data = pickle.load(open(f'holostats_URL_{year}.pkl', 'rb'))
            print(len(holo_raw_data))
            
            # Removes duplicates, but does not preserve the order
            # res = [*set(holo_raw_data)]
            # print(len(res))
            
            # Removes duplicates while preserving order of original list
            seen = set()
            seen_add = seen.add
            holo_ref_data = [x for x in holo_raw_data if not (x in seen or seen_add(x))]
            
            print(len(holo_ref_data))
            pickle.dump(holo_ref_data, open(f'holostats_URL_{year}_REF.pkl','wb'))

            driver.quit()
            




driver = webdriver.Chrome()
df = pd.DataFrame(columns=['URL', 'Video_ID', 'Video_Title', 'Creator', 'Avg_viewers', 'Max_viewers', 'SuperChat_total', 'Stream_Start_Time_UST', 'duration'])

# DURING THIS TIME YOU NEED TO CHANGE THE CURRENCY FROM YEN TO DOLLARS AND TIME TO UST MANUALLY IN THE WEBSITE SETTINGS
driver.get('https://holo.poi.cat/settings')
time.sleep(30)

#year = input('Please enter the year you want to collect')

#holostats = pickle.load(open(f'holostats_URL_2022_REF.pkl','rb'))
#holostats = pickle.load(open(f'\\Pickled data\\holostats_URL_{year}_REF.pkl','rb'))
holostats = pickle.load(open('holostats_URL_{year}_REF.pkl','rb'))

for x in range(len(holostats)):

    url = holostats[x]
    driver.get(url)

    temp = []
    temp.append(url)

    # try:
    #     WebDriverWait(driver, 10).until(EC.visibility_of_element_located(("xpath", "//div[contains(@class, 'mat-h3 m-0')]")))
    # except:
    #     print("Element not found")
    #     driver.quit()

    time.sleep(5)
    page_sour = driver.page_source
    doc = BeautifulSoup(page_sour, "lxml")

    vid_id = str(url).split('/')
    vid_id = vid_id[-1]
    temp.append(vid_id)

    try:
        #title = doc.find('title')
        title = driver.title
        title = str(title)
        title = title[:-12]
        temp.append(title)
    except:
        temp.append(" ")
        continue

    try:
        creator = doc('span', {'class': 'ml-2 align-middle'})[0].text
        temp.append(creator)
    except:
        temp.append(" ")
        continue

    try:
        avg_max_view = doc('div', {'class': 'mat-headline-5 m-0'})[0].text
        avg_max_view = avg_max_view.split('/')

        avg_view = avg_max_view[0]
        avg_view = avg_view[1:-1]
        temp.append(avg_view)

        max_view = avg_max_view[1]
        max_view = max_view[1:-1]
        temp.append(max_view)
    except:
        temp.append(" ")
        temp.append(" ")
        continue

    try:   
        #sup_chat_tot = doc.find_all(text=re.compile("\¥.*"))
        sup_chat_tot = doc.find_all(text=re.compile("\$.*"))
        temp.append(sup_chat_tot[0])
    except:
        temp.append(" ")
        continue
        
    try:
        streamt = doc('span', {'class': 'mat-body-1'})[0].text
        temp.append(streamt)
    except:
        temp.append(" ")
        continue
        
    try:
        duration = doc('span', {'class': 'mat-body-1'})[1].text
        temp.append(duration)
    except:
        temp.append(" ")
        continue
        
    df.loc[len(df)] = temp


df.to_excel('output_2022_DURATION_NULL.xlsx')
driver.quit()
#print(doc.prettify())
#title = doc.find_all('span')
#print(title)

































