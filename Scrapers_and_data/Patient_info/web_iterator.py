import time
from bs4 import BeautifulSoup
import urllib
import re
import time
import requests
import pandas as pd
import os
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import multiprocessing


def web_iterator(forum_iter):
    forum_posts = []
    driver = webdriver.Chrome()
    
    driver.get(forum_iter["forum_url"])
    
    wait = WebDriverWait(driver, 20)
    
    next_count = 0


    while next_count <=50:
        
        time.sleep(5)
    
        try: 
            wait.until(EC.presence_of_all_elements_located((By.XPATH, '//h3[@class="post__title"]')))
            h3_elements = driver.find_elements(By.XPATH, '//h3[@class="post__title"]')
            for h3_element in h3_elements:
                post_data = {}
                text = h3_element.text
                href = h3_element.find_element(By.XPATH, './a').get_attribute("href")
                post_data['group_name'] = forum_iter['forum_name']
                post_data['post_title'] = text
                post_data['post_url'] = href
                forum_posts.append(post_data)
                
            next_link = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@class='reply__control reply-ctrl-last link']")))

            print(next_count)
            next_count += 1
            next_link.click()


        except StaleElementReferenceException:

            time.sleep(10)
            continue

        except: 
            
            break     
    
#     with open('forum_posts_demo.json', 'w') as fp:
#         json.dump(forum_posts,fp)
    driver.quit()
    return forum_posts
