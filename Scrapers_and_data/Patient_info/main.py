import json
from multiprocessing import Pool
from web_iterator import web_iterator
import time
import pandas as pd

# define your list of urls as a list of dictionaries
with open("Scrapers_and_data/Patient_info/forum_links.json", "r") as links:
    forum_links = json.load(links)

forum_iterator = [{"forum_name": d['forum_name'], "forum_url":d["forum_url"]} for d in forum_links]

print(len(forum_iterator))

already_crawled = set()

def data_dump():
    counter = 0
    with open ("Scrapers_and_data/Patient_info/forum_posts_links.json", "w") as f:
        for forum_iter in forum_iterator[695: ]:
            if forum_iter['forum_url'] in already_crawled:
                continue
            already_crawled.add(forum_iter['forum_url'])
            counter+=1
            print(counter)
            time.sleep(5)
            forum_posts = web_iterator(forum_iter)
            json.dump(forum_posts,f)
            f.write('\n')
    

data_dump()

# def data_dump(forum_iter):
#     # 


