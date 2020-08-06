# -*- coding: utf-8 -*-

import threading
import scrapy.spider
import time
import urllib
import codecs

f_log = codecs.open("seed_log.txt", "w", encoding="utf-8")
f_seed = codecs.open("seed/dict1.txt", "r", encoding="utf-8")
obj_spider = spider.SpiderMain()

seeds = list()
lines = f_seed.readlines()
for line in lines:
    line = line.strip()
    seeds.append(line)
f_seed.close()

#  开启生产者消费者模式
con = threading.Condition()
new_urls_10000 = set()

def produce():
    global new_urls_10000

    if con.acquire():
        print('produce...')
        while True:
            if (len(seeds) == 0):
                con.notify()
            else:
                seed_name = seeds.pop(0)
                seed_url = 'https://baike.baidu.com/item/' + seed_name
                new_urls_single = obj_spider.craw_urls(seed_url)
                new_urls_10000.add(seed_url)
                if(new_urls_single == None):
                    continue
                for new_url in new_urls_single:
                    new_urls_10000.add(new_url)
                f_log.write(seed_name + "\n")
                print(len(new_urls_10000))
            if(len(new_urls_10000) > 20):
                con.notify()
                # 等待通知
                con.wait()
                time.sleep(5)

def consume():
    global new_urls_10000

    if con.acquire():
        print('Consume...')
        while True:
            target_urls = obj_spider.remDup(new_urls_10000)
            if(target_urls == None):
                new_urls_10000 = set()
                con.notify()
                # 等待通知
                con.wait()
                time.sleep(5)
            else:
                for target_url in target_urls:
                    obj_spider.craw(target_url)
                new_urls_10000 = set()

                con.notify()
                # 等待通知
                con.wait()
                time.sleep(5)

if __name__ == '__main__':

    t1 = threading.Thread(target=produce)
    t2 = threading.Thread(target=consume)
    t1.start()
    t2.start()
    # t1.join()
    # t2.join()
    #
    # not_over = True
    # while not_over:
    #     if(t1.is_alive() or t2.is_alive()):
    #         time.sleep(300)
    #     else:
    #         obj_spider.logfile.flush()
    #         obj_spider.logfile.close()
    #
    #         f_log.flush()
    #         f_log.close()
    #         not_over = False