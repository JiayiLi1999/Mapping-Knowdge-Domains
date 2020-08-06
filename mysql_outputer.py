# -*- coding: utf-8 -*-

import time
import pymysql
import codecs
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

class Mysql_outputer(object):
    def __init__(self):
        self.count = 0
        self.logfile = codecs.open('mysql_log.txt', 'a')
        self.logfile.write('\n\n==============================================\n')
        self.logfile.write(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()) + '\n')
        try:
            self.conn = MySQLdb.connect(host="10.2.14.90", port=3306, user="admin", passwd="xgxy", db="baidu",
                                        charset="utf8")
            print("link to mysql successful")
        except Exception as e:
            print("link to mysql error")
            self.logfile.write('link to mysql error\n')
            self.logfile.flush()
            self.logfile.close()

            try:
                self.conn.close()
            except:
                pass
            exit(1)

    def output_mysql(self, data, urls):
        title = data['title'].encode('utf-8')
        by_title = data['by_title'].encode('utf-8')
        item_url = data['url'].encode('utf-8').replace("'",'')
        item_summary = data['summary'].encode('utf-8')
        item_tag = data['tag'].encode('utf-8')
        synonym = data['synonym'].encode('utf-8')
        related_Information = data['related_Information'].encode('utf-8').replace("'",'')
        shareCounter = data['shareCount'].encode('utf-8')
        likeCounter = data['likeCount'].encode('utf-8')
        all_data = data['all_data'].encode('utf-8').replace("'",'')

        try:
            self.cursor = self.conn.cursor()

            sql= " INSERT INTO items(title,by_title,url,summary,tag,synonym,related_Information,shareCounter,likeCounter,all_data,Content_urls) " \
                 "values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"\
                 %(title,by_title,item_url,item_summary,item_tag,synonym,related_Information,shareCounter,likeCounter,all_data, ','.join(urls).encode('utf-8'))
            self.cursor.execute(sql)
            self.conn.commit()
            self.count = 0
        except Exception as e:
            print(e)
            self.logfile.write(item_url + '\tinsert into mysql error\n')
            self.count += 1
            if self.count > 5:
                print("\n连续5次输入mysql失败")
                self.logfile.write('连续5次写入mysql失败\n')
                self.logfile.flush()
                self.logfile.close()
                return -1
        finally:
            try:
                self.cursor.close()
            except:
                pass

    def get_target_urls(self,new_urls_10000):
        target_urls = list()
        self.cursor = self.conn.cursor()
        mysql_urls = list()
        sql = 'select url from items'
        self.cursor.execute(sql)
        self.conn.commit()
        data = self.cursor.fetchall()
        if (data == None):
            self.cursor.close()
            return None
        else:
            for url in data:
                mysql_urls.append(url[0])
            self.cursor.close()
            for url in new_urls_10000:
                if (url not in mysql_urls):
                    target_urls.append(url)
            return target_urls


