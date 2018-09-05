
import requests
from db_helper import DB_Helper
from requests.adapters import HTTPAdapter
import time
import json
import random
import os
import traceback

#cookie_num = 0

#set your cookies
cookies = [
    
]
i = 7

class GetActivity:
    #初始化函数
    def __init__(self):
        self.followees_url = "https://www.zhihu.com/api/v4/members/" + "%s" + \
                             "/followees?include=data%%5B*%%5D.answer_count%%2Carticles_count%%2Cgender%%2Cfollower_count%%2Cis_followed%%2Cis_following%%2Cbadge%%5B%%3F(type%%3Dbest_answerer)%%5D.topics&offset=" + "%d" + r"&limit=" + "%d"
        self.topic_url = "https://www.zhihu.com/api/v4/members/" + "%s" + \
                         "/following-topic-contributions?include=data%%5B*%%5D.topic.introduction&offset=" + "%d" + "&limit=" + "%d"
        self.activity_url = "https://www.zhihu.com/api/v4/members/" + "%s" + "/activities?limit=7"
        self.header = {
            "User-Agent": '',
            "Cookie": '',
            "Connection": "close"}

    def get_followees(self, url_token, offset=0, limit=20):
        url = self.followees_url % (url_token, offset, limit)
        print(url)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=100))
        s.mount('https://', HTTPAdapter(max_retries=100))
        res = s.get(url, headers=self.header, timeout=10)
        print(res)
        #print(res.json())


        print(res.json()["data"])
        res.close()
        s.close()
        return res.json()

    def get_topics(self, url_token, offset=0, limit=20):
        url = self.topic_url % (url_token, offset, limit)
        print(url)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=100))
        s.mount('https://', HTTPAdapter(max_retries=100))
        res = s.get(url, headers=self.header, timeout=10)
        print(res)
        #print(res.json())
        print(res.json()["data"])
        res.close()
        s.close()
        return res.json()

    def get_activity(self, url_token, i):
        url = self.activity_url % url_token
        print(url)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=100))
        s.mount('https://', HTTPAdapter(max_retries=100))

        item = cookies[i]

        self.header['Cookie'] = item[0]
        self.header['User-Agent'] = item[1]

        res = s.get(url, headers=self.header, timeout=20)
        print(res.status_code)
        if res.status_code == 410:
            return None
        # print(res.json())
        print(res.json()["data"])
        res.close()
        s.close()
        return res.json()

    def request(self, url, i):
        print(url)
        # s = requests.Session()
        # s.mount('http://', HTTPAdapter(max_retries=3))
        # s.mount('https://', HTTPAdapter(max_retries=3))
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=100))
        s.mount('https://', HTTPAdapter(max_retries=100))

        item = cookies[i]

        self.header['Cookie'] = item[0]
        self.header['User-Agent'] = item[1]

        res = s.get(url, headers=self.header, timeout=20)
        #res = requests.get(url, headers=self.header, timeout=10)
        print(res)
        res.close()
        s.close()
        #print(res.text)
        return res.json()

    def test_conn(self, url_token, i):
        item = cookies[i]

        self.header['Cookie'] = item[0]
        self.header['User-Agent'] = item[1]
        res = requests.get("https://www.zhihu.com/people/" + url_token, headers=self.header)
        return res.status_code

ga = GetActivity()
cnt = 0
db_helper = DB_Helper()
crawling_user = None

if __name__ == '__main__':

    while True:
        try:
            [uid, url_token, insert_time] = db_helper.find_user_to_crawl_activity()

            while uid is not None:
                with open("logs/log_%d.txt" % os.getpid(), "a+") as f:
                    f.write("crawling %s %s %s \n" % (uid, url_token, insert_time))
                crawling_user = [uid, url_token]
                print("crawling %s %s" % (uid, url_token))
                data = ga.get_activity(url_token, i)
                print(data is None)
                if data is None:
                    db_helper.crawl_error_activity(crawling_user[0])
                    [uid, url_token, insert_time] = db_helper.find_user_to_crawl_activity()
                    continue
                end = data["paging"]["is_end"]
                cnt = 0
                while not end and cnt < 500:
                    time.sleep(random.randint(10, 20))
                    for act in data["data"]:
                        if act["verb"] in  ["QUESTION_FOLLOW", "QUESTION_CREATE", "ANSWER_CREATE", "ANSWER_VOTE_UP"]:
                            cnt += 1
                        db_helper.save_activity(act)
                    end = data["paging"]["is_end"]
                    if not end:
                        url = data["paging"]["next"]
                        data = ga.request(url, i)

                db_helper.crawl_end_activity(uid=uid)
                time.sleep(random.randint(1, 3))
                [uid, url_token, insert_time] = db_helper.find_user_to_crawl_activity()
        except Exception as e:
            traceback.print_exc()
            # cookie_num = (cookie_num+1) % len(cookies)
            # print("switching to cookie %d" % cookie_num)
            if crawling_user is not None:

                if ga.test_conn(crawling_user[1], i) == 200:
                    db_helper.crawl_fail_activity(crawling_user[0])
                elif ga.test_conn(crawling_user[1], i) == 404:
                    db_helper.crawl_error_activity(crawling_user[0])
