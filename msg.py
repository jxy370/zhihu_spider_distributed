
import requests
from db_helper import DB_Helper
from requests.adapters import HTTPAdapter
import time
import os
import json
import random
import traceback
cookie_num = 0
#set your cookies
cookies = []
class GetMSG:
    def __init__(self):
        self.followees_url = "https://www.zhihu.com/api/v4/members/" + "%s" + \
                             "/followees?include=data%%5B*%%5D.answer_count%%2Carticles_count%%2Cgender%%2Cfollower_count%%2Cis_followed%%2Cis_following%%2Cbadge%%5B%%3F(type%%3Dbest_answerer)%%5D.topics&offset=" + "%d" + r"&limit=" + "%d"
        self.topic_url = "https://www.zhihu.com/api/v4/members/" + "%s" + \
                         "/following-topic-contributions?include=data%%5B*%%5D.topic.introduction&offset=" + "%d" + "&limit=" + "%d"
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
            "Cookie": '_zap=57d5c3c4-09d9-4411-bad0-811a1dbbcf5c; __DAYU_PP=miVIyzAZmnzmZn63Bjm2ffffffff8776c6470921; q_c1=c6e0496a7b684e30a98ffe935390e795|1525075199000|1522201685000; l_cap_id="ZjlmMzYxODIyNjFiNGExOWIyYzBiNmM4Njg1ZWZlNDY=|1525244123|847da238a5a3486b74b9ac402609cae747f86217"; r_cap_id="ODc2ZWQ3MzIzNTRjNGYxMWIxMjY2YjEzYmUxOWJmYTQ=|1525244123|df6b6caf68ad73af0837a59a8acc3cf7948eba78"; cap_id="ZWU5Njk3MGRlNjg1NDEyNGE5YjYyZDFmYTI0ZWE5NTU=|1525244123|9a5068f26ba05dc3b493b6bf126de1eff2acc724"; d_c0="ACDgXER5iA2PTrhC_Mg5Ky4jkNM216KOrzc=|1525276894"; __utma=155987696.1963552288.1525278622.1525278622.1525278622.1; __utmz=155987696.1525278622.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); aliyungf_tc=AQAAAICIHn1FuwoAsWswtuWdTxe20He/; _xsrf=b9af3bb0-8526-4ce3-932a-b552388d1f78; capsion_ticket="2|1:0|10:1525316844|14:capsion_ticket|44:NmRkM2VhMzRhZTk2NDJhOWFlMjg0NmUzMmZlN2ZkMGQ=|33736b998606ee6f29752c43ca49d7c155b46f92e02a9ba203b014c72403c548"; z_c0="2|1:0|10:1525316847|4:z_c0|92:Mi4xTUpBYUNRQUFBQUFBSU9CY1JIbUlEU1lBQUFCZ0FsVk43OHJYV3dBVTl4TUZyUkJGR3prRl9xZGZ6ZDhQSTRmVm93|4f34d684505feeb505db18ddd0f54b5983e6662e35d7faee0963ffaa56577d68"',
            "Connection": "close"}

    def get_followees(self, url_token, offset=0, limit=20):
        self.header["Cookie"] = cookies[cookie_num]
        url = self.followees_url % (url_token, offset, limit)
        print(url)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=10000))
        s.mount('https://', HTTPAdapter(max_retries=10000))
        res = s.get(url, headers=self.header, timeout=50)
        #print(res)
        #print(res.json())


        #print(res.json()["data"])
        res.close()
        s.close()
        return res.json()

    def get_topics(self, url_token, offset=0, limit=20):
        self.header["Cookie"] = cookies[cookie_num]
        url = self.topic_url % (url_token, offset, limit)
        print(url)
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=10000))
        s.mount('https://', HTTPAdapter(max_retries=10000))
        res = s.get(url, headers=self.header, timeout=50)
        #print(res)
        #print(res.json())
        #print(res.json()["data"])
        res.close()
        s.close()
        return res.json()

    def test_conn(self, url_token):
        res = requests.get("https://www.zhihu.com/people/" + url_token, headers=self.header)
        return res.status_code

getmsg = GetMSG()
cnt = 0
db_helper = DB_Helper()
crawling_user = None


if __name__ == '__main__':
    while True:
        try:
            [uid, url_token, insert_time] = db_helper.find_user_to_crawl()

            while uid is not None:
                with open("log_%d.txt" % os.getpid(), "a+") as f:
                    f.write("crawling %s %s %s \n" % (uid, url_token, insert_time))
                crawling_user = [uid, url_token]
                print("crawling %s %s" % (uid, url_token))
                end = False
                data = None
                offset = 0
                while not end:
                    #time.sleep(random.randint(1, 3))
                    data = getmsg.get_followees(url_token, offset=offset)
                    offset += 20
                    end = data["paging"]["is_end"]
                    for user in data["data"]:
                        db_helper.save_following(uid, user)
                following_count = data["paging"]["totals"]

                end = False
                offset = 0
                while not end:
                    #time.sleep(random.randint(1, 3))
                    data = getmsg.get_topics(url_token, offset)
                    offset += 20
                    end = data["paging"]["is_end"]
                    for item in data["data"]:
                        topic = item["topic"]
                        contributions_count = item["contributions_count"]
                        db_helper.save_topic(uid, topic, contributions_count)
                topic_count = data["paging"]["totals"]

                db_helper.crawl_end(uid, following_count, topic_count)
                time.sleep(random.randint(1, 3))
                [uid, url_token, insert_time] = db_helper.find_user_to_crawl()
        except Exception as e:
            traceback.print_exc()
            cookie_num = (cookie_num+1) % len(cookies)
            print("switching to cookie %d" % cookie_num)
            if crawling_user is not None:
                if getmsg.test_conn(crawling_user[1]) == 200:
                    db_helper.crawl_fail(crawling_user[0])
                elif getmsg.test_conn(crawling_user[1]) == 404:
                    db_helper.crawl_error(crawling_user[0])


