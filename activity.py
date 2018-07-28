
import requests
from db_helper import DB_Helper
from requests.adapters import HTTPAdapter
import time
import json
import random
import os
import traceback

#cookie_num = 0
#cookies = ['capsion_ticket="2|1:0|10:1525765486|14:capsion_ticket|44:ZWFjOGU4OWNhNTE4NDc4NGE3NDhiMTBiNDJhMmUyOTg=|0e0b3369cb14d31b5aa52948cdd72248477bca8ea898d92dc0a1bc8580f32201"; _zap=e51a481e-9b76-4e76-bddd-c74f6df46bd5; z_c0="2|1:0|10:1525765517|4:z_c0|80:MS4xTUpBYUNRQUFBQUFtQUFBQVlBSlZUWTJqM2x2VHlGWGxXNlM5eXJJc2ZTcTFvSHIzTWJ4Z1pnPT0=|e80520ce7faa19e8331425b6ffc258be8c26306a4e8f7dfa5d0ec88aac8b3555"; q_c1=1d82a7222b1b47c880ddac1ebcd4c3d3|1525765517000|1525765517000; __utma=155987696.1423985527.1526267108.1526267108.1526267108.1; __utmz=155987696.1526267108.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); tgw_l7_route=b3dca7eade474617fe4df56e6c4934a3; _xsrf=595322ec-3ee6-4be3-ae52-52eebfd595a0; d_c0="ACCku-ogog2PTgCYXVzu6KMZXo-lNKSDS7s=|1526998564"',
           # 'q_c1=6e4ceeb9be3b4cf384def508951d0aa1|1525403911000|1503236164000; q_c1=6e4ceeb9be3b4cf384def508951d0aa1|1508566141000|1503236164000; _zap=c4f9d423-c555-4b23-8eaf-7eacb89b0c05; __utma=155987696.515366891.1523496951.1523496951.1523496951.1; __utmz=155987696.1523496951.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); d_c0="AGACgkxUZwyPTikz8V9VikzOgvdKHFci_fg=|1505872741"; z_c0="2|1:0|10:1515409450|4:z_c0|92:Mi4xcjZzS0FBQUFBQUFBWUFLQ1RGUm5EQ1lBQUFCZ0FsVk5LcDVBV3dDS3NGeWdRLWd4S2plQ0o1aFRDSm9Sb0NNallB|75d61bf7b626f0084d12292877dbe648b2b58ae55658289d5819da7295e83307"; __DAYU_PP=YNQnyuM7ejrAqI6a3JFJffffffff82c9629f2ffd; aliyungf_tc=AQAAAOicMheV1wQAjmswtpvI8y6btuCp; _xsrf=548ab7c8-3fc0-4dc5-b151-a5c42a76d90a',
           # '_zap=57d5c3c4-09d9-4411-bad0-811a1dbbcf5c; __DAYU_PP=miVIyzAZmnzmZn63Bjm2ffffffff8776c6470921; q_c1=c6e0496a7b684e30a98ffe935390e795|1525075199000|1522201685000; l_cap_id="ZjlmMzYxODIyNjFiNGExOWIyYzBiNmM4Njg1ZWZlNDY=|1525244123|847da238a5a3486b74b9ac402609cae747f86217"; r_cap_id="ODc2ZWQ3MzIzNTRjNGYxMWIxMjY2YjEzYmUxOWJmYTQ=|1525244123|df6b6caf68ad73af0837a59a8acc3cf7948eba78"; cap_id="ZWU5Njk3MGRlNjg1NDEyNGE5YjYyZDFmYTI0ZWE5NTU=|1525244123|9a5068f26ba05dc3b493b6bf126de1eff2acc724"; d_c0="ACDgXER5iA2PTrhC_Mg5Ky4jkNM216KOrzc=|1525276894"; __utmz=155987696.1525340003.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=155987696.547684456.1525340003.1525345235.1525415003.3; _xsrf=d0b5c226-7cb1-40cd-b23e-bf492f023715; capsion_ticket="2|1:0|10:1525576325|14:capsion_ticket|44:NGUxNDhkNjNhNjI4NDk1YTkzNzk4MmJmZjI2MzU5MmI=|ee98dcfa0ff4ebb9acdea0592e7a42927c0a4abc4b5d9547da95cc9e3a609502"; z_c0="2|1:0|10:1525576329|4:z_c0|92:Mi4xS0V0RUF3QUFBQUFBSU9CY1JIbUlEU1lBQUFCZ0FsVk5pY0RiV3dESnc1M0NBbWxudjNUM3N1Q1U2N2hEZVFJellB|5bb7fc478f1cd0e269f7dd157c20315970cf1066f8c931be860d55e69230c5f2"']

cookies = [
    #13161299088  0
    ['_zap=f4e08d39-06a7-465d-bce4-495f7ba03f88; __DAYU_PP=ibfIUYAFiVean2qqzEvNffffffff82c962835bf3; d_c0="AHCuVUhbig2PTt74U2viiiLN-gaoPyl4HDI=|1525403251"; capsion_ticket="2|1:0|10:1528371146|14:capsion_ticket|44:MjdjNzRkZjQxZmUwNGU2NTlkN2Y4ZDljMGRkODg3ZmY=|54dec2a33167f724b51f3287ac39eddb41c679f67c936018f4933cdd82d267f0"; z_c0="2|1:0|10:1528371151|4:z_c0|92:Mi4xS0V0RUF3QUFBQUFBY0s1VlNGdUtEU1lBQUFCZ0FsVk56MlVHWEFDeTkyMHJlbUloM2FwSXBkXzhma0wtOGxFSWpB|f340b6e50f12f244f5aa1ef5d0f795fdb6db1f1f7db9ca83605c6be9198f3b14"; __utmv=51854390.100-1|2=registration_date=20160720=1^3=entry_date=20160720=1; q_c1=f2c727d7220d4ab3bd73abd657f1f610|1530278730000|1522241157000; __utma=51854390.1463751258.1530152596.1530190465.1530278773.3; __utmz=51854390.1530278773.3.3.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; _xsrf=746c2340-f130-4c7d-b58b-9fb3faa70e8a; tgw_l7_route=61066e97b5b7b3b0daad1bff47134a22',
     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'],
    #18763807271  1
    ['q_c1=b494e72505eb47fc89cd6b26c38165ff|1530536858000|1527837397000; d_c0=AMAkyc-grg2PTkCaoXfuTDmlMOodaQQFraA=|1527837397; _zap=cc5ea540-1b4b-40cf-aafd-92a42964cfc9; _xsrf=Sr5dfO6unOyckfqTgAv8PWG5rugUa2Us; capsion_ticket=2|1:0|10:1530536876|14:capsion_ticket|44:MzJhNTBhZWEyZWZjNGY2N2IyNjQ0NzljZmM2YjU1Zjk=|71a0bf857730af0ab7a11c319a285a2e525795674d69328d841d0ab3f2ee528e; z_c0=2|1:0|10:1530536898|4:z_c0|92:Mi4xTUpBYUNRQUFBQUFBd0NUSno2Q3VEU1lBQUFCZ0FsVk53bkVuWEFEdGxsaWhuSXVtWGd4dm5fVjNLVkdnQmFYeVl3|00c8cf616fcfdf39312cd4df07cacb4fb5729aa5b4460093a4ee989f4d549a9c; tgw_l7_route=931b604f0432b1e60014973b6cd4c7bc',
     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'],
    #18765712409  2
    ['d_c0="APAkAoXarg2PTg-cBdoAkNQCkYK7A61BqQ4=|1527852525"; q_c1=12e6531dc2564e1886c204932ba26449|1530537215000|1527852525000; _zap=87f2bead-161c-4ffb-9c43-466cf35bf16a; z_c0="2|1:0|10:1527852539|4:z_c0|92:Mi4xYzdDbENRQUFBQUFBOENRQ2hkcXVEU1lBQUFCZ0FsVk4tM3YtV3dCZEhiWlhGaS1yZll1cTBzV3dvQ2RITFFfQ0FR|a6dc2e5a2e096f11382399e6eb4d3c169e373d275806c576cd2e7be891c3bffe"; tgw_l7_route=53d8274aa4a304c1aeff9b999b2aaa0a; _xsrf=cBhE5ikJs0obYDee5AenL5a3TXkhwOTa',
     'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'],
    #songjingjing  3
    ['tgw_l7_route=7139e401481ef2f46ce98b22af4f4bed; _xsrf=6c0ca171-b83c-4506-8669-3f558c5fa498; d_c0="ADDmlV7c1g2PTrks1LedDW1ltCZusLF-gyw=|1530537364"; q_c1=aca2fa00e33340a08c91830f562bece9|1530537364000|1530537364000; _zap=a5db9bbc-a77d-46a6-a14e-67165cc26795; capsion_ticket="2|1:0|10:1530537383|14:capsion_ticket|44:ZmE5MWIyYTYwMjc0NDI0YzlkZGJjNDU0ZGQ4NTA3YTk=|bdb8c08a4eddbe3d8b41de27b4a5aa7cc68ec06d1a4f0b7d47fcd24ce2b03e61"; z_c0="2|1:0|10:1530537433|4:z_c0|92:Mi4xRTdEdkJRQUFBQUFBTU9hVlh0eldEU1lBQUFCZ0FsVk4yWE1uWEFDZmxQcFNuSWdKMXY2dVc2SktncHBGRUpLUWp3|336eb0a7c1167dd2f228a39838b8866d55c605f52d5fda205ce280f3d406e4d2"; unlock_ticket="AABChC3vXgwmAAAAYAJVTeEsOltMT_Clspou3MTWZoUhPB9m17xORQ=="',
     'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36'],
    #zhangli  4
    ['_zap=0e3faaca-32a3-4feb-8daa-3d2e974a8968; d_c0="AKAgTT6sig2PTn6JlYRsPrk1Z0omON89qcA=|1525424475"; __utma=51854390.1822248333.1526893731.1526893731.1526893731.1; __utmz=51854390.1526893731.1.1.utmcsr=zhuanlan.zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/p/37086804; __utmv=51854390.000--|3=entry_date=20180521=1; z_c0="2|1:0|10:1526893802|4:z_c0|92:Mi4xZGhXVENRQUFBQUFBb0NCTlBxeUtEU1lBQUFCZ0FsVk42dHJ2V3dBNDV6djBuUW9SaHFoc01SbmVlUHkxMzZSd3l3|a7ec848a9aa303fe7a158c39719b7b72946cd1b0d071cad08820b551cc3b9d8a"; q_c1=e72731073caf4a1f868dfa5fb989dd4b|1529583477000|1524307306000; tgw_l7_route=b3dca7eade474617fe4df56e6c4934a3; _xsrf=02e9aa74-530f-4606-8dae-2ace8dc87efa',
     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'],
    #weixiangyu 5
    ['q_c1=6e4ceeb9be3b4cf384def508951d0aa1|1531099965000|1503236164000; q_c1=6e4ceeb9be3b4cf384def508951d0aa1|1508566141000|1503236164000; _zap=c4f9d423-c555-4b23-8eaf-7eacb89b0c05; __utma=155987696.1032503178.1526284337.1526284337.1526388155.2; __utmz=155987696.1526284337.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); d_c0="AGACgkxUZwyPTikz8V9VikzOgvdKHFci_fg=|1505872741"; __DAYU_PP=YNQnyuM7ejrAqI6a3JFJffffffff82c9629f2ffd; _xsrf=1c354310-e63b-4afa-9baa-b56e23b0188c; anc_cap_id=9713c77efc3b4e39b3ed0cdfd4291a43; tgw_l7_route=7139e401481ef2f46ce98b22af4f4bed; capsion_ticket="2|1:0|10:1531099912|14:capsion_ticket|44:ZDk3MmZmNTNmMzBiNDU0M2E3OWRmMTU2YTM0ZGY1YjM=|3221c088d6b3edeb7ea154fcea8341ad192732dc07d5548fb98f64bf3413c699"; z_c0="2|1:0|10:1531099916|4:z_c0|92:Mi4xcjZzS0FBQUFBQUFBWUFLQ1RGUm5EQ1lBQUFCZ0FsVk5EQWt3WEFEaDR6bkc4SGNVZDN5S2F1YUxTYjVqUk40WXl3|3d3a7e95d13e9ff1e14f9cffacb2d7a9369a341932818fa5442d857fa995285a"',
     'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'],
    #songjingjing2 6
    ['__guid=74140564.2189840363197841400.1508664972148.5166; d_c0="AJBCmdbvkAyPTgYHDMA8pX47_vHW1lBWwOw=|1508664978"; _zap=5990b41d-1c2c-4945-92cc-eb87d0cf491b; __utma=51854390.1343251981.1508664973.1508664973.1514036469.2; __utmv=51854390.000--|2=registration_date=20170913=1^3=entry_date=20171223=1; tgw_l7_route=860ecf76daf7b83f5a2f2dc22dccf049; _xsrf=48yyi1fJdjCAHeHfM6tpNc0IrNI4A97W; q_c1=103ca46d84df48219c8856ff33478028|1530791256000|1508664978000; l_n_c=1; l_cap_id="YzQ3OGVkNGE0NDcyNDcxNDhkODc3NWFlYWIxZTg5ODE=|1530791298|d11eb566136d1dbd1b01af8b6bd1bf4649109613"; r_cap_id="OWM3NWFlNTc1YzI4NDMxYjg4MzBlYmJhNmEyNzk0N2I=|1530791298|b5e1aa9cd44b6533f933c675c3ae88d498d0ed24"; cap_id="MDFjNjlkOTRmZGQ0NDFjYWFjODAxZmFmMGVlYzE5ODM=|1530791298|d0b4ef94db59625533cdffef8320061750adb56d"; n_c=1; capsion_ticket="2|1:0|10:1530791307|14:capsion_ticket|44:NjA5YmZkNjMzNjdiNDI2NGFhYTY2YzkzMWVhNzFlZmE=|a6d8c883ff9c353dfa3130184b3f8f83870d5e41a30ad23f8d849b2f4fe08b05"; z_c0="2|1:0|10:1530791365|4:z_c0|92:Mi4xWV9yTENnQUFBQUFBa0VLWjF1LVFEQ1lBQUFCZ0FsVk54Vk1yWEFEbEpsVk4wNi1OVVVLOWIyVHhiWHFiRnVqS2hB|ed50979a519bc6aef80b7d04d5f09066778abc1cb05e775585c0bee8d93e74ea"',
     'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'],
    #songjingjing3 7
    ['_xsrf=mCB01BavZaE0qs4o36mcuS9moYiyEqKI; z_c0=2|1:0|10:1530792009|4:z_c0|92:Mi4xNEpmZkJnQUFBQUFBc0NVZGxLZmFEU1lBQUFCZ0FsVk5TVllyWEFBRlRkZHVUdmo1YUswUUFTa2pyM2RhcVVORHd3|4a316b0e6459bebb62e10f8039886d93d57c7d51b647f8bf04b56215827148d7; unlock_ticket=AGBCGf0G1AwmAAAAYAJVTVEPPlvPZbAKsTim8UBIvo9RR_tK2d9Y-w==; q_c1=853be2f637ee420c9597b61e9a893916|1530791961000|1530791961000; d_c0=ALAlHZSn2g2PTiQjUyXVaMpVmySMQmdAALE=|1530791961; capsion_ticket=2|1:0|10:1530791984|14:capsion_ticket|44:ZmRlMWJjOGI5MDJiNGQ1M2I0NzEyZjUyODk0YzliYzk=|24dba4f3c68f41c8751f84db3a0608519680d0d3605d8c5e452f3f84f32933f3; _zap=1c93d86f-443a-420f-a672-5d43b629aaed; tgw_l7_route=4902c7c12bebebe28366186aba4ffcde',
     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'],
    #songjingjing4 8
    ['_xsrf=WnC68mHr3l7AthDDmtyAeC37xf1DbKzb; z_c0=2|1:0|10:1530793153|4:z_c0|92:Mi4xdjJSMEF3QUFBQUFBME9aV2hhdmFEU1lBQUFCZ0FsVk53Vm9yWEFDMHkwT010aEZLVlptRkJDci1BSXdIVTdOQWFB|975f8fda97a1835264d81c95bec43460e5fd4d3cdda21a464cbded9b9d2328c0; q_c1=a57009c792dd400fbfb8c65ef4c08121|1530792994000|1530792994000; d_c0=ANDmVoWr2g2PTndYtP-Snlg859JMi7EVFeE=|1530792994; capsion_ticket=2|1:0|10:1530793132|14:capsion_ticket|44:OWVlMDVkYTAzNWFhNDExN2IzNGJmNjQwYzM3NGJkNWE=|27fd5e3311daa5bf9a18af84056727069fc21f450d82e6e9d6d89b937db6daba; _zap=229443ff-fb5b-4b2c-94a2-88dd451899cd',
     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134']
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
