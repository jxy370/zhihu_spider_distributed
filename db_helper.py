import MySQLdb


class DB_Helper:

    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 33061
        self.user = "root"
        self.passwd = "root"
        self.db = "zhihu_spider"

    def getConnection(self):
        return MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port, charset="utf8")

    def find_user_to_crawl(self):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "lock tables crawl_status write"
        cursor.execute(sql)
        conn.commit()
        sql = "select uid, url_token, is_crawled, insert_time from crawl_status where is_crawled = 0 order by insert_time asc limit 1"
        #sql = 'select uid, url_token,is_crawled, insert_time from zh_user where is_crawled = 0 and insert_time =(select MIN(insert_time) from zh_user  where is_crawled = 0) limit 1'
        cursor.execute(sql)
        record = cursor.fetchone()
        if record != None:
            uid = record[0]
            url_token = record[1]
            insert_time = record[3]
            print("%s %s %d %s" % (record[0], record[1], record[2], record[3]))
        else:
            uid = None
            url_token = None
            insert_time = None
        if uid is not None:
            sql = 'update crawl_status set is_crawled = 2 where uid = %s'
            values = [uid]
            cursor.execute(sql, values)
            conn.commit()

        sql = "unlock tables"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        return [uid, url_token, insert_time]


    def save_following(self, follower, following):
        conn = self.getConnection()
        cursor = conn.cursor()
        is_followed = following["is_followed"]

        uid = following["id"]
        user_type = following["user_type"]
        url = following["url"]
        url_token = following["url_token"]
        answer_count = following["answer_count"]
        articles_count = following["articles_count"]
        name = following["name"]
        headline = following["headline"]
        is_advertiser = 0
        if following["is_advertiser"]:
            is_advertiser = 1
        avatar_url = following["avatar_url"]
        is_org = 0
        if following["is_org"]:
            is_org = 1
        gender = following["gender"]
        follower_count = following["follower_count"]
        following_count = 0
        topic_count = 0
        type = following["type"]

        sql = "insert ignore into zh_user values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, null)"
        values = [uid, user_type, url, url_token, answer_count, articles_count, name, headline, is_advertiser,
                  avatar_url, is_org, gender, follower_count, following_count, topic_count, type]
        print("inserting user:")
        print(values)
        cursor.execute(sql, values)
        conn.commit()

        sql = "insert ignore into following values(%s, %s)"
        values = [follower, uid]
        cursor.execute(sql, values)
        conn.commit()
        if is_followed:
            print("@@@@@@@is_followed")
            # values = [uid, follower]
            # cursor.execute(sql,values)
            # conn.commit()
        cursor.close()
        conn.close()

    def save_topic(self, follower, topic, contributions_count):
        conn = self.getConnection()
        cursor = conn.cursor()

        tid = topic["id"]
        url = topic["url"]
        avatar_url = topic["avatar_url"]
        name = topic["name"]
        introduction = topic["introduction"]
        type = topic["type"]
        excerpt = topic["excerpt"]

        sql = "insert ignore into zh_topic values(%s, %s, %s, %s, %s, %s, %s)"
        values = [tid, introduction, avatar_url, name, url, type, excerpt]
        cursor.execute(sql, values)
        conn.commit()
        print("inserting topic:")
        print(values)

        sql = "insert ignore into following_topic values (%s, %s, %s)"
        values = [follower, tid, contributions_count]
        cursor.execute(sql, values)
        conn.commit()

        cursor.close()
        conn.close()

    def crawl_end(self, follower, following_count, topic_count):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "update zh_user set following_count = %s, topic_count = %s, is_crawled = 1 where uid = %s"
        values = [following_count, topic_count, follower]
        cursor.execute(sql, values)

        sql = "update crawl_status set is_crawled = 1 where uid = %s"
        values = [follower]
        cursor.execute(sql, values)

        conn.commit()

        cursor.close()
        conn.close()

    def crawl_fail(self, follower):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "update crawl_status set is_crawled = 0 where uid = %s"
        values = [follower]
        cursor.execute(sql, values)
        conn.commit()

        cursor.close()
        conn.close()

    def crawl_error(self, follower):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "update crawl_status set is_crawled = 3 where uid = %s"
        values = [follower]
        cursor.execute(sql, values)
        conn.commit()

        cursor.close()
        conn.close()

    def save_activity(self, act):

        conn = self.getConnection()
        cursor = conn.cursor()

        if act["verb"] in ["QUESTION_FOLLOW", "QUESTION_CREATE", "ANSWER_CREATE", "ANSWER_VOTE_UP"]:
            sql = "insert ignore into zh_activity values(%s, %s, %s, %s, %s)"
            values = [act["id"], act["actor"]["id"], act["verb"], act["target"]["id"], act["created_time"]]
            print("saving activity: ", values)

            cursor.execute(sql, values)
            conn.commit()

            if act["verb"] in ["QUESTION_FOLLOW", "QUESTION_CREATE"] :
                self.save_question(act["target"], conn, cursor)
            elif act["verb"] in ["ANSWER_CREATE", "ANSWER_VOTE_UP"]:
                self.save_answer(act["target"], conn, cursor)
        cursor.close()
        conn.close()

    def save_question(self, question, conn, cursor):
        sql =  "insert ignore into zh_question values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = [question["id"], question["author"]["id"], question["created"], question["url"], question["title"],
                  question["excerpt"], question["answer_count"], question["comment_count"], question["follower_count"],
                  question["type"]]
        print("saving question: ", values)

        cursor.execute(sql, values)
        conn.commit()

        for tid in question["bound_topic_ids"]:
            sql = "insert ignore into question_topic values(%s, %s)"
            values = [question["id"], tid]
            cursor.execute(sql, values)
            conn.commit()

    def save_answer(self, answer, conn, cursor):
        sql = "insert ignore into zh_answer values(%s, %s, %s, %s, %s, %s, %s, %s)"
        values = [answer["id"], answer["author"]["id"], answer["question"]["id"], answer["created_time"],
                  answer["updated_time"], answer["comment_count"], answer["voteup_count"], answer["thanks_count"]]
        print("saving answer: ", values)

        cursor.execute(sql, values)
        conn.commit()

        self.save_question(answer["question"], conn, cursor)

    def find_user_to_crawl_activity(self):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "lock tables crawl_status_activity write"
        cursor.execute(sql)
        conn.commit()
        sql = "select uid, url_token, is_crawled, insert_time from crawl_status_activity where is_crawled = 0 order by insert_time asc limit 1"
        # sql = 'select uid, url_token,is_crawled, insert_time from zh_user where is_crawled = 0 and insert_time =(select MIN(insert_time) from zh_user  where is_crawled = 0) limit 1'
        cursor.execute(sql)
        record = cursor.fetchone()
        if record != None:
            uid = record[0]
            url_token = record[1]
            insert_time = record[3]
            print("%s %s %d %s" % (record[0], record[1], record[2], record[3]))
        else:
            uid = None
            url_token = None
            insert_time = None
        if uid is not None:
            sql = 'update crawl_status_activity set is_crawled = 2 where uid = %s'
            values = [uid]
            cursor.execute(sql, values)
            conn.commit()

        sql = "unlock tables"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        return [uid, url_token, insert_time]

    def crawl_end_activity(self, uid):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "update crawl_status_activity set is_crawled = 1 where uid = %s"
        values = [uid]
        cursor.execute(sql, values)

        conn.commit()

        cursor.close()
        conn.close()

    def crawl_fail_activity(self, uid):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "update crawl_status_activity set is_crawled = 0 where uid = %s"
        values = [uid]
        cursor.execute(sql, values)
        conn.commit()

        cursor.close()
        conn.close()

    def crawl_error_activity(self, uid):
        conn = self.getConnection()
        cursor = conn.cursor()

        sql = "update crawl_status_activity set is_crawled = 3 where uid = %s"
        values = [uid]
        cursor.execute(sql, values)
        conn.commit()

        cursor.close()
        conn.close()
