import psycopg2
import scrapy
import validators
from defaultlist import defaultlist
from pydispatch import dispatcher
from scrapy import signals
from pprint import pprint
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
from urllib.parse import urlparse
from psycopg2 import *
import psycopg2.extras

class PageSpider(scrapy.Spider):
    name = 'page_spider'

    start_urls = ['https://yandex.ru/']
    # start_urls = ['https://coinmarketcap.com/']

    DEPTH_OF_SCRAPING = 3
    dict_with_urls = {}
    matrix_main = defaultlist(lambda: defaultlist(lambda: 0))

    def pleasekillme(self, m1, number):
        for i in range(number):
            while (len(m1[i]) < number):
                m1[i].append(0.0)

    def create_pair_of_url_rank(self, vec, dict):
        pairs = {}
        i = 0
        for key, value in dict.items():
            pairs[key] = vec[value]
            i += 1
            if i == len(vec):
                break
        return pairs  # я тоже

    def damn_bar_graph(self, dict2):
        connection = self.create_connection(
            "postgres", "postgres", "qwerty016", "mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com", "5432"
        )
        rcParams.update({'figure.autolayout': True})
        plt.autoscale()
        dict2 = dict(sorted(dict2.items(), key=lambda item: item[1], reverse=True))
        listvalues = dict2.values()
        listkeys = dict2.keys()
        listvalues = list(listvalues)[0:min(100,len(listvalues))]
        listkeys = list(listkeys)[0:min(100,len(listkeys))]
        listFinalValuesToBd = [(listkeys[i],listvalues[i]) for i in range(0,min(100,len(listkeys)))]
        print("INSERTED FINAL VALUES TO BD LEN IS :")
        print(min(100,len(listkeys)))
        pprint(listFinalValuesToBd )
        final_records = ", ".join(["%s"] * len(listFinalValuesToBd))

        insert_query = (
            f"INSERT INTO pager_runk (urls,runks) VALUES {final_records}"
        )

        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, listFinalValuesToBd)

        fig, ax = plt.subplots()
        ax.bar(listkeys, listvalues)
        ax.set_facecolor('white')
        fig.set_facecolor('floralwhite')
        plt.xticks(rotation=90)
        o = urlparse(self.start_urls[0])


    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def add_probability(self):
        for i in range(len(self.matrix_main)):
            url_num = np.sum(self.matrix_main[i])
            for j in range(len(self.matrix_main[i])):
                if self.matrix_main[i][j] == 1:
                    self.matrix_main[i][j] = 1. / url_num

    def on_closed(self):
        self.create_matrix()
        self.pleasekillme(self.matrix_main, len(self.dict_with_urls))
        # pprint(self.matrix_main)
        self.matrix_main = np.array(self.matrix_main)
        print("BEFORE FILTRING")
        # pprint(self.matrix_main)

        self.filter_bad_links()
        print("AFTER FILTRING")
        # print(self.matrix_main)
        self.add_probability()

        print("AFTER PROBABILITY")
        # print(self.matrix_main)

        self.matrix_main = self.matrix_main.transpose()
        final_matrix = np.linalg.matrix_power(self.matrix_main, len(self.matrix_main))
        vector = [1 / len(final_matrix)] * len(final_matrix)
        final_matrix = np.array(final_matrix)

        vector = np.array(vector)
        vector = final_matrix.dot(vector)
        # print(self.dict_with_urls)
        pprint("THIS IF FINAL SUM OF (VECTOR)" + str(np.sum(vector)))
        pairs = self.create_pair_of_url_rank(vector, self.dict_with_urls)
        pprint("DICTIONARY")
        # pprint(pairs)
        self.damn_bar_graph(pairs)

    def spider_closed(self, spider):
        self.on_closed()

    def create_connection(self, db_name, db_user, db_password, db_host, db_port):
        connection = None
        try:
            connection = psycopg2.connect(
                database=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
            )
            # print("Connection to PostgreSQL DB successful")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        return connection

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'depth': 1, 'referer': None})

    def save_into_db(self, url_info, update=False):
        connection = self.create_connection(
            "postgres", "postgres", "qwerty016", "mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com", "5432"
        )
        # connection = self.create_connection(
        #     "postgres", "postgres", "postgres", "localhost", "5432"
        # )
        connection.autocommit = True
        cursor = connection.cursor()
        d = url_info  # {'k1': 'v1', 'k2': 'v2'}
        # if update:
        #     keys = d.keys()
        #     final_insert = "update url_info set is_good = %s, visited = %s where id = %s"
        #     cursor.execute(final_insert, (url_info["is_good"], url_info["visited"], url_info["id"]))
        # else:
        keys = d.keys()
        columns = ','.join(keys)
        values = ','.join(['%({})s'.format(k) for k in keys])
        insert = 'insert into url_info ({0}) values ({1})'.format(columns, values)
        temp = cursor.mogrify(insert, d)
        cursor.execute(temp)

    def get_from_db(self, query):
        connection = self.create_connection(
            "postgres", "postgres", "qwerty016", "mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com", "5432"
        )
        # connection = psycopg2.connect(dbname='postgres', user='postgres', password='postgres',
        #                               host='localhost')

        cur = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        connection.autocommit = True
        cur.execute(query)
        ans = cur.fetchall()
        ans1 = []
        for row in ans:
            ans1.append(dict(row))
        return ans1

    def create_matrix(self):
        rows = self.get_from_db("SELECT url, referer FROM url_info")
        self.dict_with_urls = {}
        for row in rows:
            if self.dict_with_urls.get(row["url"]) is None:
                self.dict_with_urls[row["url"]] = len(self.dict_with_urls)
            if str(row["referer"]) != "None":
                if self.dict_with_urls.get(row["referer"]) is None:
                    self.dict_with_urls[row["referer"]] = len(self.dict_with_urls)
                referer_index = self.dict_with_urls[row["referer"]]
                url_index = self.dict_with_urls[row["url"]]
                self.matrix_main[referer_index][url_index] = 1

    def recalc_dict(self, i):
        deleted = False
        for key in list(self.dict_with_urls):
            if self.dict_with_urls.get(key) == i:
                self.dict_with_urls.pop(key, None)
                deleted = True
            else:
                if deleted:
                    self.dict_with_urls[key] = self.dict_with_urls.get(key) - 1

    def filter_bad_links(self):
        empty_line = True
        while empty_line:
            empty_line = False
            i = 0
            while i < len(self.matrix_main):
                url_num = np.sum(self.matrix_main[i])
                if url_num == 0:
                    self.matrix_main = np.delete(self.matrix_main, (i,), axis=0)
                    self.matrix_main = np.delete(self.matrix_main, (i,), axis=1)
                    self.recalc_dict(i)
                    empty_line = True
                i += 1

    def parse(self, response):
        # print("NOW PARSING " + str(response.meta['referer']))
        cur_crawl_depth = response.meta['depth']
        next_crawl_depth = cur_crawl_depth + 1

        next_urls = response.xpath('*//a/@href').extract()

        if response.meta['referer'] is None and response.request.url == self.start_urls[0]:
            row = {}
            row["url"] = response.request.url
            row["referer"] = response.meta['referer']
            row["is_good"] = 0
            row["depth"] = cur_crawl_depth
            self.save_into_db(row)

        for url in set(next_urls):
            valid = validators.url(url)
            if not valid:
                continue

            o = urlparse(url)
            url_without_query_string = o.scheme + "://" + o.netloc + o.path

            if cur_crawl_depth <= self.DEPTH_OF_SCRAPING:
                row = {}
                row["url"] = url_without_query_string
                row["referer"] = response.request.url
                row["is_good"] = 0
                row["depth"] = cur_crawl_depth
                self.save_into_db(row)
            else:
                continue

            yield scrapy.Request(url=url_without_query_string,
                                 callback=self.parse,
                                 meta={'depth': next_crawl_depth, 'referer': response.request.url})