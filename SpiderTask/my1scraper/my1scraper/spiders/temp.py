from collections import defaultdict
from functools import reduce
from queue import Queue

import psycopg2
import scrapy
import validators
from defaultlist import defaultlist
from pydispatch import dispatcher
from scrapy import signals
import copy
from pprint import pprint
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
from urllib.parse import urlparse
from psycopg2 import OperationalError
from psycopg2 import *
import psycopg2.extras
# from pandas import *
# from my1scraper.my1scraper.spiders.non_petuh_spider import NonPetuhSpider
# from my1scraper.my1scraper.spiders.petuh_spider import PetuhSpider

# kk = PetuhSpider()
# kk.temp()

# kk = NonPetuhSpider()
# kk.on_closed()
