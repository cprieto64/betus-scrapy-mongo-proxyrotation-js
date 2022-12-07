from time import process_time_ns
import scrapy
from scrapy.item import Field
from scrapy.item import Item
from scrapy.spiders import Rule
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.trackref import print_live_refs
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.options import Options
import pymongo
from pymongo import MongoClient
import hashlib
import requests
import json
import datetime
from datetime import datetime
import pytz
import time

# pip install python-telegram-bot
# pip install Scrapy-UserAgents
# pip install scrapy-user-agents
# pip install pymongo
# pip install selenium
# pip install scraperapi-sdk
# pip install scrapyx-scraperapi-proxy
# pip install scrapyx-scraperapi-v2
# pip install webdriver-manager
# pip install pytz
# IMPORTANT: in order to work with MongoDB Atltas - python3 -m pip install "pymongo[srv]"

TELEGRAM_TOKEN = "xxxxxxxxxx"
TELEGRAM_CHAT_ID = "xxxxxxxxx"

def send_to_telegram(message):
    send_text = 'https://api.telegram.org/bot' + TELEGRAM_TOKEN + '/sendMessage?chat_id=' + \
        TELEGRAM_CHAT_ID + '&text=' + json.dumps(message)

    requests.get(send_text) 

try:
    conn = pymongo.MongoClient('mongodb+srv://user:pass@cluster0.mamup.mongodb.net')       
    print("Connected to MongoDB...")
    db = conn['your-collection']
    
except:
    print("Error: Connection refused")

    response = {
            "success": False,
            "response": {
                    "message": "Could not connect to MongoDB",
                    "timestamp": str(datetime.datetime.now())
            }
    }
    send_to_telegram(response)
    
#SCRAPERAPI_KEY = 'xxxxxxxxxx'


urls = []


while len(urls) < 1:    
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(
    options=options, executable_path=GeckoDriverManager().install())
    driver.get('https://www.betus.com.pa/racebook/')

    links = driver.find_elements_by_xpath('//div[@id="horseracingamerican"]//ul/li/a')

    for link in links:
        link = link.get_attribute('href')
        urls.append(link)

    print(len(urls), urls)

    driver.quit()

    if len(urls) < 1:
        print("Error: no urls returned")    
        send_to_telegram("Error: no urls returned")
        time.sleep(60)


class Race(Item):
    race_name = Field()
    race_number = Field()
    race_date = Field()
    horse_number = Field()
    runner_name = Field()
    jockey = Field()
    ml = Field()
    win = Field()
    place = Field()
    show = Field()
    wager_type = Field()
    horses_results = Field()


class BetUsCrawler(scrapy.Spider):
    name = 'betusraces'
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            #'scrapyx_scraperapi.ScraperApiProxyMiddleware': 610,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        },
        "USER_AGENTS": [
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.110 Safari/537.36',
            # chrome
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36',
            # chrome
            # firefox
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.91 Safari/537.36',
            # chrome
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36',
            # chrome
        ],
        # "SCRAPERAPI_ENABLED": True,
        # "SCRAPERAPI_KEY": SCRAPERAPI_KEY,
        # "SCRAPERAPI_RENDER": False,
        # "SCRAPERAPI_PREMIUM": False,
        # "SCRAPERAPI_COUNTRY_CODE": 'US',
        'CONCURRENT_REQUESTS': 1,
        'CLOSESPIDER_PAGECOUNT': 2000,
        'LOG_LEVEL': 'ERROR',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'DOWNLOAD_DELAY': 5.5,
        'AUTOTHROTTLE_DEBUG': True,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 3,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 2,
    }

    start_urls = urls

    rules = {
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//div[@id="horseracing"]//ul/li/a')),
             callback='parse', follow=False)
    }

    # with proxy
    # def parse(self, response):
    #     options = response.xpath(
    #         '//div[@id="race-times-tabs"]//select/option[last()]/@value').get()
    #     print(options)

    #     for i in range(1, int(options)+1):
    #         base_url = response.url
    #         url = f'{base_url[:-81]}/Race-{i}.aspx&api_key={SCRAPERAPI_KEY}&country_code=US&scraper_sdk=python'
    #         print("Going into: ", url)
    #         yield scrapy.Request(url=url, callback=self.parse_items)

    # without proxy
    def parse(self, response):
        options = response.xpath(
            '//div[@id="race-times-tabs"]//select/option[last()]/@value').get()
        if options is None:
            print("EMPTY: ", response.url)
        else:
            for i in range (1, int(options)+1):
                base_url = response.url
                url = f'{base_url[:-5]}/Race-{i}.aspx'
                #print(current_time, "- URL: ", url)
                yield scrapy.Request(url=url, callback=self.parse_items)

    def parse_items(self, response):
        
        sel = Selector(response)
        no_race_msg = sel.xpath('//div[@id="ctl00_ctl00_M_middle_Track_htmlNoRaces"]/text()').get()
                
        if no_race_msg is not None:
            print(response.url)
            print(no_race_msg)
        else:
            print("Going into: ", response.url)
            race_name = sel.xpath('.//h5[@id="race-name"]/text()').getall()
            if race_name is None or len(race_name) < 1:
                send_to_telegram(f"{response.url} no info recived even when front-end has information")
                print("=======================================================================================")
                print(f"{response.url} no info recived even when front-end has information")
                print("=======================================================================================")
            race_number = sel.xpath(
                './/h3[@id="htmlRaceNumber"]/span/text()').getall()
            race_date = sel.xpath(
                './/div[@id="race-header-details"]/span[1]/text()').getall()
            horse_number = sel.xpath(
                './/div[contains(@class, "line")]/div[contains(@class, "gate")]/span/text()').getall()
            if horse_number is None:
                pass
            else:
                horse_number = [h.strip() for h in horse_number]

            runner_name = sel.xpath(
                './/div[contains(@class, "line")]/div[contains(@class, "horse")]/span/text()').getall()
            jockey = sel.xpath(
                './/div[contains(@class, "line")]/div[contains(@class, "jockey")]/span/text()').getall()

            win = sel.xpath(
                ".//div[contains(concat(' ', normalize-space(@class), ' '), ' win ')]/span/text()").getall()
            place = sel.xpath(
                ".//div[contains(concat(' ', normalize-space(@class), ' '), ' place ')]/span/text()").getall()
            show = sel.xpath(
                ".//div[contains(concat(' ', normalize-space(@class), ' '), ' show ')]/span/text()").getall()

            wager_type = sel.xpath(
                './/div[contains(@class, "result result-type")]/text()').getall()
            if wager_type is None:
                pass
            else:
                wager_type = [w.strip() for w in wager_type]

            horses_results = sel.xpath(
                './/div[contains(@class, "result result-horses")]/span/text()').getall()

            race_singlename = sel.xpath('.//h5[@id="race-name"]/text()').get()
            race_singlenumber = sel.xpath(
                './/h3[@id="htmlRaceNumber"]/span/text()').get()
            race_single_date = sel.xpath(
                './/div[@id="race-header-details"]/span[1]/text()').get()
            
            if race_single_date is None:  
                pass
            else:
                race_single_date = race_single_date.strip()
                date_time_obj = datetime.strptime(race_single_date, '%m/%d/%Y %I:%M %p')

                OLD_TZ = pytz.timezone("US/Eastern")
                NEW_TZ = pytz.timezone("GMT")            
                
                #print ("The WRONG date is", race_single_date)
                #print ("The type of the date is now",  type(date_time_obj))
                #print ("The date is", date_time_obj)

                new_timezone_timestamp = OLD_TZ.localize(date_time_obj).astimezone(NEW_TZ)
                #print ("The date GMT is", new_timezone_timestamp)

            # Validar que traiga fecha
            if race_single_date is not None:
                                    
                race_single_date = race_single_date.split()         
                race_single_date = race_single_date[0]
                #print(race_single_date) 

                race_id = race_singlename + " " + race_singlenumber + " " + race_single_date
                #print(race_id)
                race_id_hash = hashlib.md5(race_id.encode('utf-8')).hexdigest()
            
                current_time = datetime.now()
                #print(current_time)
                
                ml = sel.xpath(
                './/div[contains(@class, "line")]/div[contains(@class, "ml")]/text()').getall()
                
                if ml is None:
                    print("Vino None")                
                elif  len(ml) < 1:
                    print("Vino len(ml) <  1")
                else:                 
                    ml = [m.strip() for m in ml]
                    print(len(ml), ml)
                    col = db['horse_races']
                    col.update_one({
                    'race_id': race_id_hash
                }, {
                    '$set': {                   
                        'ml': ml                  
                    }
                }, upsert=True)
                    
                race_details = {
                    'race_id': race_id_hash,
                    'race_name': race_name,
                    'runner_name': runner_name,
                    'race_number': race_number,
                    'race_date_source': date_time_obj,
                    'race_date': new_timezone_timestamp,
                    'horse_number': horse_number,
                    'jockey': jockey,
                    'ml': ml,
                    'win': win,
                    'place': place,
                    'show': show,
                    'wager_type': wager_type,
                    'horses_results': horses_results,
                    'updated_at' : current_time,
                    'source' : 'betus'
                } 
                print(race_details)            

                if len(win) > 0:
                    col = db['horse_races_results']
                    col.update_one({
                    'race_id': race_id_hash
                }, {
                    '$set': {
                        'race_name': race_name[0],
                        'runner_name': runner_name,
                        'race_number': race_number[0],
                        'race_date_source': date_time_obj,
                        'race_date': new_timezone_timestamp,
                        'horse_number': horse_number,
                        'jockey': jockey,          
                        'win': win,
                        'place': place,
                        'show': show,
                        'wager_type': wager_type,
                        'horses_results': horses_results,
                        'updated_at' : current_time,
                        'source' : 'betus'
                    }
                }, upsert=True)
                    
                else:
                    col = db['horse_races']
                    col.update_one({
                    'race_id': race_id_hash
                }, {
                    '$set': {                    
                        'race_name': race_name[0],
                        'runner_name': runner_name,
                        'race_number': race_number[0],
                        'race_date_source': date_time_obj,
                        'race_date': new_timezone_timestamp,
                        'horse_number': horse_number,
                        'jockey': jockey,
                        'updated_at' : current_time,
                        'source' : 'betus'
                    }
                }, upsert=True)            
            
# scrapy crawl betusraces


