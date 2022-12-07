# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BetusItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # track locations
    #//div[@id="tabContentHorse"]/ul/li/a/@href
    race_name = scrapy.Field()
    race_number = scrapy.Field()
    race_runner = scrapy.Field()
    race_jockey = scrapy.Field()
    race_ml = scrapy.Field()
    race_horse_number = scrapy.Field()
    race_win = scrapy.Field()
    race_place = scrapy.Field()
    race_show = scrapy.Field()
    pass
