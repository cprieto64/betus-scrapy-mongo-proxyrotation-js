# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import scrapy
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from scrapy import Request
import csv
from datetime import datetime

class BetusPipeline:
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        today = datetime.today().strftime('%Y-%m-%d')
        file = open('%s_items.csv' % spider.name, 'w+b')
        #file = open('%s_items.csv' % today, 'w+b')
        self.files[spider] = file
        self.exporter = CsvItemExporter(file)
        self.exporter.fields_to_export = ['race_name', 'race_number', 'race_horse_number', 'race_runner', 'race_jockey', 'race_ml', 'race_win', 'race_place', 'race_show']
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        file.close()
        
        #given I am using Windows i need to elimate the blank lines in the csv file        
        
        # with open('%s_items.csv' % spider.name, 'r') as f:
        #     reader = csv.reader(f)
        #     original_list = list(reader)
        #     cleaned_list = list(filter(None,original_list))

        # with open('%s_items_cleaned.csv' % spider.name, 'w', newline='') as output_file:
        #     wr = csv.writer(output_file, dialect='excel')
        #     for data in cleaned_list:
        #         wr.writerow(data)  
    
    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

