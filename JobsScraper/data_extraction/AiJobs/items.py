# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field
from itemloaders.processors import TakeFirst, MapCompose
import dateparser
from datetime import datetime
from itemloaders import ItemLoader


def handle_errors_in_textdata(data):
    if data == None: #or not isinstance(data, str):
        return 'None'
    return data

def parse_date(date_posted):
    date_posted = date_posted[7:]
    try:
        date_posted = dateparser.parse(date_posted).date()   #date()
        return date_posted
    except Exception as e:
        print('! ! ! ! ! #DATE_ERROR Occured ---->:', f'{e}')
        return datetime(9999, 12, 30).date()




class AIjobsItem(Item):
    title = Field(
        input_processor=MapCompose(handle_errors_in_textdata), 
        output_processor=TakeFirst()
    )
    company = Field(
        input_processor=MapCompose(handle_errors_in_textdata),
        output_processor=TakeFirst()
    )
    location = Field(
        input_processor=MapCompose(handle_errors_in_textdata),
        output_processor=TakeFirst()
    )
    employment_type = Field(
        input_processor=MapCompose(handle_errors_in_textdata),
        output_processor=TakeFirst()
    )
    date_posted = Field(
        output_processor=TakeFirst()
    )
    description = Field(
        input_processor=MapCompose(handle_errors_in_textdata),
        output_processor=TakeFirst()
    )
    metadata_lastupdated = Field(
        output_processor=TakeFirst()
    )
    metadata_source = Field(
        output_processor=TakeFirst()
    )
    metadata_application_link = Field(
        input_processor=MapCompose(handle_errors_in_textdata),
        output_processor=TakeFirst()
    )
    

class WorkableSourceLoader(ItemLoader):
    default_item_class = AIjobsItem
    date_posted_in = MapCompose(parse_date)



    # define the fields for your item here like:
    # name = scrapy.Field()
    #companyname+website
    #title
    #country
    #regime (fulltime, parttime, contract)
    #dateposted (string)