from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from AiJobs.spiders.gather_jobs_workable import WorkableJobSpider


process = CrawlerProcess(get_project_settings())
process.crawl(WorkableJobSpider)
process.start()