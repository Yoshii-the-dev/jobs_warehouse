from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
from scrapy.selector import Selector
from datetime import datetime
import scrapy 
from AiJobs.items import WorkableSourceLoader
from urllib.parse import urlparse, parse_qs
import random
from selenium.webdriver.common.keys import Keys
import os 
#from scrapy.loader import ItemLoader
from itemloaders import ItemLoader
import logging

class WorkableJobSpider(scrapy.Spider):
    name = 'WorkableJob'
    allowed_domains = ['jobs.workable.com']
    start_urls = ['https://jobs.workable.com/search?remote=false']

    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.41'
    ]


    def start_requests(self):
        CHROMEDRIVER_DIR = os.getenv("CHROMEDRIVER_DIR")
        DRIVER_PATH = os.path.join(CHROMEDRIVER_DIR, "chromedriver")
        #DRIVER_PATH = r"D:\chromedriver\chromedriver.exe"
        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1050,660")
        options.add_argument("--disable-dev-shm-usage")
        service = ChromeService(executable_path=DRIVER_PATH)

        driver = webdriver.Chrome(options=options, service=service)
        driver.get('https://jobs.workable.com/search?remote=false')
        time.sleep(2)
        cookie_consent = driver.find_element(By.XPATH, "//div[@data-ui='cookie-consent']")
        if cookie_consent.is_displayed():
            # Dismiss the cookie consent
            driver.find_element(By.XPATH, "//button[contains(text(), 'Decline')]").click()
            time.sleep(1)
        search_input = driver.find_element(By.XPATH, "//input[@data-ui='search-input-job']")
        search_input.send_keys("Data Analyst Machine Learning AI")
        driver.find_element(By.XPATH, "//button[@data-ui='search-button'][@type='submit']").click()
        driver.page_source
        wait = WebDriverWait(driver, 5)
    
    
        while True:
            try:
                time.sleep(1)
                button = wait.until(EC.visibility_of_element_located((
                    By.XPATH, "//button[@data-ui='load-more-button']")))
                button.click()
                yield scrapy.Request(url='https://jobs.workable.com/search?remote=false', 
                             headers={'User-Agent':self.user_agents[random.randint(0,len(self.user_agents)-1)]}, 
                             callback=self.parse, 
                             meta={'driver':driver})
            except Exception as e:
                logging.error(e)
                break
        


    def parse(self, response):
        driver = response.meta['driver']
        
        results = driver.find_elements(By.XPATH, "//a[@tabindex='0'][@data-role='button-link'][@target='_self']")
        last_updated = datetime.now().strftime("%Y-%m-%d %H:%M")
        last_updated = datetime.strptime(last_updated, "%Y-%m-%d %H:%M")
        for result in results[::2]:
            time.sleep(1)
            driver.execute_script("arguments[0].click();", result)
            url = driver.current_url

            header =  WebDriverWait(driver,10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.jobOverview__job-overview--3qD-1"))
            ).get_attribute('outerHTML')
            description = WebDriverWait(driver,10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.jobBreakdown__job-breakdown--31MGR"))
            ).text
            description=' '.join(description.split())
            loader = WorkableSourceLoader(selector=Selector(text=header))
            loader.add_css('title', 'h2 strong::text')
            loader.add_css('company', 'a::text')
            loader.add_css('location', 'span[data-ui="overview-location"]::text')
            loader.add_css('employment_type', 'span[data-ui="overview-employment-type"]::text')
            loader.add_css('date_posted', 'span[data-ui="overview-date-posted"]::text')
            loader.add_value('description', description)
            loader.add_value('metadata_lastupdated', last_updated)
            loader.add_value('metadata_source', 'https://jobs.workable.com')
            loader.add_value('metadata_application_link', url)

            yield loader.load_item()

