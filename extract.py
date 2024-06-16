import asyncio
import json
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup as BS
import datetime

REQUESTS_PER_SECOND = 3
MAX_RETRIES = 3
semaphore = asyncio.Semaphore(REQUESTS_PER_SECOND)

async def fetch(session, url):
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url) as response:
                await asyncio.sleep(1 / REQUESTS_PER_SECOND)
                return await response.text()
        except aiohttp.ClientError as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            else:
                raise e

async def async_request_fetch_scrape_dictionary(session, country, page, operator):
    url = f'https://www.wakacje.pl/wczasy/{country}/?str-{page},1-28-dni,samolotem,all-inclusive,2-gwiazdkowe,ocena-6,{operator},z-warszawy'
    async with semaphore:
        return await fetch(session, url)

async def get_initial_scraping_dictionary(item):
    async with aiohttp.ClientSession() as session:
        data = []
        async with semaphore:
            url = f'https://www.wakacje.pl/wczasy/{item[0]}/?str-1,1-28-dni,samolotem,all-inclusive,2-gwiazdkowe,ocena-6,{item[1]},z-warszawy'
            data.append(await fetch(session, url))
        return data

async def get_final_scrape_data(scraping_dictionary, operators):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for operator in operators:
            tasks.extend([async_request_fetch_scrape_dictionary(session, item[0], item[1], operator) for item in scraping_dictionary])
        return await asyncio.gather(*tasks, return_exceptions=False)

def get_offers_num(raw_data):
    offer_num_country_list = []
    for object in raw_data:
        soup_object = BS(object, 'html.parser')
        offers_are_not_present = soup_object.find('div', attrs={"data-testid":"NotificationBannerContent"})
        if offers_are_not_present:
            continue
        json_data = soup_object.find('script', id="__NEXT_DATA__")
        json_object = json.loads(json_data.text)
        offers_num = json_object['props']['stores']['storeOffers']['offers']["count"]
        offers_country = json_object['props']['stores']['storeAnalytics']['_contentGroupObject']['1'].lower()
        if (offers_num/10) > 100:
            offers_num=100
        elif offers_num % 10 == 0:
            offers_num = int(offers_num / 10)
        else:
            offers_num = int(offers_num / 10)+1
        for i in range(1, offers_num + 1):
            offer_num_country_list.append([offers_country, i])
    return offer_num_country_list

def process_extracted_data(extracted_data):
    offers_list = []
    for page in extracted_data:
        soup_object = BS(page, 'html.parser')
        json_data = soup_object.find('script', id="__NEXT_DATA__")
        json_object = json.loads(json_data.text)
        try:
            for raw_offer in json_object['props']['stores']['storeOffers']['offers']['data']:
                offer = {}
                offer["hotel_name"] = raw_offer["name"]
                offer["hotel_country"] = raw_offer["place"]["country"]["name"]
                offer["hotel_region"] = raw_offer["place"]["region"]["name"]
                offer["hotel_city"] = raw_offer["place"]["city"]["name"]
                offer["hotel_category"] = raw_offer["category"]
                offer["stay_duration"] = raw_offer["duration"]
                offer["price"] = raw_offer["price"]
                offer["departure_city"] = raw_offer["departurePlace"]
                offer["departure_date"] = raw_offer["departureDate"]
                offer["return_date"] = raw_offer["returnDate"]
                offer["tour_operator_name"] = raw_offer["tourOperatorName"]
                offer["rating_value"] = raw_offer["ratingValue"]
                offer["created_at"] = datetime.datetime.now()
                offers_list.append(offer)
        except KeyError:
            pass
    return offers_list

def build_countires_operators_list(countries, operators):
    scraping_list = []
    for country in countries:
        for operator in operators:
            scraping_list.append((country, operator))
    return scraping_list
