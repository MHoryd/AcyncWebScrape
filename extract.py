import asyncio
import json
import aiohttp
from bs4 import BeautifulSoup as BS
import datetime
import logging

REQUESTS_PER_SECOND = 3
MAX_RETRIES = 3 
semaphore = asyncio.Semaphore(REQUESTS_PER_SECOND)
global used_url
used_url = []


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def fetch(session, url):
    if url not in used_url:
        for attempt in range(MAX_RETRIES):
            used_url.append(url)
            try:
                async with session.get(url) as response:
                    await asyncio.sleep(1 / REQUESTS_PER_SECOND)
                    if response.status == 200:
                        return await response.text()
                    else:
                        logging.warning(f"Failed to fetch {url}, status code: {response.status}")
            except aiohttp.ClientError as e:
                logging.error(f"Request error for {url}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    raise e
        return None

async def async_request_fetch_scrape_dictionary(session, country, page, operator):
    url = f'https://www.wakacje.pl/wczasy/{country}/?str-{page},1-28-dni,samolotem,all-inclusive,2-gwiazdkowe,ocena-6,{operator},z-warszawy'
    async with semaphore:
        return await fetch(session, url)

async def get_initial_scraping_dictionary(item):
    async with aiohttp.ClientSession() as session:
        data = []
        async with semaphore:
            url = f'https://www.wakacje.pl/wczasy/{item[0]}/?str-1,1-28-dni,samolotem,all-inclusive,2-gwiazdkowe,ocena-6,{item[1]},z-warszawy'
            page_content = await fetch(session, url)
            if page_content:
                data.append(page_content)
        return data

async def get_final_scrape_data(scraping_dictionary, operators):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for operator in operators:
            tasks.extend([async_request_fetch_scrape_dictionary(session, item[0], item[1], operator) for item in scraping_dictionary])
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [result for result in results if result is not None]

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
        offers_num = min(100, (offers_num + 9) // 10)
        for i in range(1, offers_num + 1):
            offer_num_country_list.append([offers_country, i])
    return offer_num_country_list

def process_extracted_data(extracted_data):
    offers_list = []
    seen_offers = set()

    for page in extracted_data:
        soup_object = BS(page, 'html.parser')
        json_data = soup_object.find('script', id="__NEXT_DATA__")
        if not json_data:
            continue

        json_object = json.loads(json_data.text)
        try:
            for raw_offer in json_object['props']['stores']['storeOffers']['offers']['data']:
                offer_identifier = (
                    raw_offer["name"],
                    raw_offer["departureDate"],
                    raw_offer["returnDate"],
                    raw_offer["price"],
                    raw_offer["tourOperatorName"]
                )

                if offer_identifier in seen_offers:
                    continue

                offer = {
                    "hotel_name": raw_offer["name"],
                    "hotel_country": raw_offer["place"]["country"]["name"],
                    "hotel_region": raw_offer["place"]["region"]["name"],
                    "hotel_city": raw_offer["place"]["city"]["name"],
                    "hotel_category": raw_offer["category"],
                    "stay_duration": raw_offer["duration"],
                    "price": raw_offer["price"],
                    "departure_city": raw_offer["departurePlace"],
                    "departure_date": raw_offer["departureDate"],
                    "return_date": raw_offer["returnDate"],
                    "tour_operator_name": raw_offer["tourOperatorName"],
                    "rating_value": raw_offer["ratingValue"],
                    "created_at": datetime.datetime.now()
                }

                seen_offers.add(offer_identifier)
                offers_list.append(offer)
        except KeyError as e:
            logging.error(f"KeyError processing data: {e}")

    return offers_list

def build_countires_operators_list(countries, operators):
    scraping_list = [(country, operator) for country in countries for operator in operators]
    return scraping_list
