import config
import asyncio
import extract
import database_handler

async def process_item(item):
    initial_data = await extract.get_initial_scraping_dictionary(item)
    data_to_scrape = extract.get_offers_num(initial_data)
    final_data = await extract.get_final_scrape_data(data_to_scrape, config.tour_operators)
    processed_data = extract.process_extracted_data(final_data)
    database_handler.insert_data(processed_data, config)

async def main():
    scraping_list = extract.build_countires_operators_list(config.countries_list, config.tour_operators)
    tasks = [process_item(item) for item in scraping_list]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
