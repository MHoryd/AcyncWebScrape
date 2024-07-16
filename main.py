import config
import asyncio
import extract
import database_handler

async def process_item(item):
    initial_data = await extract.get_initial_scraping_dictionary(item)
    data_to_scrape = extract.get_offers_num(initial_data)
    final_data = await extract.get_final_scrape_data(data_to_scrape, config.tour_operators)
    processed_data = extract.process_extracted_data(final_data)
    if processed_data:
        await database_handler.insert_data_to_tinydb(processed_data)

async def process_batch(items):
    tasks = [process_item(item) for item in items]
    await asyncio.gather(*tasks)

async def process_in_batches(all_items, batch_size):
    for i in range(0,len(all_items), batch_size):
        batch = all_items[i:i+batch_size]
        await process_batch(batch)

scraping_list = extract.build_countires_operators_list(config.countries_list, config.tour_operators)
asyncio.run(process_in_batches(all_items=scraping_list, batch_size = config.batch_size))

data = database_handler.get_data_from_tinydb()
processed_data = database_handler.convert_to_list_of_values(data)
database_handler.insert_data(processed_data,config)
database_handler.clear_tinydb()