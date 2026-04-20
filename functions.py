import json
import requests
from tqdm import tqdm
from urllib.parse import quote
from pprint import pprint
from urllib.parse import urlencode
import pandas as pd
import datetime
import time
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import logging
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)  # get module logger

def fetch_customers_info_basic(api_shop, api_version, headers, updated_at_min, logger, limit=250):
    total_account_fetched = 0
    customer_base_url = f"https://{api_shop}.myshopify.com/admin/api/{api_version}/customers.json"
    customer_data = []
    has_next_page = True
    page_info = None

    with tqdm(total=1000, desc="Fetching Customers") as pbar:
        while has_next_page:
            query_params = {
                'fields': 'id, created_at, updated_at, tags, first_name, last_name, addresses',
                'limit': limit
            }

            if page_info:
                query_params['page_info'] = page_info  # Continue pagination
            else:
                query_params['updated_at_min'] = updated_at_min  # Initial query

            query_string = urlencode(query_params)
            customer_url = f"{customer_base_url}?{query_string}"

            # Make the API request
            response = requests.get(customer_url, headers=headers)
            customer_info = response.json().get('customers', [])

            # Update the progress bar once per batch of customers
            num_acct = len(customer_info)
            total_account_fetched += num_acct
            pbar.update(num_acct)

            for customer in customer_info:
                customer_data.append(
                    {
                        'id': customer['id'],
                        'customer_name': (
                            (customer.get('first_name', '') or '') +
                            ' ' +
                            (customer.get('last_name', '') or '')
                        ).strip(),
                        'tags': customer['tags'],
                        'created_at': customer['created_at'],
                        'updated_at': customer['updated_at'],
                        'customer_company': customer['addresses'][0]['company'] if customer['addresses'] else None,
                        'customer_address1': customer['addresses'][0]['address1'] if customer['addresses'] else None,
                        'customer_city': customer['addresses'][0]['city'] if customer['addresses'] else None,
                        'customer_province': customer['addresses'][0]['province'] if customer['addresses'] else None,
                        'customer_zip': customer['addresses'][0]['zip'] if customer['addresses'] else None
                    }
                )

            # Check for the next page in the response headers
            links = response.headers.get('Link', '')
            if 'rel="next"' in links:
                page_info = links.split('page_info=')[-1].split('>')[0]
            else:
                has_next_page = False
                
    print("Finished fetching customers.")
    logger.info(f"finished fetching customers, total accounts fetched: {total_account_fetched}")
    return customer_data



def convert_custom_to_utc(custom_date_str):
    """
    Converts a date string in 'yyyy-mm-dd' format (assuming Toronto timezone) to UTC.

    Args:
        custom_date_str (str): The date string in 'yyyy-mm-dd' format.

    Returns:
        str: The date string in UTC format (ISO 8601 with UTC timezone).
             Returns None if the input string is not in the expected format.
    """
    try:
        # Parse the date string assuming Toronto timezone
        local_tz = pytz.timezone('America/Toronto')
        local_dt = datetime.strptime(custom_date_str, '%Y-%m-%d')
        local_dt = local_tz.localize(local_dt)

        # Convert to UTC
        utc_dt = local_dt.astimezone(pytz.utc)
        return utc_dt.isoformat()
    except ValueError:
        print(f"Error: Invalid date format '{custom_date_str}'. Please use 'yyyy-mm-dd'.")
        return None

if __name__ == '__main__':
    # Example usage
    custom_date = '2025-05-06'
    utc_date = convert_custom_to_utc(custom_date)
    if utc_date:
        print(f"Custom date '{custom_date}' in UTC is: {utc_date}")

    invalid_date = '2025/05/06'
    utc_invalid = convert_custom_to_utc(invalid_date)
    if utc_invalid is None:
        print(f"Conversion failed for '{invalid_date}' as expected.")