import logging
import os
import re
from random import randint

import pandas as pd
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)
logging.basicConfig(filename="tracker.log", encoding="utf-8", level=logging.INFO)


def _get_seperator(line):
    if "PF" in line:
        return r"PF(?!VS)"
    elif "VF" in line:
        return r"VF(?!VS)"
    elif "VS" in line:
        return r"VS(?!VS)"
    return None


def _get_lat_lon(address):
    url = f"https://nominatim.openstreetmap.org/search?q={address}&format=json&accept-language=en&addressdetails=1&limit=1"
    try:
        headers = {
            "User-Agent": f"new_york_city_decipher_{randint(0, 10_000_000)}",
        }
        result = requests.get(url=url, headers=headers)
        logging.warning(f"Request status code: {result.status_code}")
        if (result.status_code != 200) or (len(result.json()) == 0):
            return None
        result_json = result.json()[0]
        address_data = {}
        address_data["lat"] = float(result_json["lat"])
        address_data["long"] = float(result_json["lon"])
        address_data["geo_address"] = result_json["display_name"]
        logging.warning(f"Relevant Data: {address_data}; For address: {address}")
        return address_data
    except Exception:
        return None


if __name__ == "__main__":
    street_mappings = {}

    with open(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "./snd24Bcow.txt")
    ) as f:
        lines = [line.rstrip() for line in f]

    borough_codes = {
        "1": "Manhattan",
        "2": "Bronx",
        "3": "Brooklyn",
        "4": "Queens",
        "5": "Staten Island",
    }

    for line in tqdm(lines, desc="Parsing street codes"):
        sep = _get_seperator(line)
        if sep is None:
            continue
        data_line = re.split(sep, line)
        bsc10 = data_line[1].split(" ")[0]
        bsc5 = bsc10[:6]
        if bsc5 not in street_mappings:
            try:
                borough = borough_codes[bsc5[0]]
                address = data_line[0][2:].strip()
                address_data = _get_lat_lon(f"{address}, {borough}, NYC")
                if address_data:
                    address_data["stret_code"] = bsc5[1:]
                    address_data["borough"] = borough
                    address_data["address"] = address
                    street_mappings[bsc5] = address_data
                    logging.info(f"{bsc10} parsed...")
            except Exception as e:
                logging.error(f"Error for {bsc10}: {e}")

    pd.DataFrame(street_mappings).to_csv(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "street_code_mapper.csv"
        )
    )
    print("Street code mapper created")
    print(
        f"Successfully parsed {len(street_mappings)} street codes out of {len(lines)}"
    )
