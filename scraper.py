import numpy as np
import requests
import time
import re
from bs4 import BeautifulSoup
import os
import pandas as pd
import json
from datetime import datetime, date


class LocationLookupError(Exception):
    pass


def get_location_id(postcode):
    url = f"https://www.rightmove.co.uk/house-prices/{postcode.replace(' ', '-')}.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise LocationLookupError(f"couldn't load Rightmove location page for {postcode}.") from exc

    soup = BeautifulSoup(response.text, "html.parser")

    for a in soup.find_all("a", href=True):
        match = re.search(r"locationIdentifier=POSTCODE\^(\d+)", a["href"])
        if match:
            return match.group(1)

    raise LocationLookupError(
        f"couldn't find Rightmove location id for {postcode}. "
        "Check the postcode, or try again if Rightmove returned a temporary block page."
    )


def get_base_url(postcode, location_id, radius, property_types="flat"):
    return (
        f"https://www.rightmove.co.uk/property-for-sale/find.html"
        f"?searchLocation={postcode.replace(' ', '+').upper()}"
        f"&useLocationIdentifier=true"
        f"&locationIdentifier=POSTCODE%5E{location_id}"
        f"&buy=For+sale"
        f"&propertyTypes={property_types}"
        f"&radius={float(radius):.1f}"
        f"&_includeSSTC=on"
        f"&sortType=2"
        f"&channel=BUY"
        f"&transactionType=BUY"
        f"&displayLocationIdentifier=undefined"
    )


def get_urls(base_url, start_page=1, end_page=1, progress_callback=None):
    page = start_page - 1
    final_page = end_page - 1
    ids = []
    pages_to_check = end_page - start_page + 1
    pages_checked = 0

    while page <= final_page:
        url = f"{base_url}&index={page * 24}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)

        soup = BeautifulSoup(response.content, "html.parser")

        urls_found = 0
        for a in soup.find_all("a", id=True):
            match = re.match(r"prop(\d+)", a["id"])
            if match:
                id = match.group(1)
                if id not in ids:
                    ids.append(id)
                    urls_found += 1
        
        pages_checked += 1
        print(f"page {page + 1}: {urls_found} new properties found.")
        if progress_callback:
            progress_callback({
                "stage": "pages",
                "page": page + 1,
                "start_page": start_page,
                "end_page": end_page,
                "pages_checked": pages_checked,
                "pages_to_check": pages_to_check,
                "urls_found": urls_found,
                "total_urls": len(ids),
            })

        if urls_found == 0:
            break

        page += 1
        time.sleep(0.5)

    print(f"total of {len(ids)} properties found.")
    urls = [f"https://www.rightmove.co.uk/properties/{id}#/?channel=RES_BUY" for id in ids]

    return urls


def extract_json_after_marker(text, marker):
    start = text.find(marker)
    if start == -1:
        return None

    start += len(marker)
    end = start
    depth = 0
    in_string = False
    escape = False

    while end < len(text):
        char = text[end]

        if char == '"' and not escape:
            in_string = not in_string
        elif char == "\\" and in_string:
            escape = not escape
            end += 1
            continue
        elif not in_string:
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    end += 1
                    break

        escape = False
        end += 1

    if depth != 0:
        return None

    return json.loads(text[start:end])


def hydrate_page_model(data):
    cache = {}

    def hydrate(value):
        if isinstance(value, int) and not isinstance(value, bool):
            if value in cache:
                return cache[value]

            resolved = data[value]
            if isinstance(resolved, dict):
                hydrated_dict = {}
                cache[value] = hydrated_dict
                hydrated_dict.update({key: hydrate(val) for key, val in resolved.items()})
                return hydrated_dict
            if isinstance(resolved, list):
                hydrated_list = []
                cache[value] = hydrated_list
                hydrated_list.extend(hydrate(item) for item in resolved)
                return hydrated_list

            return resolved

        return value

    return hydrate(0)


def get_json_object(text):
    if not text:
        return None

    page_model = extract_json_after_marker(text, "window.PAGE_MODEL = ")
    if page_model:
        return page_model

    page_model = extract_json_after_marker(text, "window.__PAGE_MODEL = ")
    if not page_model:
        return None

    data = page_model.get("data")
    if not data:
        return None

    return hydrate_page_model(json.loads(data))


def get_json_str(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, "html.parser")

    for script in soup.find_all("script"):
        if "window.PAGE_MODEL" in script.text or "window.__PAGE_MODEL" in script.text:
            return script.text

    return None


def get_property_type(type, subtype):
    type = (type or "").lower()
    subtype = (subtype or "").lower()

    if "flats_apartments" in type or "flat" in type or "apartment" in type:
        return "F"

    if "houses" in type or "bungalows" in type or "house" in type or "bungalow" in type:
        if "semi_" in subtype or "semi" in subtype:
            return "S"
        elif "detached" in subtype and "semi" not in subtype:
            return "D"
        elif "terrace" in subtype or "terraced" in subtype or "town_house" in subtype or "townhouse" in subtype:
            return "T"
        
    return "N/A"


def filter_json(page_model):
    targeting = {item.get('key'): item.get('value', [None])[0] for item in page_model.get("propertyData", {}).get("dfpAdInfo", {}).get("targeting", []) if item.get("value")}

    match = targeting.get("P_ID")
    property_id = int(match) if match else "N/A"

    match = targeting.get("PT")
    rm_property_type = match if match else "N/A"

    match = targeting.get("PST")
    rm_property_subtype = match if match else "N/A"

    property_type = get_property_type(rm_property_type, rm_property_subtype)

    match = targeting.get("RESALEPRICE")
    price_pcm = int(match) if match else "N/A"

    match = targeting.get("FS")
    furnish = match if match else "N/A"


    property_data = page_model.get("propertyData", {})

    match = property_data.get("bedrooms")
    bedrooms = match if match else 1

    match = property_data.get("bathrooms")
    bathrooms = match if match else "N/A"

    living_costs = property_data.get("livingCosts", {})

    match = living_costs.get("annualGroundRent")
    annual_ground_rent = match if match is not None else "N/A"

    match = living_costs.get("annualServiceCharge")
    annual_service_charge = match if match is not None else "N/A"

    tenure = property_data.get("tenure", {})

    match = tenure.get("yearsRemainingOnLease")
    years_remaining_on_lease = match if match is not None else "N/A"

    sizings = property_data.get("sizings", [])
    match = next((item.get("minimumSize") for item in sizings if item.get("unit") == "sqft"), None)
    sqft = int(match) if match else "N/A"
    if match and sqft < 100:  #accidental listing of sqft instead of sqm
        sqft *= 10.7639


    price_pcm_per_sqft = price_pcm / sqft if isinstance(price_pcm, int) and isinstance(sqft, int) else "N/A"


    address = property_data.get("address", {})

    match = address.get("displayAddress")
    display_address = match if match else "N/A"

    match = address.get("outcode")
    outcode = match if match else None

    match = address.get("incode")
    incode = match if match else None
    postcode = outcode + "-" + incode if outcode and incode else "N/A"


    analytics_property = page_model.get("analyticsInfo", {}).get("analyticsProperty", {})

    match = analytics_property.get("added")
    date_added = datetime.strptime(match, "%Y%m%d") if match else "N/A"

    match = analytics_property.get("lettingType")
    let_type = match.split()[0].lower() if match else "N/A"


    return {
        "property_id": property_id,
        "date_added": date_added,
        "postcode": postcode,
        "rm_property_type": rm_property_type,
        "rm_property_subtype": rm_property_subtype,
        "property_type": property_type,
        "price_pcm": price_pcm,
        "sqft": sqft,
        "price_pcm_per_sqft": price_pcm_per_sqft,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "annual_ground_rent": annual_ground_rent,
        "annual_service_charge": annual_service_charge,
        "years_remaining_on_lease": years_remaining_on_lease,
        "furnish": furnish,
        "let_type": let_type,
        "display_address": display_address
    }


def scrape_all(urls, progress_callback=None):
    df = pd.DataFrame()

    total = len(urls)
    for i, url in enumerate(urls, start=1):
        json_str = get_json_str(url)
        if not json_str:
            print(f"\r{i}/{total} skipped: no page model found.{' ' * 20}", end="")
            if progress_callback:
                progress_callback({
                    "stage": "properties",
                    "current": i,
                    "total": total,
                    "scraped": len(df),
                    "status": "skipped: no page model found",
                })
            continue

        json_obj = get_json_object(json_str)
        if not json_obj:
            print(f"\r{i}/{total} skipped: page model could not be parsed.{' ' * 20}", end="")
            if progress_callback:
                progress_callback({
                    "stage": "properties",
                    "current": i,
                    "total": total,
                    "scraped": len(df),
                    "status": "skipped: page model could not be parsed",
                })
            continue

        entry = filter_json(json_obj)
        entry["url"] = url

        df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
        print(f"\r{i}/{total} sale properties scraped.{' ' * 20}", end="")
        if progress_callback:
            progress_callback({
                "stage": "properties",
                "current": i,
                "total": total,
                "scraped": len(df),
                "status": "scraped",
            })
    print()
    return df


def generate_sale_data(postcode, radius, overwrite=False, start_page=1, end_page=1, progress_callback=None):
    output_csv = os.path.join(
        os.getcwd(),
        "data",
        "scraped",
        f"{postcode.replace(' ', '-').upper()}_sale_prices_({radius}mi).csv"
    )

    if os.path.exists(output_csv) and not overwrite:
        print(f"sales already scraped.")
        return pd.read_csv(output_csv)

    else:
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

        location_id = get_location_id(postcode)
        base_url = get_base_url(postcode, location_id, radius)
        urls = get_urls(
            base_url,
            start_page=start_page,
            end_page=end_page,
            progress_callback=progress_callback,
        )

        if len(urls) == 0:
            print(f"No properties found within {radius} miles.")
            return None

        df = scrape_all(urls, progress_callback=progress_callback)
        
        df.to_csv(output_csv, index=False)
        print(f"file saved as {os.path.basename(output_csv)} in data/scraped.")
    
        return df


def gen_stats_df(df, postcode, radius):
    # Drop rows with missing required fields
    if df is None:
        return f'No sales within {radius} miles', df
    df = df.dropna(subset=['price_pcm', 'property_type', 'let_type'])

    # Only include long lets for stats
    df = df[df["let_type"] == "long"]

    # Ensure numeric types for aggregation
    df["price_pcm"] = pd.to_numeric(df["price_pcm"], errors='coerce')
    df["price_pcm_per_sqft"] = pd.to_numeric(df["price_pcm_per_sqft"], errors='coerce')

    # Group by property type and describe stats
    stats = df.groupby("property_type").agg(
        rsf_min=("price_pcm_per_sqft", "min"),
        rsf_25th_percentile=(
            "price_pcm_per_sqft", lambda x: np.percentile(x.dropna(), 25) if x.dropna().size > 0 else np.nan),
        rsf_median=("price_pcm_per_sqft", "median"),
        rsf_75th_percentile=(
            "price_pcm_per_sqft", lambda x: np.percentile(x.dropna(), 75) if x.dropna().size > 0 else np.nan),
        rsf_max=("price_pcm_per_sqft", "max"),
        rsf_mean=("price_pcm_per_sqft", "mean"),
        rsf_count=("price_pcm_per_sqft", lambda x: x.notna().sum()),
        rent_count=("price_pcm", "count"),
        rent_min=("price_pcm", "min"),
        rent_25th_percentile=(
            "price_pcm", lambda x: np.percentile(x.dropna(), 25) if x.dropna().size > 0 else np.nan),
        rent_median=("price_pcm", "median"),
        rent_75th_percentile=(
            "price_pcm", lambda x: np.percentile(x.dropna(), 75) if x.dropna().size > 0 else np.nan),
        rent_max=("price_pcm", "max"),
        rent_mean=("price_pcm", "mean")
    ).reset_index()

    # Add new columns
    stats["postal_code"] = postcode
    stats["search_radius_miles"] = radius
    stats["evaluation_date"] = date.today().strftime('%Y-%m-%d')

    return 'Sufficient Data', stats


if __name__ == "__main__":
    postcode = "w5 5db"
    radius = 1  #miles (0.25, 0.5, 1, 3, 5, 10, 15, 20, 30, 40)
    df = generate_sale_data(postcode, radius, overwrite=True)
