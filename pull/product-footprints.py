import requests, json, csv, logging, yaml, time, os
from pathlib import Path
from myconfig import email, password
from merge_impact_data import merge_impact_data, fetch_from_openepd_by_id, should_fetch_from_openepd

# ── US states ────────────────────────────────────────────────────────────────
us_states = [
    'US-AL', 'US-AK', 'US-AZ', 'US-AR', 'US-CA', 'US-CO', 'US-CT', 'US-DE', 'US-FL', 'US-GA',
    'US-HI', 'US-ID', 'US-IL', 'US-IN', 'US-IA', 'US-KS', 'US-KY', 'US-LA', 'US-ME', 'US-MD',
    'US-MA', 'US-MI', 'US-MN', 'US-MS', 'US-MO', 'US-MT', 'US-NE', 'US-NV', 'US-NH', 'US-NJ',
    'US-NM', 'US-NY', 'US-NC', 'US-ND', 'US-OH', 'US-OK', 'US-OR', 'US-PA', 'US-RI', 'US-SC',
    'US-SD', 'US-TN', 'US-TX', 'US-UT', 'US-VT', 'US-VA', 'US-WA', 'US-WV', 'US-WI', 'US-WY',
    'US-DC'
]

european_countries = [
    'GB', 'DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'SE', 'DK',
    'FI', 'NO', 'PL', 'CZ', 'PT', 'GR', 'IE', 'RO'
]


def load_countries_from_config(config_path: str = None) -> list:
    """
    Read the non-US country list from config.yaml (comma-separated or YAML list).
    Falls back to defaults if the file is missing.
    """
    if config_path is None:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    if not os.path.exists(config_path):
        print(f"[WARN] config.yaml not found at {config_path} — using defaults.")
        return ['IN', 'GB', 'DE', 'NL', 'CA', 'MX', 'CN']
    with open(config_path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    raw = cfg.get("countries", "")
    codes = (
        [str(c).strip().upper() for c in raw if c]
        if isinstance(raw, list)
        else [c.strip().upper() for c in str(raw).split(",") if c.strip()]
    )
    # Strip bare 'US' — US states are handled separately via us_states
    codes = [c for c in codes if c != "US"]
    return codes if codes else ['IN', 'GB', 'DE', 'NL', 'CA', 'MX', 'CN']


# Non-US countries loaded from config.yaml
countries = load_countries_from_config()

# Default combined region list (all US states + config countries)
states = us_states + countries


# ── CSV combine (Cement only — all.csv no longer generated) ──────────────────
def combine_csvs_for_country(country: str):
    """
    Combine cement category CSVs into the root Cement.csv.
    all.csv is intentionally NOT generated.
    """
    base_dir   = Path.home() / "Documents" / "GitHub" / "products-data"
    folder     = base_dir / country
    out_cement = base_dir / "Cement.csv"

    if not folder.exists():
        print(f"  Folder {folder} doesn't exist, skipping combine")
        return

    cement_keywords = ['cement', 'ready_mix', 'concrete', 'mortar']
    cement_files = sorted([
        p for p in folder.glob("*.csv")
        if any(kw in p.stem.lower() for kw in cement_keywords)
    ])

    if not cement_files:
        return

    header = None
    cement_rows = 0
    with out_cement.open("w", newline="", encoding="utf-8") as f_out:
        writer = None
        for path in cement_files:
            with path.open("r", newline="", encoding="utf-8") as f_in:
                reader = csv.reader(f_in)
                try:
                    file_header = next(reader)
                except StopIteration:
                    continue
                if header is None:
                    header = file_header
                    writer = csv.writer(f_out)
                    writer.writerow(header)
                for row in reader:
                    writer.writerow(row)
                    cement_rows += 1

    print(f"  ✓ Cement.csv — {cement_rows} rows  (all.csv not generated)")


# ── CLI args ─────────────────────────────────────────────────────────────────
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Pull product footprint data from EC3 API')
    parser.add_argument(
        '--country', type=str,
        help='Comma-separated country/state codes, e.g. IN,US,DE  or  US-CA,US-NY'
    )
    parser.add_argument(
        '--europe', action='store_true',
        help='Pull all European countries'
    )
    return parser.parse_args()


# ── API settings ─────────────────────────────────────────────────────────────
epds_url    = "https://buildingtransparency.org/api/epds"
openepd_url = "https://openepd.buildingtransparency.org/api/epds"
page_size   = 250

ENABLE_OPENEPD_FETCH = False  # Set True to merge openEPD impact/resource data

logging.basicConfig(
    level=logging.DEBUG,
    filename="output.log",
    datefmt="%Y/%m/%d %H:%M:%S",
    format="%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)


def log_error(status_code: int, response_body: str):
    logging.error(f"Request failed with status code: {status_code}")
    logging.debug("Response body:" + response_body)


def get_auth():
    url_auth = "https://buildingtransparency.org/api/rest-auth/login"
    headers_auth = {"accept": "application/json", "Content-Type": "application/json"}
    payload_auth = {"username": email, "password": password}
    response_auth = requests.post(url_auth, headers=headers_auth, json=payload_auth)
    if response_auth.status_code == 200:
        authorization = 'Bearer ' + response_auth.json()['key']
        print("Fetched the new token successfully", flush=True)
        return authorization
    else:
        print(f"Failed to login. Status code: {response_auth.status_code}")
        print("Response body:" + str(response_auth.json()))
        return None


def fetch_a_page(page: int, headers, state: str, total_pages: int = 0):
    logging.info(f'Fetching state: {state}, page: {page}')
    params = {"plant_geography": state, "page_size": page_size, "page_number": page}
    for attempt in range(5):
        try:
            response = requests.get(epds_url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = json.loads(response.text)
                if total_pages > 10 and page % 10 == 0:
                    print(f"  Progress: {page}/{total_pages} pages for {state}", flush=True)
                return data
            elif response.status_code == 401:
                print(f"  Auth expired on page {page} for {state}. Refreshing...", flush=True)
                new_auth = get_auth()
                if new_auth:
                    headers["Authorization"] = new_auth
                    response = requests.get(epds_url, headers=headers, params=params, timeout=30)
                    if response.status_code == 200:
                        return json.loads(response.text), new_auth
                log_error(401, "Failed to refresh token")
                return [], headers.get("Authorization")
            elif response.status_code == 429:
                log_error(response.status_code, "Rate limit exceeded. Retrying...")
                time.sleep(2 ** attempt + 5)
            else:
                log_error(response.status_code, str(response.json()) if response.text else "")
                return [], headers.get("Authorization", "")
        except requests.exceptions.Timeout:
            log_error(0, f"Timeout {state} p{page}")
            time.sleep(2 ** attempt + 5)
        except requests.exceptions.RequestException as e:
            log_error(0, f"Request error {state} p{page}: {e}")
            time.sleep(2 ** attempt + 5)
    return [], headers.get("Authorization", "")


def fetch_epds(state: str, authorization):
    params  = {"plant_geography": state, "page_size": page_size}
    headers = {"accept": "application/json", "Authorization": authorization}
    try:
        response = requests.get(epds_url, headers=headers, params=params, timeout=30)
    except requests.exceptions.Timeout:
        print(f"Timeout on {state}. Skipping...", flush=True)
        return [], authorization
    except requests.exceptions.RequestException as e:
        print(f"Request error {state}: {e}. Skipping...", flush=True)
        return [], authorization

    if response.status_code == 401:
        print(f"Auth expired for {state}. Refreshing...", flush=True)
        new_auth = get_auth()
        if new_auth:
            authorization = new_auth
            headers["Authorization"] = new_auth
            response = requests.get(epds_url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                print(f"Still failed after refresh for {state}", flush=True)
                return None, authorization
        else:
            return None, authorization

    if response.status_code != 200:
        log_error(response.status_code, str(response.json()) if response.text else "")
        print(f"No data for {state} (status: {response.status_code})", flush=True)
        return [], authorization

    total_pages = int(response.headers.get('X-Total-Pages', 0))
    if total_pages == 0:
        print(f"No data found for {state}", flush=True)
        return [], authorization

    print(f"Found {total_pages} pages for {state}", flush=True)
    full_response = []
    start_time    = time.time()

    for page in range(1, total_pages + 1):
        page_result = fetch_a_page(page, headers, state, total_pages)
        if isinstance(page_result, tuple):
            page_data, authorization = page_result
            headers["Authorization"] = authorization
        else:
            page_data = page_result
        if page_data:
            full_response.extend(page_data)
        else:
            print(f"  Warning: empty page {page}, continuing...", flush=True)
        if page < total_pages:
            time.sleep(1)

    time.sleep(10)
    elapsed = time.time() - start_time
    print(f"Fetched {len(full_response)} EPDs for {state} in {elapsed:.1f}s", flush=True)
    return full_response, authorization


def remove_null_values(data):
    if isinstance(data, list):
        return [remove_null_values(item) for item in data if item is not None]
    elif isinstance(data, dict):
        return {k: remove_null_values(v) for k, v in data.items() if v is not None}
    return data


def get_zipcode_from_epd(epd):
    zipcode = epd.get('manufacturer', {}).get('postal_code')
    if not zipcode:
        zipcode = epd.get('plant_or_group', {}).get('postal_code')
    return zipcode


def create_folder_path(state, zipcode, display_name):
    base_root = os.path.join("../../products-data")
    if state.startswith('US-'):
        return os.path.join(base_root, 'US', display_name)
    return os.path.join(base_root, state, display_name)


def fetch_openepd_data_for_epd(epd, authorization):
    if not ENABLE_OPENEPD_FETCH:
        return None
    epd_id = epd.get('id') or epd.get('material_id') or epd.get('open_xpd_uuid')
    if not epd_id:
        return None
    try:
        return fetch_from_openepd_by_id(epd_id, authorization)
    except Exception as e:
        logging.warning(f"Failed openEPD fetch for {epd_id}: {e}")
        return None


def save_json_to_yaml(state: str, json_data: list, authorization=None):
    filtered_data   = remove_null_values(json_data)
    openepd_fetched = 0
    openepd_merged  = 0

    for epd in filtered_data:
        if epd.get('name', '').startswith('#duplicate'):
            continue
        display_name = epd['category']['display_name'].replace(" ", "_")
        material_id  = epd['material_id']
        zipcode      = get_zipcode_from_epd(epd) or "unknown"
        folder_path  = create_folder_path(state, zipcode, display_name)
        os.makedirs(folder_path, exist_ok=True)

        merged_epd = epd
        if ENABLE_OPENEPD_FETCH and authorization and should_fetch_from_openepd(epd):
            openepd_epd = fetch_openepd_data_for_epd(epd, authorization)
            if openepd_epd:
                openepd_fetched += 1
                merged_epd = merge_impact_data(epd, openepd_epd)
                if (merged_epd.get('_data_sources', {}).get('merged_impacts') or
                        merged_epd.get('_data_sources', {}).get('merged_resources')):
                    openepd_merged += 1
                merged_epd.pop('_data_sources', None)

        with open(os.path.join(folder_path, f"{material_id}.yaml"), "w") as yf:
            yaml.dump(merged_epd, yf, default_flow_style=False)

    if ENABLE_OPENEPD_FETCH and openepd_fetched > 0:
        print(f"  openEPD: fetched {openepd_fetched}, merged {openepd_merged}", flush=True)


def map_response(epd: dict) -> dict:
    """
    Flatten one EPD to a CSV row.

    Lat/Lon (and address fields) fallback priority:
      1. plant_or_group  — the actual manufacturing plant (most specific)
      2. manufacturer    — the EPD owner's location (fallback when plant has no coords)
      3. None / blank
    """
    plant = epd.get('plant_or_group') or {}
    mfr   = epd.get('manufacturer')   or {}

    lat     = plant.get('latitude')      or mfr.get('latitude')      or None
    lon     = plant.get('longitude')     or mfr.get('longitude')     or None
    zipcode = plant.get('postal_code')   or mfr.get('postal_code')   or None
    county  = plant.get('admin_district2') or mfr.get('admin_district2') or None
    address = plant.get('address')       or mfr.get('address')       or None

    return {
        'Category_epd_name': epd['category']['openepd_name'],
        'Name':      epd['name'],
        'ID':        epd['open_xpd_uuid'],
        'Zip':       zipcode,
        'County':    county,
        'Address':   address,
        'Latitude':  lat,
        'Longitude': lon,
    }


def write_csv_others(title: str, epds: list):
    """Write non-cement EPDs to products-data/<CC>/<CC>.csv"""
    out_dir = os.path.join("../../products-data", title)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{title}.csv")
    with open(out_path, "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Name", "ID", "Zip", "County", "Address", "Latitude", "Longitude"])
        for epd in epds:
            writer.writerow([
                epd['Name'], epd['ID'], epd['Zip'], epd['County'],
                epd['Address'], epd['Latitude'], epd['Longitude']
            ])


def write_csv_cement(epds: list):
    os.makedirs("../../products-data", exist_ok=True)
    profile_cement_base = os.path.join("..", "..", "profile", "cement", "US")
    os.makedirs(profile_cement_base, exist_ok=True)

    if not epds:
        return

    state = epds[0].get('State') or epds[0].get('Plant_State') or 'unknown'
    state_pd_dir      = os.path.join("../../products-data", state)
    state_profile_dir = os.path.join(profile_cement_base, state)
    os.makedirs(state_pd_dir, exist_ok=True)
    os.makedirs(state_profile_dir, exist_ok=True)

    for csv_path in (
        os.path.join(state_pd_dir,      'Cement.csv'),
        os.path.join(state_profile_dir, 'Cement.csv'),
    ):
        write_header = not os.path.exists(csv_path)
        with open(csv_path, 'a') as csv_file:
            writer = csv.writer(csv_file)
            if write_header:
                writer.writerow(["Name", "ID", "Zip", "County", "Address", "Latitude", "Longitude"])
            for epd in epds:
                writer.writerow([
                    epd.get('Name',''), epd.get('ID',''), epd.get('Zip',''),
                    epd.get('County',''), epd.get('Address',''),
                    epd.get('Latitude',''), epd.get('Longitude','')
                ])

    for epd in epds:
        mat_id = epd.get('ID') or epd.get('material_id')
        if not mat_id:
            continue
        yaml_path = os.path.join(state_profile_dir, f"{mat_id}.yaml")
        if not os.path.exists(yaml_path):
            with open(yaml_path, 'w') as yf:
                yaml.dump(epd, yf, default_flow_style=False)


def write_epd_to_csv(epds: list, state: str):
    cement_list = []
    others_list = []
    for epd in epds:
        if epd is None:
            continue
        if 'cement' in epd['Category_epd_name'].lower():
            epd['State'] = state
            cement_list.append(epd)
        else:
            others_list.append(epd)
    write_csv_cement(cement_list)
    write_csv_others(state, others_list)


def write_products_csv(raw_epds: list, state: str):
    if state != 'IN' or not raw_epds:
        try:
            out_dir = os.path.join("../../products-data", 'IN')
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, 'products.csv')
            if not os.path.exists(out_path):
                with open(out_path, 'w') as f:
                    csv.DictWriter(f, fieldnames=[
                        'region1', 'region2', 'category_id', 'tariff_percent'
                    ]).writeheader()
        except Exception:
            pass
        return
    try:
        keyword_to_tariff = {
            'kitchen cabinet': 50, 'kitchen cabinets': 50,
            'bathroom vanity': 50, 'bathroom vanities': 50,
            'upholstered furniture': 30, 'furniture': 30,
            'tables': 30, 'wardrobes': 30,
        }
        products = []
        for epd in raw_epds:
            try:
                cat_info  = epd.get('category', {}) if isinstance(epd, dict) else {}
                text      = " ".join([
                    cat_info.get('display_name',''),
                    epd.get('name',''),
                    epd.get('description',''),
                ]).lower()
                tariff = next(
                    (rate for kw, rate in keyword_to_tariff.items() if kw in text),
                    None
                )
                if tariff is None:
                    continue
                products.append({
                    'region1': 'IN', 'region2': 'US',
                    'category_id': cat_info.get('id',''),
                    'tariff_percent': tariff,
                })
            except Exception:
                continue
        out_dir = os.path.join("../../products-data", 'IN')
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, 'products.csv'), 'w') as f:
            w = csv.DictWriter(f, fieldnames=['region1','region2','category_id','tariff_percent'])
            w.writeheader()
            w.writerows(products)
    except Exception:
        pass


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_arguments()

    selected_regions = []
    if args.europe:
        selected_regions.extend(european_countries)
    if args.country:
        codes = [c.strip().upper() for c in args.country.split(',')]
        if 'US' in codes:
            codes.remove('US')
            codes.extend(us_states)
        selected_regions.extend(codes)
    if not selected_regions:
        selected_regions = states

    selected_regions = list(dict.fromkeys(selected_regions))  # dedupe, preserve order

    authorization = get_auth()
    if authorization:
        total = len(selected_regions)
        print(f"Starting {total} regions…", flush=True)
        for idx, state in enumerate(selected_regions, 1):
            print(f"\n[{idx}/{total}] {state}", flush=True)
            results, authorization = fetch_epds(state, authorization)
            if results:
                save_json_to_yaml(state, results, authorization)
                write_products_csv(results, state)
                mapped = [map_response(epd) for epd in results]
                write_epd_to_csv(mapped, state)
                print(f"✓ {state}: {len(results)} EPDs saved", flush=True)
                combine_csvs_for_country(state if not state.startswith('US-') else 'US')
            else:
                print(f"⚠ {state}: no data", flush=True)
        print("\n✓ All regions processed!", flush=True)