import math
import string
from datetime import datetime
import pytz
from singer.utils import strftime


us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}


# CSV headers
# Initial: Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered
# 03-01-2020: Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered,Latitude,Longitude
# O3-23-2020: FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key
# Date formats: 1/22/2020 17:00, 2020-02-02T23:43:02
def transform_jh_csse_daily(record):
    # Header key variations for each field:
    province_state_keys = ['Province/State', 'Province_State']
    country_region_keys = ['Country/Region', 'Country_Region']
    last_update_keys = ['Last Update', 'Last_Update']
    confirmed_keys = ['Confirmed']
    deaths_keys = ['Deaths']
    recovered_keys = ['Recovered']
    latitude_keys = ['Latitude', 'Lat']
    longitude_keys = ['Longitude', 'Long_']
    active_keys = ['Active']
    combined_keys = ['Combined_Key']
    fips_keys = ['FIPS']
    admin_keys = ['Admin2']

    # Git file fields
    file_name = record.get('git_file_name')
    new_record = {}
    new_record['git_path'] = record.get('git_path')
    new_record['git_sha'] = record.get('git_sha')
    new_record['git_last_modified'] = record.get('git_last_modified')
    new_record['git_file_name'] = file_name
    new_record['row_number'] = record.get('row_number')

    # Date/Datetime from file_name
    timezone = pytz.timezone('UTC')
    file_date_str = file_name.lower().replace('.csv', '')
    file_dttm = timezone.localize(datetime.strptime(file_date_str, '%m-%d-%Y'))
    file_dttm_str = strftime(file_dttm)
    file_date_str = file_dttm_str[:10]
    new_record['date'] = file_date_str
    new_record['datetime'] = file_dttm_str

    # For US State code lookup
    abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))

    # Loop thru keys/values
    is_a_cruise = False
    county = None
    for key, val in list(record.items()):
        # Trim keys
        key = str(key).strip()
        if isinstance(val, str):
            val = val.strip()
        if val == '':
            val = None

        # Replace key/values and field transformations, cleansing
        if key in province_state_keys:
            if val is None or val == '':
                val = 'None'
            vals = []
            vals = val.split(',')
            val_len = len(vals)
            if val_len == 0:
                new_val = None
            elif val_len == 1:
                state = abbrev_us_state.get(val)
                if state:
                    new_val = state
                else:
                    new_val = val
            else:
                for value in vals:
                    # Trim new_val
                    new_val = str(value).strip()
                    if 'county' in new_val.lower():
                        county = new_val.replace('County', '').replace('county', '').strip()

                    # Lookup State code to get State Name
                    state = abbrev_us_state.get(new_val)
                    if state:
                        new_val = state

            if 'cruise' in new_val.lower() or 'princess' in new_val.lower() or 'from' \
                in new_val.lower():
                is_a_cruise = True

            if new_val is None or new_val == '':
                new_val = 'None'

            new_record['province_state'] = new_val
            new_record['county'] = county

        elif key in country_region_keys:
            # Remove punctuation
            new_val = val.translate(str.maketrans('', '', string.punctuation))

            if 'cruise' in new_val.lower() or 'princess' in new_val.lower() or 'from' \
                in new_val.lower():
                is_a_cruise = True

            # Replace country names
            if val == 'Korea South':
                new_val = 'South Korea'
            elif val == 'US':
                new_val = 'United States'

            new_record['country_region'] = new_val

        elif key in last_update_keys:
            new_val = val
            # Try format 1
            try:
                new_val = strftime(timezone.localize(datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')))
            except Exception as err:
                pass
            # Try format 2
            try:
                new_val = strftime(timezone.localize(datetime.strptime(val, '%m/%d/%Y %H:%M')))
            except Exception as err:
                pass
            new_record['last_update'] = new_val

        elif key in confirmed_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['confirmed'] = new_val

        elif key in deaths_keys:
            new_val = 0
            try:
                new_val = int(val)
            except Exception as err:
                pass
            new_record['deaths'] = new_val

        elif key in recovered_keys:
            new_val = 0
            try:
                new_val = int(val)
            except Exception as err:
                pass
            new_record['recovered'] = new_val

        elif key in latitude_keys:
            new_val = None
            try:
                new_val = round(float(val), 10)
            except Exception as err:
                pass
            if new_val == 0.0:
                new_val = None
            new_record['latitude'] = new_val

        elif key in longitude_keys:
            new_val = None
            try:
                new_val = round(float(val), 10)
            except Exception as err:
                pass
            if new_val == 0.0:
                new_val = None
            new_record['longitude'] = new_val

        elif key in active_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
            new_record['active'] = new_val

        elif key in combined_keys:
            new_record['combined_key'] = val

        elif key in fips_keys:
            new_record['fips'] = val

        elif key in admin_keys:
            new_record['admin_area'] = val

        # End keys loop

    new_record['is_a_cruise'] = is_a_cruise

    if new_record.get('province_state') is None:
        new_record['province_state'] = 'None'

    return new_record


def transform_record(stream_name, record):
    if stream_name == 'jh_csse_daily':
        new_record = transform_jh_csse_daily(record)
    # elif (other streams)
    else:
        new_record = record

    return new_record
