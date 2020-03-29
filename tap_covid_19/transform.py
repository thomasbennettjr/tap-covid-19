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

# JSCOTT added transformer for Italy Region
# Italy by Region, Daily
# NOTES on the transformation :
# 1. Column names are translated to english (see comments inline below)
# 2. We return a None record for file names that do NOT end with a date part (e.g. dpc-covid19-ita-regioni-latest.csv)
#    because these (we assume) have redundant info that we dont want to duplicate
def transform_italy_regions_daily(record):

    # Header key variations for each field
    # NOTE: We translate italian column names to english
    date_of_notification_keys = ['data']
    country_keys = ['stato']
    region_code_keys = ['codice_regione']
    region_keys = ['denominazione_regione']
    latitude_keys = ['lat']
    longitude_keys = ['long']
    hospitalized_with_symptoms_keys = ['ricoverati_con_sintomi']
    intensive_care_keys = ['terapia_intensiva']
    total_hospitalized_keys = ['totale_ospedalizzati']
    home_isolation_keys = ['isolamento_domiciliare']
    total_currently_positive_keys = ['totale_attualmente_positivi']
    new_currently_positive_keys = ['nuovi_attualmente_positivi']
    discharged_recovered_keys = ['dimessi_guariti']
    deaths_keys = ['deceduti']
    total_cases_keys = ['totale_casi']
    tests_performed_keys= ['tamponi']
    note_it_keys = ['note_it']
    note_en_keys = ['note_en']

    # TODO not used: Remove
    translations = [
        {'it': 'data',                          'en': 'notification_date'},
        {'it': 'stato',                         'en': 'country'},
        {'it': 'codice_regione',                'en': 'region_code'},
        {'it': 'denominazione_regione',         'en': 'region'},
        {'it': 'lat',                           'en': 'lat'},
        {'it': 'long',                          'en': 'long'},
        {'it': 'ricoverati_con_sintomi',        'en': 'hospitalized_with_symptoms'},
        {'it': 'terapia_intensiva',             'en': 'intensive_care'},
        {'it': 'totale_ospedalizzati',          'en': 'total_hospitalized'},
        {'it': 'isolamento_domiciliare',        'en': 'home_isolation'},
        {'it': 'totale_attualmente_positivi',   'en': 'total_currently_positive'},
        {'it': 'nuovi_attualmente_positivi',    'en': 'new_currently_positive'},
        {'it': 'dimessi_guariti',               'en': 'discharged_recovered'},
        {'it': 'deceduti',                      'en': 'deaths'},
        {'it': 'totale_casi',                   'en': 'total_cases'},
        {'it': 'tamponi',                       'en': 'tests_performed'},
        {'it': 'note_it',                       'en': 'note_it'},
        {'it': 'note_en',                       'en': 'note_en'}
    ],

    # Git file fields
    file_name = record.get('git_file_name')
    new_record = {}
    new_record['git_path'] = record.get('git_path')
    new_record['git_sha'] = record.get('git_sha')
    new_record['git_last_modified'] = record.get('git_last_modified')
    new_record['git_file_name'] = file_name
    new_record['row_number'] = record.get('row_number')

    # Date/Datetime from file_name ( e.g. dpc-covid19-ita-regioni-20200326.csv )
    timezone = pytz.timezone('UTC')
    file_name_part = file_name.lower().replace('.csv', '')
    file_date_str = file_name_part[-8:]
    try:
        file_dttm = timezone.localize(datetime.strptime(file_date_str, '%Y%m%d'))
    except Exception as err:
        # exit and skip this record because since we can't determine the date,
        # it is from a file we want to ignore
        return None
    file_dttm_str = strftime(file_dttm)
    file_date_str = file_dttm_str[:10]
    # TODO date and notification_date are redundant. pick one ?
    new_record['date'] = file_date_str
    new_record['datetime'] = file_dttm_str

    # For US State code lookup
    # abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))

    # Loop thru keys/values
    for key, val in list(record.items()):

        # Trim keys, nullify empty string
        key = str(key).strip()
        if isinstance(val, str):
            val = val.strip()
        if val == '':
            val = None

        # Replace key/values and field transformations, cleansing:

        # date_of_notification (data) e.g. 2020-03-26T17:00:00
        # TODO confirm datetime values are UTC (or are they Italy local time ?)
        if key in date_of_notification_keys:
            # TODO catch and ignore any error parsing the date
            new_val = strftime(timezone.localize(datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')))
            new_record['date_of_notification'] = new_val

        # country (stato), should always be 'ITA'
        elif key in country_keys:
            new_record['country'] = val

        # region_code (codice_regione) is a number
        elif key in region_code_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['region_code'] = new_val

        # region (denominazione_regione), e.g. 'Lombardia
        elif key in region_keys:
            new_record['region'] = val

        # latitude (lat) is a float
        elif key in latitude_keys:
            new_val = None
            try:
                new_val = round(float(val), 10)
            except Exception as err:
                pass
            if new_val == 0.0:
                new_val = None
            new_record['latitude'] = new_val

        # longitude (long) is a float
        elif key in longitude_keys:
            new_val = None
            try:
                new_val = round(float(val), 10)
            except Exception as err:
                pass
            if new_val == 0.0:
                new_val = None
            new_record['longitude'] = new_val

        # hospitalized_with_symptoms (ricoverati_con_sintomi) is an integer
        elif key in hospitalized_with_symptoms_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['hospitalized_with_symptoms_keys'] = new_val

        # intensive_care (terapia_intensiva) is an integer
        elif key in intensive_care_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['intensive_care'] = new_val

        # total_hospitalized (totale_ospedalizzati) is an integer
        elif key in total_hospitalized_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['total_hospitalized'] = new_val

        # home_isolation (totale_ospedalizzati) is an integer
        elif key in home_isolation_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['home_isolation'] = new_val

        # total_currently_positive (totale_attualmente_positivi) is an integer
        elif key in total_currently_positive_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['total_currently_positive'] = new_val

        # new_currently_positive (nuovi_attualmente_positivi) is an integer
        elif key in new_currently_positive_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['new_currently_positive'] = new_val

        # discharged_recovered (dimessi_guariti) is an integer
        elif key in discharged_recovered_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['discharged_recovered'] = new_val

        # deaths (deceduti) is an integer
        elif key in deaths_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['deaths'] = new_val

        # total_cases (totale_casi) is an integer
        elif key in total_cases_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['total_cases'] = new_val

        # tests_performed (tamponi) is an integer
        elif key in tests_performed_keys:
            try:
                new_val = int(val)
            except Exception as err:
                new_val = 0
                pass
            new_record['tests_performed'] = new_val

        # notes in italian
        elif key in note_it_keys:
            new_record['note_it'] = val

        # notes in english
        elif key in note_en_keys:
            new_record['note_en'] = val

        # End keys loop

    return new_record


def transform_record(stream_name, record):
    if stream_name == 'jh_csse_daily':
        new_record = transform_jh_csse_daily(record)
    elif stream_name == 'italy_daily_region':
        new_record = transform_italy_regions_daily(record)
    # elif (other streams)
    else:
        new_record = record

    return new_record
