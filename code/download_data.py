
import requests
import pandas as pd
from datetime import timedelta
import numpy as np


def ba_timezone(ba, format):
    """
    Retrieves the UTC Offset (for standard time) for each balancing area.
    """

    
    offset_dict = {'AEC': 6,
                'AECI': 6,
                'AVA': 8,
                'AVRN': 8,
                'AZPS': 7,
                'BANC': 8,
                'BPAT': 8,
                'BPA': 8,
                'CHPD': 8,
                'CISO': 8,
                'CAISO': 8,
                'CPLE': 5,
                'CPLW': 5,
                'DEAA': 7,
                'DOPD': 8,
                'DUK': 5,
                'EEI': 6,
                'EPE': 7,
                'ERCO': 6,
                'FMPP': 5,
                'FPC': 5,
                'FPL': 5,
                'GCPD': 8,
                'GRID': 8,
                'GRIF': 7,
                'GVL': 5,
                'GWA': 7,
                'HGMA': 7,
                'HST': 5,
                'IID': 8,
                'IPCO': 8,
                'ISNE': 5,
                'ISONE': 5,
                'JEA': 5,
                'LDWP': 8,
                'LGEE': 5,
                'MISO': 5,
                'NEVP': 8,
                'NSB': 5,
                'NWMT': 7,
                'NYIS': 5,
                'NYISO': 5,
                'PACE': 7,
                'PACW': 8,
                'PGE': 8,
                'PJM': 5,
                'PNM': 7,
                'PSCO': 7,
                'PSEI': 8,
                'SC': 5,
                'SCEG': 5,
                'SCL': 8,
                'SEC': 5,
                'SEPA': 6,
                'SOCO': 6,
                'SPA': 6,
                'SRP': 7,
                'SWPP': 6,
                'SPP': 6,
                'TAL': 5,
                'TEC': 5,
                'TEPC': 7,
                'TIDC': 8,
                'TPWR': 8,
                'TVA': 6,
                'WACM': 7,
                'WALC': 7,
                'WAUW': 7,
                'WWA': 7,
                'YAD': 5,
                'EIA.CISO': 8,
                'EIA.ISNE': 5,
                'EIA.PJM': 5,
                'EIA.NYIS': 5,
                'EIA.SWPP': 6,
                'EIA.MISO': 5,
                'EIA.BPAT': 8,}

    
    offset = offset_dict[ba]

    if format == 'ISO':
        timezone = f'-0{offset}:00'
    elif format == 'GMT':
        timezone = f'Etc/GMT+{offset}'

    return timezone


def check_singularity_region_exists(api_key, ba):

    # define parameters for API call
    event_type = 'carbon_intensity'
    header = {'X-Api-Key': api_key}


def download_singularity_data(api_key, ba, start_date, end_date):
    """

    """

    # set the start and end datetimes
    utc_offset = ba_timezone(ba, format='ISO')
    start_datetime = pd.to_datetime(f'{start_date}T00:00:00{utc_offset}')
    end_datetime = pd.to_datetime(f'{end_date}T23:55:00{utc_offset}')
    current_datetime = start_datetime

    # start a session object
    session = requests.Session()
    session.headers.update({'X-Api-Key': api_key})

    # define parameters for API call
    event_type = 'carbon_intensity'
    #header = {'X-Api-Key': api_key}

    # create an empty dataframe to which we will append each api response for the ba
    df_ba = pd.DataFrame()

    while current_datetime <= end_datetime:
        # format the timestamp for the API
        start = str(current_datetime).replace(' ', 'T').replace('+', '%2B')

        # the API only accepts 7 days at a time, so if the end datetime is more than a week from the current date, replace it
        if (current_datetime + timedelta(days=6, hours=23, minutes=55)) <= end_datetime:
            end = (str(current_datetime + timedelta(days=6, hours=23,
                                                    minutes=55)).replace(' ', 'T').replace('+', '%2B'))
        else:
            end = (str(end_datetime).replace(' ', 'T').replace('+', '%2B'))
        print(f'{ba}:{start}')

        # call API
        output = session.get(
            f'https://api.singularity.energy/v1/region_events/search?region={ba}&start={start}&end={end}&event_type={event_type}&per_page=1000&page=1')

        # check to see if there are multiple pages of response
        current_page = output.json()['meta']['pagination']['this']
        last_page = output.json()['meta']['pagination']['last']

        # if there are multiple pages, loop through them until finished
        while current_page <= last_page:

            # convert the json to a dataframe
            df = pd.json_normalize(output.json(), 'data')

            if df.empty:
                pass
            else:

                # only keep observations where the dedup key ends in +00:00, otherwise there will be some duplicate entries
                df = df[df['dedup_key'].str.endswith('+00:00')]

                # only keep columns that have the rate data or source metadata
                columns_to_keep = ['start_date',
                                'data.generated_rate', 'data.consumed_rate',
                                'meta.consumed_emissions_source', 'meta.generated_emissions_source']
                df = df.loc[:, df.columns.isin(columns_to_keep)]

                # create a new emission source column that combines each of the three emissions source columns into one
                source_columns = ['meta.consumed_emissions_source',
                                'meta.generated_emissions_source']
                try:
                    df['source'] = df.loc[:, df.columns.isin(source_columns)].stack().groupby(
                        level=0).apply(lambda x: x.unique().tolist()[0])
                except IndexError:
                    # if the source data column is missing, create one
                    df['source'] = np.NaN

                # if any of the source data is misisng, assume that it is EGRID_u2018, per email with Jeff Burka of Singularity Energy
                df['source'] = df['source'].fillna('EGRID_u2018')

                # drop the original emissions source columns
                df = df.drop(columns=['meta.consumed_emissions_source',
                                    'meta.generated_emissions_source'], errors='ignore')

                # rename the columns
                df = df.rename(columns={'start_date': 'datetime_local', 'data.generated_rate': 'production_ef',
                                        'data.consumed_rate': 'consumption_ef'})

                # pivot the data to get unique columns for each emissions source
                df = df.set_index(['datetime_local', 'source'])
                if df.index.duplicated().any():
                    df['missing'] = df.isna().sum(axis=1)
                    df = df.sort_values(by=['datetime_local', 'source', 'missing'])
                    df = df[~df.index.duplicated(keep='first')]
                    df = df.drop(columns=['missing'])
                df = df.unstack(level=-1)
                df.columns = ['_'.join(col).strip() for col in df.columns.values]

                df_ba = pd.concat([df_ba, df])

            if current_page < last_page:
                # get the next page of data
                output = session.get(
                    f'https://api.singularity.energy/v1/region_events/search?region={ba}&start={start}&end={end}&event_type={event_type}&per_page=1000&page={current_page+1}')

                current_page = output.json()['meta']['pagination']['this']
            elif current_page == last_page:
                break

        current_datetime = (current_datetime + timedelta(days=7))

    # set the datetime index
    df_ba.index = pd.to_datetime(df_ba.index)

    # if there are duplicate datetimes, group by datetime and only keep non-NA values
    df_ba = df_ba.groupby('datetime_local').agg(
        lambda x: np.nan if x.isnull().all() else x.dropna())

    # convert values from lb/MWh to kg/kWh
    df_ba = df_ba * 0.453592 / 1000

    # convert datetime to local
    df_ba.index = df_ba.index.tz_convert(ba_timezone(ba, format='GMT'))

    # resample the data to 1 hour frequency
    df_ba = df_ba.resample('H').mean()

    return df_ba

def download_doe_building_data():
    pass