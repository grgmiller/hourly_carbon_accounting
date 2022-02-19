
import pandas as pd
import numpy as np
import os
import re


def load_hourly_efs(ba_list, ef_year, ef_type):
    for ba in ba_list:
        # if this is the first item in the list
        if ba_list.index(ba) == 0:
            ef = pd.read_csv(f'../data/processed/singularity_efs/{ba}.csv', usecols=[
                             'datetime_local', ef_type], index_col='datetime_local', parse_dates=True).rename(columns={ef_type: ba})
            # filter the dates to the year of interest
            if ef_year is not None:
                ef = ef[ef.index.year == ef_year]

            # change the datetimeindex to a tz-naive timestamp
            ef.index = ef.index.tz_localize(None)
        else:
            try:
                ef_1 = pd.read_csv(f'../data/processed/singularity_efs/{ba}.csv', usecols=[
                               'datetime_local', ef_type], index_col='datetime_local', parse_dates=True).rename(columns={ef_type: ba})
                # filter the dates to the year of interest
                if ef_year is not None:
                    ef_1 = ef_1[ef_1.index.year == ef_year]

                # change the datetimeindex to a tz-naive timestamp
                ef_1.index = ef_1.index.tz_localize(None)

                # merge into the main dataframe
                ef = ef.merge(ef_1, how='left', left_index=True, right_index=True)
            except ValueError:
                print(f'Missing data for {ba}')
            
    return ef


def load_doe_demand_data(tmy3, ef_year):

    # specify the category for each building type
    building_categories = {'FullServiceRestaurant': 'Restaurant',
                           'Hospital': 'Hospital',
                           'LargeHotel': 'Hotel',
                           'LargeOffice': 'Office',
                           'MediumOffice': 'Office',
                           'MidriseApartment': 'Residential',
                           'OutPatient': 'Office',
                           'PrimarySchool': 'School',
                           'QuickServiceRestaurant': 'Restaurant',
                           'SecondarySchool': 'School',
                           'SmallHotel': 'Hotel',
                           'SmallOffice': 'Office',
                           'Stand-aloneRetail': 'Retail',
                           'StripMall': 'Retail',
                           'SuperMarket': 'Retail',
                           'Warehouse': 'Warehouse'}

    # get a list of all unique USAF codes
    usaf_list = list(tmy3['tmy3_usaf_code'].unique())

    # create a blank dataframe to hold the demand data
    demand = pd.DataFrame(
        columns=['datetime_local', 'location', 'building_type', 'demand_kw'])

    for usaf in usaf_list:

        print(f'loading data for station {usaf}')

        # get the name of the climate zone
        cz = tmy3[tmy3['tmy3_usaf_code'] == usaf]['climate_zone'].unique()[0]

        # get a list of all BAs that use this tmy3 location
        bas_for_tmy3 = list(tmy3[tmy3['tmy3_usaf_code'] == usaf]['ba_code'].unique())

        # LOAD COMMERCIAL DATA
        com_dir = 'A:/Research/COMMERCIAL_LOAD_DATA_E_PLUS_OUTPUT'

        # get the name of the folder in the commercial building directory that matches the usaf code
        folder_name = [f for f in os.listdir(com_dir) if int(
            f.split('_')[2].split('.')[-1]) == usaf][0]
        # for each building file
        for filename in os.listdir(f'{com_dir}/{folder_name}'):
            # get the name of the building from the filename string
            building_name = re.search('RefBldg(.*)New2004', filename).group(1)

            data = pd.read_csv(f'{com_dir}/{folder_name}/{filename}',
                               usecols=['Date/Time', 'Electricity:Facility [kW](Hourly)'])

            # extract date data
            date = data['Date/Time'].str.extract(
                r'(\d*)\/(\d*) *(\d*)', expand=False)
            # add a year to the date and shift back an hour to the start of each interval
            data['datetime_local'] = date[0] + '/' + date[1] + \
                f'/{ef_year} ' + (date[2].astype(int) - 1).astype(str) + ':00:00'
            # convert to datetime
            data['datetime_local'] = pd.to_datetime(data['datetime_local'])

            data = data.drop(columns=['Date/Time'])

            # rename the columns
            data = data.rename(
                columns={'Electricity:Facility [kW](Hourly)': 'demand_kw'})

            data['building_type'] = f'{building_categories[building_name]}_{building_name}_{cz}'

            # need to shift the data to align the day of week with the correct date in the current year
            # The original data was from a year where the first day of they year was a Sunday
            # Jan 1, 2019 is a Tuesday, so we need to roll the original data back by 48 hours
            dow_start = pd.to_datetime(f'01/01/{ef_year}').dayofweek
            if dow_start < 3:
                roll_days = - (dow_start + 1)
            else:
                roll_days = 6 - dow_start
            data['demand_kw'] = np.roll(data['demand_kw'], (roll_days * 24))

            # assign a location to the data

            for ba in bas_for_tmy3:

                data['location'] = ba

                demand = demand.append(data, ignore_index=True)

        # LOAD RESIDENTIAL DATA
        res_dir = 'A:/Research/RESIDENTIAL_LOAD_DATA_E_PLUS_OUTPUT'

        # there are LOW, BASE, and HIGH load models
        load_models = ['LOW','BASE','HIGH']

        # we want to rename these as Small Medium and Large
        model_name = {'LOW':'Small','BASE':'Medium','HIGH':'Large'}

        for model in load_models:

            # get the name of the file that matches the usaf code
            filename = [f for f in os.listdir(f'{res_dir}/{model}/') if int(
                f.split('_')[2].split('.')[-1]) == usaf][0]

            try:
                data = pd.read_csv(f'{res_dir}/{model}/{filename}', usecols=['Date/Time', 'Electricity:Facility [kW](Hourly)'])
            # sometimes there is a typo in the header that reads [J] instead of [kW]
            except ValueError:
                data = pd.read_csv(f'{res_dir}/{model}/{filename}', usecols=['Date/Time', 'Electricity:Facility [J](Hourly)'])
                data = data.rename(columns={'Electricity:Facility [J](Hourly)':'Electricity:Facility [kW](Hourly)'})

            # extract date data
            date = data['Date/Time'].str.extract(
                r'(\d*)\/(\d*) *(\d*)', expand=False)
            # add a year to the date and shift back an hour to the start of each interval
            data['datetime_local'] = date[0] + '/' + date[1] + \
                f'/{ef_year} ' + (date[2].astype(int) - 1).astype(str) + ':00:00'
            # convert to datetime
            data['datetime_local'] = pd.to_datetime(data['datetime_local'])

            data = data.drop(columns=['Date/Time'])

            # rename the columns
            data = data.rename(
                columns={'Electricity:Facility [kW](Hourly)': 'demand_kw'})
            data['building_type'] = f'Residential_{model_name[model]}SingleFamily_{cz}'

            # need to shift the data to align the day of week with the correct date in the current year
            # The original data was from a year where the first day of they year was a Sunday
            # Jan 1, 2019 is a Tuesday, so we need to roll the original data back by 48 hours
            dow_start = pd.to_datetime(f'01/01/{ef_year}').dayofweek
            if dow_start < 3:
                roll_days = - (dow_start + 1)
            else:
                roll_days = 6 - dow_start
            data['demand_kw'] = np.roll(data['demand_kw'], (roll_days * 24))

            # for each BA for which this TMY3 location is used, add a new entry to the dataframe
            for ba in bas_for_tmy3:

                data['location'] = ba

                demand = demand.append(data, ignore_index=True)

    demand = demand.pivot(index='datetime_local', columns=['location', 'building_type'], values='demand_kw')

    return demand


def load_lbnl_demand_data(ba_list):
    interesting_files = ['pge-ind-PGSA-food_bev-gt200kW-nonCare-0.0_0.33',
                         'pge-ind-PGSA-food_bev-gt200kW-nonCare-0.66_1.0',
                         'sce-ind-SCEW-food_bev-gt200kW-nonCare-0.1_0.2',
                         'pge-ind-PGSB-data_center-gt200kW-nonCare-0.0_1.0',
                         'sdge-ind-SDG1-metals-50_200kW-nonCare-0.0_0.25',
                         'pge-com-PGF1-ref_wh-lt50kW-nonCare-0.0_1.0',
                         'sce-com-PGLP-com_other-noKW-nonCare-0.0_1.0',
                         'sce-ind-SCEW-water-gt200kW-nonCare-0.1_0.2',
                         'sdge-ind-SDG1-crop-lt50kW-nonCare-0.5_0.6',
                         'sce-ind-SCEN-chemical-50_200kW-nonCare-0.0_1.0',
                         'sce-ind-SCEC-chemical-50_200kW-nonCare-0.0_1.0'
                         ]

    interesting_files = [
        'pge-ind-PGSB-data_center-gt200kW-nonCare-0.0_1.0',
        'sce-ind-SCEC-chemical-50_200kW-nonCare-0.0_1.0',
        'pge-ind-PGSA-food_bev-gt200kW-nonCare-0.0_0.33',
        'sce-ind-SCEW-data_center-50_200kW-nonCare-0.0_1.0',
        'sce-ind-SCEN-chemical-50_200kW-nonCare-0.0_1.0',
    ]

    building_names = {'pge-ind-PGSA-food_bev-gt200kW-nonCare-0.0_0.33': 'ag_tree_nut_processor',
                      'pge-ind-PGSA-food_bev-gt200kW-nonCare-0.66_1.0': 'ag_tomato_processor',
                      'sce-ind-SCEW-food_bev-gt200kW-nonCare-0.1_0.2': 'ag_prune_processor',
                      'pge-ind-PGSB-data_center-gt200kW-nonCare-0.0_1.0': 'large_data_center',
                      'sce-ind-SCEW-data_center-50_200kW-nonCare-0.0_1.0': 'medium_data_center',
                      'sdge-ind-SDG1-metals-50_200kW-nonCare-0.0_0.25': 'metals_day_shift',
                      'pge-com-PGF1-ref_wh-lt50kW-nonCare-0.0_1.0': 'warehouse_overnight',
                      'sce-com-PGLP-com_other-noKW-nonCare-0.0_1.0': 'warehouse_midday',
                      'sce-ind-SCEW-water-gt200kW-nonCare-0.1_0.2': 'water_constant_load',
                      'sdge-ind-SDG1-crop-lt50kW-nonCare-0.5_0.6': 'crop_overnight_pumping',
                      'sce-ind-SCEN-chemical-50_200kW-nonCare-0.0_1.0': 'chem_night_shift',
                      'sce-ind-SCEC-chemical-50_200kW-nonCare-0.0_1.0': 'chem_day_shift'
                      }

    # we will use 1-in-2 profiles, representing a typical weather year, rather than the 1-in-10 profiles, which represent a "hot" year
    lbnl_dir = 'A:/Research/lbnl-load-enduse-shapes/lbnl-load-enduse-shapes/anonymized_1in2_actual_actual_2014/'

    # create a blank dataframe
    lbnl_demand = pd.DataFrame()

    lbnl_data = pd.DataFrame()

    for filename in interesting_files:
        # let's take a look a single building type with different kwh bins
        df = pd.read_csv(lbnl_dir + f'{filename}.csv', usecols=['total']).rename(
            columns={'total': building_names[filename]})

        # need to shift the data to align the day of week with the correct date in the current year
        # The original data was from 2014, where the first day of the year is a Wednesday
        # Jan 1, 2019 is a Tuesday, so we need to roll the original data forward by 24 hours
        df[building_names[filename]] = np.roll(
            df[building_names[filename]], 24)

        lbnl_data = lbnl_data.join(df, how='right')

    for ba in ba_list:

        ba_data = lbnl_data.copy()
        ba_data['location'] = ba
        # add an index
        ba_data['datetime_local'] = pd.date_range(
            start='2019-01-01 00:00:00', end='2019-12-31 23:00:00', freq='H')

        lbnl_demand = lbnl_demand.append(ba_data, ignore_index=True)

    # first melt the data
    lbnl_demand = lbnl_demand.melt(id_vars=[
                                   'location', 'datetime_local'], var_name='building_type', value_name='lbnl_demand_kw')

    # then re-pivot the data
    lbnl_demand = lbnl_demand.pivot(index='datetime_local', columns=[
                                    'location', 'building_type'], values='lbnl_demand_kw')

    return lbnl_demand

def load_nrel_eulp_data():
    """
    """

    # get a list of all unique USAF codes
    usaf_list = list(tmy3['tmy3_usaf_code'].unique())
    # test with one code
    usaf_list = [722235]

    for usaf in usaf_list:
        # check if the file already exists
        if path.exists(f'../data/processed/nrel_demand/{usaf}.csv.zip'):
            pass
        else:
            # get the name of the climate zone
            cz = tmy3[tmy3['tmy3_usaf_code'] == usaf]['climate_zone'].unique()[0]

            # get a list of all BAs that use this tmy3 location
            bas_for_tmy3 = list(tmy3[tmy3['tmy3_usaf_code'] == usaf]['ba_code'].unique())

            # COMMERCIAL DATA
            #################

            # find all buildings that contain the usaf code
            bldgs_in_location = com_metadata.copy()[com_metadata['in.weather_file_2018'].str.contains(str(usaf), na=False)]

            # get the state code
            # double check that all buildings are in the same state
            state_list = list(bldgs_in_location['in.state_abbreviation'].unique())
            if len(state_list) > 1:
                print(f'Error: this location is in multiple states: {state_list}')
            else:
                state = state_list[0]

            # get a list of all building ids in this location
            com_bldg_id_dict = dict(zip(list(bldgs_in_location.index), list(bldgs_in_location['in.building_type'])))

            # create a variable that tracks if the dataframe that will contain all the buildind timeseries has been created yet
            dataframe_exists = 0

            for bldg_id, bldg_type in com_bldg_id_dict.items():

                # construct the url for the individual building timeseries file
                url_to_download = f'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2021/comstock_tmy3_release_1/timeseries_individual_buildings/by_state/upgrade=0/state={state}/{bldg_id}-0.parquet'

                # read the building data into a dataframe, keeping only the total electricity timeseries data
                bldg_data = pd.read_parquet(url_to_download, columns=['timestamp','out.electricity.total.energy_consumption'])

                # resample the data from 15 min interval to 1 hour interval, and round to 2 decimal points
                bldg_data = bldg_data.set_index(pd.to_datetime(bldg_data['timestamp']), drop=True).resample('H', label='left', closed='right').sum().round(2)

                # rename the column to describe the building
                bldg_data = bldg_data.rename(columns={'out.electricity.total.energy_consumption':f'{cz}_{building_categories[bldg_type]}_{bldg_type}_{bldg_id}'})

                # for each balancing area that this location corresponds to
                for ba in bas_for_tmy3:

                    # create a new dataframe if it hasn't been done already
                    if dataframe_exists == 0:
                        # create a two-level column index from the BA name and the building name
                        data_to_add = bldg_data.copy()
                        data_to_add.columns = pd.MultiIndex.from_product([[ba],data_to_add.columns], names=['ba','bldg_name'])
                        demand_data = data_to_add.copy()
                        dataframe_exists +=1
                    else:
                        # create a two-level column index from the BA name and the building name
                        data_to_add = bldg_data.copy()
                        data_to_add.columns = pd.MultiIndex.from_product([[ba],data_to_add.columns], names=['ba','bldg_name'])
                        # concat this data to the existing data
                        demand_data = pd.concat([demand_data, data_to_add], axis='columns')

            # RESIDENTIAL DATA

            # find all buildings that contain the usaf code
            bldgs_in_location = res_metadata.copy()[res_metadata['in.weather_file_2018'].str.contains(str(usaf), na=False)]

            # get the state code
            # double check that all buildings are in the same state
            state_list = list(bldgs_in_location['in.state'].unique())
            if len(state_list) > 1:
                print(f'Error: this location is in multiple states: {state_list}')
            else:
                state = state_list[0]

            # get a list of all building ids in this location
            res_bldg_id_dict = dict(zip(list(bldgs_in_location.index), list(bldgs_in_location['in.geometry_building_type_acs'])))


            for bldg_id, bldg_type in res_bldg_id_dict.items():

                # construct the url for the individual building timeseries file
                url_to_download = f'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2021/resstock_tmy3_release_1/timeseries_individual_buildings/by_state/upgrade=0/state={state}/{bldg_id}-0.parquet'

                # read the building data into a dataframe, keeping only the total electricity timeseries data
                bldg_data = pd.read_parquet(url_to_download, columns=['timestamp','out.electricity.total.energy_consumption'])

                # resample the data from 15 min interval to 1 hour interval, and round to 2 decimal points
                bldg_data = bldg_data.set_index(pd.to_datetime(bldg_data['timestamp']), drop=True).resample('H', label='left', closed='right').sum().round(2)

                # scale the data
                bldg_data['out.electricity.total.energy_consumption'] = bldg_data['out.electricity.total.energy_consumption'] * res_metadata.loc[bldg_id,'scaling_factor']

                # rename the column to describe the building
                bldg_data = bldg_data.rename(columns={'out.electricity.total.energy_consumption':f'{cz}_Residential_{bldg_type}_{bldg_id}'})

                # for each balancing area that this location corresponds to
                for ba in bas_for_tmy3:

                    # create a new dataframe if it hasn't been done already
                    if dataframe_exists == 0:
                        # create a two-level column index from the BA name and the building name
                        data_to_add = bldg_data.copy()
                        data_to_add.columns = pd.MultiIndex.from_product([[ba],data_to_add.columns], names=['ba','bldg_name'])
                        demand_data = data_to_add.copy()
                        dataframe_exists +=1
                    else:
                        # create a two-level column index from the BA name and the building name
                        data_to_add = bldg_data.copy()
                        data_to_add.columns = pd.MultiIndex.from_product([[ba],data_to_add.columns], names=['ba','bldg_name'])
                        # concat this data to the existing data
                        demand_data = pd.concat([demand_data, data_to_add], axis='columns')

            # rename the timestamp column to datetime_local
            demand_data.index = demand_data.index.rename('datetime_local')
            
            # save the data as a zipped csv
            demand_data.to_csv(f'../data/processed/nrel_demand/{usaf}.csv.zip', compression='zip')

            # reset the dataframe
            demand_data = pd.DataFrame()
