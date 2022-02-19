"""
Used for screening anomolies in demand data

Copied and modified from https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code/blob/master/step2_anomaly_screening.ipynb
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

def add_rolling_dem(df, short_hour_window, value_column):
    df["rollingDem"] = df[value_column].rolling(
        short_hour_window * 2, min_periods=1, center=True
    ).median()
    return df


def add_rolling_dem_long(df, nDays, value_column):
    df["rollingDemLong"] = df[value_column].rolling(
        nDays * 24 * 2, min_periods=1, center=True
    ).median()
    return df


def add_demand_minus_rolling_dem(df, value_column):
    diff = df[value_column] - df['rollingDem']
    df = df.assign(dem_minus_rolling=diff)
    return df


def add_demand_rel_diff_wrt_hourly(df, value_column):
    diff = df[value_column] / (df['rollingDem'] * df['hourly_median_dem_dev'])
    df = df.assign(dem_rel_diff_wrt_hourly=diff)
    diff2 = df[value_column] / (df['rollingDemLong'] * df['hourly_median_dem_dev'])
    df = df.assign(dem_rel_diff_wrt_hourly_long=diff2)
    return df


def add_delta_demand_rel_diff_wrt_hourly(df):
    diff3 = df['dem_rel_diff_wrt_hourly'].diff()
    df = df.assign(dem_rel_diff_wrt_hourly_delta_pre=diff3)
    diff4 = df['dem_rel_diff_wrt_hourly'].diff(periods=-1)
    df = df.assign(dem_rel_diff_wrt_hourly_delta_post=diff4)
    return df


# This is a global value so does not need to be added to the df
def calculate_relative_demand_difference_IQR(df):
    iqr_relative_deltas = np.nanpercentile(df['dem_rel_diff_wrt_hourly_delta_pre'], 75) - \
            np.nanpercentile(df['dem_rel_diff_wrt_hourly_delta_pre'], 25)
    return iqr_relative_deltas

    
def add_demand_minus_rolling_dem_iqr(df, iqr_hours):
    rolling_window = df["dem_minus_rolling"].rolling(iqr_hours * 2, min_periods=1, center=True)
    df["dem_minus_rolling_IQR"] = rolling_window.quantile(0.75) - rolling_window.quantile(0.25)
    return df


def add_hourly_median_dem_deviations(df, nDays):
    # Create a df to hold all values to take nanmedian later
    vals_dem_minus_rolling = df['dem_minus_rolling']
    # Loop over nDays days on each side
    for i in range(-nDays, nDays+1):
        # Already initialized with zero value
        if i == 0:
            continue
        vals_dem_minus_rolling = pd.concat(
            [vals_dem_minus_rolling, df.shift(periods=i*24)['dem_minus_rolling']], axis=1)

    df['vals_dem_minus_rolling'] = vals_dem_minus_rolling.median(axis=1, skipna=True)
    # 1+vals to make it a scale factor
    return df.assign(hourly_median_dem_dev=1.+df['vals_dem_minus_rolling']/df['rollingDemLong'])


def add_deltas(df, value_column):
    diff = df[value_column].diff()
    df = df.assign(delta_pre=diff)
    diff = df[value_column].diff(periods=-1)
    df = df.assign(delta_post=diff)
    return df


def add_rolling_delta_iqr(df, iqr_hours):
    rolling_window = df["delta_pre"].rolling(iqr_hours * 2, min_periods=1, center=True)
    df["delta_rolling_IQR"] = rolling_window.quantile(0.75) - rolling_window.quantile(0.25)
    return df


def add_categories(df, value_column):
    df['category'] = np.where(df[value_column].isna(), 'MISSING', 'OKAY')
    return df


def filter_neg_and_zeros(df, value_column):
    df['category'] = np.where(df[value_column] <= 0., 'NEG_OR_ZERO', df['category'])
    filtered = np.where(df[value_column] <= 0., df[value_column], np.nan)
    df['negAndZeroFiltered'] = filtered
    df[value_column] = df[value_column].mask(df[value_column] <= 0.)
    return df

    
def filter_extrem_demand(df, multiplier, value_column):
    med = np.nanmedian(df[value_column])
    filtered = df[value_column].where(df[value_column] < med * multiplier)
    df['globalDemandFiltered'] = np.where(df[value_column] != filtered, df[value_column], np.nan)
    df['category'] = df['category'].mask(((df[value_column] != filtered) & \
                    (df[value_column].notna())), other='GLOBAL_DEM')
    df[value_column] = filtered
    return df


def filter_global_plus_minus_one(df, value_column):
    globalDemPlusMinusFiltered = [np.nan for _ in df.index]
    for idx in df.index:
        if df.loc[idx, 'category'] == 'GLOBAL_DEM':
            if df.loc[idx-1, 'category'] == 'OKAY':
                df.loc[idx-1, 'category'] = 'GLOBAL_DEM_PLUS_MINUS'
                globalDemPlusMinusFiltered[idx-1] = df.loc[idx-1, value_column]
                df.loc[idx-1, value_column] = np.nan
            if df.loc[idx+1, 'category'] == 'OKAY':
                df.loc[idx+1, 'category'] = 'GLOBAL_DEM_PLUS_MINUS'
                globalDemPlusMinusFiltered[idx+1] = df.loc[idx+1, value_column]
                df.loc[idx+1, value_column] = np.nan
    df['globalDemPlusMinusFiltered'] = globalDemPlusMinusFiltered
    return df
    

def filter_local_demand(df, multiplier_up, multiplier_down, value_column):
    
    # Filter in two steps to provide different labels for the categories
    filtered = df[value_column].where(
            (df[value_column] < df['rollingDem'] * df['hourly_median_dem_dev'] + \
                     multiplier_up * df['dem_minus_rolling_IQR']))
    df['localDemandFilteredUp'] = np.where(df[value_column] != filtered, df[value_column], np.nan)
    df['category'] = df['category'].mask(((df[value_column] != filtered) & \
                    (df[value_column].notna())), other='LOCAL_DEM_UP')
    df[value_column] = filtered
    
    filtered = df[value_column].where(
            (df[value_column] > df['rollingDem'] * df['hourly_median_dem_dev'] - \
                     multiplier_down * df['dem_minus_rolling_IQR']))
    df['localDemandFilteredDown'] = np.where(df[value_column] != filtered, df[value_column], np.nan)
    df['category'] = df['category'].mask(((df[value_column] != filtered) & \
                    (df[value_column].notna())), other='LOCAL_DEM_DOWN')
    df[value_column] = filtered
    
    return df


# Filter on a multiplier of the IQR and set
# the associated value_column value to NAN.
# Only consider "double deltas", hours with
# large deltas on both sides
def filter_deltas(df, multiplier, value_column):
    
    filtered = df[value_column].mask(
            ((df['delta_pre'] > df['delta_rolling_IQR'] * multiplier) & \
            (df['delta_post'] > df['delta_rolling_IQR'] * multiplier)) | \
            ((df['delta_pre'] < -1. * df['delta_rolling_IQR'] * multiplier) & \
            (df['delta_post'] < -1. * df['delta_rolling_IQR'] * multiplier)))

    df['deltaFiltered'] = np.where(df[value_column] != filtered, df[value_column], np.nan)
    df['category'] = df['category'].mask(((df[value_column] != filtered) & \
                    (df[value_column].notna())), other='DELTA')
    df[value_column] = filtered
    return df


# March through all hours recording previous "good"
# demand value and its index.  Calculate deltas between
# this value and next "good" hour.  If delta is LARGE
# mark NAN.
# Go forwards once, then backwards once to get all options.
def filter_single_sided_deltas(df, multiplier, rel_multiplier, iqr_relative_deltas, value_column):


    # Go through forwards first, then reverse
    prev_good_index = np.nan
    
    deltaSingleFiltered = []
    for idx in df.index:
        deltaSingleFiltered.append(np.nan)
        if np.isnan(df.loc[idx, value_column]):
            continue
        
        
        # Initialize first good entry, this will never be flagged
        if np.isnan(prev_good_index):
            prev_good_index = idx
            
        
        # Check deltas demand and relative wrt hourly adjustment
        prev_good_delta_dem = abs(df.loc[prev_good_index, value_column] - df.loc[idx, value_column])
        prev_good_delta_dem_rel_diff_wrt_hourly = abs(df.loc[prev_good_index, 
                        'dem_rel_diff_wrt_hourly'] - df.loc[idx, 'dem_rel_diff_wrt_hourly'])

        
        # delta_rolling_IQR is over 5 days on each side so should be
        # similar regardless of which hours' we use. If delta is
        # large, mark this hour anomalous
        if (prev_good_delta_dem > df.loc[idx, 'delta_rolling_IQR'] * multiplier) and \
                (prev_good_delta_dem_rel_diff_wrt_hourly > rel_multiplier * iqr_relative_deltas):
            
            
            # If the previous "good" value was farther from expected values, then consider current hour good
            # and the previous hour will be caught on the way back through the reverse direction.
            # The max deviation from the rolling 4 day dem and the rolling 10 day dem is taken
            # to help catch cases where a large deviation pulls the rolling 4 day dem to center
            # on its values.  i.e. SCL 2016 Dec 15.
            prev_max = max(abs(1. - df.loc[prev_good_index, 'dem_rel_diff_wrt_hourly']),
                            abs(1. - df.loc[prev_good_index, 'dem_rel_diff_wrt_hourly_long']))
            current_max = max(abs(1. - df.loc[idx, 'dem_rel_diff_wrt_hourly']),
                            abs(1. - df.loc[idx, 'dem_rel_diff_wrt_hourly_long']))         
            if abs(current_max) < abs(prev_max):
                prev_good_index = idx
            
            # else, continue to filter this hour
            else:
                deltaSingleFiltered[-1] = df.loc[idx, value_column]
                df.loc[idx, value_column] = np.nan
                df.loc[idx, 'category'] = 'SINGLE_DELTA'
        else:
            prev_good_index = idx

    
    df['deltaSingleFilteredFwd'] = deltaSingleFiltered
    
    
    ### Go through reversed, ~ copy of above code ###
    prev_good_index = np.nan
    
    deltaSingleFiltered = []
    for idx in reversed(df.index):
        deltaSingleFiltered.append(np.nan)
        if np.isnan(df.loc[idx, value_column]):
            continue
        
        
        # Initialize first good entry, this will never be flagged
        if pd.isnull(prev_good_index): #original - if np.isnan(prev_good_index):
            prev_good_index = idx
            
        
        # Check deltas demand and relative wrt hourly adjustment
        prev_good_delta_dem = abs(df.loc[prev_good_index, value_column] - df.loc[idx, value_column])
        prev_good_delta_dem_rel_diff_wrt_hourly = abs(df.loc[prev_good_index, 
                        'dem_rel_diff_wrt_hourly'] - df.loc[idx, 'dem_rel_diff_wrt_hourly'])

        
        # delta_rolling_IQR is over 5 days on each side so should be
        # similar regardless of which hours' we use. If delta is
        # large, mark this hour anomalous
        if (prev_good_delta_dem > df.loc[idx, 'delta_rolling_IQR'] * multiplier) and \
                (prev_good_delta_dem_rel_diff_wrt_hourly > rel_multiplier * iqr_relative_deltas):
            
            
            deltaSingleFiltered[-1] = df.loc[idx, value_column]
            df.loc[idx, value_column] = np.nan
            df.loc[idx, 'category'] = 'SINGLE_DELTA'
        else:
            prev_good_index = idx

    to_app = [val for val in reversed(deltaSingleFiltered)]
    df['deltaSingleFilteredBkw'] = to_app
    
    return df


def filter_runs(df, value_column):
    
    d1 = df[value_column].diff(periods=1)
    d2 = df[value_column].diff(periods=2)

    # cannot compare a dtyped [float64] array with a scalar of type [bool]
    filtered = df[value_column].mask((d1 == 0) & (d2 == 0))
    df['runFiltered'] = np.where(df[value_column] != filtered, df[value_column], np.nan)
    df[value_column] = filtered
    df['category'] = np.where(df['runFiltered'].notna(), 'IDENTICAL_RUN', df['category'])
    return df
    

def filter_anomalous_regions(df, width, anomalous_pct, value_column):
    
    percent_good_data_cnt = [0. for _ in df.index]
    percent_good_data_pre = [0. for _ in df.index]
    percent_good_data_post = [0. for _ in df.index]
    df['len_good_data'] = [0 for _ in df.index]
    data_quality_cnt = []
    data_quality_short = []
    start_good_data = np.nan
    end_good_data = np.nan
    for idx in df.index:
            
        # Remove the oldest item in the list
        if len(data_quality_short) > width:
            data_quality_short.pop(0)
        if len(data_quality_cnt) > 2 * width:
            data_quality_cnt.pop(0)
        
        # Add new item and don't count MISSING as 'bad' data
        if df.loc[idx, 'category'] == 'OKAY' or df.loc[idx, 'category'] == 'MISSING':
            data_quality_cnt.append(1)
            data_quality_short.append(1)
            # Track length of good data chunks
            if np.isnan(start_good_data):
                start_good_data = idx
            end_good_data = idx
        else:
            data_quality_cnt.append(0)
            data_quality_short.append(0)
            # Fill in length of good data chunk
            if not (np.isnan(start_good_data) or np.isnan(end_good_data)):
                len_good = end_good_data - start_good_data + 1
                df.loc[start_good_data:end_good_data, 'len_good_data'] = len_good
            start_good_data = np.nan
            end_good_data = np.nan

        
        # centered measurements have length 2 * width
        if len(data_quality_cnt) > 2 * width:
            percent_good_data_cnt[idx-width] = np.mean(data_quality_cnt)
        # left and right / pre and post measurements have length = width + 1
        if len(data_quality_short) > width:
            percent_good_data_pre[idx] = np.mean(data_quality_short)
            percent_good_data_post[idx-width] = np.mean(data_quality_short)



    
    anomalousRegionsFiltered = [np.nan for _ in df.index]
    for idx in df.index:
        if percent_good_data_cnt[idx] <= anomalous_pct:
            for j in range(idx-width, idx+width):
                if j < 1 or j >= len(df.index):
                    continue
                if df.loc[j, 'category'] == 'OKAY':
                    # If this is the start or end of continuous good data, don't filter
                    if percent_good_data_pre[j] == 1.0 or percent_good_data_post[j] == 1.0:
                        continue
                    if df.loc[j, 'len_good_data'] > width:
                        continue
                    df.loc[j, 'category'] = 'ANOMALOUS_REGION'
                    anomalousRegionsFiltered[j] = df.loc[j, value_column]
                    df.loc[j, value_column] = np.nan
    

    df['anomalousRegionsFiltered'] = anomalousRegionsFiltered
    return df

def screen_anomolies(data, value_column,
                    short_hour_window, # 48 hour moving median (M_{t,48hr})
                    iqr_hours, # width in hours of IQR values of relative deviations from diurnal cycle template (IQR_{dem,t})
                    nDays, # Used for normalized hourly demand template (h_{t,diurnal}) and 480 hour moving median (M_{t,480hr})
                    global_dem_cut, # threshold selection for global demand filter
                    local_dem_cut_up, # upwards threshold for local demand filter
                    local_dem_cut_down, # downwards threshold for local demand filter
                    delta_multiplier, # selection threshold for double-sided delta filter
                    delta_single_multiplier, # selection threshold for single-sided delta filter
                    rel_multiplier, # other selection threshold for single-sided delta filter
                    anomalous_regions_width, # width in hours of anomalous region filter
                    anomalous_pct # required pct of good data in anomalous region filter
                    ):
    
    df = data.copy()
    
    # Add category labels to track which algo screens an hourly value
    df = add_categories(df, value_column)

    # Mark missing and empty values
    df = df.assign(missing=df[value_column].isna())
    
    #---------------------------------------------
    # Screening Step 1
    #---------------------------------------------
    
    # Set all negative and zero values to NAN
    # (negative or zero filter)
    df = filter_neg_and_zeros(df, value_column)

    # Set last demand values in runs of 3+ to NAN
    # (identical run filter)
    df = filter_runs(df, value_column)

    # Global demand filter on 10x the median value
    # (global demand filter)
    df = filter_extrem_demand(df, global_dem_cut, value_column)

    # Filter +/- 1 hour from any global deman filtered hours
    # (global demand plus/minus 1 hour filter)
    df = filter_global_plus_minus_one(df, value_column)



    
    #---------------------------------------------
    # Calculate demand characteristics for Step 2
    #---------------------------------------------
    
    # 48 hour moving median (M_{t,48hr})
    df = add_rolling_dem(df, short_hour_window, value_column)

    # 480 hour moving median (M_{t,480hr})
    df = add_rolling_dem_long(df, nDays, value_column)

    # demand minus moving median (Delta(d_{t},M_{t,48hr}))
    df = add_demand_minus_rolling_dem(df, value_column)

    # IQR values of relative deviations from diurnal cycle template (IQR_{dem,t})
    df = add_demand_minus_rolling_dem_iqr(df, iqr_hours)

    # demand deltas (delta(d_{t-1},d_{t}))
    df = add_deltas(df, value_column)

    # IQR values of demand deltas (IQR_{delta,t})
    df = add_rolling_delta_iqr(df, iqr_hours)

    # normalized hourly demand template (h_{t,diurnal})
    df = add_hourly_median_dem_deviations(df, nDays)

    # Demand deviation from hourly diurnal template (r_{t})
    # This adds both the short and long moving medians
    df = add_demand_rel_diff_wrt_hourly(df, value_column)

    # Hour-to-hour differences between hourly diurnal templates
    # (delta(r_{t-1},r_{t}))
    # This adds differences for both the short and long moving medians
    df = add_delta_demand_rel_diff_wrt_hourly(df)

    # Calculate the global IQR for the hour-to-hour differences 
    # between hourly diurnal templates (IQR_{r})
    # This is a global value and is not added to the dataframe
    iqr_relative_deltas = calculate_relative_demand_difference_IQR(df)


    
    #---------------------------------------------
    # Screening Step 2
    #---------------------------------------------
    
    # (local demand filter)
    df = filter_local_demand(df, local_dem_cut_up, local_dem_cut_down, value_column)
    
    # (double-sided delta filter)
    df = filter_deltas(df, delta_multiplier, value_column)
    
    # (single-sided delta filter)
    df = filter_single_sided_deltas(df, delta_single_multiplier,
                                rel_multiplier, iqr_relative_deltas, value_column)
    
    # (anomalous regions filter)
    df = filter_anomalous_regions(df, anomalous_regions_width, anomalous_pct, value_column)

    return df