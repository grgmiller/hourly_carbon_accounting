import numpy as np


def cov(df, groupby_columns:list, value_column:str):
    """
    Calculates the coefficient of variation for data grouped by specific columns
    Args:
        df: pandas dataframe
        groupby_columns: list of column names to group the data by
        value_column: string name of column containing the values for which you want cov calculated
    Returns:
        result: a pandas df with grouped statistics for count, mean, population standard deviation, and cov
    """

    # define a function to calculate the population standard deviation with ddof=0
    std_p = lambda x: x.std(ddof=0)
    std_p.__name__ = 'std_p'

    columns_to_keep = groupby_columns + [value_column]

    df = df.copy()[columns_to_keep]

    result = df.groupby(groupby_columns).agg(['count','mean',std_p])

    result = result.droplevel(level=0, axis=1)

    result['cov'] = result['std_p'] / result['mean']

    return result


def mae(df, y:str, yhats:list):
    """
    Calculates Mean Absolute Error.
    Args:
        df: pandas dataframe
        y: name of column with actual data
        yhats: list of column names with estimated data
    Returns:
        df: containing the MAE for each column
    """

    columns_to_keep = yhats + [y]
    df = df.copy()[columns_to_keep]

    #calculate the absolute difference for each column
    for col in columns_to_keep:
        df[col] = abs(df[y] - df[col])

    #sum the values and take the average
    df = df.sum() / df.count()

    return df


def mape(df, y:str, yhats:list):
    """
    Calculates Mean Absolute Percentage Error.
    Args:
        df: pandas dataframe
        y: name of column with actual data
        yhats: list of column names with estimated data
    Returns:
        df: containing the MAPE for each column
    """

    columns_to_keep = yhats + [y]
    df = df.copy()[columns_to_keep]

    #calculate the absolute difference for each column
    for col in columns_to_keep:
        df[col] = abs((df[y] - df[col]) / df[y])

    # if there are any inf values (if a y value was 0), replace with 0
    df = df.replace([np.inf, -np.inf], 0)

    #get the mean value
    df = df.mean()

    return df


def mpe(df, y:str, yhats:list):
    """
    Calculates Mean Percentage Error.
    Args:
        df: pandas dataframe
        y: name of column with actual data
        yhats: list of column names with estimated data
    Returns:
        df: containing the MPE for each column
    """

    columns_to_keep = yhats + [y]
    df = df.copy()[columns_to_keep]

    #calculate the absolute difference for each column
    for col in columns_to_keep:
        df[col] = (df[col] - df[y]) / df[y]

    # if there are any inf values (if a y value was 0), replace with 0
    df = df.replace([np.inf, -np.inf], 0)

    #get the mean value
    df = df.mean()

    return df


def mse(df, y:str, yhats:list):
    """
    Calculates Mean Squared Error.
    Args:
        df: pandas dataframe
        y: name of column with actual data
        yhats: list of column names with estimated data
    Returns:
        df: containing the MPE for each column
    """

    columns_to_keep = yhats + [y]
    df = df.copy()[columns_to_keep]

    #calculate the absolute difference for each column
    for col in columns_to_keep:
        df[col] = (df[y] - df[col])**2

    #sum the values and take the average
    df = df.mean()

    return df   