import pandas as pd
import numpy as np
def handle_missing(df: pd.DataFrame, numeric_strategy: str = 'mean', categorical_strategy: str = 'mode') -> pd.DataFrame:
    """
    Fill missing values in a DataFrame.

    :param df: The input DataFrame
    :param numeric_strategy: Strategy for handling missing numeric values ('mean', 'median', or 'mode')
    :param categorical_strategy: Strategy for handling missing categorical values ('mode' or 'dummy')
    :returns: A DataFrame with missing values filled
    """
    for column in df.columns:
        if df[column].dtype in ['int64', 'float64']:
            if numeric_strategy == 'mean':
                df[column].fillna(df[column].mean(), inplace=True)
            elif numeric_strategy == 'median':
                df[column].fillna(df[column].median(), inplace=True)
            elif numeric_strategy == 'mode':
                df[column].fillna(df[column].mode()[0], inplace=True)
        else:
            if categorical_strategy == 'mode':
                df[column].fillna(df[column].mode()[0], inplace=True)
            elif categorical_strategy == 'dummy':
                d

# Testing
df = pd.DataFrame({'A': [1, 2, np.nan, 4], 'B': ['x', 'y', np.nan, 'z']})
cleaned_df = handle_missing(df)
print(cleaned_df)