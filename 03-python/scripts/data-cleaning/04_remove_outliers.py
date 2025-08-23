import pandas as pd
import numpy as np
from typing import List

def remove_outliers_iqr(df: pd.DataFrame, columns: List[str], factor: float = 1.5) -> pd.DataFrame:
    """
    Remove outliers from specified columns using the Interquartile Range (IQR) method.

    :param df: The input DataFrame
    :param columns: List of column names to check for outliers
    :param factor: The IQR factor to use (default is 1.5)
    :returns: A DataFrame with outliers removed
    """
    mask = pd.Series(True, index=df.index)
    for col in columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - factor * IQR
        upper_bound = Q3 + factor * IQR
        mask &= (df[col] >= lower_bound) & (df[col] <= upper_bound)
    
    cleaned_df = df[mask]
    
    return cleaned_df

# Testing
df = pd.DataFrame({'A': [1, 2, 3, 100, 4, 5], 'B': [10, 20, 30, 40, 50, 1000]})
print("Original DataFrame:")
print(df)
print("\nCleaned DataFrame:")
cleaned_df = remove_outliers_iqr(df, ['A', 'B'])
print(cleaned_df)