import pandas as pd

def read_excel_file(file_path):
    """Read the Excel file into a DataFrame."""
    return pd.read_excel(file_path)

def identify_duplicates(df, column):
    """Identify duplicates based on the specified column."""
    df["DuplicateFlag"] = df.groupby(column)[column].transform("count")
    return df

def filter_duplicates(df):
    """Filter the DataFrame to include only duplicates."""
    return df[df["DuplicateFlag"] > 1]

def sort_and_add_row_numbers(df, group_column, sort_column):
    """Sort the DataFrame and add row numbers within each group."""
    df = df.sort_values([group_column, sort_column], ascending=[True, False])
    df["RowNumber"] = df.groupby(group_column).cumcount() + 1
    return df

def flag_retain_or_delete(df):
    """Flag rows as 'Retain' or 'Delete' based on row numbers."""
    df["Action"] = df.apply(lambda row: "Retain" if row["RowNumber"] == 1 else "Delete", axis=1)
    return df

def save_to_csv(df, output_file):
    """Save the DataFrame to a CSV file, ensuring MobileNumberWithCountryCode__c is a string."""
    # Convert the column to string to preserve formatting
    df["MobileNumberWithCountryCode__c"] = df["MobileNumberWithCountryCode__c"].astype(str)
    df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")

def process_duplicates(file_path, output_file, group_column, sort_column):
    """Main function to process duplicates and save results."""
    # Step 1: Read the Excel file
    df = read_excel_file(file_path)

    # Step 2: Identify duplicates
    df = identify_duplicates(df, group_column)

    # Step 3: Filter only duplicates
    duplicates_df = filter_duplicates(df)

    # Step 4: Sort and add row numbers
    duplicates_df = sort_and_add_row_numbers(duplicates_df, group_column, sort_column)

    # Step 5: Flag rows for retain or delete
    duplicates_df = flag_retain_or_delete(duplicates_df)

    # Step 6: Save the results to a CSV file
    save_to_csv(duplicates_df, output_file)

# File paths and column names
input_file = "../lead-data-excel-v1.xlsx"  # Replace with your input file path
output_file = "duplicate-leads-data.csv"  # Replace with your desired output file name
group_column = "MobileNumberWithCountryCode__c"  # Column to group by
sort_column = "CreatedDate"  # Column to sort by

# Run the script
process_duplicates(input_file, output_file, group_column, sort_column)