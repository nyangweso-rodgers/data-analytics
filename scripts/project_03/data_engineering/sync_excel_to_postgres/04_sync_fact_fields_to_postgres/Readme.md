# Sync Fact Fields CSV Data to PostgreSQL

# Data Diagnostic Tool

- **Features**
  - Automatic Analysis: Analyzes all columns without manual configuration
  - Data Type Detection: Identifies dates, numbers, strings with smart format detection
  - Special Character Detection: Finds commas, dashes, parentheses, dollar signs, percent signs
  - Null/Blank Analysis: Shows percentage of null and blank values
  - PostgreSQL Type Suggestions: Automatically suggests appropriate PostgreSQL data types
  - Code Generation: Can generate FIELD_MAPPING code for your sync script
  - Report Export: Save analysis to file for documentation

- **What It Analyzes**: For each column, the tool reports:
  - Data Type: Both pandas and suggested PostgreSQL type
  - Row Counts: Total, null, blank, non-null, unique
  - Special Characters: Commas, dashes, parentheses, $, %
  - Date Detection: Automatically detects date formats
  - Numeric Detection: Detects numbers with formatting (commas, currency)
  - Value Range: Min, max, mean for numeric columns
  - Sample Values: First values and non-null samples

- **Usage**
  - Basic Usage: `python diagnose_data.py --file your_file.csv`
  - Analyze Specific Number of Rows: `python diagnose_data.py --file data.xlsx --rows 5000`
  - Generate FIELD_MAPPING Code: `python diagnose_data.py --file data.csv --generate-mapping`
  - Save Report to File: `python diagnose_data.py --file data.csv --output report.txt`
  - Verbose Mode: `python diagnose_data.py --file data.csv --verbose`
  - Combined Options: `python diagnose_data.py --file data.csv --rows 10000 --generate-mapping --output report.txt`
