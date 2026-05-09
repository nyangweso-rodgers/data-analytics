"""
CSV/Excel Data Diagnostic Tool
================================
A reusable script to analyze data files before building sync scripts.

Usage:
    python diagnose_data.py [--file PATH] [--rows N] [--output report.txt]

Features:
    - Analyzes all columns automatically
    - Detects data types and formats
    - Identifies special characters (commas, dashes, parentheses)
    - Shows null/blank patterns
    - Detects date formats
    - Suggests PostgreSQL data types
"""

import pandas as pd
import sys
import argparse
from typing import Dict, List, Any
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_FILE_PATH = "../../../../../../../../2024.xlsx"
DEFAULT_ROWS = 1000  # Number of rows to analyze


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def analyze_column(df: pd.DataFrame, col: str, sample_size: int = 10) -> Dict[str, Any]:
    """Analyze a single column and return comprehensive statistics"""
    
    series = df[col]
    analysis = {
        'column_name': col,
        'data_type': str(series.dtype),
        'total_rows': len(series),
        'null_count': series.isna().sum(),
        'null_percentage': (series.isna().sum() / len(series)) * 100,
        'unique_count': series.nunique(),
        'sample_values': series.head(sample_size).tolist(),
    }
    
    # Non-null values for further analysis
    non_null = series.dropna()
    analysis['non_null_count'] = len(non_null)
    
    if len(non_null) == 0:
        analysis['all_null'] = True
        analysis['suggested_pg_type'] = 'TEXT'
        return analysis
    
    analysis['all_null'] = False
    
    # Check for blanks (empty strings)
    if series.dtype == 'object':
        blank_count = (series == '').sum()
        analysis['blank_count'] = blank_count
        analysis['blank_percentage'] = (blank_count / len(series)) * 100
        
        # Convert to string for pattern analysis
        str_series = series.astype(str)
        
        # Check for special characters
        analysis['has_commas'] = str_series.str.contains(',', na=False).sum()
        analysis['has_dashes'] = str_series.str.contains('-', na=False).sum()
        analysis['has_parentheses'] = str_series.str.contains(r'\(', na=False).sum()
        analysis['has_dollar_sign'] = str_series.str.contains(r'\$', na=False).sum()
        analysis['has_percent'] = str_series.str.contains('%', na=False).sum()
        
        # Sample non-null values
        analysis['non_null_samples'] = non_null.head(sample_size).tolist()
        
        # Try to detect if it's numeric with formatting
        analysis['is_numeric_with_commas'] = False
        analysis['is_date_like'] = False
        analysis['detected_date_format'] = None
        
        # Test if numeric after removing commas
        test_numeric = str_series.str.replace(',', '').str.replace('$', '').str.replace('%', '')
        numeric_test = pd.to_numeric(test_numeric, errors='coerce')
        if numeric_test.notna().sum() > len(series) * 0.5:
            analysis['is_numeric_with_commas'] = True
            analysis['min_value'] = numeric_test.min()
            analysis['max_value'] = numeric_test.max()
            analysis['mean_value'] = numeric_test.mean()
        
        # Try to detect date patterns
        date_patterns = [
            (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d', 'YYYY-MM-DD'),
            (r'^\d{2}/\d{2}/\d{4}$', '%d/%m/%Y', 'DD/MM/YYYY'),
            (r'^\d{2}-[A-Za-z]{3}-\d{2}$', '%d-%b-%y', 'DD-Mon-YY'),
            (r'^\d{2}-[A-Za-z]{3}$', '%y-%b', 'YY-Mon'),
            (r'^[A-Za-z]{3}-\d{2}$', '%b-%y', 'Mon-YY'),
        ]
        
        for pattern, fmt, desc in date_patterns:
            sample = str(non_null.iloc[0]) if len(non_null) > 0 else ''
            if re.match(pattern, sample):
                try:
                    test_date = pd.to_datetime(non_null.head(100), format=fmt, errors='coerce')
                    if test_date.notna().sum() > 50:
                        analysis['is_date_like'] = True
                        analysis['detected_date_format'] = desc
                        break
                except:
                    pass
    else:
        # Numeric columns
        analysis['blank_count'] = 0
        analysis['blank_percentage'] = 0.0
        analysis['has_commas'] = 0
        analysis['has_dashes'] = 0
        analysis['has_parentheses'] = 0
        analysis['has_dollar_sign'] = 0
        analysis['has_percent'] = 0
        analysis['non_null_samples'] = non_null.head(sample_size).tolist()
        
        if series.dtype in ['int64', 'float64']:
            analysis['min_value'] = series.min()
            analysis['max_value'] = series.max()
            analysis['mean_value'] = series.mean()
    
    # Suggest PostgreSQL data type
    analysis['suggested_pg_type'] = suggest_postgres_type(analysis)
    
    return analysis


def suggest_postgres_type(analysis: Dict[str, Any]) -> str:
    """Suggest appropriate PostgreSQL data type based on analysis"""
    
    if analysis['all_null']:
        return 'TEXT'
    
    # Check if it's a date
    if analysis.get('is_date_like', False):
        return 'DATE'
    
    # Check if it's numeric
    if analysis['data_type'] in ['int64', 'float64']:
        max_val = analysis.get('max_value', 0)
        min_val = analysis.get('min_value', 0)
        
        # Check if it's an integer type
        if analysis['data_type'] == 'int64':
            if abs(max_val) > 2147483647 or abs(min_val) > 2147483647:
                return 'BIGINT'
            else:
                return 'INTEGER'
        else:
            return 'DOUBLE PRECISION'
    
    # Check if it's numeric with formatting
    if analysis.get('is_numeric_with_commas', False):
        max_val = analysis.get('max_value', 0)
        min_val = analysis.get('min_value', 0)
        
        # Check for BIGINT range
        if abs(max_val) > 2147483647 or abs(min_val) > 2147483647:
            return 'BIGINT'
        elif abs(max_val) > 999999 or abs(min_val) > 999999:
            return 'DOUBLE PRECISION'
        else:
            return 'DOUBLE PRECISION'
    
    # String columns
    if analysis['data_type'] == 'object':
        max_length = max([len(str(x)) for x in analysis['non_null_samples']], default=0)
        if max_length > 500:
            return 'TEXT'
        else:
            return f'VARCHAR({max(max_length * 2, 50)})'
    
    return 'TEXT'


def sanitize_column_name(col_name: str) -> str:
    """Sanitize column name to be PostgreSQL-compatible"""
    # Replace dots with underscores
    sanitized = col_name.replace('.', '_')
    # Replace spaces with underscores
    sanitized = sanitized.replace(' ', '_')
    # Remove any other problematic characters
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', sanitized)
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = 'col_' + sanitized
    # Convert to lowercase for PostgreSQL convention
    sanitized = sanitized.lower()
    return sanitized


def print_column_report(analysis: Dict[str, Any], verbose: bool = False):
    """Print formatted report for a single column"""
    
    col_name = analysis['column_name']
    sanitized = sanitize_column_name(col_name)
    
    print(f"\n{'='*80}")
    print(f"Column: {col_name}")
    if col_name != sanitized:
        print(f"PostgreSQL Name: {sanitized}")
    print(f"{'='*80}")
    print(f"Data Type (Pandas):     {analysis['data_type']}")
    print(f"Suggested PostgreSQL:   {analysis['suggested_pg_type']}")
    print(f"Total Rows:             {analysis['total_rows']}")
    print(f"Null Count:             {analysis['null_count']} ({analysis['null_percentage']:.1f}%)")
    print(f"Blank Count:            {analysis.get('blank_count', 0)} ({analysis.get('blank_percentage', 0):.1f}%)")
    print(f"Non-Null Count:         {analysis['non_null_count']}")
    print(f"Unique Values:          {analysis['unique_count']}")
    
    if analysis.get('has_commas', 0) > 0:
        print(f"Has Commas:             {analysis['has_commas']} rows")
    if analysis.get('has_dashes', 0) > 0:
        print(f"Has Dashes:             {analysis['has_dashes']} rows")
    if analysis.get('has_parentheses', 0) > 0:
        print(f"Has Parentheses:        {analysis['has_parentheses']} rows")
    if analysis.get('has_dollar_sign', 0) > 0:
        print(f"Has Dollar Signs:       {analysis['has_dollar_sign']} rows")
    if analysis.get('has_percent', 0) > 0:
        print(f"Has Percent Signs:      {analysis['has_percent']} rows")
    
    if analysis.get('is_date_like', False):
        print(f"Detected Date Format:   {analysis['detected_date_format']}")
    
    if analysis.get('is_numeric_with_commas', False):
        print(f"Numeric with Formatting: YES")
        print(f"Min Value:              {analysis.get('min_value', 'N/A')}")
        print(f"Max Value:              {analysis.get('max_value', 'N/A')}")
        print(f"Mean Value:             {analysis.get('mean_value', 'N/A'):.2f}")
    elif 'min_value' in analysis:
        print(f"Min Value:              {analysis['min_value']}")
        print(f"Max Value:              {analysis['max_value']}")
        print(f"Mean Value:             {analysis['mean_value']:.2f}")
    
    print(f"\nSample Values (first 5):")
    for i, val in enumerate(analysis['sample_values'][:5], 1):
        print(f"  {i}. {val}")
    
    if verbose and not analysis['all_null']:
        print(f"\nNon-Null Samples (first 5):")
        for i, val in enumerate(analysis.get('non_null_samples', [])[:5], 1):
            print(f"  {i}. {val}")


def generate_field_mapping(analyses: List[Dict[str, Any]]) -> str:
    """Generate Python code for FIELD_MAPPING dictionary"""
    
    lines = ["# Field mapping: column_name -> postgres_data_type", "FIELD_MAPPING = {"]
    
    for analysis in analyses:
        col_name = analysis['column_name']
        sanitized_name = sanitize_column_name(col_name)
        pg_type = analysis['suggested_pg_type']
        
        # Add comment if original name was changed
        if col_name != sanitized_name:
            lines.append(f"    '{sanitized_name}': '{pg_type}',  # Original: {col_name}")
        else:
            lines.append(f"    '{sanitized_name}': '{pg_type}',")
    
    lines.append("}")
    
    # Also generate column rename mapping
    lines.append("\n\n# Column rename mapping (if needed for reading CSV)")
    lines.append("COLUMN_RENAME_MAPPING = {")
    for analysis in analyses:
        col_name = analysis['column_name']
        sanitized_name = sanitize_column_name(col_name)
        if col_name != sanitized_name:
            lines.append(f"    '{col_name}': '{sanitized_name}',")
    lines.append("}")
    
    return "\n".join(lines)


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Diagnose CSV/Excel files for data sync scripts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--file', type=str, default=DEFAULT_FILE_PATH,
                       help='Path to CSV or Excel file')
    parser.add_argument('--rows', type=int, default=DEFAULT_ROWS,
                       help='Number of rows to analyze (default: 1000)')
    parser.add_argument('--output', type=str, default=None,
                       help='Save report to file (optional)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed output')
    parser.add_argument('--generate-mapping', action='store_true',
                       help='Generate FIELD_MAPPING code')
    
    args = parser.parse_args()
    
    # Redirect output to file if specified
    if args.output:
        sys.stdout = open(args.output, 'w')
    
    try:
        print("="*80)
        print("CSV/EXCEL DATA DIAGNOSTIC TOOL")
        print("="*80)
        print(f"\nFile: {args.file}")
        print(f"Analyzing first {args.rows} rows...\n")
        
        # Read the file
        if args.file.endswith('.csv'):
            df = pd.read_csv(args.file, low_memory=False, nrows=args.rows)
        elif args.file.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(args.file, nrows=args.rows)
        else:
            print(f"Error: Unsupported file format. Use .csv, .xlsx, or .xls")
            return
        
        print(f"Successfully loaded {len(df)} rows × {len(df.columns)} columns")
        print(f"\nColumns found: {len(df.columns)}")
        print(f"Column names: {df.columns.tolist()}")
        
        # Analyze each column
        print("\n" + "="*80)
        print("ANALYZING ALL COLUMNS")
        print("="*80)
        
        analyses = []
        for col in df.columns:
            analysis = analyze_column(df, col)
            analyses.append(analysis)
            print_column_report(analysis, verbose=args.verbose)
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"\nTotal Columns: {len(analyses)}")
        print(f"Total Rows Analyzed: {len(df)}")
        
        # Count by type
        type_counts = {}
        for analysis in analyses:
            pg_type = analysis['suggested_pg_type']
            type_counts[pg_type] = type_counts.get(pg_type, 0) + 1
        
        print("\nSuggested PostgreSQL Types:")
        for pg_type, count in sorted(type_counts.items()):
            print(f"  {pg_type}: {count} columns")
        
        # Columns with issues
        print("\nColumns with Potential Issues:")
        issues = []
        for analysis in analyses:
            if analysis['null_percentage'] > 50:
                issues.append(f"  - {analysis['column_name']}: {analysis['null_percentage']:.1f}% null values")
            if analysis.get('has_commas', 0) > 0 and not analysis.get('is_numeric_with_commas', False):
                issues.append(f"  - {analysis['column_name']}: Has commas but may not be numeric")
        
        if issues:
            for issue in issues:
                print(issue)
        else:
            print("  No major issues detected!")
        
        # Generate FIELD_MAPPING if requested
        if args.generate_mapping:
            print("\n" + "="*80)
            print("GENERATED FIELD_MAPPING CODE")
            print("="*80)
            print()
            print(generate_field_mapping(analyses))
        
        print("\n" + "="*80)
        print("Analysis complete!")
        if args.output:
            print(f"Report saved to: {args.output}")
        print("="*80)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if args.output:
            sys.stdout.close()
            sys.stdout = sys.__stdout__
            print(f"Report saved to: {args.output}")


if __name__ == "__main__":
    main()