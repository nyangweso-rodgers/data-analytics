import re
from datetime import datetime
from typing import Optional

def standardize_date(date_string: str) -> Optional[str]:
    """
    Convert various date formats to YYYY-MM-DD.

    :param date_string: The input date string to standardize
    :returns: A standardized date string in YYYY-MM-DD format, or None if parsing fails
    """
    date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%B %d, %Y"]
    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Return None if no format matches
    return None

dates = ["2023-04-01", "01-04-2023", "04/01/2023", "April 1, 2023"]
standardized_dates = [standardize_date(date) for date in dates]
print(standardized_dates)