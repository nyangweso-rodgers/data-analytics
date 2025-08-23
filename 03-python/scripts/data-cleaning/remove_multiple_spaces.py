import re

def clean_spaces(text: str):
    """
    Remove multiple spaces from a string and trim leading/trailing spaces.
 
    :param text: The input string to clean
    :returns: A string with multiple spaces removed and trimmed
    """
    return re.sub(" +", ' ', str(text).strip())

messy_string = "  This   is  a    string with   irregular   spacing.  "
cleaned_string = clean_spaces(messy_string)
print(f"Cleaned: '{cleaned_string}'")