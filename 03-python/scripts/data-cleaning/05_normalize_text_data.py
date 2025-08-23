import re
def normalize_text(text: str) -> str:
    """
    Normalize text data by converting to lowercase, removing special characters, and extra spaces.

    :param text: The input text to normalize
    :returns: Normalized text
    """
    # Convert to lowercase
    text = str(text).lower()

    # Remove special characters
    text = re.sub(r'[^\w\s]', '', text)

    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()

    return text

# Testing
messy_text = "This is MESSY!!! Text   with $pecial ch@racters."
clean_text = normalize_text(messy_text)
print(clean_text)