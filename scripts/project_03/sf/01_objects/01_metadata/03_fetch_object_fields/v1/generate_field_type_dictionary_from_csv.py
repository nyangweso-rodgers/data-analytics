import csv

# Use your actual CSV filename
csv_file = "./Lead_fields_metadata_20251009_103101.csv"  # Replace with actual filename

print("SF_OBJECT_FIELDS = {")
with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        field_name = row['field_name']  # ← Use this, not 'label'!
        field_type = row['type']
        print(f'    "{field_name}": "{field_type}",')
print("}")