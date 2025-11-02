# Fetch Salesforce Objects Lists

# Usage

## Fetch all objects (will show one summary message about errors at the end)

```py
python fetch_sf_objects.py --all
```

## Fetch specific objects

```py
python fetch_sf_objects.py --objects Account Contact Lead Opportunity
```

## Fetch custom objects

```py
python fetch_sf_objects.py --objects Agent__c Property__c Timeline__c
```
