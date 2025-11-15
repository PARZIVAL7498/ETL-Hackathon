# Raw Zone

This directory contains the raw, unprocessed data ingested from various source systems.

## Folder Structure

The data is organized by source system, source entity, and ingestion date:

```
/{source_system}/{source_entity}/year={YYYY}/month={MM}/day={DD}/
```

For example, data from Salesforce opportunities ingested on November 15, 2025, would be stored in:

```
/salesforce/opportunities/year=2025/month=11/day=15/
```