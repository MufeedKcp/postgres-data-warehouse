# PostgreSQL Data Warehouse Loader with Staging 

## Overview

This script builds a **PostgreSQL star schema** and loads cleaned sales data from a CSV file using a **staging → warehouse merge workflow**.

It prepares structured dimension and fact tables for analytical use by transforming CSV data with pandas and inserting it into the database using sqllchemy.

---

## Workflow

1. Connects to a PostgreSQL database
    
2. Creates warehouse tables if they do not exist:
    
    - `dim_products`
        
    - `dim_customers`
        
    - `dim_date`
        
    - `fact_sales`
        
3. Reads cleaned sales data from a CSV file
    
4. Transforms data into dimension and fact datasets
    
5. Loads data into staging tables:
    
    - `stage_products`
        
    - `stage_customers`
        
    - `stage_date`
        
    - `stage_sales`
        
6. Merges staging data into warehouse tables
    
    - Updates existing dimension records
        
    - Inserts new fact records
        
7. Leaves data ready for analytical queries
    

---

## Database Model

### Staging Layer

Temporary tables used for controlled data loading before merging.
### Dimension Tables

- **dim_products** → product information
    
- **dim_customers** → customer details
    
- **dim_date** → calendar attributes
    
### Fact Table

- **fact_sales** → transactional sales data linked to dimensions
    

---

## Requirements

```
- Python 3
- pandas
- SQLAlchemy
- psycopg2
- PostgreSQL
``` 


>Install dependencies:
```
pip install pandas sqlalchemy psycopg2
```

---

## Result

- Warehouse schema created if missing
    
- Data loaded into staging tables like a temporary table
    
- Dimension tables updated 
    
- Fact table populated with sales records
    
-  If new files are loaded to stage tables the old data will TRUNCATED 

---

## Purpose

Demonstrates a structured approach to loading CSV data into a PostgreSQL data warehouse using staging tables and merge logic in Python.
