from sqlalchemy import create_engine
from sqlalchemy import text


# data = '''
# sale_id,customer_id,customer_name,email,region,product_id,product_name,category,order_date,quantity,revenue
# 1,101,Alice Johnson,alice@email.com,East,501,Laptop,Electronics,2024-01-10,1,1200
# 2,102,Bob Smith,bob@email.com,West,502,Mouse,Electronics,2024-01-11,2,40
# 3,101,Alice Johnson,alice@email.com,East,503,Desk Chair,Furniture,2024-01-12,1,300
# 4,103,Charlie Lee,charlie@email.com,North,501,Laptop,Electronics,2024-01-12,1,1200
# 5,104,Diana Prince,diana@email.com,South,504,Notebook,Stationery,2024-01-13,5,25
# 6,102,Bob Smith,bob@email.com,West,503,Desk Chair,Furniture,2024-01-14,1,300
# '''


# with open(r'C:\Users\biten\OneDrive\Documents\python_learning\store_sales\clean_store_sales.csv', 'w') as file:
#     file.write(data)

engine = create_engine('postgresql+psycopg2://postgres:shinmentekezo12345@localhost:5432/warehouse_db')

create_sql_table =  """
                        CREATE TABLE IF NOT EXISTS dim_products (
                        product_id INTEGER PRIMARY KEY,
                        product_name TEXT,
                        category TEXT
                    );
                        
                        CREATE TABLE IF NOT EXISTS dim_customers (
                        customer_id INTEGER PRIMARY KEY,
                        customer_name TEXT,
                        email TEXT,
                        region TEXT
                    );

                        CREATE TABLE IF NOT EXISTS dim_date (
                        order_date DATE,
                        date_id BIGINT PRIMARY KEY,
                        month INTEGER,
                        year INTEGER
                    );

                        CREATE TABLE IF NOT EXISTS fact_sales (
                        sale_id INTEGER PRIMARY KEY,
                        product_id INTEGER REFERENCES dim_products(product_id) ON UPDATE CASCADE ON DELETE CASCADE,
                        customer_id INTEGER REFERENCES dim_customers(customer_id) ON UPDATE CASCADE ON DELETE CASCADE,
                        date_id BIGINT REFERENCES dim_date(date_id) ON UPDATE CASCADE ON DELETE CASCADE,
                        quantity INTEGER,
                        revenue NUMERIC
                        
                     );

                    """

with engine.connect() as conn:
    conn.execute(text(create_sql_table))
    conn.commit()

import pandas as pd

df = pd.read_csv(r'store_sales\clean_store_sales.csv')
df['date_id'] = pd.to_datetime(df['order_date']).dt.strftime('%Y%m%d').astype('int64')

print(df)
print()
dim_products = df[['product_id', 'product_name', 'category']].drop_duplicates().copy()
print(dim_products)

print()
dim_customers = df[['customer_id', 'customer_name', 'email', 'region']].drop_duplicates()
print(dim_customers)
print()



dim_date = df[['order_date']].drop_duplicates().copy()

dim_date['order_date'] = pd.to_datetime(dim_date['order_date'])
dim_date['date_id'] = dim_date['order_date'].dt.strftime('%Y%m%d').astype(int)
dim_date['month'] = dim_date['order_date'].dt.month
dim_date['year'] = dim_date['order_date'].dt.year


dim_date = dim_date[['order_date', 'date_id', 'month', 'year']]
print(dim_date)
print()


fact_sales = df[['sale_id', 'customer_id', 'product_id', 'date_id', 'quantity', 'revenue']].drop_duplicates()
print(fact_sales)
print()

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE fact_sales, dim_date, dim_customers, dim_products CASCADE"))

dim_products.to_sql('dim_products', engine, if_exists='append', index=False)
dim_customers.to_sql('dim_customers', engine, if_exists='append', index=False)
dim_date.to_sql('dim_date', engine, if_exists='append', index=False)
fact_sales.to_sql('fact_sales', engine, if_exists='append', index=False)


