from sqlalchemy import create_engine
from sqlalchemy import text

engine = create_engine('postgresql+psycopg2://postgres:shinmentekezo12345@localhost:5432/python_warehouse_db')

stage_table =   """
                    CREATE TABLE IF NOT EXISTS stage_product(
                    product_id INTEGER,
                    product_name TEXT,
                    category TEXT
                    );
                    
                    CREATE TABLE IF NOT EXISTS stage_customers(
                    customer_id INTEGER,
                    customer_name TEXT,
                    email TEXT,
                    region TEXT
                    );

                    CREATE TABLE IF NOT EXISTS stage_date(
                    order_date DATE,
                    date_id INTEGER,
                    month INTEGER,
                    year INTEGER
                    );

                    CREATE TABLE IF NOT EXISTS stage_sales(
                    sale_id INTEGER,
                    product_id INTEGER,
                    customer_id INTEGER,
                    date_id INTEGER,
                    quantity INTEGER,
                    revenue NUMERIC
                    );        
                """

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
    # conn.execute(text(stage_table))
    conn.execute(text(create_sql_table))
    conn.commit()


import pandas as pd

df = pd.read_csv(r'store_sales\clean_store_sales.csv')
df['date_id'] = pd.to_datetime(df['order_date']).dt.strftime('%Y%m%d').astype('int64')

dim_products = df[['product_id', 'product_name', 'category']].drop_duplicates().copy()

dim_customers = df[['customer_id', 'customer_name', 'email', 'region']].drop_duplicates()

dim_date = df[['order_date']].drop_duplicates().copy()

dim_date['order_date'] = pd.to_datetime(dim_date['order_date'])
dim_date['date_id'] = dim_date['order_date'].dt.strftime('%Y%m%d').astype(int)
dim_date['month'] = dim_date['order_date'].dt.month
dim_date['year'] = dim_date['order_date'].dt.year

dim_date = dim_date[['order_date', 'date_id', 'month', 'year']]

fact_sales = df[['sale_id', 'customer_id', 'product_id', 'date_id', 'quantity', 'revenue']].drop_duplicates()

dim_products.to_sql('stage_products', engine, if_exists='append', index=False)
dim_customers.to_sql('stage_customers', engine, if_exists='append', index=False)
dim_date.to_sql('stage_date', engine, if_exists='append', index=False)
fact_sales.to_sql('stage_sales', engine, if_exists='append', index=False)


merging_dim = """
                INSERT INTO dim_products(product_id, product_name, category)
                SELECT DISTINCT product_id, product_name, category
                FROM stage_products
                ON CONFLICT (product_id)
                DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category;

                INSERT INTO dim_customers(customer_id, customer_name, email, region)
                SELECT DISTINCT customer_id, customer_name, email, region
                FROM stage_customers
                ON CONFLICT (customer_id)
                DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                email = EXCLUDED.email,
                region = EXCLUDED.region;

                INSERT INTO dim_date(order_date, date_id, month, year)
                SELECT DISTINCT order_date, date_id, month, year
                FROM stage_date
                ON CONFLICT (date_id)
                DO NOTHING;

                """

merging_fact = """
               INSERT INTO fact_sales(sale_id, customer_id, product_id, date_id, quantity, revenue)
               SELECT DISTINCT sale_id, customer_id, product_id, date_id, quantity, revenue
               FROM stage_sales
               ON CONFLICT (sale_id)
               DO NOTHING;
               """

clear_stage = """
              TRUNCATE TABLE stage_products, stage_customers, stage_date, stage_sales"""


with engine.begin() as conn:
    conn.execute(text(merging_dim))
    conn.execute(text(merging_fact))