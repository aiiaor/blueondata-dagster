from dagster import asset, AssetExecutionContext, asset_check, AssetCheckResult
import pandas as pd
@asset(required_resource_keys={"postgres"})
def orders_file_raw(context: AssetExecutionContext) -> None:
    conn = context.resources.postgres
    cur = conn.cursor()
    query = "SELECT order_id, customer, order_date, product, quantity, price FROM ecommerce_orders;"
    cur.execute(query)
    rows = cur.fetchall()
    context.log.info(f"Fetched rows: {rows}")
    cur.close()
    df = pd.DataFrame(rows, columns=['order_id', 'customer', 'order_date', 'product', 'quantity', 'price'])
    with open("data/orders_file_raw.csv", "w") as f:
        f.write(df.to_csv(index=False))


@asset(
    deps=[orders_file_raw]
)
def orders_file_cleaned() -> None:
    orders_file_raw = pd.read_csv("data/orders_file_raw.csv")
    # Handle missing values
    df = orders_file_raw
    df['quantity'] = df['quantity'].fillna(df['quantity'].median())
    df['price'] = df['price'].fillna(df['price'].mean())
    
    # Remove duplicates
    df = df.drop_duplicates(subset='order_id', keep='first')
    
    # Standardize formats
    df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=True)
    df['customer'] = df['customer'].str.strip().str.title()
    df['product'] = df['product'].str.strip().str.lower().str.title()
    
    # Add computed columns
    df['total_cost'] = df['quantity'] * df['price']

    with open("data/orders_file_cleaned.csv", "w") as f:
        f.write(df.to_csv(index=False))


@asset_check(asset=orders_file_cleaned)
def check_orders_file_cleaned():
    df = pd.read_csv("data/orders_file_cleaned.csv")
    # Check for null values in quantity
    assert df['quantity'].notnull().all(), "Quantity contains null values"
    
    # Check for null values in order_date
    assert df['order_date'].notnull().all(), "Order date contains null values"
    
    # Check if quantity is within valid range
    assert ((df['quantity'] >= 1) & (df['quantity'] <= 1000)).all(), "Quantity values outside valid range (1-1000)"
    
    # Check if price is within valid range
    assert ((df['price'] >= 0) & (df['price'] <= 10000)).all(), "Price values outside valid range (0-10000)"
    
    # Check for null values in total_cost
    assert df['total_cost'].notnull().all(), "Total cost contains null values"
    
    return AssetCheckResult(
        passed=True
    )

@asset(
    required_resource_keys={"postgres"},
    deps=[orders_file_cleaned]
)
def orders_table(context: AssetExecutionContext) -> None:
    conn = context.resources.postgres
    cur = conn.cursor()
    
    # Read the cleaned data
    df = pd.read_csv("data/orders_file_cleaned.csv")
    
    # Drop table if exists
    cur.execute("DROP TABLE IF EXISTS orders")
    
    # Create table
    query = """
    CREATE TABLE orders (
        order_id VARCHAR(255) PRIMARY KEY,
        customer VARCHAR(255),
        order_date TIMESTAMP,
        product VARCHAR(255),
        quantity NUMERIC,
        price NUMERIC,
        total_cost NUMERIC
    )
    """
    cur.execute(query)
    
    # Insert data
    for _, row in df.iterrows():
        insert_query = """
        INSERT INTO orders (order_id, customer, order_date, product, quantity, price, total_cost)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            row['order_id'],
            row['customer'],
            row['order_date'],
            row['product'],
            row['quantity'],
            row['price'],
            row['total_cost']
        ))
    
    # Commit changes
    conn.commit()
    
    # Close cursor
    cur.close()
    
    context.log.info(f"Successfully saved {len(df)} records to orders table")

    