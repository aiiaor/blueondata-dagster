import os
import pandas as pd
import pytest
from dagster import build_op_context, DagsterInstance, AssetExecutionContext
from unittest.mock import MagicMock, patch
from ecommerce.assets import orders_file_raw, orders_file_cleaned, check_orders_file_cleaned, orders_table

# Test data
SAMPLE_ORDERS = [
    ('ORD001', 'John Doe', '2023-01-01', 'Laptop', 1, 999.99),
    ('ORD002', 'Jane Smith', '2023-01-02', 'Mouse', 2, 29.99),
    ('ORD003', 'Bob Johnson', '2023-01-03', 'Keyboard', 1, 59.99),
]

# Create test data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection for testing."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = SAMPLE_ORDERS
    return mock_conn

@pytest.fixture
def sample_raw_data():
    """Create a sample raw data file for testing."""
    df = pd.DataFrame(SAMPLE_ORDERS, columns=['order_id', 'customer', 'order_date', 'product', 'quantity', 'price'])
    df.to_csv('data/orders_file_raw.csv', index=False)
    return df

@pytest.fixture
def sample_cleaned_data():
    """Create a sample cleaned data file for testing."""
    df = pd.DataFrame(SAMPLE_ORDERS, columns=['order_id', 'customer', 'order_date', 'product', 'quantity', 'price'])
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['customer'] = df['customer'].str.strip().str.title()
    df['product'] = df['product'].str.strip().str.lower().str.title()
    df['total_cost'] = df['quantity'] * df['price']
    df.to_csv('data/orders_file_cleaned.csv', index=False)
    return df

def test_orders_file_raw(mock_postgres_connection):
    """Test the orders_file_raw asset."""
    # Create a proper context with the mock postgres connection
    context = build_op_context(resources={"postgres": mock_postgres_connection})
    
    # Execute the asset
    orders_file_raw(context)
    
    # Check if file was created
    assert os.path.exists('data/orders_file_raw.csv')
    
    # Check file contents
    df = pd.read_csv('data/orders_file_raw.csv')
    assert len(df) == len(SAMPLE_ORDERS)
    assert list(df.columns) == ['order_id', 'customer', 'order_date', 'product', 'quantity', 'price']

def test_orders_file_cleaned(sample_raw_data):
    """Test the orders_file_cleaned asset."""
    orders_file_cleaned()
    
    # Check if file was created
    assert os.path.exists('data/orders_file_cleaned.csv')
    
    # Check file contents
    df = pd.read_csv('data/orders_file_cleaned.csv')
    assert len(df) == len(SAMPLE_ORDERS)
    assert 'total_cost' in df.columns
    assert df['total_cost'].notnull().all()
    
    # Check data cleaning
    assert df['customer'].str.contains('^[A-Z][a-z]+ [A-Z][a-z]+$').all()  # Title case
    assert df['product'].str.contains('^[A-Z][a-z]+$').all()  # Title case

def test_check_orders_file_cleaned(sample_cleaned_data):
    """Test the check_orders_file_cleaned asset check."""
    result = check_orders_file_cleaned()
    assert result.passed

def test_orders_table(mock_postgres_connection, sample_cleaned_data):
    """Test the orders_table asset."""
    # Create a proper context with the mock postgres connection
    context = build_op_context(resources={"postgres": mock_postgres_connection})
    
    # Execute the asset
    orders_table(context)
    
    # Check if the cursor executed the expected queries
    mock_cursor = mock_postgres_connection.cursor.return_value
    mock_cursor.execute.assert_any_call("DROP TABLE IF EXISTS orders")
    mock_cursor.execute.assert_any_call("""
    CREATE TABLE orders (
        order_id VARCHAR(255) PRIMARY KEY,
        customer VARCHAR(255),
        order_date TIMESTAMP,
        product VARCHAR(255),
        quantity NUMERIC,
        price NUMERIC,
        total_cost NUMERIC
    )
    """)
    
    # Check if data was inserted
    assert mock_cursor.execute.call_count >= len(SAMPLE_ORDERS) + 2  # +2 for DROP and CREATE
    
    # Check if commit was called
    mock_postgres_connection.commit.assert_called_once()
