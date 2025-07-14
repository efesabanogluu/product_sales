import sqlite3
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_db(db_path: str):
    """Connect to the SQLite database located at db_path."""
    try:
        logging.info("Connecting to database at %s", db_path)
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        logging.error("Failed to connect to database: %s", e)
        raise 

def load_data(conn):
    """Load 'product' and 'sales' tables from the database into pandas DataFrames."""
    try:
        logging.info("Loading product and sales tables")
        product_df = pd.read_sql_query("SELECT * FROM product", conn)
        sales_df = pd.read_sql_query("SELECT * FROM sales", conn)
        return product_df, sales_df
    except Exception as e:
        logging.error("Error loading data from database: %s", e)
        raise

def preprocess_sales(sales_df):
    """Convert the 'orderdate_utc' column in sales DataFrame to datetime format."""
    try:
        logging.info("Preprocessing sales data")
        sales_df["orderdate_utc"] = pd.to_datetime(sales_df["orderdate_utc"])
        return sales_df
    except Exception as e:
        logging.error("Error preprocessing sales data: %s", e)
        raise

def create_date_range(start_date: datetime, end_date: datetime):
    """Create a daily date range from start_date to end_date inclusive."""
    try:
        logging.info("Creating date range from %s to %s", start_date, end_date)
        return pd.date_range(start=start_date, end=end_date)
    except Exception as e:
        logging.error("Error creating date range: %s", e)
        raise

def generate_all_combinations(product_df, date_range):
    """Generate all possible combinations of product SKUs and dates.

    This simulates a CROSS JOIN between products and dates to get every SKU for each date in the range.
    """
    try:
        logging.info("Generating all product-date combinations")
        all_combinations = pd.MultiIndex.from_product(
            [product_df["sku_id"], date_range],
            names=["sku_id", "date_id"]
        ).to_frame(index=False)
        return all_combinations
    except Exception as e:
        logging.error("Error generating all combinations: %s", e)
        raise


def aggregate_sales(sales_df):
    """Aggregate sales by sku_id and date (sum the 'sales' column).

    Extract the date part from 'orderdate_utc' and group sales accordingly.
    """
    try:
        logging.info("Aggregating sales data")
        agg_sales = (
            sales_df
            .assign(date_id=sales_df["orderdate_utc"].dt.date)  # Extract date part only
            .groupby(["sku_id", "date_id"], as_index=False)["sales"]
            .sum()
        )
        # Convert 'date_id' back to datetime for consistency with all_combinations
        agg_sales['date_id'] = pd.to_datetime(agg_sales['date_id'])
        return agg_sales
    except Exception as e:
        logging.error("Error aggregating sales: %s", e)
        raise

def build_revenue_table(all_combinations, product_df, agg_sales):
    """Build the final revenue DataFrame by combining products, dates, prices, and sales.

    Steps:
    - Merge all SKU-date combos with product prices
    - Merge the result with aggregated sales data (left join to keep all combos)
    - Fill missing sales with zero
    - Calculate revenue as price * sales
    - Sort results and format date for output
    """
    try:
        logging.info("Building revenue table")
        
        revenue_df = all_combinations.merge(product_df[["sku_id", "price"]], on="sku_id", how="left")
        revenue_df['date_id'] = pd.to_datetime(revenue_df['date_id'])
        
        revenue_df = revenue_df.merge(agg_sales, on=["sku_id", "date_id"], how="left")
        revenue_df["sales"] = revenue_df["sales"].fillna(0).astype(int)  # Replace NaN sales with 0
        revenue_df["revenue"] = revenue_df["price"] * revenue_df["sales"]
        
        revenue_df = revenue_df.sort_values(["sku_id", "date_id"])
        revenue_df["date_id"] = revenue_df["date_id"].astype(str)  # Convert date to string for display
        
        return revenue_df
    
    except Exception as e:
        logging.error("Error building revenue table: %s", e)
        raise

def save_revenue_to_db(revenue_df, conn):
    """Save the revenue DataFrame into the SQLite database as a new 'revenue' table."""
    try:
        logging.info("Saving revenue table to database")
        revenue_df[["sku_id", "date_id", "price", "sales", "revenue"]].to_sql("revenue", conn, if_exists="replace", index=False)
    except Exception as e:
        logging.error("Error saving revenue table to database: %s", e)
        raise

def main():
    db_path = "/path/to/your/product_sales.db"
    
    try:
        conn = connect_db(db_path)
        product_df, sales_df = load_data(conn)
        sales_df = preprocess_sales(sales_df)
        
        # Define the date range for January 2025
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 31)
        date_range = create_date_range(start_date, end_date)
        
        all_combinations = generate_all_combinations(product_df, date_range)
        agg_sales = aggregate_sales(sales_df)
        revenue_df = build_revenue_table(all_combinations, product_df, agg_sales)
        
        print(revenue_df.head())  # Print first few rows of the result for quick inspection
        
        save_revenue_to_db(revenue_df, conn)
        conn.close()
        logging.info("Process completed successfully")
    except Exception as e:
        logging.error("An error occurred during processing: %s", e)
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()
