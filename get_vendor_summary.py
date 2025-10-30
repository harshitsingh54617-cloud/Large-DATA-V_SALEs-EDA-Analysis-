import sqlite3
import pandas as pd
import logging
import time
import os

# ===========================
# LOGGING CONFIGURATION
# ===========================
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler("logs/ingestion_db.log", mode="a")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logger.addHandler(console_handler)


# ===========================
# HELPER FUNCTIONS
# ===========================
def table_columns(conn, table_name):
    """Return list of columns for a given table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    return [row[1] for row in cursor.fetchall()]


def detect_product_column(columns):
    """Automatically detect which column represents product ID."""
    for candidate in ["ProductID", "ItemID", "SKU", "ProductCode"]:
        if candidate in columns:
            return candidate
    return None


def verify_and_create_tables(conn):
    """Verify required tables and create product_prices if missing."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing_tables = {t[0] for t in cursor.fetchall()}

    required_tables = ['vendor_invoice', 'purchases', 'sales', 'product_prices']
    missing = [t for t in required_tables if t not in existing_tables]

    # Create product_prices if missing
    if 'product_prices' in missing:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_prices (
            ProductID INT PRIMARY KEY,
            Brand INT,
            Price DECIMAL(10,2),
            Volume DECIMAL(10,2)
        );
        """)
        conn.commit()
        logger.info("Table 'product_prices' created automatically.")
        missing.remove('product_prices')

    if missing:
        raise Exception(f"Missing required tables: {', '.join(missing)}")

    logger.info("All required tables verified successfully.")


def create_vendor_summary(conn):
    """Build the vendor summary dynamically with auto-detected join column."""

    purchase_cols = table_columns(conn, "purchases")
    product_col = detect_product_column(purchase_cols)

    if product_col:
        join_clause = f"LEFT JOIN product_prices pp ON p.{product_col} = pp.ProductID"
        select_actual_price = "pp.Price AS ActualPrice"
        select_volume = "pp.Volume"
    else:
        join_clause = ""
        select_actual_price = "NULL AS ActualPrice"
        select_volume = "NULL AS Volume"

    query = f"""
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            {select_actual_price},
            {select_volume},
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        {join_clause}
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, ActualPrice, Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.*,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """

    return pd.read_sql_query(query, conn)


def clean_data(df):
    """Clean and transform the vendor summary DataFrame."""
    df = df.copy()
    df.fillna(0, inplace=True)

    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    df['VendorName'] = df['VendorName'].astype(str).str.strip()
    df['Description'] = df['Description'].astype(str).str.strip()

    # Compute business metrics
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = df['GrossProfit'] / df['TotalSalesDollars'].replace(0, 1)
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, 1)
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, 1)

    return df


def ingest_db(df, table_name, conn):
    """Insert the cleaned DataFrame into SQLite."""
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    logger.info(f"Data successfully ingested into table '{table_name}'.")


# ===========================
# MAIN SCRIPT
# ===========================
if __name__ == '__main__':
    total_start = time.time()

    try:
        conn = sqlite3.connect('inventory.db')
        logger.info('Database connection established.')

        # Step 1: Verify tables
        verify_and_create_tables(conn)

        # Step 2: Create Vendor Summary
        logger.info('Creating Vendor Summary...')
        start = time.time()
        summary_df = create_vendor_summary(conn)
        logger.info(f'Vendor Summary created in {time.time() - start:.2f} seconds.')

        # Step 3: Clean Data
        logger.info('Cleaning Vendor Summary Data...')
        start = time.time()
        clean_df = clean_data(summary_df)
        logger.info(f'Data cleaned in {time.time() - start:.2f} seconds.')

        # Step 4: Ingest into database
        logger.info('Ingesting cleaned data into database...')
        start = time.time()
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logger.info(f'Data ingestion completed in {time.time() - start:.2f} seconds.')

        # Step 5: Print sample output
        print("\n================ SAMPLE OUTPUT ================")
        print(clean_df.head(5).to_string(index=False))
        print("\n================ SUMMARY STATS ================")
        print(f"Total Vendors: {clean_df['VendorNumber'].nunique()}")
        print(f"Total Records: {len(clean_df)}")
        print(f"Total Gross Profit: ${clean_df['GrossProfit'].sum():,.2f}")
        print(f"Average Profit Margin: {clean_df['ProfitMargin'].mean() * 100:.2f}%")
        print("==============================================")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

    finally:
        conn.close()
        logger.info('Database connection closed.')
        logger.info(f'Total Script Runtime: {time.time() - total_start:.2f} seconds.')
