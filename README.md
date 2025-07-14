# Revenue Creator

This project builds a daily revenue table from a SQLite database containing product and sales data. The revenue table combines all possible product-date combinations, actual sales, and revenue calculations. The same logic is also implemented in SQL.

## Project Structure
    
      ├── revenue_creator.py            # Main Python data pipeline
      ├── revenue_creator.sql           # Equivalent SQL query
      ├── tests/
      │   └── test_revenue_creator.py   # Unit tests for Python pipeline
      ├── README.md                     # Project documentation
      └── .gitignore
    

## Features

- 📊 Load product and sales data from a SQLite database
- 📅 Generate all product-date combinations using cross joins
- ➕ Aggregate daily sales per product
- 💰 Calculate daily revenue (price × sales)
- 💾 Save the resulting revenue table back to the database
- 🧪 Includes unit tests for core pipeline components
- 🛠️ SQL script version included for database-only environments

## Requirements

- Python 3.8+
- pandas
- sqlite3 (Python built-in)

## Setup

1. **Clone the repository:**

   ```bash
   git clone <your-repo-url>
   cd <your-repo-directory>

2. **Create and activate a virtual environment (optional but recommended):**

   ```bash
   python -m venv .venv
   source .venv/bin/activate

3. **Install project dependencies::**

    ```bash
    pip install pandas


4. **Specify your SQLite database path:**

   ***In revenue_creator.py, locate the following line inside the main() function. Replace it with the actual path of your SQLite database file:*** 
     ```bash
     # db_path = "/path/to/your/product_sales.db"
     db_path = "C:/Users/your_username/Documents/product_sales.db"

5. **Run the project:**

   ```bash
   python revenue_creator.py

6. **Run unit tests:**

   ```bash
   python -m unittest discover tests

## SQL File: revenue_creator.sql
This SQL script provides an alternative way to generate the revenue report directly inside SQLite.
Usage: Run the SQL script in your SQLite client
