import requests
import psycopg2

# Step 1: Fetch data from the API
response = requests.get("https://dummyjson.com/products")
data = response.json()
products = data["products"]

# Step 2: Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="9087"
)
cur = conn.cursor()

# Step 3: Insert each product (UPSERT = insert, or update if it already exists)
for p in products:
    cur.execute("""
        INSERT INTO staging.api_products (product_id, title, category, price, stock)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            title = EXCLUDED.title,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            stock = EXCLUDED.stock;
    """, (p["id"], p["title"], p["category"], p["price"], p["stock"]))

conn.commit()
cur.close()
conn.close()

print(f"Inserted/updated {len(products)} products.")