import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        dbname="northwind"
    )

def get_top_5_customers_by_orders():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT c.company_name, COUNT(o.order_id) as total_orders
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.company_name
        ORDER BY total_orders DESC
        LIMIT 5;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_total_sales_by_category():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT c.category_name, SUM(od.unit_price * od.quantity * (1 - od.discount)) as total_sales
        FROM categories c
        JOIN products p ON c.category_id = p.category_id
        JOIN order_details od ON p.product_id = od.product_id
        GROUP BY c.category_name
        ORDER BY total_sales DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def main():
    print("--- Top 5 Customers by Orders ---")
    try:
        top_customers = get_top_5_customers_by_orders()
        for row in top_customers:
            print(f"{row[0]}: {row[1]} orders")
            
        print("\n--- Total Sales by Category ---")
        sales_by_category = get_total_sales_by_category()
        for row in sales_by_category:
            print(f"{row[0]}: ${row[1]:.2f}")
    except Exception as e:
        print(f"Error querying database: {e}")

if __name__ == "__main__":
    main()
