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

def get_top_5_most_expensive_products():
    conn = get_connection()
    cursor = conn.cursor()
    query = "SELECT product_name, unit_price FROM products ORDER BY unit_price DESC LIMIT 5;"
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_employees_with_most_orders():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT first_name || ' ' || last_name as employee_name, COUNT(order_id) as total_orders 
        FROM employees e 
        JOIN orders o ON e.employee_id = o.employee_id 
        GROUP BY employee_name 
        ORDER BY total_orders DESC 
        LIMIT 5;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_products_needing_reorder():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT product_name, units_in_stock, reorder_level 
        FROM products 
        WHERE units_in_stock <= reorder_level AND discontinued = 0
        ORDER BY units_in_stock ASC
        LIMIT 5;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_sales_per_year():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT EXTRACT(YEAR FROM order_date) as sales_year, COUNT(order_id) as total_orders
        FROM orders
        GROUP BY sales_year
        ORDER BY sales_year DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_customers_with_no_orders():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT company_name 
        FROM customers c 
        LEFT JOIN orders o ON c.customer_id = o.customer_id 
        WHERE o.order_id IS NULL;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_top_5_suppliers_by_products():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT s.company_name, COUNT(p.product_id) as product_count
        FROM suppliers s
        JOIN products p ON s.supplier_id = p.supplier_id
        GROUP BY s.company_name
        ORDER BY product_count DESC
        LIMIT 5;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_order_count_by_country():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT ship_country, COUNT(order_id) as total_orders
        FROM orders
        GROUP BY ship_country
        ORDER BY total_orders DESC
        LIMIT 10;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_average_freight_by_shipper():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT s.company_name, AVG(o.freight) as avg_freight
        FROM shippers s
        JOIN orders o ON s.shipper_id = o.ship_via
        GROUP BY s.company_name
        ORDER BY avg_freight DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def main():
    print("--- 1. Top 5 Customers by Orders ---")
    try:
        for row in get_top_5_customers_by_orders():
            print(f"{row[0]}: {row[1]} orders")
            
        print("\n--- 2. Total Sales by Category ---")
        for row in get_total_sales_by_category():
            print(f"{row[0]}: ${row[1]:.2f}")
            
        print("\n--- 3. Top 5 Most Expensive Products ---")
        for row in get_top_5_most_expensive_products():
            print(f"{row[0]}: ${row[1]:.2f}")
            
        print("\n--- 4. Top 5 Employees with Most Orders ---")
        for row in get_employees_with_most_orders():
            print(f"{row[0]}: {row[1]} orders")
            
        print("\n--- 5. Products Needing Reorder ---")
        for row in get_products_needing_reorder():
            print(f"{row[0]} (In Stock: {row[1]}, Reorder Level: {row[2]})")
            
        print("\n--- 6. Orders Per Year ---")
        for row in get_sales_per_year():
            print(f"Year {int(row[0])}: {row[1]} orders")
            
        print("\n--- 7. Customers With No Orders ---")
        for row in get_customers_with_no_orders():
            print(f"{row[0]}")
            
        print("\n--- 8. Top 5 Suppliers by Product Count ---")
        for row in get_top_5_suppliers_by_products():
            print(f"{row[0]}: {row[1]} products")
            
        print("\n--- 9. Top 10 Countries by Order Count ---")
        for row in get_order_count_by_country():
            print(f"{row[0]}: {row[1]} orders")
            
        print("\n--- 10. Average Freight by Shipper ---")
        for row in get_average_freight_by_shipper():
            print(f"{row[0]}: ${row[1]:.2f}")
            
    except Exception as e:
        print(f"Error querying database: {e}")

if __name__ == "__main__":
    main()
