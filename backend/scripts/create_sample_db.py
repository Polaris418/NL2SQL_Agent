from __future__ import annotations

import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "backend" / "tests" / "fixtures" / "sample_ecommerce.db"


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.executescript(
            """
            DROP TABLE IF EXISTS reviews;
            DROP TABLE IF EXISTS orders;
            DROP TABLE IF EXISTS products;
            DROP TABLE IF EXISTS categories;
            DROP TABLE IF EXISTS users;

            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                city TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id INTEGER
            );

            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                category_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE reviews (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                rating INTEGER NOT NULL,
                comment TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

        cities = ["Shanghai", "Beijing", "Shenzhen", "Hangzhou", "Chengdu", "Guangzhou", "Nanjing", "Wuhan"]
        users = []
        for user_id in range(1, 31):
            city = cities[(user_id - 1) % len(cities)]
            users.append(
                (
                    user_id,
                    f"User {user_id}",
                    f"user{user_id}@example.com",
                    city,
                    f"2024-01-{(user_id % 28) + 1:02d}",
                )
            )

        categories = [
            (1, "Electronics", None),
            (2, "Home", None),
            (3, "Books", None),
            (4, "Sports", None),
            (5, "Fashion", None),
        ]

        products = []
        for product_id in range(1, 21):
            category_id = ((product_id - 1) % len(categories)) + 1
            products.append(
                (
                    product_id,
                    category_id,
                    f"Product {product_id}",
                    float(50 + product_id * 13),
                    20 + product_id,
                    f"2024-01-{(product_id % 28) + 1:02d}",
                )
            )

        orders = []
        for order_id in range(1, 121):
            user_id = ((order_id - 1) % len(users)) + 1
            product_id = ((order_id - 1) % len(products)) + 1
            quantity = (order_id % 4) + 1
            price = products[product_id - 1][3]
            orders.append(
                (
                    order_id,
                    user_id,
                    product_id,
                    quantity,
                    round(price * quantity, 2),
                    "completed" if order_id % 3 == 0 else ("shipped" if order_id % 3 == 1 else "paid"),
                    f"2024-02-{(order_id % 28) + 1:02d}",
                )
            )

        reviews = []
        for review_id in range(1, 121):
            user_id = ((review_id - 1) % len(users)) + 1
            product_id = ((review_id - 1) % len(products)) + 1
            reviews.append(
                (
                    review_id,
                    user_id,
                    product_id,
                    (review_id % 5) + 1,
                    f"Review {review_id}",
                    f"2024-03-{(review_id % 28) + 1:02d}",
                )
            )

        cur.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", users)
        cur.executemany("INSERT INTO categories VALUES (?, ?, ?)", categories)
        cur.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?)", products)
        cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", orders)
        cur.executemany("INSERT INTO reviews VALUES (?, ?, ?, ?, ?, ?)", reviews)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
