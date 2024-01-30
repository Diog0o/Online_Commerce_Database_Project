#!/usr/bin/python3
import os
from logging.config import dictConfig

import psycopg
from flask import flash
from flask import Flask
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool




# postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@postgres/db")

pool = ConnectionPool(conninfo=DATABASE_URL)
# the pool starts connecting immediately.

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
log = app.logger


@app.route("/", methods=("GET",))
@app.route("/products", methods=("GET",))
def account_index():
    """Show all the accounts, most recent first."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            accounts = cur.execute(
                """
                SELECT *
                FROM product

                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(accounts)

    return render_template("product/index.html", accounts=accounts)


@app.route("/products/<product_sku>/update", methods=["GET", "POST"])
def product_update(product_sku):
    """Update the product price and description."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            if request.method == "POST":
                new_price = request.form.get("price")
                new_description = request.form.get("description")
                cur.execute(
                    """
                    UPDATE product SET price = %s, description = %s WHERE SKU = %s
                    """,
                    (new_price, new_description, product_sku),
                )
                conn.commit()
            
            cur.execute(
                """
                SELECT * FROM product WHERE SKU = %s
                """,
                (product_sku,),
            )
            product = cur.fetchone()

    return render_template("product/update.html", product=product)





@app.route("/products/<product_sku>/delete", methods=["POST"])
def product_delete(product_sku):
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            # Delete the associated rows in the "supplier" table
            cur.execute(
                """
                DELETE FROM supplier WHERE SKU = %s
                """,
                (product_sku,),
            )
            conn.commit()

            # Delete the product from the database
            cur.execute(
                """
                DELETE FROM product WHERE SKU = %s
                """,
                (product_sku,),
            )
            conn.commit()


    return redirect(url_for("account_index"))





@app.route("/products/new", methods=["GET", "POST"])
def create_product():
    if request.method == "POST":
        # Get the form data
        sku = request.form.get("sku")
        name = request.form.get("name")
        description = request.form.get("description")
        price = request.form.get("price")
        ean = request.form.get("ean")

        # Insert the new product into the database
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO product (SKU, name, description, price, ean)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (sku, name, description, price, ean),
                )
                conn.commit()
               
                return redirect(url_for("account_index"))

    return render_template("product/create.html")



@app.route("/customers/new", methods=["GET", "POST"])
def create_customer():
    if request.method == "POST":
        # Get the form data
        name = request.form.get("name")
        email = request.form.get("email")
        phone = request.form.get("phone")
        address = request.form.get("address")

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Retrieve the last customer number
                cur.execute("SELECT MAX(cust_no) FROM customer")
                last_cust_no = cur.fetchone()[0]

                # Increment the last customer number by 1
                new_cust_no = last_cust_no + 1 if last_cust_no else 1

                cur.execute(
                    """
                    INSERT INTO customer(cust_no, name, email, phone, address)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (new_cust_no, name, email, phone, address),
                )
                conn.commit()

                return redirect(url_for("customer_index"))

    return render_template("customer/create.html")



# Update the "/customers" route to "/customers/index"
@app.route("/customers", methods=["GET"])
def customer_index():
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            customers = cur.execute(
                """
                SELECT *
                FROM customer
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(customers)

    return render_template("customer/index.html", customers=customers)

   


@app.route("/customers/<int:customer_id>/delete", methods=["POST"])
def customer_delete(customer_id):
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            # Delete the customer from the database
            cur.execute(
                """
                DELETE FROM customer WHERE cust_no = %s
                """,
                (customer_id,),
            )
            conn.commit()
           
    return redirect(url_for("customer_index"))


@app.route("/suppliers", methods=["GET"])
def supplier_index():
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            suppliers = cur.execute(
                """
                SELECT *
                FROM supplier
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} suppliers.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(suppliers)

    return render_template("supplier/index.html", suppliers=suppliers)

@app.route("/suppliers/new", methods=["GET", "POST"])
def create_supplier():
    if request.method == "POST":
        # Get the form data
        tin = request.form.get("tin")
        name = request.form.get("name")
        address = request.form.get("address")
        sku = request.form.get("sku")
        date = request.form.get("date")

        # Insert the new supplier into the database
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO supplier (TIN, name, address, SKU, date)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (tin, name, address, sku, date),
                )
                conn.commit()
               
                return redirect(url_for("supplier_index"))

    return render_template("supplier/create.html")


@app.route("/suppliers/<tin>/delete", methods=["POST"])
def delete_supplier(tin):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Delete the associated rows in the "delivery" table
            cur.execute(
                """
                DELETE FROM delivery WHERE TIN = %s
                """,
                (tin,),
            )
            conn.commit()

            # Delete the supplier from the database
            cur.execute(
                """
                DELETE FROM supplier WHERE TIN = %s
                """,
                (tin,),
            )
            conn.commit()

          
    return redirect(url_for("supplier_index"))

@app.route("/orders", methods=["GET"])
def order_index():
    """Show all the orders, most recent first."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            orders = cur.execute(
                """
                SELECT orders.order_no, orders.cust_no, orders.date,
                       CASE WHEN pay.order_no IS NOT NULL THEN 'Paid' ELSE 'Not Paid' END AS payment_status
                FROM orders
                LEFT JOIN pay ON orders.order_no = pay.order_no
                ORDER BY orders.date DESC
                """
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(orders)

    # Check if the order has already been paid and display a flash message
    

    return render_template("order/index.html", orders=orders)
@app.route("/orders/<order_no>/pay", methods=["POST"])
def pay_order(order_no):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Check if the order has already been paid
            cur.execute(
                """
                SELECT order_no FROM pay WHERE order_no = %s
                """,
                (order_no,),
            )
            existing_payment = cur.fetchone()

            if existing_payment:
                flash("Order has already been paid.")
            else:
                # Get the customer number associated with the order
                cur.execute(
                    """
                    SELECT cust_no FROM orders WHERE order_no = %s
                    """,
                    (order_no,),
                )
                cust_no = cur.fetchone()[0]

                # Insert the payment record into the database
                cur.execute(
                    """
                    INSERT INTO pay (order_no, cust_no) VALUES (%s, %s)
                    """,
                    (order_no, cust_no),
                )
                conn.commit()
                

    return redirect(url_for("order_index"))

@app.route("/orders/new", methods=["GET", "POST"])
def create_order():
    if request.method == "POST":
        # Get the form data
        cust_no = request.form.get("cust_no")
        date = request.form.get("date")

        # Retrieve the last order number
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(order_no) FROM orders")
                last_order_no = cur.fetchone()[0]

                # Increment the last order number by 1
                new_order_no = last_order_no + 1 if last_order_no else 1

                # Insert the new order into the database
                cur.execute(
                    """
                    INSERT INTO orders (order_no, cust_no, date)
                    VALUES (%s, %s, %s)
                    """,
                    (new_order_no, cust_no, date),
                )
                conn.commit()
               
                return redirect(url_for("order_index"))

    return render_template("order/create.html")


@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


if __name__ == "__main__":
    app.run()
