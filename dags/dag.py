from airflow import DAG
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
import importlib
PythonOperator = None
try:
    mod = importlib.import_module('airflow.operators.python')
    PythonOperator = getattr(mod, 'PythonOperator')
except Exception:
    try:
        mod = importlib.import_module('airflow.operators.python_operator')
        PythonOperator = getattr(mod, 'PythonOperator')
    except Exception:

        class PythonOperator:  # type: ignore
            def __init__(self, *args, **kwargs):
                raise RuntimeError("PythonOperator is not available in this environment")
            # provide no-op dependency methods so static analysis won't warn
            def set_downstream(self, other):
                return None
            def set_upstream(self, other):
                return None
            def __rshift__(self, other):
                return other

from airflow.providers.postgres.hooks.postgres import PostgresHook




headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

def get_amazon_data_books(num_books, **kwargs):
    """Extract books from Amazon search pages and push to XCom as list of dicts."""
    ti = kwargs.get('ti')
    # Base URL for amazon books:
    base_url = "https://www.amazon.com/s?k=data+engineering+books"

    books = []
    seen_titles = set()
    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"

        # send request to the url
        response = requests.get(url, headers=headers)

        # check if the request was successful
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            book_containers = soup.find_all("div", {"class": "s-result-item"})

            for container in book_containers:
                title = container.find("span", {"class": "a-text-normal"})
                author = container.find("a", {"class": "a-size-base"})
                price = container.find("span", {"class": "a-price-whole"})
                rating = container.find("span", {"class": "a-icon-alt"})

                if title and author and price and rating:
                    book_title = title.get_text(strip=True)

                    if book_title not in seen_titles:
                        seen_titles.add(book_title)

                        books.append({
                            "title": book_title,
                            "author": author.get_text(strip=True),
                            "price": price.get_text(strip=True),
                            "rating": rating.get_text(strip=True)
                        })

            page += 1
        else:
            print(f"Failed to retrieve data from Amazon. Status code: {response.status_code}")
            break

    # Limit to the requested number of books
    books = books[:num_books]

    # convert to DataFrame
    df = pd.DataFrame(books)

    if not df.empty:
        df.drop_duplicates(subset=["title"], inplace=True)

    if ti:
        ti.xcom_push(key="amazonbooks", value=df.to_dict(orient="records"))
    else:
        # Fallback: return the data for direct-call testing
        return df.to_dict(orient="records")


# 3 Load data to Postgresql
def insert_book_data_into_postgres(**kwargs):
    """Pull book data from XCom and insert into Postgres using PostgresHook."""
    ti = kwargs.get('ti')
    book_data = ti.xcom_pull(key="amazonbooks", task_ids="fetch_amazon_books") if ti else None
    if not book_data:
        print("No data to insert.")
        return

    postgres_hook = PostgresHook(postgres_conn_id="books_connection")
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(
            insert_query,
            parameters=(book.get("title"), book.get("author"), book.get("price"), book.get("rating"))
        )


# Create table using PostgresHook (provider may not be installed for PostgresOperator)
def create_books_table(**kwargs):
    hook = PostgresHook(postgres_conn_id="books_connection")
    sql = """
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        authors VARCHAR(255),
        price VARCHAR(50),
        rating VARCHAR(10)
    );
    """
    hook.run(sql)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Fetch_and_store_amazon_books",
    default_args=default_args,
    description="A simple DAG to fetch book data from amazon pipeline",
)
# Set schedule separately to avoid some static-checker issues
dag.schedule_interval = '@daily'

fetch_amazon_books = PythonOperator(
    task_id="fetch_amazon_books",
    python_callable=get_amazon_data_books,
    op_args=[50],
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_books_table",
    python_callable=create_books_table,
    dag=dag,
)

insert_books_into_postgres = PythonOperator(
    task_id="insert_books_into_postgres",
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

# dependency: set with methods to avoid operator-overload warnings
create_table_task.set_downstream(fetch_amazon_books)
fetch_amazon_books.set_downstream(insert_books_into_postgres)
