import csv
import os
import pymysql
import streamlit as st


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from scraper import scrape_reviews_for_asin

print("MYSQL_PASSWORD seen by Python:", os.getenv("MYSQL_PASSWORD"))
conn = pymysql.connect(
    host="127.0.0.1",
    port=3306,
    user="llm_reader",
    password=st.secrets["mysql"]["password"],
    database="world",
    charset="utf8mb4"
)
cur = conn.cursor()


options = Options()
options.add_argument("--start-maximized")
options.add_argument(r"--user-data-dir=C:\amazon_profile")

driver = webdriver.Chrome(
    service=Service("C:\\chromedriver\\chromedriver.exe"),
    options=options
)


with open("asins.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(f"Scraping ASIN: {row['asin']}")
        asin = row["asin"]
        product_name = row["product_name"]


        reviews = scrape_reviews_for_asin(
            driver,
            row["asin"],
            row["product_name"]
        )

        for r in reviews:
            cur.execute(
                """
                INSERT IGNORE INTO raw_reviews
                (review_id, asin, product_name, rating, title, review,
                 review_date, review_url, scrape_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    r["review_id"],
                    asin,
                    product_name,
                    r["rating"],
                    r["title"],
                    r["review"],
                    r["review_date"],
                    r["review_url"],
                    r["scrape_date"],
                )
            )

        conn.commit()
        print(f"Inserted {len(reviews)} reviews for {row['asin']}")



driver.quit()
conn.close()