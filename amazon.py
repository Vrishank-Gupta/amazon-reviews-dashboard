import time
import random
import csv
import re
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


ASIN = "B0CGQXY29P"
MARKETPLACE = "in" 
MAX_PAGES = 10     
CUTOFF_DAYS = 30
CHROMEDRIVER_PATH = "C:\\chromedriver\\chromedriver.exe"
OUTPUT_FILE = f"{ASIN}_last_{CUTOFF_DAYS}_days_reviews.csv"



def human_sleep(a=3, b=6):    # need to check this logic
    time.sleep(random.uniform(a, b))


cutoff_date = datetime.now() - timedelta(days=CUTOFF_DAYS)
print(f"Scraping reviews newer than: {cutoff_date.date()}")



options = Options()
options.add_argument("--start-maximized")
options.add_argument("--user-data-dir=C:\\amazon_profile")
# options.add_argument("--headless=new")
# options.add_argument("--disable-gpu")
# options.add_argument("--window-size=1920,1080")

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)

url = f"https://www.amazon.{MARKETPLACE}/product-reviews/{ASIN}/ref=cm_cr_arp_d_viewopt_srt?sortBy=recent"
driver.get(url)
human_sleep(6, 8) #this might cause issue in headless mode


try:
    driver.execute_script("""   
        const btn = document.querySelector('#sp-cc-accept'); 
        if (btn) btn.click();
    """)
    human_sleep(2, 3)
    print("Cookie consent handled")
except:
    pass


reviews_data = []
stop_scraping = False



for page in range(1, MAX_PAGES + 1):
    print(f"\n Scraping page {page}...")

    # progressive scrolling
    for _ in range(4):
        driver.execute_script("window.scrollBy(0, 800);")
        human_sleep(1.5, 2.5)

    # extraction logic
    reviews = driver.execute_script("""
        return Array.from(document.querySelectorAll('[data-hook="review"]')).map(r => {
            const ratingEl = r.querySelector('[data-hook="review-star-rating"], [data-hook="cmps-review-star-rating"]');
            const titleEl  = r.querySelector('[data-hook="review-title"]');
            const bodyEl   = r.querySelector('[data-hook="review-body"] span');
            const dateEl   = r.querySelector('[data-hook="review-date"]');

            return {
                rating: ratingEl ? ratingEl.innerText.trim() : "",
                title:  titleEl  ? titleEl.innerText.trim()  : "",
                review: bodyEl   ? bodyEl.innerText.trim()   : "",
                date:   dateEl   ? dateEl.innerText.trim()   : "",
            };
        });
    """)

    print(f"Found {len(reviews)} reviews on this page")

    if not reviews:
        print("No reviews returned. Stopping.")
        break

    for r in reviews:
        if not r["review"] or not r["date"]:
            continue

        # Example: "Reviewed in India on 9 February 2026"
        match = re.search(r"on (\d{1,2} .+ \d{4})", r["date"]) 
        if not match:
            continue

        review_date = datetime.strptime(match.group(1), "%d %B %Y")

        #  STOP CONDITION
        if review_date < cutoff_date:
            stop_scraping = True
            print(f"Hit review older than cutoff: {review_date.date()}")
            break

        reviews_data.append({
            "rating": r["rating"],
            "title": r["title"],
            "review": r["review"],
            "date": review_date.strftime("%Y-%m-%d")
        })

    if stop_scraping:
        print("Stopping pagination (older reviews reached).")
        break

    # go to next page
    try:
        driver.execute_script(
            "document.querySelector('li.a-last a').click();"
        )
        human_sleep(4, 6)
    except:
        print("No next page button. Ending.")
        break


#writing to csv
if reviews_data:
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["rating", "title", "review", "date"]
        )
        writer.writeheader()
        writer.writerows(reviews_data)

    print(f"\n Saved {len(reviews_data)} reviews to {OUTPUT_FILE}")
else:
    print("\n Found 0 reviews")


input("\nPress enter...")
driver.quit()
