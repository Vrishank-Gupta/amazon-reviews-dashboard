# scraper.py
import time
import random
from datetime import date

def scrape_reviews_for_asin(driver, asin, product_name, max_pages=5):
    reviews = []
    today = date.today().isoformat()

    base_url = f"https://www.amazon.in/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_srt?sortBy=recent"
    driver.get(base_url)
    time.sleep(6)

    for page in range(max_pages):
        # progressive scroll
        for _ in range(4):
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(random.uniform(1.5, 2.5))

        page_reviews = driver.execute_script("""
            return Array.from(document.querySelectorAll('[data-hook="review"]')).map(r => {
                const id = r.getAttribute("id");
                const ratingEl = r.querySelector('[data-hook="review-star-rating"], [data-hook="cmps-review-star-rating"]');
                const titleEl = r.querySelector('[data-hook="review-title"]');
                const bodyEl  = r.querySelector('[data-hook="review-body"] span');
                const dateEl  = r.querySelector('[data-hook="review-date"]');

                return {
                    review_id: id,
                    rating: ratingEl ? ratingEl.innerText.trim() : "",
                    title: titleEl ? titleEl.innerText.trim() : "",
                    review: bodyEl ? bodyEl.innerText.trim() : "",
                    review_date: dateEl ? dateEl.innerText.trim() : "",
                    review_url: id ? "https://www.amazon.in/review/" + id : ""
                };
            });
        """)

        for r in page_reviews:
            if r["review"]:
                r["asin"] = asin
                r["product_name"] = product_name
                r["scrape_date"] = today
                reviews.append(r)

        # next page
        try:
            driver.execute_script("document.querySelector('li.a-last a').click();")
            time.sleep(5)
        except:
            break

    return reviews