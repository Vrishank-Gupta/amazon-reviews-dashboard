import json
import os
import pymysql
from openai import OpenAI
import streamlit as st

from taxonomy import TAXONOMY


BATCH_SIZE = 5
MODEL = "gpt-4o-mini"


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
if not os.getenv("OPENAI_API_KEY"):
    raise RuntimeError("OPENAI_API_KEY is not set")



conn = pymysql.connect(
    host="localhost",
    user="llm_reader",
    password = st.secrets["mysql"]["password"],
    database="world",
    charset="utf8mb4"
)
cur = conn.cursor()



cur.execute("""
    SELECT r.review_id, r.asin, r.review
    FROM raw_reviews r
    LEFT JOIN review_tags t ON r.review_id = t.review_id
    WHERE t.review_id IS NULL
""")

rows = cur.fetchall()
print(f"Total untagged reviews: {len(rows)}")


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


for batch in chunks(rows, BATCH_SIZE):
    review_payload = [
        {"id": r[0], "text": r[2]}
        for r in batch
    ]

    prompt = f"""
You are a strict classification engine.

Allowed taxonomy:
{json.dumps(TAXONOMY, indent=2)}

Rules:
- Classify EACH review independently
- Only use provided categories
- Multiple categories allowed
- Return VALID JSON ONLY
- Do NOT add explanations

Expected output format:
{{
  "results": [
    {{
      "id": "review_id",
      "sentiment": "Positive | Neutral | Negative",
      "primary_categories": [],
      "sub_tags": []
    }}
  ]
}}

Reviews:
{json.dumps(review_payload, indent=2)}
"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You output strict JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        content = response.choices[0].message.content.strip()
        parsed = json.loads(content)

    except Exception as e:
        print(f"Batch failed: {e}")
        continue

    results = {r["id"]: r for r in parsed.get("results", [])}

    for review_id, asin, _ in batch:
        if review_id not in results:
            print(f"Missing result for {review_id}")
            continue

        r = results[review_id]

        cur.execute("""
            INSERT INTO review_tags
            (review_id, asin, sentiment, primary_categories, sub_tags)
            VALUES (%s,%s,%s,%s,%s)
        """, (
            review_id,
            asin,
            r.get("sentiment"),
            json.dumps(r.get("primary_categories", [])),
            json.dumps(r.get("sub_tags", []))
        ))

    conn.commit()
    print(f"Tagged batch of {len(batch)} reviews")


conn.close()
print("Batch tagging complete")