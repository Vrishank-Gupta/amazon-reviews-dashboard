import streamlit as st
import pymysql
import pandas as pd
import json
import os
import subprocess
import time

st.set_page_config(
    page_title="Amazon VOC Dashboard",
    layout="wide"
)

st.title("Amazon Reviews – Voice of Customer")

@st.cache_resource
def get_connection():
    return pymysql.connect(
        host=st.secrets["mysql"]["host"],
        user=st.secrets["mysql"]["user"],
        password=st.secrets["mysql"]["password"],
        database=st.secrets["mysql"]["database"],
        charset="utf8mb4"
    )

conn = get_connection()

@st.cache_data(ttl=5)
def get_pipeline_status():
    q = """
        SELECT status, message, started_at, finished_at
        FROM pipeline_runs
        WHERE id = 1
    """
    return pd.read_sql(q, conn).iloc[0]

@st.cache_data(ttl=300)
def load_data():
    query = """
        SELECT
            r.review_id,
            r.asin,
            r.product_name,
            r.rating,
            r.review,
            r.review_url,
            r.scrape_date,
            t.sentiment,
            t.primary_categories,
            t.sub_tags
        FROM raw_reviews r
        JOIN review_tags t
        ON r.review_id = t.review_id
    """
    df = pd.read_sql(query, conn)

    # Convert JSON columns
    df["primary_categories"] = df["primary_categories"].apply(json.loads)
    df["sub_tags"] = df["sub_tags"].apply(json.loads)

    return df

df = load_data()

if df.empty:
    st.warning("No reviews found.")
    st.stop()


st.sidebar.header("🔎 Filters")

all_categories = sorted(
    {cat for cats in df["primary_categories"] for cat in cats}
)

selected_category = st.sidebar.selectbox(
    "Category",
    options=["All"] + all_categories
)

selected_sentiment = st.sidebar.multiselect(
    "Sentiment",
    options=["Positive", "Neutral", "Negative"],
    default=["Negative"]
)

all_products = sorted(df["product_name"].dropna().unique())

selected_products = st.sidebar.multiselect(
    "Product",
    options=all_products,
    default=all_products
)


filtered_df = df.copy()

if selected_category != "All":
    filtered_df = filtered_df[
        filtered_df["primary_categories"].apply(
            lambda cats: selected_category in cats
        )
    ]

filtered_df = filtered_df[
    filtered_df["sentiment"].isin(selected_sentiment)
]

filtered_df = filtered_df[
    filtered_df["product_name"].isin(selected_products)
]

st.sidebar.divider()
st.sidebar.header("⚙️ Data Pipeline")

pipeline = get_pipeline_status()

status = pipeline["status"]
st.sidebar.write(f"**Status:** {status}")

if status == "RUNNING":
    st.sidebar.info("Pipeline is running…")
    st.sidebar.write(f"Started at: {pipeline['started_at']}")
    time.sleep(2)
    st.rerun()

if st.sidebar.button("▶ Run Pipeline"):
    if status == "RUNNING":
        st.sidebar.warning("Pipeline already running")
    else:
        subprocess.Popen(
        ["python", "start_pipeline.py"],
        creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        st.sidebar.success("Pipeline started")
        time.sleep(1)
        st.rerun()

col1, col2, col3 = st.columns(3)

col1.metric("Total Reviews", len(filtered_df))
col2.metric(
    "Negative Reviews",
    len(filtered_df[filtered_df["sentiment"] == "Negative"])
)
col3.metric(
    "Unique Categories",
    filtered_df["primary_categories"].explode().nunique()
)

st.divider()


st.subheader("📊 Reviews by Category")

category_counts = (
    filtered_df
    .explode("primary_categories")
    .groupby("primary_categories")
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
)

st.bar_chart(
    category_counts.set_index("primary_categories")
)

st.divider()

st.subheader("📝 Tagged Reviews")

display_df = filtered_df[[
    "product_name",
    "rating",
    "sentiment",
    "review",
    "primary_categories",
    "sub_tags",
    "review_url",
    "scrape_date"
]]

st.dataframe(
    display_df,
    use_container_width=True,
    column_config={
        "review_url": st.column_config.LinkColumn(
            "Amazon Review",
            display_text="Open"
        )
    }
)