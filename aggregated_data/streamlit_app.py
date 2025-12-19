import os
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import logging
import plotly.express as px
from dotenv import load_dotenv

# ------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Streamlit page config
# ------------------------------------------------------------
st.set_page_config(
    page_title="US Accidents Analysis",
    layout="wide"
)

st.title("US Accidents Aggregation Dashboard")
st.write("State × Severity based accident statistics (from MongoDB accidents_aggregated)")

# ------------------------------------------------------------
# MongoDB connection 
# ------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    st.error("MONGO_URI not found in .env")
    st.stop()

DB_NAME = "bigdata_capstone"
AGG_COL = "accidents_aggregated"   # <-- THIS is your gold collection

# ------------------------------------------------------------
# Load data from MongoDB
# ------------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_data():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    agg_col = db[AGG_COL]

    data = list(agg_col.find())
    df = pd.DataFrame(data)
    return df

logger.info("Loading aggregated data from MongoDB")
df = load_data()

# ------------------------------------------------------------
# Basic checks
# ------------------------------------------------------------
if df.empty:
    st.error("No aggregated data found in accidents_aggregated.")
    st.stop()

# Drop MongoDB _id if you don’t want it displayed
if "_id" in df.columns:
    df = df.drop(columns=["_id"])

# Make sure numeric columns are numeric 
for c in ["accident_count", "avg_distance", "avg_temperature", "Severity"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

st.subheader("Preview of Aggregated Data")
st.dataframe(df.head(50), use_container_width=True)

# ------------------------------------------------------------
# Sidebar filters
# ------------------------------------------------------------
st.sidebar.header("Filters")

states = sorted(df["State"].dropna().unique().tolist())
selected_states = st.sidebar.multiselect(
    "Select State(s)",
    options=states,
    default=states[:5] if len(states) >= 5 else states
)

severity_levels = sorted(df["Severity"].dropna().unique().tolist())
selected_severity = st.sidebar.multiselect(
    "Select Severity Level(s)",
    options=severity_levels,
    default=severity_levels
)

filtered_df = df[
    (df["State"].isin(selected_states)) &
    (df["Severity"].isin(selected_severity))
]

if filtered_df.empty:
    st.warning("No results for your filter selection.")
    st.stop()

# ------------------------------------------------------------
# KPIs
# ------------------------------------------------------------
st.subheader("Key Metrics")
col1, col2, col3 = st.columns(3)

col1.metric("Total Accidents", int(filtered_df["accident_count"].sum()))
col2.metric("Average Distance (mi)", round(filtered_df["avg_distance"].mean(), 4))
col3.metric("Average Temperature (F)", round(filtered_df["avg_temperature"].mean(), 2))

# ------------------------------------------------------------
# Visualization 1: Accident Count by State & Severity
# ------------------------------------------------------------
st.subheader("Accident Count by State and Severity")

fig_count = px.bar(
    filtered_df,
    x="State",
    y="accident_count",
    color="Severity",
    barmode="group",
    title="Accident Count by State and Severity"
)
st.plotly_chart(fig_count, use_container_width=True)

# ------------------------------------------------------------
# Visualization 2: Avg Distance by Severity (boxplot)
# ------------------------------------------------------------
st.subheader("Average Accident Distance (mi) by Severity")

fig_distance = px.box(
    filtered_df,
    x="Severity",
    y="avg_distance",
    color="Severity",
    title="Average Accident Distance by Severity (aggregated across states)"
)
st.plotly_chart(fig_distance, use_container_width=True)

# ------------------------------------------------------------
# Visualization 3: Avg Temperature by Severity
# ------------------------------------------------------------
st.subheader("Average Temperature (F) by Severity")

temp_df = (
    filtered_df.groupby("Severity", as_index=False)["avg_temperature"]
    .mean()
    .sort_values("Severity")
)

fig_temp = px.line(
    temp_df,
    x="Severity",
    y="avg_temperature",
    markers=True,
    title="Average Temperature by Severity (mean across selected states)"
)
st.plotly_chart(fig_temp, use_container_width=True)

logger.info("Streamlit app rendered successfully")
