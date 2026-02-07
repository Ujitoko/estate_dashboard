from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="奥沢駅 SUUMOダッシュボード", layout="wide")

BASE_DIR = Path(__file__).resolve().parents[2]
LATEST_CSV = BASE_DIR / "data" / "processed" / "listings_latest.csv"
SQLITE_PATH = BASE_DIR / "data" / "processed" / "suumo.db"


@st.cache_data(ttl=300)
def load_latest() -> pd.DataFrame:
    if not LATEST_CSV.exists():
        return pd.DataFrame()
    return pd.read_csv(LATEST_CSV, encoding="utf-8-sig")


@st.cache_data(ttl=300)
def load_runs() -> pd.DataFrame:
    if not SQLITE_PATH.exists():
        return pd.DataFrame()
    con = sqlite3.connect(SQLITE_PATH)
    try:
        return pd.read_sql_query("SELECT run_date, total_records, updated_at FROM runs ORDER BY run_date DESC", con)
    finally:
        con.close()


@st.cache_data(ttl=300)
def load_history_listings() -> pd.DataFrame:
    if not SQLITE_PATH.exists():
        return pd.DataFrame()
    con = sqlite3.connect(SQLITE_PATH)
    try:
        cols = {row[1] for row in con.execute("PRAGMA table_info(listings)").fetchall()}
        if "unit_price_per_tsubo" in cols:
            return pd.read_sql_query(
                """
                SELECT run_date, sub_category, price_text, price_yen, area_sqm, area_tsubo,
                       unit_price_per_sqm, unit_price_per_tsubo
                FROM listings
                ORDER BY run_date
                """,
                con,
            )
        if "price_yen" in cols:
            return pd.read_sql_query(
                """
                SELECT run_date, sub_category, price_text, price_yen,
                       NULL as area_sqm, NULL as area_tsubo,
                       NULL as unit_price_per_sqm, NULL as unit_price_per_tsubo
                FROM listings
                ORDER BY run_date
                """,
                con,
            )
        return pd.read_sql_query(
            """
            SELECT run_date, sub_category, price_text, NULL as price_yen,
                   NULL as area_sqm, NULL as area_tsubo,
                   NULL as unit_price_per_sqm, NULL as unit_price_per_tsubo
            FROM listings
            ORDER BY run_date
            """,
            con,
        )
    finally:
        con.close()


def detail_value(detail_text: str, key: str) -> str:
    if not isinstance(detail_text, str) or not detail_text:
        return ""
    if not detail_text.startswith("{"):
        return ""
    try:
        obj = json.loads(detail_text)
    except Exception:
        return ""
    return str(obj.get(key, ""))


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text or "")
    return re.sub(r"\s+", " ", text).strip()


def short_address_label(address: str) -> str:
    a = normalize_text(address)
    if a.startswith("東京都"):
        return a[len("東京都") :]
    return a


def extract_area_sqm(text: str) -> float | None:
    t = normalize_text(text).replace(",", "")
    if not t:
        return None
    vals = [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)\s*(?:m\s*2|m²|㎡)", t)]
    if vals:
        return float(sum(vals) / len(vals))
    return None


def extract_area_tsubo(text: str) -> float | None:
    t = normalize_text(text).replace(",", "")
    if not t:
        return None
    vals = [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)\s*坪", t)]
    if vals:
        return float(sum(vals) / len(vals))
    sqm = extract_area_sqm(t)
    if sqm is None:
        return None
    return sqm / 3.305785


st.title("奥沢駅 SUUMOダッシュボード")
st.caption("対象: 賃貸・戸建て(新築/中古)・土地")

latest = load_latest()
runs = load_runs()

if latest.empty:
    st.warning("データがありません。先に `python apps/scraper/suumo_scraper.py` を実行してください。")
    st.stop()

last_date = latest["run_date"].iloc[0] if "run_date" in latest.columns else "-"
st.metric("最新取得日", last_date)
st.metric("最新件数", int(len(latest)))

c1, c2 = st.columns([1, 2])
with c1:
    st.subheader("カテゴリ件数")
    summary = (
        latest.groupby("sub_category", as_index=False)
        .size()
        .sort_values("size", ascending=False)
        .rename(columns={"size": "件数"})
    )
    st.dataframe(summary, use_container_width=True, hide_index=True)

with c2:
    st.subheader("履歴")
    if runs.empty:
        st.info("履歴はまだありません。")
    else:
        st.dataframe(runs, use_container_width=True, hide_index=True)

st.subheader("最新物件一覧")
sub_types = ["すべて"] + sorted(latest["sub_category"].dropna().unique().tolist())
selected = st.selectbox("sub_category", sub_types, index=0)

address_options = ["すべて"] + sorted(latest["address"].dropna().unique().tolist())
selected_address = st.selectbox("address", address_options, index=0)

view = latest.copy()
if selected != "すべて":
    view = view[view["sub_category"] == selected]
if selected_address != "すべて":
    view = view[view["address"] == selected_address]

show_cols = [
    c
    for c in ["sub_category", "title", "price_text", "address", "detail_url", "fetched_at"]
    if c in view.columns
]
st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

st.subheader("土地・戸建て 詳細")
detail_view = latest[latest["sub_category"].isin(["土地", "戸建て(新築)", "戸建て(中古)"])].copy()
if detail_view.empty:
    st.info("土地・戸建てのデータがありません。")
else:
    for c in ["area_sqm", "area_tsubo", "unit_price_per_sqm", "unit_price_per_tsubo", "price_yen"]:
        if c not in detail_view.columns:
            detail_view[c] = None

    detail_view["沿線・駅"] = detail_view["detail_text"].map(lambda x: detail_value(x, "沿線・駅"))
    detail_view["土地面積"] = detail_view["detail_text"].map(lambda x: detail_value(x, "土地面積"))
    detail_view["建物面積"] = detail_view["detail_text"].map(lambda x: detail_value(x, "建物面積"))
    detail_view["専有面積"] = detail_view["detail_text"].map(lambda x: detail_value(x, "専有面積"))
    detail_view["間取り"] = detail_view["detail_text"].map(lambda x: detail_value(x, "間取り"))
    detail_view["建ぺい率・容積率"] = detail_view["detail_text"].map(lambda x: detail_value(x, "建ぺい率・容積率"))

    area_text_fallback = (
        detail_view["土地面積"].fillna("")
        + " "
        + detail_view["建物面積"].fillna("")
        + " "
        + detail_view["専有面積"].fillna("")
    )

    area_sqm_raw = pd.to_numeric(detail_view["area_sqm"], errors="coerce")
    area_tsubo_raw = pd.to_numeric(detail_view["area_tsubo"], errors="coerce")
    area_sqm_fb = area_text_fallback.map(extract_area_sqm)
    area_tsubo_fb = area_text_fallback.map(extract_area_tsubo)

    detail_view["面積(m2)"] = area_sqm_raw.fillna(area_sqm_fb).round(2)
    detail_view["面積(坪)"] = area_tsubo_raw.fillna(area_tsubo_fb).round(2)

    price_yen = pd.to_numeric(detail_view["price_yen"], errors="coerce")
    unit_sqm_raw = pd.to_numeric(detail_view["unit_price_per_sqm"], errors="coerce")
    unit_tsubo_raw = pd.to_numeric(detail_view["unit_price_per_tsubo"], errors="coerce")
    unit_sqm_fb = price_yen / detail_view["面積(m2)"]
    unit_tsubo_fb = price_yen / detail_view["面積(坪)"]

    detail_view["平米単価(円/m2)"] = unit_sqm_raw.fillna(unit_sqm_fb).round(0)
    detail_view["坪単価(円/坪)"] = unit_tsubo_raw.fillna(unit_tsubo_fb).round(0)

    st.dataframe(
        detail_view[
            [
                "sub_category",
                "title",
                "price_text",
                "address",
                "沿線・駅",
                "土地面積",
                "建物面積",
                "面積(m2)",
                "面積(坪)",
                "平米単価(円/m2)",
                "坪単価(円/坪)",
                "間取り",
                "建ぺい率・容積率",
                "detail_url",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("平均坪単価の時系列")
hist = load_history_listings()
if hist.empty:
    st.info("時系列データがありません。")
else:
    target_categories = ["土地", "戸建て(中古)", "戸建て(新築)"]
    hist = hist[hist["sub_category"].isin(target_categories)].copy()
    hist = hist.dropna(subset=["unit_price_per_tsubo"])
    if hist.empty:
        st.info("坪単価データがありません。次回スクレイプ以降に表示されます。")
    else:
        hist["run_date"] = pd.to_datetime(hist["run_date"])
        mean_df = (
            hist.groupby(["run_date", "sub_category"], as_index=False)["unit_price_per_tsubo"]
            .mean()
            .rename(columns={"unit_price_per_tsubo": "avg_tsubo_price_yen"})
        )
        if not runs.empty and "run_date" in runs.columns:
            all_dates = pd.to_datetime(runs["run_date"].dropna().unique())
        else:
            all_dates = mean_df["run_date"].dropna().unique()
        all_dates = pd.DatetimeIndex(sorted(all_dates))

        pivot = mean_df.pivot(index="run_date", columns="sub_category", values="avg_tsubo_price_yen")
        pivot = pivot.reindex(index=all_dates, columns=target_categories).sort_index()
        st.line_chart(pivot)

        yen_view = mean_df.copy()
        yen_view["平均坪単価(万円/坪)"] = (yen_view["avg_tsubo_price_yen"] / 10_000).round(1)
        st.dataframe(
            yen_view[["run_date", "sub_category", "平均坪単価(万円/坪)"]],
            use_container_width=True,
            hide_index=True,
        )

st.subheader("住所ごとの平均単価（最新）")
addr_view = latest[latest["sub_category"].isin(["土地", "戸建て(中古)", "戸建て(新築)"])].copy()
if addr_view.empty:
    st.info("売買データがありません。")
else:
    for c in ["area_sqm", "area_tsubo", "unit_price_per_sqm", "unit_price_per_tsubo", "price_yen"]:
        if c not in addr_view.columns:
            addr_view[c] = None

    # Fallback: compute unit prices from price/area when missing.
    sqm = pd.to_numeric(addr_view["area_sqm"], errors="coerce")
    tsubo = pd.to_numeric(addr_view["area_tsubo"], errors="coerce")
    price = pd.to_numeric(addr_view["price_yen"], errors="coerce")
    unit_sqm = pd.to_numeric(addr_view["unit_price_per_sqm"], errors="coerce").fillna(price / sqm)
    unit_tsubo = pd.to_numeric(addr_view["unit_price_per_tsubo"], errors="coerce").fillna(price / tsubo)

    addr_view["unit_price_per_sqm"] = unit_sqm
    addr_view["unit_price_per_tsubo"] = unit_tsubo

    addr_summary = (
        addr_view.groupby("address", as_index=False)[["unit_price_per_sqm", "unit_price_per_tsubo"]]
        .mean()
        .rename(
            columns={
                "unit_price_per_sqm": "平均平米単価(円/m2)",
                "unit_price_per_tsubo": "平均坪単価(円/坪)",
            }
        )
        .sort_values("平均坪単価(円/坪)", ascending=False)
    )
    addr_summary["address_label"] = addr_summary["address"].map(short_address_label)
    addr_summary["平均平米単価(万円/m2)"] = (addr_summary["平均平米単価(円/m2)"] / 10_000).round(2)
    addr_summary["平均坪単価(万円/坪)"] = (addr_summary["平均坪単価(円/坪)"] / 10_000).round(2)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**平均坪単価（万円/坪）**")
        st.bar_chart(addr_summary.set_index("address_label")["平均坪単価(万円/坪)"])
    with c2:
        st.markdown("**平均平米単価（万円/m2）**")
        st.bar_chart(addr_summary.set_index("address_label")["平均平米単価(万円/m2)"])

    st.dataframe(
        addr_summary[
            [
                "address",
                "平均坪単価(万円/坪)",
                "平均平米単価(万円/m2)",
                "平均坪単価(円/坪)",
                "平均平米単価(円/m2)",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
