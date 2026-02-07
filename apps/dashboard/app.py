from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
try:
    from st_aggrid import AgGrid, GridOptionsBuilder

    AGGRID_AVAILABLE = True
except ModuleNotFoundError:
    AGGRID_AVAILABLE = False

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
                SELECT run_date, sub_category, address, price_text, price_yen, area_sqm, area_tsubo,
                       detail_text,
                       unit_price_per_sqm, unit_price_per_tsubo
                FROM listings
                ORDER BY run_date
                """,
                con,
            )
        if "price_yen" in cols:
            return pd.read_sql_query(
                """
                SELECT run_date, sub_category, address, price_text, price_yen,
                       detail_text,
                       NULL as area_sqm, NULL as area_tsubo,
                       NULL as unit_price_per_sqm, NULL as unit_price_per_tsubo
                FROM listings
                ORDER BY run_date
                """,
                con,
            )
        return pd.read_sql_query(
            """
            SELECT run_date, sub_category, address, price_text, NULL as price_yen,
                   detail_text,
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


def extract_walk_minutes(text: str) -> float | None:
    t = normalize_text(text)
    if not t:
        return None
    vals = [int(x) for x in re.findall(r"徒歩\s*(\d+)\s*分", t)]
    if not vals:
        return None
    return float(min(vals))


st.title("奥沢駅 SUUMOダッシュボード")
st.caption("対象: 賃貸・戸建て(新築/中古)・土地")

latest = load_latest()
runs = load_runs()

if latest.empty:
    st.warning("データがありません。先に `python apps/scraper/suumo_scraper.py` を実行してください。")
    st.stop()

last_fetched = "-"
if "fetched_at" in latest.columns and latest["fetched_at"].notna().any():
    last_fetched = str(latest["fetched_at"].dropna().iloc[0])
elif "run_date" in latest.columns and latest["run_date"].notna().any():
    last_fetched = str(latest["run_date"].dropna().iloc[0])
st.metric("最終取得時間", last_fetched)
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

    detail_table = detail_view[
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
    ].copy()

    # Option filters from actual values in records.
    fcol1, fcol2 = st.columns(2)
    sub_options = sorted(detail_table["sub_category"].dropna().unique().tolist())
    with fcol1:
        selected_sub_categories = st.multiselect(
            "表示する sub_category",
            options=sub_options,
            default=sub_options,
        )

    filtered_table = detail_table.copy()
    if selected_sub_categories:
        filtered_table = filtered_table[filtered_table["sub_category"].isin(selected_sub_categories)]
    else:
        filtered_table = filtered_table.iloc[0:0]

    addr_options = sorted(filtered_table["address"].dropna().unique().tolist())
    with fcol2:
        selected_addresses = st.multiselect(
            "表示する address",
            options=addr_options,
            default=addr_options,
            format_func=short_address_label,
        )

    if selected_addresses:
        filtered_table = filtered_table[filtered_table["address"].isin(selected_addresses)]
    else:
        filtered_table = filtered_table.iloc[0:0]

    if filtered_table.empty:
        st.info("選択条件に一致するデータがありません。")
    elif AGGRID_AVAILABLE:
        gb = GridOptionsBuilder.from_dataframe(filtered_table)
        gb.configure_default_column(filter=True, floatingFilter=True, sortable=True, resizable=True)
        gb.configure_column("sub_category", filter="agSetColumnFilter")
        gb.configure_column("address", filter="agSetColumnFilter")
        grid_options = gb.build()
        AgGrid(
            filtered_table,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=False,
            enable_enterprise_modules=False,
            height=420,
            theme="streamlit",
        )
    else:
        st.warning("`st_aggrid` が未インストールです。`pip install streamlit-aggrid` 後に再起動してください。")
        st.dataframe(filtered_table, use_container_width=True, hide_index=True)

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
        if not runs.empty and "run_date" in runs.columns:
            all_dates = pd.to_datetime(runs["run_date"].dropna().unique())
        else:
            all_dates = hist["run_date"].dropna().unique()
        all_dates = pd.DatetimeIndex(sorted(all_dates))

        for cat in target_categories:
            cat_df = hist[hist["sub_category"] == cat].copy()
            label_map = {
                "土地": "土地",
                "戸建て(中古)": "戸建て（中古）",
                "戸建て(新築)": "戸建て（新築）",
            }
            label = label_map.get(cat, cat)
            if cat_df.empty:
                st.subheader(label)
                st.info("データがありません。")
                continue

            cat_df["address_label"] = cat_df["address"].map(short_address_label)
            mean_df = (
                cat_df.groupby(["run_date", "address_label"], as_index=False)["unit_price_per_tsubo"]
                .mean()
                .rename(columns={"unit_price_per_tsubo": "avg_tsubo_price_yen"})
            )
            pivot = mean_df.pivot(index="run_date", columns="address_label", values="avg_tsubo_price_yen")
            pivot = pivot.reindex(index=all_dates).sort_index()

            st.subheader(label)
            st.line_chart(pivot)
        # Okusawa 3-chome only: hue = walk minutes
        okusawa3 = cat_df[
            cat_df["address"]
            .fillna("")
            .map(lambda x: bool(re.search(r"奥沢\s*([3三])\s*(丁目|[-−ー])?", normalize_text(x))))
        ].copy()
        okusawa3["station_text"] = okusawa3["detail_text"].map(lambda x: detail_value(x, "沿線・駅"))
        okusawa3["walk_minutes"] = okusawa3["station_text"].map(extract_walk_minutes)
        okusawa3 = okusawa3.dropna(subset=["walk_minutes"])
        if not okusawa3.empty:
            wm_df = (
                okusawa3.groupby(["run_date", "walk_minutes"], as_index=False)["unit_price_per_tsubo"]
                .mean()
                .rename(columns={"unit_price_per_tsubo": "avg_tsubo_price_yen"})
            )
            wm_df["walk_label"] = wm_df["walk_minutes"].map(lambda x: f"徒歩{int(x)}分")
            wm_pivot = wm_df.pivot(index="run_date", columns="walk_label", values="avg_tsubo_price_yen")
            wm_pivot = wm_pivot.reindex(index=all_dates).sort_index()
            st.subheader(f"{label}（奥沢3丁目・徒歩分別）")
            st.line_chart(wm_pivot)
