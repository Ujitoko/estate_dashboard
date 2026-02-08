from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://suumo.jp"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

JST = ZoneInfo("Asia/Tokyo")


def now_jst() -> dt.datetime:
    return dt.datetime.now(tz=JST)


def today_jst() -> dt.date:
    return now_jst().date()


@dataclass
class CategoryConfig:
    category: str
    seed_url: str
    card_selector: str
    parser: Callable[[BeautifulSoup], list[dict]]
    max_pages: int = 8


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text or "")
    return re.sub(r"\s+", " ", text).strip()


def parse_jpy_amount(token: str) -> float | None:
    t = normalize_text(token).replace(",", "")
    if not t:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*億", t)
    if m:
        oku = float(m.group(1))
        man = 0.0
        m2 = re.search(r"億\s*(\d+(?:\.\d+)?)\s*万", t)
        if m2:
            man = float(m2.group(1))
        return oku * 100_000_000 + man * 10_000
    m = re.search(r"(\d+(?:\.\d+)?)\s*万", t)
    if m:
        return float(m.group(1)) * 10_000
    m = re.search(r"(\d+(?:\.\d+)?)\s*円", t)
    if m:
        return float(m.group(1))
    return None


def extract_price_yen(price_text: str) -> float | None:
    t = normalize_text(price_text).replace(",", "")
    if not t:
        return None
    parts = re.split(r"[~〜～]", t)
    vals = [parse_jpy_amount(p) for p in parts]
    vals = [v for v in vals if v is not None]
    if vals:
        return float(sum(vals) / len(vals))
    found: list[float] = []
    for token in re.findall(r"\d+(?:\.\d+)?\s*(?:億\d+(?:\.\d+)?万|億|万|円)", t):
        v = parse_jpy_amount(token)
        if v is not None:
            found.append(v)
    if not found:
        return None
    return float(max(found))


def extract_area_sqm(area_text: str) -> float | None:
    t = normalize_text(area_text).replace(",", "")
    if not t:
        return None
    # Supports m2 / m 2 / m² / ㎡
    vals = [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)\s*(?:m\s*2|m²|㎡)", t)]
    if vals:
        return float(sum(vals) / len(vals))
    return None


def extract_area_tsubo(area_text: str) -> float | None:
    t = normalize_text(area_text).replace(",", "")
    if not t:
        return None
    vals = [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)\s*坪", t)]
    if vals:
        return float(sum(vals) / len(vals))
    sqm = extract_area_sqm(t)
    if sqm is None:
        return None
    return sqm / 3.305785


def extract_layout_text(text: str) -> str:
    t = normalize_text(text)
    if not t:
        return ""
    # Remove area and keep the layout token (e.g. "3LDK", "ワンルーム")
    t = re.sub(r"\d+(?:\.\d+)?\s*(?:m\s*2|m²|㎡)", "", t)
    t = re.sub(r"\([^)]*\)", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def is_noisy_address(address: str) -> bool:
    a = normalize_text(address)
    if not a:
        return True
    if "の一部" in a:
        return True
    # Exclude lot-level addresses like "...奥沢7-22-13"
    if re.search(r"(奥沢|東玉川|田園調布|等々力)\s*\d+\s*-\s*\d+", a):
        return True
    return False


def absolute(url: str) -> str:
    return urljoin(BASE, url)


def fetch_html(session: requests.Session, url: str) -> str:
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def crawl_list_pages(session: requests.Session, seed_url: str, max_pages: int) -> list[str]:
    visited: set[str] = set()
    queue = [seed_url]

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        html = fetch_html(session, url)
        soup = BeautifulSoup(html, "html.parser")
        path_seed = urlparse(seed_url).path

        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href:
                continue
            nxt = absolute(href)
            pu = urlparse(nxt)
            if pu.netloc != urlparse(BASE).netloc:
                continue
            # keep only listing pagination pages around the same listing path
            if not pu.path.startswith(path_seed):
                continue
            if nxt not in visited and nxt not in queue:
                queue.append(nxt)

    return sorted(visited)


def parse_rent_page(soup: BeautifulSoup) -> list[dict]:
    rows: list[dict] = []
    for card in soup.select("div.cassetteitem"):
        title = normalize_text(card.select_one(".cassetteitem_content-title") and card.select_one(".cassetteitem_content-title").get_text(" ", strip=True) or "")
        addr = ""
        addr_li = card.select_one("li.cassetteitem_detail-col1")
        if addr_li:
            addr = normalize_text(addr_li.get_text(" ", strip=True))

        link = card.select_one("a[href*='/chintai/jnc_']")
        detail_url = absolute(link.get("href", "")) if link else ""

        for tr in card.select("table.cassetteitem_other tr.js-cassette_link"):
            tds = tr.select("td")
            if len(tds) < 4:
                continue
            # SUUMO rent row often includes [checkbox, thumbnail, floor, price/fee, deposit/key, layout/area, ...]
            if len(tds) >= 6:
                floor = normalize_text(tds[2].get_text(" ", strip=True))
                price_fee_raw = normalize_text(tds[3].get_text(" ", strip=True))
                deposit_key = normalize_text(tds[4].get_text(" ", strip=True))
                layout_area = normalize_text(tds[5].get_text(" ", strip=True))
            else:
                floor = normalize_text(tds[0].get_text(" ", strip=True))
                price_fee_raw = normalize_text(tds[1].get_text(" ", strip=True))
                deposit_key = normalize_text(tds[2].get_text(" ", strip=True))
                layout_area = normalize_text(tds[3].get_text(" ", strip=True))

            m_price = re.search(r"\d+(?:\.\d+)?\s*(?:億\d+(?:\.\d+)?万|億|万|円)", price_fee_raw)
            price_fee = normalize_text(m_price.group(0)) if m_price else price_fee_raw
            area_sqm = extract_area_sqm(layout_area)
            area_tsubo = extract_area_tsubo(layout_area)
            layout_text = extract_layout_text(layout_area)

            room_link = tr.select_one("a[href*='bc=']")
            room_url = absolute(room_link.get("href", "")) if room_link else detail_url
            listing_id = ""
            if room_url:
                m = re.search(r"bc=(\d+)", room_url)
                listing_id = m.group(1) if m else room_url

            rows.append(
                {
                    "category": "rent",
                    "sub_category": "賃貸",
                    "listing_id": listing_id,
                    "title": title,
                    "address": addr,
                    "price_text": price_fee,
                    "price_yen": extract_price_yen(price_fee),
                    "area_sqm": area_sqm,
                    "area_tsubo": area_tsubo,
                    "unit_price_per_sqm": None,
                    "unit_price_per_tsubo": None,
                    "layout_text": layout_text,
                    "detail_text": f"{floor} | {deposit_key} | {layout_area}",
                    "detail_url": room_url,
                }
            )
    return rows


def parse_baibai_page(soup: BeautifulSoup, sub_category: str) -> list[dict]:
    rows: list[dict] = []
    for card in soup.select("div.property_unit"):
        detail_map: dict[str, str] = {}
        for dl in card.select("dl"):
            dt_el = dl.select_one("dt")
            dd_el = dl.select_one("dd")
            if not dt_el or not dd_el:
                continue
            k = normalize_text(dt_el.get_text(" ", strip=True))
            v = normalize_text(dd_el.get_text(" ", strip=True))
            if k:
                detail_map[k] = v

        addr = detail_map.get("所在地", "")
        title = detail_map.get("物件名", "")
        price = detail_map.get("販売価格", "")
        area_text = detail_map.get("土地面積", "") or detail_map.get("建物面積", "") or detail_map.get("専有面積", "")
        area_sqm = extract_area_sqm(area_text)
        area_tsubo = extract_area_tsubo(area_text)
        layout_text = normalize_text(detail_map.get("間取り", ""))
        price_yen = extract_price_yen(price)
        unit_per_sqm = (price_yen / area_sqm) if (price_yen is not None and area_sqm and area_sqm > 0) else None
        unit_per_tsubo = (price_yen / area_tsubo) if (price_yen is not None and area_tsubo and area_tsubo > 0) else None

        link = card.select_one("a[href*='nc_']")
        detail_url = absolute(link.get("href", "")) if link else ""
        listing_id = ""
        if detail_url:
            m = re.search(r"/nc_(\d+)/", detail_url)
            listing_id = m.group(1) if m else detail_url

        rows.append(
            {
                "category": "sale",
                "sub_category": sub_category,
                "listing_id": listing_id,
                "title": title,
                "address": addr,
                "price_text": price,
                "price_yen": price_yen,
                "area_sqm": area_sqm,
                "area_tsubo": area_tsubo,
                "unit_price_per_sqm": unit_per_sqm,
                "unit_price_per_tsubo": unit_per_tsubo,
                "layout_text": layout_text,
                "detail_text": json.dumps(detail_map, ensure_ascii=False),
                "detail_url": detail_url,
            }
        )
    return rows


def parse_mansion_new(soup: BeautifulSoup) -> list[dict]:
    return parse_baibai_page(soup, "マンション(新築)")


def parse_mansion_used(soup: BeautifulSoup) -> list[dict]:
    return parse_baibai_page(soup, "マンション(中古)")


def parse_house_new(soup: BeautifulSoup) -> list[dict]:
    return parse_baibai_page(soup, "戸建て(新築)")


def parse_house_used(soup: BeautifulSoup) -> list[dict]:
    return parse_baibai_page(soup, "戸建て(中古)")


def parse_land(soup: BeautifulSoup) -> list[dict]:
    return parse_baibai_page(soup, "土地")


def build_configs() -> list[CategoryConfig]:
    return [
        CategoryConfig(
            category="rent",
            seed_url="https://suumo.jp/chintai/tokyo/ek_06660/",
            card_selector="div.cassetteitem",
            parser=parse_rent_page,
            max_pages=10,
        ),
        CategoryConfig(
            category="house_new",
            seed_url="https://suumo.jp/ikkodate/tokyo/ek_06660/",
            card_selector="div.property_unit",
            parser=parse_house_new,
            max_pages=10,
        ),
        CategoryConfig(
            category="house_used",
            seed_url="https://suumo.jp/chukoikkodate/tokyo/ek_06660/",
            card_selector="div.property_unit",
            parser=parse_house_used,
            max_pages=10,
        ),
        CategoryConfig(
            category="land",
            seed_url="https://suumo.jp/tochi/tokyo/ek_06660/",
            card_selector="div.property_unit",
            parser=parse_land,
            max_pages=10,
        ),
    ]


def parse_run_date(run_date: str | None) -> dt.date:
    if not run_date:
        return today_jst()
    return dt.date.fromisoformat(run_date)


def save_sqlite(df: pd.DataFrame, sqlite_path: Path, run_date: str) -> None:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(sqlite_path)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS listings (
                run_date TEXT NOT NULL,
                category TEXT NOT NULL,
                sub_category TEXT NOT NULL,
                listing_id TEXT,
                title TEXT,
                address TEXT,
                price_text TEXT,
                price_yen REAL,
                area_sqm REAL,
                area_tsubo REAL,
                unit_price_per_sqm REAL,
                unit_price_per_tsubo REAL,
                layout_text TEXT,
                detail_text TEXT,
                detail_url TEXT,
                PRIMARY KEY (run_date, sub_category, listing_id)
            )
            """
        )
        cols = {row[1] for row in con.execute("PRAGMA table_info(listings)").fetchall()}
        if "price_yen" not in cols:
            con.execute("ALTER TABLE listings ADD COLUMN price_yen REAL")
        if "area_sqm" not in cols:
            con.execute("ALTER TABLE listings ADD COLUMN area_sqm REAL")
        if "area_tsubo" not in cols:
            con.execute("ALTER TABLE listings ADD COLUMN area_tsubo REAL")
        if "unit_price_per_sqm" not in cols:
            con.execute("ALTER TABLE listings ADD COLUMN unit_price_per_sqm REAL")
        if "unit_price_per_tsubo" not in cols:
            con.execute("ALTER TABLE listings ADD COLUMN unit_price_per_tsubo REAL")
        if "layout_text" not in cols:
            con.execute("ALTER TABLE listings ADD COLUMN layout_text TEXT")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_date TEXT PRIMARY KEY,
                total_records INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        con.execute("DELETE FROM listings WHERE run_date = ?", (run_date,))
        if not df.empty:
            df.to_sql("listings", con, if_exists="append", index=False)

        con.execute(
            "INSERT OR REPLACE INTO runs(run_date,total_records,updated_at) VALUES(?,?,?)",
            (run_date, int(len(df)), now_jst().isoformat(timespec="seconds")),
        )
        con.commit()
    finally:
        con.close()


def run(output_dir: Path, run_date: dt.date | None = None) -> pd.DataFrame:
    session = requests.Session()
    all_rows: list[dict] = []

    for cfg in build_configs():
        urls = crawl_list_pages(session, cfg.seed_url, cfg.max_pages)
        for u in urls:
            html = fetch_html(session, u)
            soup = BeautifulSoup(html, "html.parser")
            if not soup.select(cfg.card_selector):
                continue
            rows = cfg.parser(soup)
            all_rows.extend(rows)

    columns = [
        "run_date",
        "fetched_at",
        "category",
        "sub_category",
        "listing_id",
        "title",
        "address",
        "price_text",
        "price_yen",
        "area_sqm",
        "area_tsubo",
        "unit_price_per_sqm",
        "unit_price_per_tsubo",
        "layout_text",
        "detail_text",
        "detail_url",
    ]
    run_dt = run_date or today_jst()
    run_date_str = run_dt.isoformat()
    fetched_at = now_jst().isoformat(timespec="seconds")

    df = pd.DataFrame(all_rows)
    if df.empty:
        df = pd.DataFrame(columns=columns)
    else:
        df["address"] = df["address"].fillna("").map(normalize_text)
        if "price_yen" not in df.columns:
            df["price_yen"] = df["price_text"].fillna("").map(extract_price_yen)
        df = df[~df["address"].map(is_noisy_address)].copy()
        df["run_date"] = run_date_str
        df["fetched_at"] = fetched_at
        # De-duplicate cross-posted listings by requested key:
        # sub_category + area + price + layout
        df["dedupe_area"] = pd.to_numeric(df.get("area_sqm"), errors="coerce").round(2)
        df["dedupe_price"] = pd.to_numeric(df.get("price_yen"), errors="coerce").round(0)
        df["dedupe_layout"] = df.get("layout_text", "").fillna("").map(normalize_text)
        df = df.drop_duplicates(subset=["sub_category", "dedupe_area", "dedupe_price", "dedupe_layout"])
        df = df[columns].drop_duplicates(subset=["sub_category", "listing_id", "detail_url"])

    output_dir.mkdir(parents=True, exist_ok=True)
    history_dir = output_dir.parent / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    latest_csv = output_dir / "listings_latest.csv"
    history_csv = history_dir / f"listings_{run_dt.strftime('%Y%m%d')}.csv"
    sqlite_path = output_dir / "suumo.db"

    df.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    df.to_csv(history_csv, index=False, encoding="utf-8-sig")
    to_db = df.drop(columns=["fetched_at"]) if "fetched_at" in df.columns else df
    save_sqlite(to_db, sqlite_path, run_date_str)

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="SUUMO scraper for Okusawa station pages")
    parser.add_argument("--output-dir", default="data/processed", help="Output directory")
    parser.add_argument("--run-date", default=None, help="Run date in YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    target_date = parse_run_date(args.run_date)
    df = run(output_dir, run_date=target_date)

    print(f"records={len(df)}")
    if not df.empty:
        print(df[["sub_category", "title", "price_text", "address"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
