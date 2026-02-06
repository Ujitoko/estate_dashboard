# SUUMO 奥沢駅 収集 + ダッシュボード

1日1回、SUUMOから奥沢駅の以下カテゴリを収集し、ダッシュボードで可視化します。

- 賃貸
- 戸建て(新築)
- 戸建て(中古)
- 土地

固定URL:

- 新築戸建て: `https://suumo.jp/ikkodate/tokyo/ek_06660/`
- 中古戸建て: `https://suumo.jp/chukoikkodate/tokyo/ek_06660/`
- 賃貸: `https://suumo.jp/chintai/tokyo/ek_06660/`
- 土地: `https://suumo.jp/tochi/tokyo/ek_06660/`

## セットアップ

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 手動実行

```powershell
python apps/scraper/suumo_scraper.py --output-dir data/processed
```

出力:

- `data/processed/listings_latest.csv` 最新結果
- `data/history/listings_YYYYMMDD.csv` 日次スナップショット
- `data/processed/suumo.db` 履歴DB

## クラウド運用 (無料)

### 1. GitHub Actions で日次スクレイプ

- ワークフロー: `.github/workflows/daily.yml`
- 実行時刻: 毎日 JST 06:30（cronはUTCで `30 21 * * *`）
- 内容: スクレイプ実行 → `data/processed` と `data/history` をコミット

手動実行:

- GitHub の `Actions` タブで `Daily SUUMO Scrape` を `Run workflow`

### 2. Streamlit Community Cloud で公開

1. Streamlit Community Cloud に GitHub 連携
2. このリポジトリを選択
3. Main file path を `apps/dashboard/app.py` に設定
4. Deploy

## ローカル定期実行 (Windows Task Scheduler)

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_task.ps1 -Time "06:30"
```

タスク削除:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\unregister_task.ps1
```

## ローカルダッシュボード

```powershell
streamlit run apps/dashboard/app.py
```

## 注意

- 取得対象は公開一覧ページ情報です。
- サイト仕様変更でセレクタが変わると取得できなくなるため、その場合は `apps/scraper/suumo_scraper.py` を更新してください。
- GitHub Actions の schedule は遅延する場合があります。
