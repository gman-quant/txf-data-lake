# txf-data-lake — AI Agent 指南

Shioaji API → `D:/txf-data` parquet 資料湖的 ETL。**本 repo 是湖內 kbar / raw tick 的唯一寫入者**——但注意兩個合法的外部寫入:txf-gale-engine 每日 sync 會把 `{date}_TXF_bidask.parquet` 寫進 `raw_ticks/`;txf-quant-platform viewer 寫 DATA_ROOT 根的 `user_settings.json`。**這些不是髒資料,別清。** 也含舊版看盤 `view_chart.py`(已被 quant-platform viewer 取代,仍可用)。

## 文件權威序

程式碼 > `notes/txf-data-lake-reference.md`(~70KB 權威技術參考,2026-06-16 快照 —— 模組內部/schema/問題清單 I-SEC..I-16 都在這;**引用前注意它未反映之後的修正**:週日/幻影守衛、validate_lake、.env 註解已清)> `README.md`(金鑰指引已於 2026-07-04 修正為 .env + python-dotenv)> `AutoRun.md`(**純歷史**:主體是已退役的 macOS launchd,標頭有現行 Windows 排程摘要)。

## 環境與執行

- 一律 `.venv\Scripts\python.exe`(Python 3.13;2026-07 工作站統一到 3.13)+ 前綴 `PYTHONUTF8=1`(main_etl/view_chart/fix_kbars **沒有**自我 reconfigure stdout,cp950 會炸)。
- 設定在 `config/settings.py`:DATA_ROOT(env 覆寫,預設 D:\txf-data)、TIMEFRAMES=[5s,1m,5m,1h,1d]、金鑰從 `.env` 載入。
- `.env` 含 API 金鑰(從第一個 commit 起即 gitignore,**未進 git 史**)。**絕不讀取、絕不印出、絕不 commit。**

```bash
cd /c/Projects/TXF-Trading-Workspace/txf-data-lake
PYTHONUTF8=1 .venv/Scripts/python.exe main_etl.py --date 2026-07-03   # 單日 ETL(唯一參數)
PYTHONUTF8=1 .venv/Scripts/python.exe validate_lake.py --date 2026-07-03  # 驗證;--all 全庫
```

- `batch_run.py` 回補歷史:**無 CLI 參數**,改檔內 START/END 常數;Shioaji 有日配額,一次 2-3 個月。
- 重建 kbar:`fix_kbars.py`(1d 以外、從 raw 重算)。⚠️ `scripts/etl_true_vwap.py` 的 1d 路徑**沒有 (date,session) 去重**,亂跑會把 1d 重複列 bug 種回去。
- `view_chart.py` 會開 GUI 視窗,agent 別無人值守啟動;py_compile 是唯一安全煙測。

## 資料湖事實(判斷髒資料前必讀)

- 佈局:`raw_ticks/{sym}/{yyyy}/{mm}/`(月分層)、`kbars/{tf}/{sym}/{yyyy}/`(年分層)、**1d 例外**:`kbars/1d/{sym}/{SYM}_1d_{year}.parquet` 年檔(無年目錄)——glob 打錯會靜默回空。
- 交易日歸檔:檔案日期 D = 前一晚夜盤(15:00→05:00)+ D 日盤(08:45–13:45)。**判斷用 `date` 欄,不是 ts、不是檔名**(週五夜盤 ts 落週六凌晨、1m 檔名可以是週六 —— 合法)。1d bar 身分 = `(date, session)`。
- 寫入前的髒資料防線:`_clean_sunday`(週日)、Phase-1.6 幻影守衛(max(ts) 日期 ≠ 請求日 → 整批跳過,**這就是自動化不需要假日日曆的原因**)、resampler 的 `date.weekday()<6` 過濾;事後另有 validate_lake。(⚠ 命名注意:main_etl 註解與 memory 裡說的「三道守衛」指的是**只防週末**的那組 = resampler `date<6` + `_clean_sunday` + validate_lake ②,不含 Phase-1.6 —— 兩份清單成員不同,別搞混。)
- 假日測試盤判別看 **volume**(正常日盤 7–9 萬 vs 測試盤個位數~幾百)。
- 已知 schema 地雷:盤中 kbar ts 是 `us`、1d/raw 是 `ns`(跨檔 concat 會 SchemaError);動態 TF(15m/30m/4h)重採樣沒帶 true_pv_sum → VWAP 是 (H+L+C)/3 近似;TSE 收 13:33 vs TXF 13:44 → 尾盤與夜盤 basis 是 stale-spot。
- Polars weekday:Mon=1…Sun=7(`<6` 留平日)。

## 自動化(現行真相;repo 內僅 AutoRun.md 標頭有摘要)

Windows 排程任務 **"TXF Daily Sync"**(週一~五 13:50 + StartWhenAvailable)→ workspace 根 **`daily_sync.py`**(2026-07-10 起,單一 Python 編排器直接呼 python.exe,取代舊 `catchup_sync.sh`+`sync.sh`)→ 就緒→掃缺→逐缺日四步(本 repo 的 main_etl + validate_lake → gale 匯出)。日誌在 `C:\Projects\TXF-Trading-Workspace\logs\`(`catchup-<date>.log` + `sync_state.json`)。**validate 失敗只警告不中斷**(每步 rc 記進 state)。手動補單日:`python daily_sync.py --date <date>`。詳見 workspace skill `data-ops`。

## 紅線與現況

- **repo 處於 mid-fix 狀態(2026-07-04)**:分支 `fix-etl-nontrading-date`(非 main),`core/resampler.py` 的週末守衛**未 commit**、`validate_lake.py` **untracked** —— 但生產排程已依賴兩者。**別丟棄工作樹、別不 stash 就切分支**;要 commit 只 commit 自己的檔,用戶的 dirty 檔別動。
- 1d 年檔寫入非原子(read-merge-overwrite);log 出現「Merge error, overwriting」= 整年檔被單日覆蓋的警訊,要深究。
- 別在日盤收盤前(~13:45)對「今天」跑 ETL(會歸檔不完整的日盤;排程排 13:50 就是為此)。
- 別手動測試 sync 腳本「玩玩」:step 1 是真實 Shioaji 登入 + 寫生產湖。
