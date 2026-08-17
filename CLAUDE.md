# txf-data-lake — AI Agent 指南

Shioaji API → `D:/txf-data` parquet 資料湖的 ETL。**本 repo 是湖內 kbar / raw tick 的唯一寫入者**——但注意兩個合法的外部寫入:txf-gale-engine 每日 sync 會把 `{date}_TXF_bidask.parquet` 寫進 `raw_ticks/`;txf-quant-platform viewer 寫 DATA_ROOT 根的 `user_settings.json`。**這些不是髒資料,別清。** 也含舊版看盤 `view_chart.py`(已被 quant-platform viewer 取代,仍可用)。

## 文件權威序

程式碼 > `notes/txf-data-lake-reference.md`(~70KB 權威技術參考,2026-06-16 快照 —— 模組內部/schema/問題清單 I-SEC..I-16 都在這;**引用前注意它未反映之後的修正**:週日/幻影守衛、validate_lake、.env 註解已清)> `README.md`(金鑰指引已於 2026-07-04 修正為 .env + python-dotenv)> `AutoRun.md`(**純歷史**:主體是已退役的 macOS launchd,標頭有現行 Windows 排程摘要)。

## 環境與執行

- 一律 `.venv\Scripts\python.exe`(Python 3.13;2026-07 工作站統一到 3.13)+ 前綴 `PYTHONUTF8=1`(main_etl/view_chart/fix_kbars **沒有**自我 reconfigure stdout,cp950 會炸)。
- 設定在 `config/settings.py`:DATA_ROOT(env 覆寫,預設 D:\txf-data)、TIMEFRAMES=[5s,1m,5m,**30m**,1h,1d]、金鑰從 `.env` 載入。
  - **30m 是 2026-07-31 加的**,不是為了直接看 30m,是給 platform 當「粗 TF 的聚合底層」
    (3seg 需要 ≤30m 粒度才切得到美股開盤那條邊界)。歷史 4807 檔**由既有 5m 回填**,
    等價性已驗:1h 從 30m 聚 == 原生 1h(逐筆產的 ground truth)逐根相同。
  - ⚠ 這份清單必須與 **platform 的 `PARQUET_TIMEFRAMES` 一致**(產出端 vs 消費端),
    不一致會被 platform 的 `test_resample_base_map_exactly_covers_ui` 擋下。
  - 六個 TF 構成一道**整除階梯**(5s→1m×12→5m×5→30m×6→1h×2→1d×24),
    每層整除上一層 —— 這是分層聚合無損、以及任何顯示 TF 都找得到底層的前提。
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
- **2026-08-15 起 kbar 一律 12 欄**:`true_pt_sum`(∫P dt,棒邊界切片)+ `dur_s`(恆=桶名目長)——TWMA 的 tick 級可加量(語意正典=platform `wiki/MA-Semantics.md` §6)。全史已回填+例外已修復(嫁接→裁決→重建,`tools/backfill_pt_sum.py` + `tools/repair_pt_exceptions.py`);**唯一殘留 pt=null = TXFR2 2025-01-06**(重抓實測 Shioaji 只回日盤 —— 該日週五夜盤資料 API 端已滅失,舊 kbars 的夜盤棒是唯一倖存副本,**別再試圖修**)。
  🔒 **2026-08-17 起那份倖存副本已升級進 `D:/txf-data/orphan_bars/`**(6 檔 43 KB,含鑑識時間線與還原步驟的 README)。理由:它物理上是 kbar(平常屬「可重建的建置產物」),語意上已是**來源真值** —— 放在 `kbars/` 底下只靠「記得別刪」保護,遲早出事。**分層的判準是「這是不是最原始的倖存副本」,不是「它放在哪個目錄」。** 同日再次向 SJ 實測確認(`api.ticks` 回 965 筆、夜盤 0 筆,與湖裡現存 raw 完全相同);`backup_lake.PRIORITY` 已把它排第一;`tools/verify_rebuild.py` 的 `KNOWN_UNREBUILDABLE` 已列入,免得每次對帳都紅一次而失去訊號價值。
  ⚠️ 鑑識更正:**不是「好資料被覆蓋」** —— TXFR2 整條線(raw 1,608 / kbars 1,607)沒有任何檔早於 2026-06-14,那天是**初次整批補建**,補建當下 SJ 就只回日盤。用 TXF 當對照組掃 656 個共同交易日,「TXF 有夜盤但 TXFR2 完全沒有」= **0 天**,沒有其他天受害。⚠ resampler 的兩處 sort 帶 `maintain_order=True` 是**契約不是裝飾**:平手序=raw 檔到達序;拿掉它,特定 chunk 型態的檔重算就會換序(2026-08-15 實證 13 天中招)。備份:`kbars_backup_pre_ptsum/`(回填前)、`repair_backup_20260815/`(修復前)。
- Polars weekday:Mon=1…Sun=7(`<6` 留平日)。

## 自動化(現行真相;repo 內僅 AutoRun.md 標頭有摘要)

Windows 排程任務 **"TXF Daily Sync"**(週一~五 13:50 + StartWhenAvailable)→ workspace 根 **`daily_sync.py`**(2026-07-10 起,單一 Python 編排器直接呼 python.exe,取代舊 `catchup_sync.sh`+`sync.sh`)→ 就緒→掃缺→逐缺日四步(本 repo 的 main_etl + validate_lake → gale 匯出)。日誌在 `C:\Projects\TXF-Trading-Workspace\logs\`(`catchup-<date>.log` + `sync_state.json`)。**validate 失敗只警告不中斷**(每步 rc 記進 state)。手動補單日:`python daily_sync.py --date <date>`。詳見 workspace skill `data-ops`。

## 紅線與現況

- ~~repo 處於 mid-fix 狀態(2026-07-04)~~ **已解除(2026-08-03 複驗)**:現在在 `main`、工作樹乾淨;週末守衛與 `validate_lake.py` 已於 `d08b51d` 一併提交進版控。`fix-etl-nontrading-date` 分支還在但未 checkout。
  (仍然成立的通則:**要 commit 只 commit 自己改的檔,用戶的 dirty 檔別動、別 `git add -A`**。)
- 1d 年檔寫入非原子(read-merge-overwrite);log 出現「Merge error, overwriting」= 整年檔被單日覆蓋的警訊,要深究。
- 別在日盤收盤前(~13:45)對「今天」跑 ETL(會歸檔不完整的日盤;排程排 13:50 就是為此)。
- 別手動測試 sync 腳本「玩玩」:step 1 是真實 Shioaji 登入 + 寫生產湖。
