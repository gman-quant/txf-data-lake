# macOS 自動執行 Shioaji TXF/TSE Collector SOP

**服務名稱（唯一識別）**
`com.garrett.shioaji.txf_tse_collector`

**專案路徑**

```text
/Users/gtai/Projects/txf-data-lake
```

**執行指令**

```bash
(.venv) gtai@Garretts-MacBook-Pro txf-data-lake % python main_etl.py
```

**行為規格**

| 情境          | 是否執行 |
| ----------- | ---- |
| 週一～週五 13:46 | ✅    |
| 週六登入        | ✅    |
| 週日          | ❌    |

---

## STEP 1｜建立 LaunchAgents 資料夾（若已存在可跳過）

```bash
mkdir -p ~/Library/LaunchAgents
```

---

## STEP 2｜建立 LaunchAgent plist

```bash
nano ~/Library/LaunchAgents/com.garrett.shioaji.txf_tse_collector.plist
```

---

## STEP 3｜貼上 plist（最終定版）

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>

    <!-- 唯一識別 -->
    <key>Label</key>
    <string>com.garrett.shioaji.txf_tse_collector</string>

    <!-- 執行內容 -->
    <key>ProgramArguments</key>
    <array>
        <string>/bin/zsh</string>
        <string>-lc</string>
        <string>
            <![CDATA[
            set -e

            DOW=$(date +%u)      # 1=Mon ... 6=Sat 7=Sun
            HOUR=$(date +%H)
            MIN=$(date +%M)

            SHOULD_RUN=0

            # 週一～週五 13:46
            if [[ "$DOW" -ge 1 && "$DOW" -le 5 && "$HOUR" == "13" && "$MIN" == "46" ]]; then
                SHOULD_RUN=1
            fi

            # 週六 05:01
            if [[ "$DOW" == "6" && "$HOUR" == "05" && "$MIN" == "01" ]]; then
                SHOULD_RUN=1
            fi

            # 週六登入（RunAtLoad）
            if [[ "$DOW" == "6" && "$HOUR" != "05" ]]; then
                SHOULD_RUN=1
            fi

            if [[ "$SHOULD_RUN" == "1" ]]; then
                cd /Users/gtai/Projects/txf-data-lake
                source .venv/bin/activate
                python main_etl.py
            else
                echo "[SKIP] Not a scheduled ETL window"
            fi
            ]]>
        </string>
    </array>

    <!-- 定時觸發 -->
    <key>StartCalendarInterval</key>
    <array>
        <!-- 週一～週五 13:46 -->
        <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>46</integer></dict>
        <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>46</integer></dict>
        <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>46</integer></dict>
        <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>46</integer></dict>
        <dict><key>Weekday</key><integer>6</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>46</integer></dict>

        <!-- 週六 05:01 -->
        <dict><key>Weekday</key><integer>7</integer><key>Hour</key><integer>5</integer><key>Minute</key><integer>1</integer></dict>
    </array>

    <!-- 登入時觸發（僅週六會真的跑） -->
    <key>RunAtLoad</key>
    <true/>

    <!-- log -->
    <key>StandardOutPath</key>
    <string>/tmp/txf_tse_collector.out.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/txf_tse_collector.err.log</string>

</dict>
</plist>
```

---

## STEP 4｜存檔並離開 nano

```text
Ctrl + O → Enter
Ctrl + X
```

---

## STEP 5｜載入 LaunchAgent

```bash
launchctl load ~/Library/LaunchAgents/com.garrett.shioaji.txf_tse_collector.plist
```

✅ 沒輸出 = 成功

---

## STEP 6｜手動測試（一定要做）

```bash
launchctl start com.garrett.shioaji.txf_tse_collector
```

查看 log：

```bash
tail -f /tmp/shioaji_txf_tse_collector.out.log
tail -f /tmp/shioaji_txf_tse_collector.err.log
```

> 判斷結果：
>
> * 今天是週六 → 會執行 `python main_etl.py`
> * 其他日子 → 不會執行（正確行為）

---

## STEP 7｜確認 LaunchAgent 已註冊

```bash
launchctl list | grep shioaji
```

應看到：

```text
com.garrett.shioaji.txf_tse_collector
```

---

## STEP 8｜未來管理指令（macOS 新版 launchd 正確用法）


### 🔴 停用（卸載 LaunchAgent）

> 只有在 `launchctl list | grep shioaji` **看得到** 時才需要執行

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.garrett.shioaji.txf_tse_collector.plist
```

✔ 沒有任何輸出 = 成功
✔ 再 `list` 就看不到該 job

---

### 🔄 修改後重載（標準流程）

```bash
# 1. 先卸載（若已存在）
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.garrett.shioaji.txf_tse_collector.plist

# 2. 重新載入
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.garrett.shioaji.txf_tse_collector.plist
```

✔ 沒輸出 = 成功
✔ 新設定立即生效

---

### ▶️ 手動立即執行（不等時間）

```bash
launchctl start com.garrett.shioaji.txf_tse_collector
```

常用於：

* 測試新邏輯
* Debug Shioaji 登入
* 驗證週六 / 平日行為

---

### 🔍 查詢狀態（是否已註冊）

```bash
launchctl list | grep shioaji
```

結果判讀：

| 顯示結果                                      | 意義       |
| ----------------------------------------- | -------- |
| 有 `com.garrett.shioaji.txf_tse_collector` | 已載入、待命   |
| 沒有任何輸出                                    | 尚未載入或已停用 |

---

## 📌 你可以在 SOP 最後加這一句（很實務）

> **原則**
>
> * 看不到 → `bootstrap`
> * 看得到 → 需要停用才 `bootout`
> * 只是想跑一次 → `launchctl start`

---

## 現在你已經做到的等級

* macOS 官方 launchd 正規方式管理
* venv 正確載入
* 登入 Shioaji session 可 debug
* 職責明確（collector）
* 可無痛升級成準 production market data pipeline

---

這份就是你可以**直接存檔、備份或交接給未來自己的 SOP**，完整、乾淨、可直接照做。

---