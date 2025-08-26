import asyncio, csv, os, re, threading, random
import tkinter as tk
from tkinter import scrolledtext, messagebox
from tkinter import ttk
from urllib.parse import urlparse, quote_plus
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pathlib
import sys

# === Playwright 用ブラウザパス修正 ===
# .exe から実行された場合は _MEIPASS を参照
base_path = pathlib.Path(getattr(sys, "_MEIPASS", "."))  # PyInstaller 一時フォルダ or カレントディレクトリ
playwright_browsers_path = base_path / ".playwright-browsers"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(playwright_browsers_path.resolve())

AMZ = "https://www.amazon.co.jp"

# -------- Device profiles (device-capture style) --------
DEVICE_PROFILES = [
    dict(ua=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
         viewport={"width": 1366, "height": 768}, dsf=1.25, mobile=False, touch=False),
    dict(ua=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"),
         viewport={"width": 1440, "height": 900}, dsf=1.0, mobile=False, touch=False),
    dict(ua=("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
             "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"),
         viewport={"width": 390, "height": 844}, dsf=3.0, mobile=True, touch=True),
    dict(ua=("Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36"),
         viewport={"width": 412, "height": 915}, dsf=2.75, mobile=True, touch=True),
]

BLOCK_RES_TYPES = {"image", "media", "font", "stylesheet"}

SPREADSHEET_ID = "1YBeUTW7sMpmb4KODo0sNcxHJah8WuAe9YIDQCvDCE6Q"
SHEET_NAME = "Sheet1"
SERVICE_ACCOUNT_FILE = "weighty-vertex-464012-u4-7cd9bab1166b.json"

# === AUTHENTICATE ===
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("sheets", "v4", credentials=creds)

HEADER = ["キーワード", "asin", "オーガニック", "スポンサープロダクト", "スポンサーブランド"]

def append_google_sheet(results_json):
    """
    Append rows to Google Sheet.
    If the header row is missing, it will be added automatically.
    """
    # 1. Check if header exists
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1:E1"
    ).execute()

    values = result.get("values", [])

    if not values or values[0] != HEADER:
        # Write header if not exists or wrong
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [HEADER]}
        ).execute()

    # 2. Convert JSON → rows
    rows = []
    for item in results_json:
        rows.append([
            item.get("キーワード", ""),
            item.get("asin", ""),
            item.get("オーガニック", "-"),
            item.get("スポンサープロダクト", "-"),
            item.get("スポンサーブランド", "-")
        ])

    # 3. Append rows
    if rows:
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()

# ------------------ Helpers ------------------
def load_sku_map(path="sku_map.csv"):
    asin_to_sku, sku_to_asin = {}, {}
    if not os.path.exists(path):
        return asin_to_sku, sku_to_asin
    with open(path, newline="", encoding="utf-8-sig") as f:
        rdr = csv.reader(f)
        rows = list(rdr)
        if not rows:
            return asin_to_sku, sku_to_asin
        start = 0
        first = rows[0]
        if first and len(first) >= 2 and (("sku" in first[0].lower()) or ("asin" in first[0].lower())):
            start = 1
        for r in rows[start:]:
            if len(r) >= 2 and r[0].strip() and r[1].strip():
                sku = r[0].strip()
                asin = r[1].strip().upper()
                asin_to_sku[asin] = sku
                sku_to_asin[sku] = asin
    return asin_to_sku, sku_to_asin

def extract_asin_from_url(u: str):
    try:
        p = urlparse(u).path
        m = re.search(r"/dp/([A-Z0-9]{10})", p, flags=re.I)
        if m: return m.group(1).upper()
        m = re.search(r"/gp/aw/d/([A-Z0-9]{10})", p, flags=re.I)
        if m: return m.group(1).upper()
    except:
        pass
    return None

def is_asin(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{10}", (s or "").upper()))

# UI logger (thread-safe)
def log_to_ui(msg):
    output_box.after(0, lambda: (output_box.insert(tk.END, msg + "\n"), output_box.see(tk.END)))

def set_ui_busy(is_busy: bool):
    def _apply():
        if is_busy:
            start_btn.config(state="disabled", text="スクレイピング中…")
            progress.start(10)
            status_var.set("スクレイピングを開始しました。しばらくお待ちください…")
        else:
            start_btn.config(state="normal", text="スクレイピングを開始")
            progress.stop()
            status_var.set("アイドル状態")
    root.after(0, _apply)

# ------------------ Playwright context ------------------
async def make_context(browser):
    prof = random.choice(DEVICE_PROFILES)
    ctx = await browser.new_context(
        user_agent=prof["ua"],
        viewport=prof["viewport"],
        device_scale_factor=prof["dsf"],
        is_mobile=prof["mobile"],
        has_touch=prof["touch"],
        locale="ja-JP",
        java_script_enabled=True
    )
    await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

    async def _route_handler(route):
        try:
            if route.request.resource_type in BLOCK_RES_TYPES:
                await route.abort()
            else:
                await route.continue_()
        except:
            try:
                await route.continue_()
            except:
                pass

    await ctx.route("**/*", _route_handler)
    return ctx

# --------- Ultra-fast search-page parsing ---------
SP_BADGE_PATTERNS = ("スポンサー", "Sponsored", "スポンサード")

def _card_type(card_el) -> str:
    txt = card_el.get_text(" ", strip=True)
    for pat in SP_BADGE_PATTERNS:
        if pat in txt:
            return "SP"
    return "Organic"

def _get_text(el):
    return el.get_text(strip=True) if el else ""

def parse_cards_from_html(html, キーワード, page_index):
    soup = BeautifulSoup(html, "html.parser")
    results = []

    cards = soup.select("div.s-main-slot div.s-result-item[data-asin]")
    if not cards:
        cards = soup.select("div.s-result-item[data-asin]")

    for idx, card in enumerate(cards, start=1):
        asin = (card.get("data-asin") or "").strip().upper()
        if not asin:
            continue

        title_el = card.select_one("h2 a span")
        price_el = card.select_one(".a-price .a-offscreen")

        総合順位 = (page_index - 1) * 60 + idx
        if 総合順位 < 1:
            総合順位 = 1  # fix start rank

        row = {
            "キーワード": キーワード,
            "page": page_index,
            "position_on_page": idx,
            "総合順位": 総合順位,
            "asin": asin,
            "title": _get_text(title_el),
            "price": _get_text(price_el),
            "type": _card_type(card),
            "source": "card"
        }
        results.append(row)
    return results

def parse_sb_order_from_html(html, max_links=120):
    order = []
    if any(p in html for p in SP_BADGE_PATTERNS):
        found = re.findall(r"/(?:dp|gp/aw/d)/([A-Z0-9]{10})", html, flags=re.I)
        seen = set()
        for a in found:
            a = a.upper()
            if a not in seen:
                seen.add(a)
                order.append(a)
            if len(order) >= max_links:
                break
    return order

# ------------------ Scraping ------------------
async def scrape_キーワード(page, kw: str, pages_to_scan: int = 2, ui_log=None):
    rows = []
    sb_asins_global = []
    await asyncio.sleep(random.uniform(0.1, 0.35))
    url = f"{AMZ}/s?k={quote_plus(kw)}"
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)

    for p in range(1, pages_to_scan + 1):
        try:
            await page.wait_for_selector("div.s-main-slot", timeout=10000)
        except:
            if ui_log: ui_log(f"[WARN] main slot not found page {p} for '{kw}'")
            break

        html = await page.content()
        page_rows = parse_cards_from_html(html, kw, p)
        rows.extend(page_rows)

        if ui_log:
            ui_log(f"[デバッグ] キーワード {kw} | ページ {p} | 商品数: {len(page_rows)}")

        sb_asins_global.extend(parse_sb_order_from_html(html))

        if p < pages_to_scan:
            next_btn = page.locator("a.s-pagination-next")
            if await next_btn.count() > 0:
                try:
                    await next_btn.first.click()
                    await page.wait_for_load_state("domcontentloaded", timeout=12000)
                    await asyncio.sleep(random.uniform(0.08, 0.22))
                except:
                    break
            else:
                break
        else:
            break

    seen = set()
    sb_order = []
    for a in sb_asins_global:
        if a not in seen:
            seen.add(a)
            sb_order.append(a)

    return rows, sb_order

async def run(キーワードs, asin_to_sku, target_asins, target_skus,
              pages_to_scan=2, headless=True, ui_log=None, concurrency=8):
    
    base_path = pathlib.Path(getattr(sys, "_MEIPASS", "."))  # PyInstaller 一時フォルダ or カレントディレクトリ
    playwright_browsers_path = base_path / ".playwright-browsers"
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(playwright_browsers_path.resolve())

    async with async_playwright() as pw:
        launch_args = dict(headless=headless, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage"
        ])
        browser = await pw.chromium.launch(**launch_args)

        sem = asyncio.Semaphore(concurrency)
        all_rows = []
        sb_by_キーワード = {}

        async def task_for_キーワード(kw):
            async with sem:
                ctx = await make_context(browser)
                page = await ctx.new_page()
                try:
                    rows, sb_order = await scrape_キーワード(page, kw, pages_to_scan=pages_to_scan, ui_log=ui_log)
                    return kw, rows, sb_order
                finally:
                    try:
                        await ctx.close()
                    except:
                        pass

        tasks = [asyncio.create_task(task_for_キーワード(kw)) for kw in キーワードs]
        for coro in asyncio.as_completed(tasks):
            kw, rows, sb_order = await coro
            if rows:
                all_rows.extend(rows)
            sb_by_キーワード[kw] = sb_order

        try:
            await browser.close()
        except:
            pass

    # attach SKU
    for r in all_rows:
        r_asin = (r.get("asin") or "").upper()
        r["sku"] = asin_to_sku.get(r_asin, "")

    # compute ranks
    rows_by_kw = {}
    for r in all_rows:
        rows_by_kw.setdefault(r["キーワード"], []).append(r)

    for kw in rows_by_kw:
        rows_by_kw[kw].sort(key=lambda x: x.get("総合順位") or 10**9)

    type_orders = {}
    for kw, rows in rows_by_kw.items():
        organic = [r["asin"] for r in rows if r.get("type") == "Organic"]
        sp      = [r["asin"] for r in rows if r.get("type") == "SP"]
        sb      = sb_by_キーワード.get(kw, [])
        type_orders[kw] = {"Organic": organic, "SP": sp, "SB": sb}

    def rank_in_list(lst, asin):
        try:
            return lst.index(asin) + 1
        except ValueError:
            return None

    for kw, rows in rows_by_kw.items():
        orders = type_orders[kw]
        for r in rows:
            asin = r.get("asin")
            sku  = r.get("sku")
            is_target = (asin in target_asins) or (sku in target_skus)

            r["オーガニック"] = r.get("総合順位")  # always set オーガニック = 総合順位
            r["スポンサープロダクト"] = ""
            r["スポンサーブランド"] = ""

            if is_target:
                if r.get("type") == "SP":
                    スポンサープロダクト = rank_in_list(orders["SP"], asin)
                    if スポンサープロダクト is not None: r["スポンサープロダクト"] = スポンサープロダクト
                スポンサーブランド = rank_in_list(orders["SB"], asin)
                if スポンサーブランド is not None: r["スポンサーブランド"] = スポンサーブランド

    if ui_log:
        results_json = []
        for kw in キーワードs:
            orders = type_orders.get(kw, {"Organic": [], "SP": [], "SB": []})
            msg = [f"[RESULT] '{kw}':"]
            kw_rows = rows_by_kw.get(kw, [])
            any_target = False
            for r in kw_rows:
                asin = r.get("asin")
                sku  = r.get("sku")
                if (asin in target_asins) or (sku in target_skus):
                    any_target = True
                    o = r.get("オーガニック") or "-"
                    s = r.get("スポンサープロダクト") or "-"
                    b = r.get("スポンサーブランド") or "-"
                    msg.append(f" Target {asin} => オーガニック={o}, スポンサープロダクト={s}, スポンサーブランド={b} (全体={r.get('総合順位')})")
                    results_json.append({
                        "キーワード": kw,
                        "asin": asin,
                        "オーガニック": o,
                        "スポンサープロダクト": s,
                        "スポンサーブランド": b
                    })
            append_google_sheet(results_json)
            msg.append(f"[情報] Google スプレッドシートに {len(results_json)} 行を追加しました。")
            if not any_target:
                msg.append(" スキャンしたページに対象が見つかりませんでした。")
            ui_log(" ".join(msg))

    return all_rows

def save_csv(rows, out="amazon_results_with_sku_sb.csv"):
    fields = [
        "キーワード","sku","asin","type","page","position_on_page","総合順位",
        "title","price","source",
        "オーガニック","スポンサープロダクト","スポンサーブランド"
    ]
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})
    return out

# ------------------ UI ------------------
def start_scraping_ui():
    raw_kw = キーワード_entry.get().strip()
    raw_sku = sku_entry.get().strip()
    if not raw_kw:
        messagebox.showerror("エラー", "キーワードを少なくとも1つ入力してください。")
        return
    if not raw_sku:
        messagebox.showerror("エラー", "ASINを入力してください")
        return

    try:
        pages = int(pages_entry.get() or 2)
    except:
        pages = 2
    キーワードs = [k.strip() for k in raw_kw.split(",") if k.strip()]
    inputs = [s.strip() for s in raw_sku.split(",") if s.strip()]

    set_ui_busy(True)
    log_to_ui("[情報] スクレイピングを開始しました。しばらくお待ちください…")
    log_to_ui(f"[情報] キーワード: {キーワードs} | 各キーワードのページ数: {pages}")
    log_to_ui(f"[情報] ターゲット（未加工）: {inputs}")

    asin_to_sku, sku_to_asin = load_sku_map("sku_map.csv")

    target_asins = set()
    target_skus  = set()
    for it in inputs:
        up = it.upper()
        if is_asin(up):
            target_asins.add(up)
            if up in asin_to_sku:
                target_skus.add(asin_to_sku[up])
        else:
            target_skus.add(it)
            if it in sku_to_asin:
                target_asins.add(sku_to_asin[it])

    log_to_ui(f"[情報] 対象ASINの確定: {sorted(target_asins)}")
    log_to_ui(f"[情報] 対象SKUの確定: {sorted(target_skus)}")

    async def runner():
        try:
            rows = await run(
                キーワードs,
                asin_to_sku,
                target_asins=target_asins,
                target_skus=target_skus,
                pages_to_scan=pages,
                headless=True,
                ui_log=log_to_ui,
                concurrency=min(10, max(2, len(キーワードs)))
            )
            out = save_csv(rows)
            log_to_ui(f"[情報] 結果を {out} に保存しました")
            messagebox.showinfo("Completed", f"スクレイピングが終了しました！\n{out} に保存されました。")
        except Exception as e:
            log_to_ui(f"[エラー] {e}")
            messagebox.showerror("エラー", str(e))
        finally:
            set_ui_busy(False)

    threading.Thread(target=lambda: asyncio.run(runner()), daemon=True).start()

# ------------------ Tkinter App ------------------
root = tk.Tk()
root.title("Amazonランキングチェッカー（超高速＋デバイスキャプチャ対応）")

frm = tk.Frame(root, padx=10, pady=10)
frm.pack(fill="both", expand=True)

tk.Label(frm, text="キーワードを入力してください（カンマ区切り）：").grid(row=0, column=0, sticky="w")
キーワード_entry = tk.Entry(frm, width=50)
キーワード_entry.grid(row=0, column=1, sticky="we", padx=(6,0))

tk.Label(frm, text="あなたのSKUまたはASIN（カンマ区切り）：").grid(row=1, column=0, sticky="w", pady=(6,0))
sku_entry = tk.Entry(frm, width=50)
sku_entry.grid(row=1, column=1, sticky="we", padx=(6,0), pady=(6,0))

tk.Label(frm, text="スキャンするページ").grid(row=2, column=0, sticky="w")
pages_entry = tk.Entry(frm, width=10)
pages_entry.insert(0, "2")
pages_entry.grid(row=2, column=1, sticky="w", padx=(6,0))

start_btn = tk.Button(frm, text="スクレイピングを開始", command=start_scraping_ui)
start_btn.grid(row=3, column=0, columnspan=2, pady=(10,6))

status_var = tk.StringVar(value="アイドル状態")
status_lbl = tk.Label(frm, textvariable=status_var, fg="blue")
status_lbl.grid(row=4, column=0, sticky="w")

progress = ttk.Progressbar(frm, mode="indeterminate", length=220)
progress.grid(row=4, column=1, sticky="w", padx=(6,0))

output_box = scrolledtext.ScrolledText(frm, width=96, height=24)
output_box.grid(row=5, column=0, columnspan=2, pady=(10,0))

frm.grid_columnconfigure(1, weight=1)
root.mainloop()
