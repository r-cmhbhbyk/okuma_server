"""
Factory Machine Monitor
=======================
1. Clear old CSV files
2. Download fresh CSV from web interface
3. Analyze last hour:
   - Program cycle times + averages
   - Downtime detection with reasons
   - Timeline chart per machine
   - 7-day history (SQLite)
   - Telegram alert if idle > 60 min
4. Open HTML report in browser
"""

import os
import time
import sqlite3
import csv
import sys
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException

# ── Configuration ─────────────────────────────────────────────────────────────
DOWNLOAD_DIR        = r"C:\Connectplan_raports"
CSV_FILE            = os.path.join(DOWNLOAD_DIR, "123.csv")
OUTPUT_HTML         = os.path.join(DOWNLOAD_DIR, "report.html")
DB_FILE             = os.path.join(DOWNLOAD_DIR, "history.db")
URL                 = "http://192.168.1.210/csv/OutputCSVWeb.aspx?FactoryID=1&AreaID=1"
WAIT_TIME           = 300
HOURS_BACK          = 1
ALERT_THRESHOLD_MIN = 45

TELEGRAM_TOKEN   = "8474596481:AAEGyP1nB0vuRo4DkCLwzDbBXDV7Lab7lvU"
TELEGRAM_CHAT_ID = "656625394"

GITHUB_TOKEN = "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"
GITHUB_USER  = "wisefab1"
GITHUB_REPO  = "factory_monitor"
GITHUB_URL   = "https://wisefab1.github.io/factory_monitor/"

# =============================================================================
# PART 1 — DOWNLOAD
# =============================================================================

def clear_old_csv():
    for filename in os.listdir(DOWNLOAD_DIR):
        if filename.lower().endswith(".csv"):
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, filename))
                print(f"Deleted: {filename}")
            except Exception as exc:
                print(f"Could not delete {filename}: {exc}")

def download_csv() -> bool:
    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory":   DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade":   True,
        "safebrowsing.enabled":         True,
    })
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(URL)
        wait = WebDriverWait(driver, WAIT_TIME)

        checkbox = wait.until(EC.element_to_be_clickable((By.ID, "all_machines_check")))
        if not checkbox.is_selected():
            checkbox.click()
            time.sleep(3)

        btn = wait.until(EC.element_to_be_clickable((By.ID, "btn_Download")))
        click_time = time.time()
        btn.click()
        print("Download button clicked — waiting for file...")

        deadline   = time.time() + WAIT_TIME
        downloaded = None
        while time.time() < deadline:
            for f in os.listdir(DOWNLOAD_DIR):
                if f.lower().endswith(".csv"):
                    fp = os.path.join(DOWNLOAD_DIR, f)
                    if os.path.getmtime(fp) >= click_time:
                        downloaded = fp
                        break
            if downloaded:
                break
            time.sleep(2)

        if downloaded:
            if os.path.exists(CSV_FILE):
                os.remove(CSV_FILE)
            os.rename(downloaded, CSV_FILE)
            print(f"Saved: {CSV_FILE}")
            return True
        else:
            print("Error: file did not download within timeout")
            return False

    except TimeoutException:
        print("Error: element not found on page")
        return False
    except WebDriverException as exc:
        print(f"WebDriver error: {exc}")
        return False
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        return False
    finally:
        driver.quit()

# =============================================================================
# PART 2 — ANALYSIS
# =============================================================================

# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(message: str):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
        print("Telegram alert sent")
    except Exception as e:
        print(f"Telegram error: {e}")

# ── SQLite ────────────────────────────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            date TEXT, machine TEXT, run_min INTEGER, down_min INTEGER,
            total_min INTEGER, cycles INTEGER, avg_cycle REAL, efficiency REAL,
            PRIMARY KEY (date, machine)
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS downtime_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, machine TEXT, start_time TEXT, end_time TEXT,
            duration INTEGER, reason TEXT
        )""")
    conn.commit()
    return conn

def save_to_db(conn, date_str, cycles, downtimes):
    for mname in cycles:
        c_list    = cycles[mname]
        d_data    = downtimes[mname]
        run_min   = d_data["total_run"]
        down_min  = d_data["total_down"]
        total_min = d_data["total_min"]
        eff       = round(run_min / total_min * 100, 1) if total_min else 0
        avg_cycle = round(sum(c["duration"] for c in c_list) / len(c_list), 1) if c_list else 0
        conn.execute("""
            INSERT OR REPLACE INTO daily_summary
            (date,machine,run_min,down_min,total_min,cycles,avg_cycle,efficiency)
            VALUES (?,?,?,?,?,?,?,?)
        """, (date_str, mname, run_min, down_min, total_min, len(c_list), avg_cycle, eff))
        for d in d_data["downtimes"]:
            conn.execute("""
                INSERT OR IGNORE INTO downtime_events
                (date,machine,start_time,end_time,duration,reason)
                VALUES (?,?,?,?,?,?)
            """, (date_str, mname,
                  d["start"].strftime("%H:%M"),
                  d["end"].strftime("%H:%M") if d.get("end") else "ongoing",
                  d["duration"], d["reason"]))
    conn.commit()

def load_history(conn, machine: str, days: int = 7) -> list:
    cur = conn.execute("""
        SELECT date, efficiency, run_min, down_min, cycles, avg_cycle
        FROM daily_summary WHERE machine=? ORDER BY date DESC LIMIT ?
    """, (machine, days))
    return list(reversed(cur.fetchall()))

# ── Data processing ───────────────────────────────────────────────────────────
def load_csv() -> list[dict]:
    with open(CSV_FILE, encoding="utf-8") as f:
        return list(csv.DictReader(f))

def filter_last_hours(rows, hours):
    parse   = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    last_ts = max(parse(r["Date"]) for r in rows)
    cutoff  = last_ts - timedelta(hours=hours)
    return [r for r in rows if parse(r["Date"]) >= cutoff], cutoff, last_ts

def analyze_cycles(rows):
    parse    = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    machines = defaultdict(list)
    for r in rows:
        machines[r["MachineName"]].append(r)
    result = {}
    for mname, mrows in machines.items():
        mrows.sort(key=lambda r: r["Date"])
        cycles, prev_run, cycle_start, cycle_prog = [], None, None, ""
        for r in mrows:
            ts, run, prog = parse(r["Date"]), r["RunState"], r["ProgramFileName"]
            if prev_run in (None, "0") and run == "1":
                cycle_start, cycle_prog = ts, prog
            elif prev_run == "1" and run == "0" and cycle_start:
                cycles.append({"start": cycle_start, "end": ts, "program": cycle_prog,
                                "duration": int((ts - cycle_start).total_seconds() // 60)})
                cycle_start = None
            prev_run = run
        if cycle_start:
            last_ts = parse(mrows[-1]["Date"])
            cycles.append({"start": cycle_start, "end": None, "program": cycle_prog,
                           "duration": int((last_ts - cycle_start).total_seconds() // 60),
                           "ongoing": True})
        result[mname] = cycles
    return result

def analyze_downtime(rows):
    parse    = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    machines = defaultdict(list)
    for r in rows:
        machines[r["MachineName"]].append(r)
    result = {}
    for mname, mrows in machines.items():
        mrows.sort(key=lambda r: r["Date"])
        downtimes, prev_run, dt_start, dt_reason = [], None, None, ""
        for r in mrows:
            ts, run = parse(r["Date"]), r["RunState"]
            if run == "0":
                if   r["AlarmState"]       == "1": reason = "Alarm: " + (r["AlarmMessage"] or r["AlarmNo"] or "—")
                elif r["PowerOn"]          == "0": reason = "Power off"
                elif r["SetUp"]            == "1": reason = "Setup"
                elif r["Maintenance"]      == "1": reason = "Maintenance"
                elif r["NoOperator"]       == "1": reason = "No operator"
                elif r["Wait"]             == "1": reason = "Waiting"
                elif r["FeedHoldState"]    == "1": reason = "Feed Hold"
                elif r["ProgramStopState"] == "1": reason = "Program Stop"
                else:                              reason = "Idle"
            else:
                reason = ""
            if prev_run in (None, "1") and run == "0":
                dt_start, dt_reason = ts, reason
            elif prev_run == "0" and run == "1" and dt_start:
                dur = int((ts - dt_start).total_seconds() // 60)
                if dur > 0:
                    downtimes.append({"start": dt_start, "end": ts,
                                      "duration": dur, "reason": dt_reason})
                dt_start = None
            prev_run = run
        if dt_start:
            last_ts = parse(mrows[-1]["Date"])
            dur = int((last_ts - dt_start).total_seconds() // 60)
            if dur > 0:
                downtimes.append({"start": dt_start, "end": None, "duration": dur,
                                  "reason": dt_reason, "ongoing": True})
        result[mname] = {
            "downtimes":  downtimes,
            "total_run":  sum(1 for r in mrows if r["RunState"] == "1"),
            "total_down": sum(1 for r in mrows if r["RunState"] == "0"),
            "total_min":  len(mrows),
        }
    return result

def build_timeline_data(rows, period_from, period_to):
    parse     = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    machines  = defaultdict(list)
    for r in rows:
        machines[r["MachineName"]].append(r)
    total_sec = max((period_to - period_from).total_seconds(), 1)
    result    = {}

    for mname, mrows in machines.items():
        mrows.sort(key=lambda r: r["Date"])
        segments  = []
        seg_start = period_from
        seg_state = None
        seg_label = ""
        seg_idx   = 0

        def _get_label(r):
            run = r["RunState"]
            if run == "1":
                return r["ProgramFileName"] or "Running"
            if   r["AlarmState"]       == "1": return "Alarm: " + (r["AlarmMessage"] or r["AlarmNo"] or "—")
            elif r["PowerOn"]          == "0": return "Power off"
            elif r["SetUp"]            == "1": return "Setup"
            elif r["Maintenance"]      == "1": return "Maintenance"
            elif r["NoOperator"]       == "1": return "No operator"
            elif r["Wait"]             == "1": return "Waiting"
            elif r["FeedHoldState"]    == "1": return "Feed Hold"
            elif r["ProgramStopState"] == "1": return "Program Stop"
            return "Idle"

        for r in mrows:
            ts, run = parse(r["Date"]), r["RunState"]
            lbl = _get_label(r)
            if seg_state is None:
                seg_state, seg_start, seg_label = run, ts, lbl
            elif run != seg_state or (run == "1" and lbl != seg_label):
                # нова програма або зміна стану — закриваємо сегмент
                x = (seg_start - period_from).total_seconds() / total_sec * 100
                w = (ts - seg_start).total_seconds() / total_sec * 100
                if w > 0.05:
                    segments.append({
                        "x": x, "w": w, "state": seg_state,
                        "label": seg_label,
                        "start": seg_start.strftime("%H:%M"),
                        "end":   ts.strftime("%H:%M"),
                        "id":    f"{mname.split('_')[0]}_{seg_idx}",
                    })
                    seg_idx += 1
                seg_start, seg_state, seg_label = ts, run, lbl

        if seg_state is not None:
            x = (seg_start - period_from).total_seconds() / total_sec * 100
            w = (period_to - seg_start).total_seconds() / total_sec * 100
            if w > 0.05:
                segments.append({
                    "x": x, "w": w, "state": seg_state,
                    "label": seg_label,
                    "start": seg_start.strftime("%H:%M"),
                    "end":   period_to.strftime("%H:%M"),
                    "id":    f"{mname.split('_')[0]}_{seg_idx}",
                })
        result[mname] = segments
    return result

# ── GitHub Pages publish ──────────────────────────────────────────────────────
def publish_to_github(html: str) -> bool:
    """Push index.html to GitHub Pages via API — no git install required."""
    import base64
    try:
        api     = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/index.html"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Content-Type":  "application/json",
            "Accept":        "application/vnd.github+json",
        }
        # Отримуємо SHA якщо файл вже існує
        sha = None
        try:
            req = urllib.request.Request(api, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:
                sha = json.loads(r.read().decode())["sha"]
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
        # Пушимо файл
        payload = {
            "message": f"update {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": base64.b64encode(html.encode("utf-8")).decode(),
        }
        if sha:
            payload["sha"] = sha
        req = urllib.request.Request(
            api, data=json.dumps(payload).encode(),
            headers=headers, method="PUT"
        )
        urllib.request.urlopen(req, timeout=20)
        print(f"Published: {GITHUB_URL}")
        return True
    except Exception as e:
        print(f"GitHub publish error: {e}")
        return False

def check_and_alert(downtimes, period_to):
    alerts = [(m, d) for m, dd in downtimes.items()
              for d in dd["downtimes"] if d["duration"] >= ALERT_THRESHOLD_MIN]
    if not alerts:
        return
    lines = [f"⚠️ <b>Machine Downtime Alert</b>  {period_to.strftime('%H:%M')}\n🔗 <a href=\"{GITHUB_URL}\">Open report</a>"]
    for mname, d in alerts:
        short = mname.split("_")[0]
        end_s = d["end"].strftime("%H:%M") if d.get("end") else "ongoing"
        lines.append(
            f"\n🔴 <b>{short}</b>  {d['start'].strftime('%H:%M')}–{end_s}"
            f"  <b>{d['duration']} min</b>\n   Reason: {d['reason']}"
        )
    send_telegram("\n".join(lines))

# ── HTML generation ───────────────────────────────────────────────────────────
def fmt_time(dt):   return dt.strftime("%H:%M") if dt else "—"
def eff_color(pct): return "#22c55e" if pct >= 75 else ("#f59e0b" if pct >= 50 else "#ef4444")

def generate_html(cycles, downtimes, period_from, period_to, timeline_data, conn):
    generated  = datetime.now().strftime("%d.%m.%Y %H:%M")
    period_str = f"{fmt_time(period_from)} – {fmt_time(period_to)}"

    def timeline_bar(mname):
        segs      = timeline_data.get(mname, [])
        bars      = ""
        for s in segs:
            if s["w"] <= 0.1:
                continue
            color   = "#22c55e" if s["state"] == "1" else "#ef4444"
            seg_id  = s["id"]
            label   = s["label"].replace("'", "&#39;").replace('"', "&quot;")
            tip     = f'{s["start"]}–{s["end"]} | {label}'
            bars += (
                f'<div class="tl-seg" '
                f'data-id="{seg_id}" '
                f'data-tip="{tip}" '
                f'style="position:absolute;left:{s["x"]:.2f}%;width:{s["w"]:.2f}%;'
                f'height:100%;background:{color};cursor:pointer;'
                f'transition:filter .15s,opacity .15s"></div>'
            )
        total_min = max(int((period_to - period_from).total_seconds() / 60), 1)
        ticks     = ""
        for i in range(0, total_min + 1, 15):
            pct       = i / total_min * 100
            tick_time = (period_from + timedelta(minutes=i)).strftime("%H:%M")
            # кожні 30 хв — завжди видно; кожні 15 хв — ховати на mobile
            extra_cls = "" if i % 30 == 0 else " tl-tick-15"
            ticks += (f'<div class="tl-tick{extra_cls}" '
                      f'style="left:{pct:.1f}%">{tick_time}</div>')
        return (
            f'<div class="tl-wrap" data-machine="{mname.split("_")[0]}" '
            f'style="position:relative;height:22px;background:#f1f5f9">'
            f'{bars}</div>'
            f'<div class="tl-ticks">{ticks}</div>'
        )

    def history_chart(mname):
        rows_h = load_history(conn, mname)
        if not rows_h:
            return ""
        cid    = mname.replace(" ", "_").replace("-", "_")
        labels = json.dumps([r[0][5:] for r in rows_h])
        eff    = json.dumps([r[1] for r in rows_h])
        run    = json.dumps([r[2] for r in rows_h])
        down   = json.dumps([r[3] for r in rows_h])
        return f'''
        <div class="section-title">📈 7-Day History</div>
        <div style="padding:12px 20px 16px">
          <canvas id="chart_{cid}" height="80"></canvas>
        </div>
        <script>
        (function(){{
          new Chart(document.getElementById("chart_{cid}").getContext("2d"),{{
            type:"bar",
            data:{{labels:{labels},datasets:[
              {{label:"Run (min)",data:{run},backgroundColor:"#22c55e88"}},
              {{label:"Down (min)",data:{down},backgroundColor:"#ef444488"}},
              {{label:"Efficiency %",data:{eff},type:"line",borderColor:"#3b82f6",
               backgroundColor:"transparent",yAxisID:"y2",pointRadius:4,borderWidth:2}}
            ]}},
            options:{{responsive:true,interaction:{{mode:"index"}},
              plugins:{{legend:{{position:"top"}}}},
              scales:{{
                y:{{title:{{display:true,text:"minutes"}}}},
                y2:{{position:"right",min:0,max:100,
                     title:{{display:true,text:"efficiency %"}},
                     grid:{{drawOnChartArea:false}}}}
              }}
            }}
          }});
        }})();
        </script>'''

    def cycles_section(c_list, mname):
        if not c_list:
            return '<p class="empty">No cycles detected</p>'
        short    = mname.split("_")[0] if "_" in mname else mname
        segs     = timeline_data.get(mname, [])

        # Знаходимо відповідний seg_id для циклу за часом старту
        def find_seg_id(cycle):
            for s in segs:
                if s["state"] == "1" and s["start"] == cycle["start"].strftime("%H:%M"):
                    return s["id"]
            # якщо точного збігу немає — шукаємо сегмент що містить цей час
            for s in segs:
                if s["state"] == "1" and s["start"] <= cycle["start"].strftime("%H:%M") <= s["end"]:
                    return s["id"]
            return ""

        rows_html = ""
        for i, c in enumerate(c_list, 1):
            badge  = ' <span class="badge ongoing">in progress</span>' if c.get("ongoing") else ""
            end_s  = "…" if not c.get("end") else fmt_time(c["end"])
            sid    = find_seg_id(c)
            rows_html += (
                f'<tr class="tl-row" data-id="{sid}" '
                f'style="cursor:pointer" title="Highlight on timeline">'
                f'<td>{i}</td><td>{c["program"] or "—"}</td>'
                f'<td>{fmt_time(c["start"])}</td>'
                f'<td>{end_s}{badge}</td>'
                f'<td><strong>{c["duration"]} min</strong></td></tr>'
            )

        by_prog  = defaultdict(list)
        for c in c_list:
            by_prog[c["program"] or "—"].append(c["duration"])
        avg_rows = "".join(
            f'<tr><td><em>{p}</em></td><td>{len(d)} cycles</td>'
            f'<td><strong>{round(sum(d)/len(d),1)} min avg</strong></td>'
            f'<td>{min(d)} / {max(d)} min</td></tr>'
            for p, d in by_prog.items())

        cycles_max_h = "200px" if len(c_list) > 5 else "auto"
        return (
            f'<div class="table-scroll-x">'
            f'<div class="scroll-table-wrap">'
            f'<table class="scroll-table"><thead><tr><th>#</th><th>Program</th><th>Start</th><th>End</th><th>Duration</th></tr></thead></table>'
            f'<div class="scroll-tbody-wrap" style="max-height:{cycles_max_h};overflow-y:auto">'
            f'<table class="scroll-table"><tbody>{rows_html}</tbody></table>'
            f'</div>'
            f'</div></div>'
            f'<div class="section-title" style="border-top:2px dashed #e2e8f0">📊 Average Cycle Time</div>'
            f'<div class="table-scroll-x"><table><thead><tr><th>Program</th><th>Count</th><th>Average</th><th>Min / Max</th></tr></thead>'
            f'<tbody>{avg_rows}</tbody></table></div>'
        )

    machines_html = ""
    for mname in sorted(cycles.keys()):
        c_list     = cycles.get(mname, [])
        d_data     = downtimes.get(mname, {})
        d_list     = d_data.get("downtimes", [])
        total_min  = d_data.get("total_min", 1)
        total_run  = d_data.get("total_run", 0)
        total_down = d_data.get("total_down", 0)
        eff        = round(total_run / total_min * 100) if total_min else 0
        short_name = mname.split("_")[0] if "_" in mname else mname

        segs_for_down = timeline_data.get(mname, [])

        def find_down_seg_id(d):
            for s in segs_for_down:
                if s["state"] == "0" and s["start"] == d["start"].strftime("%H:%M"):
                    return s["id"]
                if s["state"] == "0" and s["start"] <= d["start"].strftime("%H:%M") <= s["end"]:
                    return s["id"]
            return ""

        down_rows = "".join(
            f'<tr class="tl-row" data-id="{find_down_seg_id(d)}" '
            f'style="cursor:pointer" title="Highlight on timeline">'
            f'<td>{fmt_time(d["start"])}</td>'
            f'<td>{"…" if not d.get("end") else fmt_time(d["end"])}'
            f'{"<span class=\"badge ongoing\">ongoing</span>" if d.get("ongoing") else ""}</td>'
            f'<td><strong>{d["duration"]} min</strong></td><td>{d["reason"]}</td></tr>'
            for d in d_list)
        down_max_h = "200px" if len(d_list) > 5 else "auto"
        down_table = (
            f'<div class="table-scroll-x"><div class="scroll-table-wrap">'
            f'<table class="scroll-table"><thead><tr><th>Start</th><th>End</th><th>Duration</th><th>Reason</th></tr></thead></table>'
            f'<div class="scroll-tbody-wrap" style="max-height:{down_max_h};overflow-y:auto">'
            f'<table class="scroll-table"><tbody>{down_rows}</tbody></table>'
            f'</div></div></div>'
            if d_list else '<p class="empty">No downtime detected</p>')

        machines_html += f"""
        <div class="machine-card">
          <div class="machine-header">
            <div class="machine-title">
              <span class="machine-id">{short_name}</span>
              <span class="machine-full">{mname}</span>
            </div>
            <div class="eff-badge" style="background:{eff_color(eff)}">
              Efficiency: {eff}%
              <span class="eff-detail">({total_run} / {total_min} min)</span>
            </div>
          </div>
          <div class="section-title">⏱ Timeline</div>
          <div style="padding:10px 20px 4px">{timeline_bar(mname)}</div>
          <div class="section-title">🔄 Program Cycles ({len(c_list)})</div>
          <div style="padding:0 0 4px">{cycles_section(c_list, mname)}</div>
          <div class="section-title">⏸ Downtime ({len(d_list)}) — {total_down} min total</div>
          {down_table}
          {history_chart(mname)}
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Machine Report — {generated}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',Arial,sans-serif;background:#f0f2f5;color:#1a1a2e;font-size:15px}}

  /* ── Header ── */
  .header{{background:#1a1a2e;color:white;padding:16px 20px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}}
  .header h1{{font-size:1.2rem;font-weight:600}}
  .header .meta{{font-size:.8rem;opacity:.7;text-align:right}}

  /* ── Layout ── */
  .container{{max-width:1100px;margin:16px auto;padding:0 12px}}

  /* ── Machine card ── */
  .machine-card{{background:white;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.08);margin-bottom:20px;overflow:hidden}}
  .machine-header{{background:#1e293b;color:white;padding:14px 16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}}
  .machine-title{{display:flex;flex-direction:column;gap:2px}}
  .machine-id{{font-size:1.1rem;font-weight:700}}
  .machine-full{{font-size:.7rem;opacity:.6;word-break:break-all}}
  .eff-badge{{padding:5px 12px;border-radius:20px;font-weight:600;font-size:.85rem;color:white;white-space:nowrap}}
  .eff-detail{{font-size:.72rem;font-weight:400;opacity:.85;margin-left:4px}}

  /* ── Section title ── */
  .section-title{{padding:10px 16px 5px;font-weight:600;font-size:.8rem;color:#475569;border-top:1px solid #f1f5f9;text-transform:uppercase;letter-spacing:.03em}}

  /* ── Tables (desktop) ── */
  table{{width:100%;border-collapse:collapse;font-size:.85rem}}
  th{{background:#f8fafc;padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:1px solid #e2e8f0;white-space:nowrap}}
  td{{padding:8px 12px;border-bottom:1px solid #f1f5f9;word-break:break-word}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#f8fafc}}

  /* ── Scrollable tables ── */
  .scroll-table-wrap{{width:100%;border-bottom:1px solid #e2e8f0}}
  .scroll-table{{width:100%;border-collapse:collapse;table-layout:fixed;font-size:.85rem}}
  .scroll-table th,.scroll-table td{{padding:8px 12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .scroll-table thead th{{background:#f8fafc;font-weight:600;color:#64748b;border-bottom:1px solid #e2e8f0;position:sticky;top:0;z-index:1}}
  .scroll-table tbody tr:last-child td{{border-bottom:none}}
  .scroll-table tbody tr:hover td{{background:#f8fafc}}
  .scroll-tbody-wrap{{overflow-y:auto;display:block}}
  .scroll-tbody-wrap::-webkit-scrollbar{{width:5px}}
  .scroll-tbody-wrap::-webkit-scrollbar-track{{background:#f1f5f9}}
  .scroll-tbody-wrap::-webkit-scrollbar-thumb{{background:#cbd5e1;border-radius:3px}}

  /* ── Timeline ── */
  .tl-wrap{{overflow:hidden!important;border-radius:4px}}
  .tl-ticks{{position:relative;height:18px}}
  .tl-tick{{position:absolute;font-size:9px;color:#94a3b8;transform:translateX(-50%)}}

  /* ── Legend ── */
  .legend{{display:flex;gap:14px;padding:4px 16px 10px;font-size:.78rem;flex-wrap:wrap}}
  .legend span{{display:flex;align-items:center;gap:4px}}
  .dot{{width:11px;height:11px;border-radius:2px;display:inline-block;flex-shrink:0}}

  /* ── Misc ── */
  .badge{{display:inline-block;padding:2px 7px;border-radius:10px;font-size:.72rem;font-weight:600;margin-left:3px}}
  .badge.ongoing{{background:#dbeafe;color:#1d4ed8}}
  .empty{{padding:12px 16px;color:#94a3b8;font-style:italic;font-size:.85rem}}
  .footer{{text-align:center;color:#94a3b8;font-size:.75rem;padding:16px;word-break:break-all}}

  /* ── Tooltip ── */
  #tl-tooltip{{
    position:fixed;pointer-events:none;z-index:9999;
    background:#1e293b;color:white;
    padding:6px 10px;border-radius:6px;font-size:.78rem;
    box-shadow:0 4px 12px rgba(0,0,0,.3);
    display:none;max-width:240px;white-space:normal;line-height:1.4
  }}

  /* ── Highlight ── */
  .tl-seg.dim{{opacity:.25}}
  .tl-seg.highlight{{filter:brightness(1.25);outline:2px solid white;z-index:2;position:relative}}
  .tl-row.highlight td{{background:#fef9c3!important}}
  .tl-row:hover td{{background:#f0f9ff!important}}

  /* ══════════════════════════════════════════
     MOBILE  ≤ 600px
  ══════════════════════════════════════════ */
  @media(max-width:600px){{
    body{{font-size:14px}}
    .header{{padding:12px 14px}}
    .header h1{{font-size:1rem}}
    .container{{padding:0 8px;margin:10px auto}}
    .machine-card{{border-radius:8px;margin-bottom:14px}}
    .machine-header{{padding:12px 12px}}
    .machine-id{{font-size:1rem}}
    .eff-badge{{font-size:.78rem;padding:4px 10px}}
    .eff-detail{{display:none}}          /* прибираємо (x/y min) на малих екранах */
    .section-title{{padding:8px 12px 4px;font-size:.75rem}}

    /* Таблиці — горизонтальний скрол замість truncate */
    .table-scroll-x{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
    table,.scroll-table{{font-size:.78rem;min-width:360px}}
    th,td,.scroll-table th,.scroll-table td{{padding:7px 9px}}

    /* Timeline — тікі тільки кожні 30 хв */
    .tl-tick-15{{display:none}}
    .tl-ticks{{height:16px}}
    .tl-tick{{font-size:8px}}

    /* Tooltip — завжди знизу екрану на mobile */
    #tl-tooltip{{
      position:fixed;bottom:12px;left:50%;transform:translateX(-50%);
      top:auto!important;max-width:92vw;text-align:center
    }}

    .legend{{padding:4px 12px 8px;font-size:.74rem;gap:10px}}
    .footer{{font-size:.7rem;padding:12px}}
  }}
</style>
</head>
<body>
<div class="header">
  <h1>📊 Machine Report</h1>
  <div class="meta">Period: {period_str}<br>Generated: {generated}</div>
</div>
<div class="container">
  <div class="legend">
    <span><span class="dot" style="background:#22c55e"></span>Running</span>
    <span><span class="dot" style="background:#ef4444"></span>Downtime</span>
  </div>
  {machines_html}
</div>
<div class="footer">Source: {CSV_FILE} &nbsp;|&nbsp; DB: {DB_FILE}</div>
<div id="tl-tooltip"></div>
<script>
(function(){{
  var tip = document.getElementById("tl-tooltip");

  // ── Tooltip + highlight при наведенні на сегмент таймлайну ────────────────
  document.querySelectorAll(".tl-seg").forEach(function(seg){{
    seg.addEventListener("mouseenter", function(e){{
      var id  = seg.dataset.id;
      var txt = seg.dataset.tip;
      tip.textContent = txt;
      tip.style.display = "block";
      var wrap = seg.closest(".tl-wrap");
      wrap.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.add("dim"); }});
      seg.classList.remove("dim");
      seg.classList.add("highlight");
      if(id) document.querySelectorAll('.tl-row[data-id="'+id+'"]').forEach(function(r){{
        r.classList.add("highlight");
      }});
    }});
    seg.addEventListener("mousemove", function(e){{
      tip.style.left = (e.clientX + 14) + "px";
      tip.style.top  = (e.clientY - 32) + "px";
    }});
    seg.addEventListener("mouseleave", function(){{
      tip.style.display = "none";
      var wrap = seg.closest(".tl-wrap");
      wrap.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.remove("dim","highlight"); }});
      var id = seg.dataset.id;
      if(id) document.querySelectorAll('.tl-row[data-id="'+id+'"]').forEach(function(r){{
        r.classList.remove("highlight");
      }});
    }});
  }});

  // ── Highlight сегменту при наведенні на рядок таблиці ─────────────────────
  document.querySelectorAll(".tl-row").forEach(function(row){{
    row.addEventListener("mouseenter", function(){{
      var id = row.dataset.id;
      if(!id) return;
      var seg = document.querySelector('.tl-seg[data-id="'+id+'"]');
      if(!seg) return;
      var wrap = seg.closest(".tl-wrap");
      wrap.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.add("dim"); }});
      seg.classList.remove("dim");
      seg.classList.add("highlight");
      tip.textContent = seg.dataset.tip;
      tip.style.display = "block";
      tip.style.left = (row.getBoundingClientRect().right + 10) + "px";
      tip.style.top  = (row.getBoundingClientRect().top + window.scrollY) + "px";
    }});
    row.addEventListener("mouseleave", function(){{
      var id = row.dataset.id;
      if(!id) return;
      var seg = document.querySelector('.tl-seg[data-id="'+id+'"]');
      if(seg){{
        var wrap = seg.closest(".tl-wrap");
        wrap.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.remove("dim","highlight"); }});
      }}
      tip.style.display = "none";
    }});
  }});

  // ── Touch support for mobile ──────────────────────────────────────────
  document.querySelectorAll(".tl-seg").forEach(function(seg){{
    seg.addEventListener("touchstart", function(e){{
      e.preventDefault();
      tip.textContent = seg.dataset.tip;
      tip.style.display = "block";
      var wrap = seg.closest(".tl-wrap");
      wrap.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.add("dim"); }});
      seg.classList.remove("dim"); seg.classList.add("highlight");
      var id = seg.dataset.id;
      if(id) document.querySelectorAll('.tl-row[data-id="'+id+'"]').forEach(function(r){{
        r.classList.add("highlight");
      }});
    }}, {{passive:false}});
    seg.addEventListener("touchend", function(){{
      setTimeout(function(){{
        tip.style.display = "none";
        var wrap = seg.closest(".tl-wrap");
        wrap.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.remove("dim","highlight"); }});
        var id = seg.dataset.id;
        if(id) document.querySelectorAll('.tl-row[data-id="'+id+'"]').forEach(function(r){{
          r.classList.remove("highlight");
        }});
      }}, 1400);
    }});
  }});
}})();
</script>
</body>
</html>"""
# =============================================================================
def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Step 1 — clear old files
    print("── Step 1: Clearing old CSV files ──")
    clear_old_csv()

    # Step 2 — download
    print("── Step 2: Downloading CSV ──")
    if not download_csv():
        print("Download failed — aborting.")
        sys.exit(1)

    # Step 3 — analyze
    print("── Step 3: Analyzing data ──")
    rows = load_csv()
    print(f"Rows loaded: {len(rows)}")

    filtered, period_from, period_to = filter_last_hours(rows, HOURS_BACK)
    date_str = period_to.strftime("%Y-%m-%d")
    print(f"Period: {period_from.strftime('%H:%M')} – {period_to.strftime('%H:%M')} ({len(filtered)} rows)")

    cycles        = analyze_cycles(filtered)
    downtimes     = analyze_downtime(filtered)
    timeline_data = build_timeline_data(filtered, period_from, period_to)

    conn = init_db()
    save_to_db(conn, date_str, cycles, downtimes)
    print("History saved to DB")

    check_and_alert(downtimes, period_to)

    # Step 4 — report
    print("── Step 4: Generating report ──")
    html = generate_html(cycles, downtimes, period_from, period_to, timeline_data, conn)
    conn.close()

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Report saved: {OUTPUT_HTML}")

    # Step 5 — publish to GitHub Pages
    print("── Step 5: Publishing to GitHub Pages ──")
    publish_to_github(html)

if __name__ == "__main__":
    main()
