# VERSION: 2026-03-06-v9
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
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, WebDriverException

# ── Configuration ─────────────────────────────────────────────────────────────
DOWNLOAD_DIR        = r"C:\Connectplan_raports"
CSV_FILE            = os.path.join(DOWNLOAD_DIR, "operation_history.csv")
MACHINING_RESULT    = os.path.join(DOWNLOAD_DIR, "machining_results.csv")
OUTPUT_HTML         = os.path.join(DOWNLOAD_DIR, "index.html")
DB_FILE             = os.path.join(DOWNLOAD_DIR, "history.db")
LOG_FILE            = os.path.join(DOWNLOAD_DIR, "factory_monitor.log")
URL                 = "http://192.168.1.210/csv/OutputCSVWeb.aspx?FactoryID=1&AreaID=1"
WAIT_TIME           = 300
HOURS_BACK          = 24
ALERT_THRESHOLD_MIN = 45

# Повний список станків дільниці — використовується для графіків і розрахунку SITE
# Станки без даних отримують ефективність 0 і відображаються на графіку
ALL_MACHINES = ["M1_M560R-V-e_0712-100198", "M2_M560R-V-e-M5V01235",
                "T1_L300E-M_PEA351",        "T2_ L300-MYW-e_MYW197",
                "T3_LB2000EXII_254633"]

GITHUB_USER  = "wisefab1"
GITHUB_REPO  = "factory_monitor"
GITHUB_URL   = "https://wisefab1.github.io/factory_monitor/"

# ── Secrets (завантажуються з файлу, не зберігаються в коді) ─────────────────
_SECRETS_FILE = os.path.join(DOWNLOAD_DIR, "secrets.json")
def _load_secrets():
    try:
        with open(_SECRETS_FILE, encoding="utf-8") as _f:
            return json.load(_f)
    except Exception:
        return {}
_secrets         = _load_secrets()
TELEGRAM_TOKEN   = _secrets.get("telegram_token",   "")
TELEGRAM_CHAT_ID = _secrets.get("telegram_chat_id", "")
GITHUB_TOKEN     = _secrets.get("github_token",     "")

TARGET_TIME_FILE = r"\\wisefile\Wisefile\WF_Tootmine\Planeerimine\CNC toodete ajad pildid\CNC tehno.xlsm"
TARGET_CACHE_FILE = os.path.join(DOWNLOAD_DIR, "target_times_cache.json")  # Кеш файл

# ── Logging ───────────────────────────────────────────────────────────────────
def log(message: str):
    """Виводить повідомлення в консоль та зберігає у файл логів."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    print(log_line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception as e:
        print(f"Failed to write log: {e}")

# ── Excel Target Time ─────────────────────────────────────────────────────────
# ── Excel Target Time ─────────────────────────────────────────────────────────
def normalize_program_name(name: str) -> str:
    """Нормалізує назву програми для порівняння
    
    Порядок обробки:
    1. Видалення розширення
    2. Визначення операції та видалення (op1-5, p1-5, -1 до -5)
    3. Видалення останньої L або R
    4. Переведення у верхній регістр
    5. Видалення пробілів, дефісів, підкреслень
    6. Заміна 101 на 100
    
    Приклад: "WF861-100L-P2.MIN" → "WF861100"
    Приклад: "WF080-920-2.MIN" → "WF080920"
    """
    if not name:
        return ""
    
    # 1. Видаляємо розширення
    if '.' in name:
        name = name.rsplit('.', 1)[0]
    
    # 2. Визначаємо та видаляємо номер операції з кінця
    # Спочатку шукаємо op1-5 або p1-5
    name_lower = name.lower()
    found_op = False
    for op in ['op5', 'p5', 'op4', 'p4', 'op3', 'p3', 'op2', 'p2', 'op1', 'p1']:
        for variant in [f'-{op}', f'_{op}', op]:
            if name_lower.endswith(variant):
                name = name[:len(name) - len(variant)]
                found_op = True
                break
        if found_op:
            break
    
    # Якщо не знайшли op/p, шукаємо просто -1, -2, -3, -4, -5 в кінці
    if not found_op:
        for digit in ['5', '4', '3', '2', '1']:
            if name.endswith(f'-{digit}') or name.endswith(f'_{digit}'):
                name = name[:-2]
                break
    
    # 3. Видаляємо всі букви після останньої цифри
    # Знаходимо позицію останньої цифри
    last_digit_pos = -1
    for i in range(len(name) - 1, -1, -1):
        if name[i].isdigit():
            last_digit_pos = i
            break
    
    # Якщо знайшли цифру - обрізаємо все після неї
    if last_digit_pos >= 0 and last_digit_pos < len(name) - 1:
        name = name[:last_digit_pos + 1]
    
    # 4. Переводимо в верхній регістр
    name = name.upper()
    
    # 5. Видаляємо пробіли, дефіси, підкреслення
    name = name.replace(' ', '').replace('-', '').replace('_', '')
    
    # 6. Замінюємо 101 на 100 (WF861-100L та WF861_101L - одна деталь)
    name = name.replace('101', '100')
    
    return name

def parse_program_name(program_name: str) -> tuple:
    """Розбирає назву програми на базову назву та номер операції
    
    Args:
        program_name: повна назва програми (напр. "WF861-100L-P3.MIN")
    
    Returns:
        tuple: (normalized_base, operation_number)
        Приклад: "WF861-100L-P3.MIN" → ("WF861100", 3)
                 "WF080-920-2.MIN" → ("WF080920", 2)
    """
    normalized = normalize_program_name(program_name)  # "WF861100"
    operation = get_operation_number(program_name)      # 3
    return (normalized, operation)

def get_operation_number(program_name: str) -> int:
    """Визначає номер операції з назви програми
    
    Номер операції завжди перед крапкою що йде перед розширенням файлу.
    Наприклад: WF861-100L-P2.MIN → OP2
               WF861-100L.MIN → OP1 (немає номера)
               PART-OP3.PRG → OP3
    """
    if not program_name or program_name == "—":
        return 1
    
    # Видаляємо розширення (все після останньої крапки)
    if '.' in program_name:
        base_name = program_name.rsplit('.', 1)[0]
    else:
        base_name = program_name
    
    # Переводимо в нижній регістр для перевірки
    base_lower = base_name.lower()
    
    # Шукаємо op5/p5/-5 в кінці назви
    if base_lower.endswith('op5') or base_lower.endswith('p5') or base_lower.endswith('-p5') or base_lower.endswith('-5') or base_lower.endswith('_5'):
        return 5
    # Шукаємо op4/p4/-4
    elif base_lower.endswith('op4') or base_lower.endswith('p4') or base_lower.endswith('-p4') or base_lower.endswith('-4') or base_lower.endswith('_4'):
        return 4
    # Шукаємо op3/p3/-3
    elif base_lower.endswith('op3') or base_lower.endswith('p3') or base_lower.endswith('-p3') or base_lower.endswith('-3') or base_lower.endswith('_3'):
        return 3
    # Шукаємо op2/p2/-2
    elif base_lower.endswith('op2') or base_lower.endswith('p2') or base_lower.endswith('-p2') or base_lower.endswith('-2') or base_lower.endswith('_2'):
        return 2
    # Шукаємо op1/p1/-1
    elif base_lower.endswith('op1') or base_lower.endswith('p1') or base_lower.endswith('-p1') or base_lower.endswith('-1') or base_lower.endswith('_1'):
        return 1
    # Якщо не знайдено жодного номера - це OP1
    else:
        return 1

def load_target_times():
    """Завантажує Target Time з Excel файлу з кешуванням
    
    Логіка:
    1. Перевіряємо чи доступний Excel файл
    2. Якщо доступний - порівнюємо timestamp з кешем
    3. Якщо Excel новіший - завантажуємо і оновлюємо кеш
    4. Якщо Excel не доступний - використовуємо кеш
    5. Якщо немає ні Excel ні кешу - повертаємо {}
    """
    import json
    from datetime import datetime
    
    excel_available = False
    excel_mtime = None
    cache_mtime = None
    
    # Перевіряємо доступність Excel файлу
    try:
        if os.path.exists(TARGET_TIME_FILE):
            excel_mtime = os.path.getmtime(TARGET_TIME_FILE)
            excel_available = True
            log(f"Excel file found: {TARGET_TIME_FILE}")
            log(f"Excel modified: {datetime.fromtimestamp(excel_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            log(f"Excel file not accessible: {TARGET_TIME_FILE}")
    except Exception as e:
        log(f"Cannot access Excel file: {e}")
        excel_available = False
    
    # Перевіряємо наявність кешу
    cache_exists = os.path.exists(TARGET_CACHE_FILE)
    if cache_exists:
        cache_mtime = os.path.getmtime(TARGET_CACHE_FILE)
        log(f"Cache file found: {TARGET_CACHE_FILE}")
        log(f"Cache modified: {datetime.fromtimestamp(cache_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Вирішуємо звідки завантажувати
    use_excel = False
    
    if excel_available:
        if cache_exists:
            # Є обидва - порівнюємо timestamp
            if excel_mtime > cache_mtime:
                log("✓ Excel is newer than cache - will update from Excel")
                use_excel = True
            else:
                log("✓ Cache is up-to-date - will use cache")
                use_excel = False
        else:
            # Є тільки Excel - використовуємо його
            log("✓ No cache exists - will load from Excel")
            use_excel = True
    else:
        if cache_exists:
            # Excel недоступний, є кеш - використовуємо кеш
            log("⚠ Excel not accessible - will use cache")
            use_excel = False
        else:
            # Немає ні Excel ні кешу
            log("✗ No Excel and no cache - returning empty")
            return {}
    
    # Завантажуємо з Excel
    if use_excel:
        target_times = _load_from_excel()
        if target_times:
            # Зберігаємо в кеш
            _save_to_cache(target_times)
        return target_times
    
    # Завантажуємо з кешу
    else:
        return _load_from_cache()


def _load_from_excel():
    """Завантажує дані з Excel файлу
    
    Returns:
        dict: {(program, operation, machine): time} або {}
    """
    try:
        import openpyxl
        log(f"Loading target times from Excel...")
        
        # Спроба 1: Прямий доступ до UNC шляху
        try:
            wb = openpyxl.load_workbook(TARGET_TIME_FILE, read_only=True, data_only=True)
            log("✓ Direct UNC access successful")
        except Exception as e1:
            log(f"Direct UNC access failed: {e1}")
            
            # Спроба 2: Через pathlib
            try:
                from pathlib import Path
                path = Path(TARGET_TIME_FILE)
                wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
                log("✓ Pathlib access successful")
            except Exception as e2:
                log(f"Pathlib access failed: {e2}")
                
                # Спроба 3: Через os.path.normpath
                try:
                    normalized = os.path.normpath(TARGET_TIME_FILE)
                    wb = openpyxl.load_workbook(normalized, read_only=True, data_only=True)
                    log("✓ Normalized path access successful")
                except Exception as e3:
                    log(f"All access methods failed: {e3}")
                    return {}
        
        # Шукаємо вкладку "Tehnoloogiad"
        if "Tehnoloogiad" not in wb.sheetnames:
            log(f"Warning: Sheet 'Tehnoloogiad' not found. Available: {wb.sheetnames}")
            wb.close()
            return {}
        
        ws = wb["Tehnoloogiad"]
        target_times = {}
        
        # Маппінг колонок: (станок_col, час_col)
        op_columns = {
            1: (11, 14),   # K, N
            2: (16, 19),   # P, S
            3: (21, 24),   # U, X
            4: (26, 29),   # Z, AC
            5: (31, 34),   # AE, AH
        }
        
        rows_processed = 0
        for row in ws.iter_rows(min_row=2, values_only=False):
            program_cell = row[0]
            if not program_cell or not program_cell.value:
                continue
            
            program_name = str(program_cell.value).strip()
            if not program_name:
                continue
            
            rows_processed += 1
            
            for op_num, (machine_col, time_col) in op_columns.items():
                machine_cell = row[machine_col - 1]
                time_cell = row[time_col - 1]
                
                if machine_cell and machine_cell.value and time_cell and time_cell.value:
                    machine = str(machine_cell.value).strip()
                    try:
                        time_val = float(time_cell.value)
                        key = (program_name, op_num, machine)
                        target_times[key] = time_val
                    except (ValueError, TypeError):
                        continue
        
        wb.close()
        log(f"✓ Loaded {len(target_times)} target times from {rows_processed} rows")
        return target_times
        
    except ImportError:
        log("Warning: openpyxl not installed")
        return {}
    except Exception as e:
        log(f"Error loading from Excel: {e}")
        return {}


def _save_to_cache(target_times):
    """Зберігає target times в JSON кеш
    
    Args:
        target_times: словник {(program, op, machine): time}
    """
    import json
    from datetime import datetime
    
    try:
        # Конвертуємо ключі-tuple в строки для JSON
        cache_data = {
            "updated": datetime.now().isoformat(),
            "count": len(target_times),
            "data": {
                f"{prog}|{op}|{machine}": time_val
                for (prog, op, machine), time_val in target_times.items()
            }
        }
        
        with open(TARGET_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)
        
        log(f"✓ Cache saved: {len(target_times)} records")
        
    except Exception as e:
        log(f"Warning: Failed to save cache: {e}")


def _load_from_cache():
    """Завантажує target times з JSON кешу
    
    Returns:
        dict: {(program, operation, machine): time} або {}
    """
    import json
    from datetime import datetime
    
    try:
        with open(TARGET_CACHE_FILE, "r", encoding="utf-8") as f:
            cache_data = json.load(f)
        
        updated = cache_data.get("updated", "unknown")
        count = cache_data.get("count", 0)
        data = cache_data.get("data", {})
        
        log(f"✓ Loaded from cache: {count} records (updated: {updated})")
        
        # Конвертуємо строкові ключі назад в tuple
        target_times = {}
        for key_str, time_val in data.items():
            parts = key_str.split("|")
            if len(parts) == 3:
                prog, op_str, machine = parts
                try:
                    op = int(op_str)
                    target_times[(prog, op, machine)] = time_val
                except ValueError:
                    continue
        
        return target_times
        
    except Exception as e:
        log(f"Error loading from cache: {e}")
        return {}


def get_actual_cycle_time_from_mr(machine_name, program_name, mr_data):
    """Бере фактичний час циклу з MachiningResult (RunStateTime / Counter)
    
    Це НАЙТОЧНІШИЙ метод - дані прямо з верстата.
    Береться МЕДІАНА з усіх записів для програми (щоб відсіяти паузи).
    
    Args:
        machine_name: повна назва машини (напр. "M1_M560R-V-e_0712-100198")
        program_name: назва програми (напр. "WF901-907-1.MIN")
        mr_data: список записів з machining_results.csv
    
    Returns:
        float: фактичний час одного циклу в хвилинах або None
    """
    if not mr_data:
        log(f"  get_actual_cycle_time_from_mr: mr_data is empty!")
        return None
    
    log(f"  get_actual_cycle_time_from_mr: machine={machine_name}, program={program_name}, mr_data records={len(mr_data)}")
    
    # Нормалізуємо назву програми для порівняння
    prog_base, prog_op = parse_program_name(program_name)
    
    # Збираємо ВСІ записи для цієї програми
    cycle_times = []
    
    for mr in mr_data:
        mr_machine = mr.get("MachineName", "")
        mr_prog = mr.get("ProgramFileName", "")
        
        if mr_machine != machine_name:
            continue
        
        # Порівнюємо програми
        mr_prog_base, mr_prog_op = parse_program_name(mr_prog)
        if mr_prog_base != prog_base or mr_prog_op != prog_op:
            continue
        
        # Знайшли запис для цієї програми
        run_time = int(mr.get("RunStateTime", 0))  # секунди
        counter = int(mr.get("Counter", 1))
        
        if run_time > 0 and counter > 0:
            cycle_time = run_time / counter / 60  # хвилини
            cycle_times.append(cycle_time)
            log(f"  • MR record: RunStateTime={run_time}s, Counter={counter} → {round(cycle_time, 2)} min/cycle")
    
    if not cycle_times:
        log(f"  ✗ No MR records found for {machine_name} / {program_name}")
        return None
    
    # Беремо МЕДІАНУ (щоб відсіяти викиди з паузами)
    sorted_times = sorted(cycle_times)
    n = len(sorted_times)
    
    if n == 1:
        result = sorted_times[0]
    elif n % 2 == 0:
        result = (sorted_times[n//2 - 1] + sorted_times[n//2]) / 2
    else:
        result = sorted_times[n//2]
    
    result = round(result, 2)
    log(f"  ✓ Median from {n} records = {result} min")
    
    return result


def filter_completed_cycles(cycles_list):
    """Фільтрує тільки 'хороші' завершені цикли для розрахунку

    Відсіює:
    - Перервані цикли (без end)
    - Занадто короткі (<0.5 хв = 30 сек)
    """
    good_durations = []

    for c in cycles_list:
        duration = c.get("duration", 0)
        if duration < 0.5:
            continue
        good_durations.append(duration)
    
    return good_durations


def calculate_real_cycle_time(durations):
    """KDE, bandwidth=0.3. Знаходить найщільніший пік → середнє кластеру."""
    if not durations:
        return None
    if isinstance(durations[0], (tuple, list)):
        vals = [d for d, _ in durations if d > 0]
    else:
        vals = [d for d in durations if d > 0]
    if not vals:
        return None

    bandwidth = 0.3
    step = 0.05
    best_center, best_count = None, 0
    v = min(vals)
    while v <= max(vals) + step:
        count = sum(1 for x in vals if abs(x - v) <= bandwidth)
        if count > best_count:
            best_count, best_center = count, v
        v = round(v + step, 4)

    cluster = [x for x in vals if abs(x - best_center) <= bandwidth]
    return round(sum(cluster) / len(cluster), 2) if cluster else round(sum(vals) / len(vals), 2)


def calculate_cycle_time_smart(machine_name, program_name, cycles_list, mr_data):
    vals = [c["duration"] for c in cycles_list if c.get("duration", 0) > 0]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 2)


def calculate_real_cycle_time_OLD(durations):
    """Визначає реальну довжину циклу методом медіани та щільного кластеру
    
    Args:
        durations: список тривалостей циклів (хвилини)
    
    Returns:
        float: реальна довжина циклу або None якщо недостатньо даних
    """
    if not durations or len(durations) < 3:
        return None
    
    # 1. Сортуємо
    sorted_durations = sorted(durations)
    n = len(sorted_durations)
    
    # 2. Знаходимо медіану всього масиву
    if n % 2 == 0:
        median = (sorted_durations[n//2 - 1] + sorted_durations[n//2]) / 2
    else:
        median = sorted_durations[n//2]
    
    # 3. Вікно допуску = медіана × 0.30
    window = median * 0.30
    
    # 4. Для кожного елементу рахуємо кількість сусідів
    neighbor_counts = []
    for i, value in enumerate(sorted_durations):
        count = sum(1 for d in sorted_durations if value - window <= d <= value + window)
        neighbor_counts.append((i, value, count))
    
    # 5. Знаходимо елемент(и) з максимальною кількістю сусідів
    max_neighbors = max(nc[2] for nc in neighbor_counts)
    centers = [nc for nc in neighbor_counts if nc[2] == max_neighbors]
    
    # Якщо кілька центрів - беремо той що ближче до середини списку
    if len(centers) > 1:
        middle_idx = n / 2
        center = min(centers, key=lambda nc: abs(nc[0] - middle_idx))
    else:
        center = centers[0]
    
    center_value = center[1]
    
    # 6. Зібрати всі елементи в діапазоні ±вікно навколо центру
    cluster = [d for d in sorted_durations if center_value - window <= d <= center_value + window]
    
    # 7. Медіана кластеру
    cluster_sorted = sorted(cluster)
    cluster_n = len(cluster_sorted)
    
    if cluster_n == 0:
        return None
    elif cluster_n % 2 == 0:
        result = (cluster_sorted[cluster_n//2 - 1] + cluster_sorted[cluster_n//2]) / 2
    else:
        result = cluster_sorted[cluster_n//2]
    
    return round(result, 1)

# PART 1 — DOWNLOAD
# =============================================================================

# =============================================================================
# PART 1 — DOWNLOAD (інтегрована функція download_both_files)
# =============================================================================

def download_both_files():
    """Завантажує operation_history.csv та machining_results.csv за один запуск браузера"""
    log("============================================================")
    log("DOWNLOADING BOTH FILES FROM CONNECT PLAN")
    log("============================================================")
    
    # Крок 1: Видаляємо всі CSV файли
    log("── Step 1: Clearing all CSV files ──")
    for filename in os.listdir(DOWNLOAD_DIR):
        if filename.lower().endswith(".csv"):
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, filename))
                log(f"Deleted: {filename}")
            except Exception as exc:
                log(f"Could not delete {filename}: {exc}")
    
    # Налаштування Chrome
    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory":   DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade":   True,
        "safebrowsing.enabled":         True,
    })
    
    # Оптимізації для швидкості:
    options.add_argument("--headless=new")          # Без GUI - швидше!
    options.add_argument("--disable-gpu")           # Не потрібна графіка
    options.add_argument("--no-sandbox")            # Швидший запуск
    options.add_argument("--disable-dev-shm-usage") # Менше пам'яті
    options.add_argument("--disable-extensions")    # Без розширень
    options.add_argument("--disable-logging")       # Без логів
    options.add_argument("--log-level=3")           # Мінімум логування
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # Крок 2: Відкриваємо сторінку
        log("── Step 2: Opening page in Chrome ──")
        driver.get(URL)
        wait = WebDriverWait(driver, WAIT_TIME)
        # Видалено time.sleep(3) - WebDriverWait вже чекає готовності
        
        # Крок 3: Обираємо всі машини
        log("── Step 3: Selecting all machines ──")
        checkbox = wait.until(EC.element_to_be_clickable((By.ID, "all_machines_check")))
        if not checkbox.is_selected():
            checkbox.click()
            log("✓ All machines selected")
        else:
            log("✓ All machines already selected")
        # Видалено time.sleep(3) - непотрібно
        
        # Крок 4: Тиснемо Download (operation_history)
        log("── Step 4: Downloading operation_history.csv ──")
        btn = wait.until(EC.element_to_be_clickable((By.ID, "btn_Download")))
        click_time_1 = time.time()
        btn.click()
        log("✓ Download button clicked — waiting for first file...")
        
        # Чекаємо перший файл (прискорено: перевірка кожні 0.5 сек)
        deadline = time.time() + WAIT_TIME
        downloaded_1 = None
        while time.time() < deadline:
            for f in os.listdir(DOWNLOAD_DIR):
                if f.lower().endswith(".csv"):
                    fp = os.path.join(DOWNLOAD_DIR, f)
                    if os.path.getmtime(fp) >= click_time_1:
                        downloaded_1 = fp
                        break
            if downloaded_1:
                break
            time.sleep(0.5)  # Було 2 сек → тепер 0.5 сек!
            print(".", end="", flush=True)
        
        print()
        
        if not downloaded_1:
            log("✗ Error: First file did not download")
            return False
        
        log(f"✓ First file downloaded: {os.path.basename(downloaded_1)}")
        
        # Крок 5: Вибираємо MACHINING RESULT з dropdown
        log("── Step 5: Selecting MACHINING RESULT from dropdown ──")
        # Видалено time.sleep(2) - непотрібно
        
        try:
            dropdown = wait.until(EC.presence_of_element_located((By.ID, "ddl_SelectTable")))
            select = Select(dropdown)
            select.select_by_value("MachningResult")  # З опечаткою як в HTML!
            log("✓ Selected: MACHINING RESULT")
            # Видалено time.sleep(3) - чекаємо на кнопку замість sleep
        except Exception as e:
            log(f"✗ Could not select MACHINING RESULT: {e}")
            return False
        
        # Крок 6: Перейменовуємо перший файл (до завантаження другого!)
        log("── Step 6: Renaming first file ──")
        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)
        os.rename(downloaded_1, CSV_FILE)
        log(f"✓ Renamed to: operation_history.csv")
        
        # Показуємо інфо про перший файл
        size1 = os.path.getsize(CSV_FILE)
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            lines1 = sum(1 for _ in f)
        log(f"  Size: {size1:,} bytes, Rows: {lines1}")
        
        # Крок 7: Тиснемо Download (MachiningResult)
        log("── Step 7: Downloading machining_results.csv ──")
        btn = wait.until(EC.element_to_be_clickable((By.ID, "btn_Download")))
        click_time_2 = time.time()
        btn.click()
        log("✓ Download button clicked — waiting for second file...")
        
        # Чекаємо другий файл з КОРОТКИМ timeout (5 секунд)
        # Якщо немає даних - файл не завантажиться, це нормально
        short_timeout = 5  # секунд
        deadline = time.time() + short_timeout
        downloaded_2 = None
        
        while time.time() < deadline:
            for f in os.listdir(DOWNLOAD_DIR):
                if f.lower().endswith(".csv"):
                    fp = os.path.join(DOWNLOAD_DIR, f)
                    # Шукаємо файл новіший за другий клік і не той що вже downloaded_1
                    if os.path.getmtime(fp) >= click_time_2 and fp != downloaded_1:
                        downloaded_2 = fp
                        break
            if downloaded_2:
                break
            time.sleep(0.5)
            print(".", end="", flush=True)
        
        print()
        
        # Якщо другий файл не завантажився - це ОК, можливо немає даних
        if not downloaded_2:
            log("⚠ Second file did not download within 5 seconds (possibly no data)")
            log("✓ Continuing without machining_results.csv")
            # Створюємо порожній файл щоб скрипт не падав
            with open(MACHINING_RESULT, 'w', encoding='utf-8', newline='') as f:
                import csv
                writer = csv.writer(f)
                # Пишемо тільки header без даних
                writer.writerow(['Date', 'MachineName', 'ProgramFileName', 'RunStateTime', 'Counter'])
            log(f"✓ Created empty machining_results.csv")
        else:
            log(f"✓ Second file downloaded: {os.path.basename(downloaded_2)}")
            
            # Перейменовуємо другий файл в machining_results.csv
            if os.path.exists(MACHINING_RESULT):
                os.remove(MACHINING_RESULT)
            os.rename(downloaded_2, MACHINING_RESULT)
            log(f"✓ Renamed to: machining_results.csv")
            
            # Показуємо інфо про другий файл
            size2 = os.path.getsize(MACHINING_RESULT)
            with open(MACHINING_RESULT, 'r', encoding='utf-8') as f:
                lines2 = sum(1 for _ in f)
            log(f"  Size: {size2:,} bytes, Rows: {lines2}")
        
        log("============================================================")
        log("DOWNLOAD COMPLETE")
        log("============================================================")
        return True
        
    except TimeoutException:
        log("✗ Error: element not found on page")
        return False
    except WebDriverException as exc:
        log(f"✗ WebDriver error: {exc}")
        return False
    except Exception as exc:
        log(f"✗ Unexpected error: {exc}")
        import traceback
        log(traceback.format_exc())
        return False
    finally:
        log("Closing browser...")
        # Видалено time.sleep(2) - непотрібно
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
        log("Telegram alert sent")
    except Exception as e:
        log(f"Telegram error: {e}")

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cycle_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, machine TEXT, program TEXT, start_time TEXT, end_time TEXT,
            duration INTEGER
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
        
        # Зберігаємо окремі цикли
        for c in c_list:
            conn.execute("""
                INSERT OR IGNORE INTO cycle_events
                (date,machine,program,start_time,end_time,duration)
                VALUES (?,?,?,?,?,?)
            """, (date_str, mname, c.get("program", "—"),
                  c["start"].strftime("%H:%M") if c.get("start") else "—",
                  c["end"].strftime("%H:%M") if c.get("end") else "—",
                  c["duration"]))
        
        # Зберігаємо простої
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

def get_recent_cycles(conn, machine: str, program: str, current_cycles: list, target_count: int = 20) -> list:
    """Отримує цикли для програми: останні 3 робочі дні станку + з історії якщо потрібно
    
    Args:
        conn: з'єднання з БД
        machine: назва станку
        program: назва програми
        current_cycles: поточні цикли за сьогодні (список dict з duration)
        target_count: цільова кількість циклів (за замовчуванням 20)
    
    Returns:
        list: список тривалостей циклів (int)
    """
    from datetime import datetime
    
    # Збираємо тривалості з поточних циклів
    durations = [c["duration"] for c in current_cycles]
    
    # Знаходимо останні 3 РОБОЧІ ДНІ СТАНКУ (дні коли станок працював, будь-які програми)
    today = datetime.now().strftime("%Y-%m-%d")
    cur = conn.execute("""
        SELECT DISTINCT date 
        FROM cycle_events 
        WHERE machine = ? AND date != ?
        ORDER BY date DESC 
        LIMIT 3
    """, (machine, today))
    
    last_3_working_days = [row[0] for row in cur.fetchall()]
    
    if not last_3_working_days:
        # Немає робочих днів - повертаємо поточні або добираємо з історії
        if len(durations) >= target_count:
            return durations
        # Добираємо з усієї історії
        needed = target_count - len(durations)
        cur = conn.execute("""
            SELECT duration 
            FROM cycle_events 
            WHERE machine = ? AND program = ? AND date != ?
            ORDER BY date DESC, start_time DESC
            LIMIT ?
        """, (machine, program, today, needed))
        historical = [row[0] for row in cur.fetchall()]
        return durations + historical
    
    # Завантажуємо цикли ЦІЄЇ ПРОГРАМИ з останніх 3 робочих днів
    placeholders = ','.join('?' * len(last_3_working_days))
    cur = conn.execute(f"""
        SELECT duration 
        FROM cycle_events 
        WHERE machine = ? AND program = ? AND date IN ({placeholders})
        ORDER BY date DESC, start_time DESC
    """, (machine, program, *last_3_working_days))
    
    cycles_from_3_days = [row[0] for row in cur.fetchall()]
    
    # Об'єднуємо: поточні + з останніх 3 днів
    all_cycles = durations + cycles_from_3_days
    
    # ВАЖЛИВО: Якщо за останні 3 дні >= 20 циклів - беремо ВСІ з цього періоду
    if len(all_cycles) >= target_count:
        return all_cycles
    
    # Якщо менше 20 - добираємо з історії (старіші дні)
    used_days = [today] + last_3_working_days
    used_days_placeholders = ','.join('?' * len(used_days))
    
    needed = target_count - len(all_cycles)
    cur = conn.execute(f"""
        SELECT duration 
        FROM cycle_events 
        WHERE machine = ? AND program = ? AND date NOT IN ({used_days_placeholders})
        ORDER BY date DESC, start_time DESC
        LIMIT ?
    """, (machine, program, *used_days, needed))
    
    historical = [row[0] for row in cur.fetchall()]
    all_cycles.extend(historical)
    
    return all_cycles

# ── Data processing ───────────────────────────────────────────────────────────
def load_csv() -> list[dict]:
    with open(CSV_FILE, encoding="utf-8") as f:
        return list(csv.DictReader(f))

def get_counter_markers(mr_data, cycles_dict):
    """Повертає {machine_name: [datetime, ...]} — моменти COUNTER.MIN що реально
    потрапляють в межі циклу програми на цій машині (Counter >= 1).
    Відображаються як фіолетові лінії на таймлайні.
    """
    parse = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    markers = defaultdict(list)
    for mr in mr_data:
        prog = mr.get("ProgramFileName", "")
        if not prog.upper().startswith("COUNTER"):
            continue
        try:
            ctr = int(mr.get("Counter", 0))
        except (ValueError, TypeError):
            ctr = 0
        if ctr < 1:
            continue
        machine = mr.get("MachineName", "")
        date_str = mr.get("Date", "")
        if not machine or not date_str:
            continue
        try:
            marker_dt = parse(date_str)
        except ValueError:
            continue
        # Додаємо мітку тільки якщо вона потрапляє в межі якогось циклу цієї машини
        for c in cycles_dict.get(machine, []):
            cs = c.get("start")
            ce = c.get("end")
            if cs and ce and cs <= marker_dt <= ce:
                markers[machine].append(marker_dt)
                break
    return markers


def split_cycles_by_counter(cycles_dict, counter_markers):
    """Розбиває цикли на підцикли по мітках COUNTER.MIN.

    COUNTER.MIN в machining_results записується в момент кінця циклу
    (Date = кінець). Тому мітка може збігатися з c_end або бути трохи пізніше.

    Мітка є роздільником якщо потрапляє у вікно [c_start+30s ... c_end+60s].
    Якщо мітка > c_end - використовуємо c_end як точку розрізу.
    Ongoing цикли не розбиваємо.
    """
    TOL_AFTER = timedelta(seconds=60)
    TOL_MIN   = timedelta(seconds=30)

    result = {}
    for mname, cycles in cycles_dict.items():
        markers = sorted(counter_markers.get(mname, []))
        new_cycles = []
        for c in cycles:
            c_start = c.get("start")
            c_end   = c.get("end")
            if not c_start or not c_end:
                new_cycles.append(c)
                continue
            inner = []
            for m in markers:
                if m <= c_start + TOL_MIN:
                    continue
                if m > c_end + TOL_AFTER:
                    continue
                cut = min(m, c_end)
                inner.append(cut)
            if not inner:
                new_cycles.append(c)
                continue
            inner = sorted(set(inner))
            if inner and inner[-1] == c_end:
                inner = inner[:-1]
            if not inner:
                new_cycles.append(c)
                continue
            boundaries = [c_start] + inner + [c_end]
            for i in range(len(boundaries) - 1):
                seg_start = boundaries[i]
                seg_end   = boundaries[i + 1]
                new_cycles.append({
                    "start":    seg_start,
                    "end":      seg_end,
                    "program":  c["program"],
                    "duration": round((seg_end - seg_start).total_seconds() / 60, 2),
                    "ongoing":  False,
                })
        result[mname] = new_cycles
    return result

def apply_start_to_start_cycles(cycles_dict, counter_markers, mr_data=None):
    """Перераховує межі циклів на рівні окремої програми.

    Критерій: програма використовує COUNTER якщо в machining_results є
    запис COUNTER з Counter>=1 в проміжку [c_start, c_end] хоча б одного циклу
    цієї програми на цій машині.

    З COUNTER: цикл = зелений сектор між маркерами (вже розрізані split_cycles_by_counter).
               Маркери не змінюються.

    БЕЗ COUNTER: цикл = від старту поточного до старту наступного циклу
                 тієї ж програми (start-to-start).
                 Маркери генеруються на кожному старті нового циклу.
    """
    from collections import defaultdict as _dd

    # Будуємо індекс COUNTER подій з mr_data: {mname: [datetime, ...]}
    counter_events = _dd(list)
    if mr_data:
        parse_mr = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
        for r in mr_data:
            prog = r.get("ProgramFileName", "")
            if not prog.upper().startswith("COUNTER"):
                continue
            try:
                ctr = int(r.get("Counter", 0))
            except (ValueError, TypeError):
                ctr = 0
            if ctr < 1:
                continue
            mname = r.get("MachineName", "")
            date_str = r.get("Date", "")
            if mname and date_str:
                try:
                    counter_events[mname].append(parse_mr(date_str))
                except ValueError:
                    pass

    def prog_has_counter(mname, prog_cycles):
        """Перевіряє чи хоча б один цикл програми мав COUNTER всередині."""
        cevents = sorted(counter_events.get(mname, []))
        if not cevents:
            return False
        for c in prog_cycles:
            cs = c.get("start")
            ce = c.get("end")
            if not cs or not ce:
                continue
            for ct in cevents:
                if cs <= ct <= ce:
                    return True
        return False

    new_cycles = {}
    new_markers = dict(counter_markers)

    for mname, cycles in cycles_dict.items():
        # Групуємо цикли по програмах
        by_prog = _dd(list)
        for c in cycles:
            if c.get("start") and not c.get("program", "").upper().startswith("COUNTER"):
                by_prog[c["program"]].append(c)

        result = []
        extra_markers = list(counter_markers.get(mname, []))

        for prog, prog_cycles in by_prog.items():
            prog_cycles = sorted(prog_cycles, key=lambda c: c["start"])

            if prog_has_counter(mname, prog_cycles):
                # ── З COUNTER: цикли вже правильні (split_cycles_by_counter) ──
                for c in prog_cycles:
                    if "cycle_time" not in c:
                        c = dict(c, cycle_time=c.get("duration"))
                    result.append(c)
            else:
                # ── БЕЗ COUNTER: start-to-start ──────────────────────────────
                for i, c in enumerate(prog_cycles):
                    c_start = c["start"]
                    if i + 1 < len(prog_cycles):
                        c_end = prog_cycles[i + 1]["start"]
                    else:
                        c_end = c.get("end")
                    green_duration = c.get("duration", 0)  # реальний час зеленого сектору
                    s2s_duration = round((c_end - c_start).total_seconds() / 60, 2) if c_end else green_duration
                    # Маркер на старті ПЕРШОГО циклу програми (cycle = s2s першого)
                    if i == 0:
                        extra_markers.append((c_start, prog, s2s_duration))
                    # Маркер = старт наступного циклу, cycle = s2s наступного циклу
                    if i + 1 < len(prog_cycles):
                        next_c = prog_cycles[i + 1]
                        next_c_end = prog_cycles[i + 2]["start"] if i + 2 < len(prog_cycles) else next_c.get("end")
                        next_s2s = round((next_c_end - next_c["start"]).total_seconds() / 60, 2) if next_c_end else next_c.get("duration", 0)
                        extra_markers.append((c_end, prog, next_s2s))
                    result.append({
                        "start":      c_start,
                        "end":        c_end,
                        "program":    prog,
                        "duration":   green_duration,   # Duration колонка = зелений сектор
                        "cycle_time": s2s_duration,     # Cycle колонка = start-to-start
                        "ongoing":    c.get("ongoing", False) if i + 1 >= len(prog_cycles) else False,
                    })

        new_cycles[mname] = sorted(result, key=lambda c: c["start"])
        if extra_markers:
            # extra_markers містить (datetime, prog, green_dur) або просто datetime
            plain = []
            prog_markers = {}   # {datetime: prog}
            green_durs   = {}   # {datetime: green_dur}
            for m in extra_markers:
                if isinstance(m, tuple):
                    dt = m[0]; pr = m[1]
                    gd = m[2] if len(m) > 2 else None
                    plain.append(dt)
                    prog_markers[dt] = pr
                    if gd is not None:
                        green_durs[dt] = gd
                else:
                    plain.append(m)
            # Зберігаємо в new_markers як datetime
            existing = list(counter_markers.get(mname, []))
            new_markers[mname] = sorted(set(plain + existing))
            new_markers[f"__prog_{mname}"]  = prog_markers
            new_markers[f"__green_{mname}"] = green_durs

    return new_cycles, new_markers

def add_runstate_boundary_markers(counter_markers, rows, counter_machines):
    """Додає маркери на межах RunState 1↔0 і 0↔1 — тільки для машин що реально мають COUNTER.

    counter_machines — множина машин з get_counter_markers (до start-to-start розширення).
    """
    parse = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    from collections import defaultdict as _dd
    machines = _dd(list)
    for r in rows:
        machines[r["MachineName"]].append(r)

    result = dict(counter_markers)

    for mname, mrows in machines.items():
        if mname not in counter_machines:
            continue
        mrows = sorted(mrows, key=lambda r: r["Date"])
        existing = set(counter_markers.get(mname, []))

        prev_run = None
        for r in mrows:
            run = r["RunState"]
            if prev_run is not None and run != prev_run:
                existing.add(parse(r["Date"]))
            prev_run = run

        result[mname] = sorted(existing)

    return result

def filter_last_hours(rows, hours):
    parse   = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    last_ts = max(parse(r["Date"]) for r in rows)
    # Для 24 годин — починаємо з 00:00 того ж дня
    if hours >= 24:
        cutoff = last_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        cutoff = last_ts - timedelta(hours=hours)
    return [r for r in rows if parse(r["Date"]) >= cutoff], cutoff, last_ts

def analyze_cycles(rows):
    parse    = lambda s: datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    machines = defaultdict(list)
    for r in rows:
        machines[r["MachineName"]].append(r)
    result = {}
    for mname, mrows in machines.items():
        mrows.sort(key=lambda r: r["Date"])
        cycles, prev_run, prev_prog_parsed, cycle_start, cycle_prog = [], None, None, None, ""
        for r in mrows:
            ts, run, prog = parse(r["Date"]), r["RunState"], r["ProgramFileName"]
            prog_parsed = parse_program_name(prog)  # (base, operation)
            
            # Фіксуємо старт циклу коли:
            # 1. RunState змінився з 0→1 АБО
            # 2. Програма змінилась при RunState=1
            if run == "1":
                if prev_run in (None, "0"):
                    # Старт нового циклу
                    cycle_start, cycle_prog = ts, prog
                elif prev_run == "1" and prog_parsed != prev_prog_parsed and cycle_start:
                    # Програма змінилась - закриваємо попередній цикл
                    cycles.append({"start": cycle_start, "end": ts, "program": cycle_prog,
                                    "duration": round((ts - cycle_start).total_seconds() / 60, 2)})
                    # Стартуємо новий цикл з новою програмою
                    cycle_start, cycle_prog = ts, prog
            
            # Фіксуємо кінець циклу коли RunState змінився з 1→0
            elif prev_run == "1" and run == "0" and cycle_start:
                cycles.append({"start": cycle_start, "end": ts, "program": cycle_prog,
                                "duration": round((ts - cycle_start).total_seconds() / 60, 2)})
                cycle_start = None
            
            prev_run = run
            prev_prog_parsed = prog_parsed
            
        if cycle_start:
            last_ts = parse(mrows[-1]["Date"])
            cycles.append({"start": cycle_start, "end": None, "program": cycle_prog,
                           "duration": round((last_ts - cycle_start).total_seconds() / 60, 2),
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
        
        # Визначаємо межі робочого часу залежно від дня тижня
        # weekday: 0=Пн, 1=Вт, 2=Ср, 3=Чт, 4=Пт, 5=Сб, 6=Нд
        def is_in_efficiency_window(ts, weekend_first, weekend_last):
            wd = ts.weekday()
            hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0

            if wd in (0, 4):  # Пн, Пт: 07:00–19:00
                return 7.0 <= hour < 19.0

            elif wd in (1, 2, 3):  # Вт, Ср, Чт: 06:30–(кінець дня) — перша частина зміни
                return 6.5 <= hour <= 24.0

            elif wd in (2, 3, 4):  # Ср, Чт, Пт: 00:00–00:30 — друга частина зміни (ніч після вт/ср/чт)
                return 0.0 <= hour < 0.5

            elif wd in (5, 6):  # Сб, Нд: перший–останній цикл
                if weekend_first is None or weekend_last is None:
                    return False
                return weekend_first <= ts <= weekend_last

            return False
        
        # Знаходимо межі вихідного дня (перший/останній запис RunState=1)
        weekend_first = None
        weekend_last = None
        for r in mrows:
            ts = parse(r["Date"])
            if ts.weekday() in (5, 6) and r["RunState"] == "1":
                if weekend_first is None:
                    weekend_first = ts
                weekend_last = ts
        
        filtered_rows = [
            r for r in mrows
            if is_in_efficiency_window(parse(r["Date"]), weekend_first, weekend_last)
        ]
        
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
                dur = round((ts - dt_start).total_seconds() / 60, 2)
                if dur > 0:
                    downtimes.append({"start": dt_start, "end": ts,
                                      "duration": dur, "reason": dt_reason})
                dt_start = None
            prev_run = run
        if dt_start:
            last_ts = parse(mrows[-1]["Date"])
            dur = round((last_ts - dt_start).total_seconds() / 60, 2)
            if dur > 0:
                downtimes.append({"start": dt_start, "end": None, "duration": dur,
                                  "reason": dt_reason, "ongoing": True})
        
        # Рахуємо ефективність БЕЗ нічного періоду
        result[mname] = {
            "downtimes":  downtimes,
            "total_run":  sum(1 for r in filtered_rows if r["RunState"] == "1"),
            "total_down": sum(1 for r in filtered_rows if r["RunState"] == "0"),
            "total_min":  len(filtered_rows),
        }
    return result

def split_timeline_by_counter(timeline_data, counter_markers, period_from, period_to):
    """Розрізає зелені сегменти таймлайну по мітках COUNTER.MIN.

    COUNTER.MIN записується в кінці циклу, тому використовуємо вікно:
    [seg_start+30s ... seg_end+60s] - аналогічно до split_cycles_by_counter.
    """
    total_sec = max((period_to - period_from).total_seconds(), 1)
    TOL_AFTER = timedelta(seconds=60)
    TOL_MIN   = timedelta(seconds=30)

    def pct(dt):
        return (dt - period_from).total_seconds() / total_sec * 100

    def dt_from_pct(p):
        return period_from + timedelta(seconds=p / 100 * total_sec)

    result = {}
    for mname, segments in timeline_data.items():
        markers = sorted(counter_markers.get(mname, []))
        new_segs = []
        for seg in segments:
            if seg["state"] != "1" or not markers:
                new_segs.append(seg)
                continue
            seg_start_dt = dt_from_pct(seg["x"])
            seg_end_dt   = dt_from_pct(seg["x"] + seg["w"])
            inner = []
            for m in markers:
                if m <= seg_start_dt + TOL_MIN:
                    continue
                if m > seg_end_dt + TOL_AFTER:
                    continue
                cut = min(m, seg_end_dt)
                inner.append(cut)
            if not inner:
                new_segs.append(seg)
                continue
            inner = sorted(set(inner))
            if inner and inner[-1] == seg_end_dt:
                inner = inner[:-1]
            if not inner:
                new_segs.append(seg)
                continue
            boundaries_dt = [seg_start_dt] + inner + [seg_end_dt]
            base_id = seg["id"]
            for i in range(len(boundaries_dt) - 1):
                b_start = boundaries_dt[i]
                b_end   = boundaries_dt[i + 1]
                x = pct(b_start)
                w = pct(b_end) - x
                if w > 0.01:
                    new_segs.append({
                        "x":     x,
                        "w":     w,
                        "state": "1",
                        "label": seg["label"],
                        "start": b_start.strftime("%H:%M"),
                        "end":   b_end.strftime("%H:%M"),
                        "id":    f"{base_id}_{i}",
                    })
        result[mname] = new_segs
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
            if r["ProgramFileName"].upper().startswith("COUNTER"):
                continue  # службовий рядок — не розриваємо сегмент
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
    import traceback
    try:
        api     = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/index.html"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Content-Type":  "application/json",
            "Accept":        "application/vnd.github+json",
        }
        log(f"GitHub API URL: {api}")
        log(f"GitHub User: {GITHUB_USER}")
        log(f"GitHub Repo: {GITHUB_REPO}")
        
        # Отримуємо SHA якщо файл вже існує
        sha = None
        try:
            log("Checking if index.html exists...")
            req = urllib.request.Request(api, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode())
                sha = data["sha"]
                log(f"File exists, SHA: {sha[:8]}...")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                log("File doesn't exist, will create new")
            else:
                log(f"HTTP Error checking file: {e.code} {e.reason}")
                raise
        
        # Пушимо файл
        payload = {
            "message": f"update {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": base64.b64encode(html.encode("utf-8")).decode(),
            "branch": "main"  # явно вказуємо гілку
        }
        if sha:
            payload["sha"] = sha
        
        log(f"Uploading HTML ({len(html)} bytes)...")
        req = urllib.request.Request(
            api, data=json.dumps(payload).encode(),
            headers=headers, method="PUT"
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            response = json.loads(r.read().decode())
            log(f"Upload successful!")
            log(f"Commit SHA: {response['commit']['sha'][:8]}...")
        
        log(f"✓ Published: {GITHUB_URL}")
        return True
    except urllib.error.HTTPError as e:
        log(f"✗ GitHub HTTP Error: {e.code} {e.reason}")
        try:
            error_body = e.read().decode()
            log(f"Error details: {error_body}")
        except:
            pass
        return False
    except Exception as e:
        log(f"✗ GitHub publish error: {type(e).__name__}: {e}")
        import io
        s = io.StringIO()
        traceback.print_exc(file=s)
        log(s.getvalue())
        return False

def check_and_alert(downtimes, period_to, cycles, excel_targets, mr_data):
    """Перевіряє простої та відправляє Telegram алерти
    
    Умови відправлення алерту:
    1. Є новий невідрапортований простій ≥45 хв, АБО
    2. Є старий ongoing простій що збільшився на +45 хв, АБО
    3. Є різниця між Calculated та Target >10%
    
    Додаткові правила:
    - Не відправляємо з 20:00 до 08:00
    - Закінчені простої не повторюємо
    - Target алерти відправляємо раз на добу
    - Через 24 години скидаємо список
    """
    from datetime import datetime, timedelta
    import json
    
    log("── Step 3.6: Checking alerts ──")
    
    current_hour = datetime.now().hour
    current_time = datetime.now()
    
    log(f"Current hour: {current_hour}, downtimes: {len(downtimes)} machines, cycles: {len(cycles)} machines")
    
    # 1. Перевіряємо тихі години (20:00 - 08:00)
    if current_hour >= 20 or current_hour < 8:
        log("Silent hours (20:00-08:00) - no alerts sent")
        return
    
    # Файл для збереження вже відправлених алертів
    sent_alerts_file = os.path.join(DOWNLOAD_DIR, "sent_alerts.json")
    
    # Завантажуємо список відправлених алертів
    sent_alerts = {}
    reset_needed = False
    
    if os.path.exists(sent_alerts_file):
        try:
            with open(sent_alerts_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                last_reset = data.get("last_reset", "")
                
                if last_reset:
                    last_reset_dt = datetime.fromisoformat(last_reset)
                    hours_since_reset = (current_time - last_reset_dt).total_seconds() / 3600
                    if hours_since_reset > 24:
                        reset_needed = True
                    else:
                        sent_alerts = data.get("alerts", {})
                else:
                    reset_needed = True
        except:
            reset_needed = True
    else:
        reset_needed = True
    
    if reset_needed:
        sent_alerts = {}
    
    # Збираємо алерти про простої
    downtime_alerts = []
    
    log(f"Checking downtimes for {len(downtimes)} machines...")
    for mname, dd in downtimes.items():
        machine_downtimes = dd.get("downtimes", [])
        log(f"  {mname}: {len(machine_downtimes)} downtime events")
        
        for d in machine_downtimes:
            duration = d["duration"]
            is_ongoing = not d.get("end")
            log(f"    - Duration: {duration} min, Ongoing: {is_ongoing}, Threshold: {ALERT_THRESHOLD_MIN} min")
            
            if duration >= ALERT_THRESHOLD_MIN:
                # Унікальний ключ: машина + час початку (не змінюється для ongoing)
                alert_key = f"downtime_{mname}_{d['start'].strftime('%Y-%m-%d_%H:%M')}"
                already_sent = alert_key in sent_alerts
                
                log(f"    - Qualifies for alert! Key: {alert_key}, Already sent: {already_sent}")
                
                # Перевіряємо чи вже відправляли алерт для цього простою
                if already_sent:
                    # Якщо це ongoing простій - перевіряємо чи треба повторити
                    if is_ongoing:
                        prev_alert = sent_alerts[alert_key]
                        prev_duration = prev_alert.get("duration", 0)
                        additional_duration = duration - prev_duration
                        
                        log(f"    - Ongoing: prev_duration={prev_duration}, additional={additional_duration}")
                        
                        # Якщо протривав ще мінімум 45 хв - повторюємо алерт
                        if additional_duration >= 45:
                            log(f"    - ✓ Adding repeat alert (additional {additional_duration} min)")
                            downtime_alerts.append((mname, d, alert_key, True))  # True = repeat
                        else:
                            log(f"    - ✗ Not enough additional time ({additional_duration} < 45 min)")
                    else:
                        log(f"    - ✗ Downtime finished, not repeating")
                    # Якщо простій закінчився (є end) - НЕ повторюємо, пропускаємо
                else:
                    # Новий простій - відправляємо
                    log(f"    - ✓ Adding new alert")
                    downtime_alerts.append((mname, d, alert_key, False))  # False = new
            else:
                log(f"    - ✗ Below threshold ({duration} < {ALERT_THRESHOLD_MIN} min)")
    
    log(f"Found {len(downtime_alerts)} downtime alerts")
    
    # Збираємо алерти про перевищення Target >10%
    target_alerts = []
    machines_checked = set()  # Унікальні машини які перевірялись
    machines_with_issues = set()  # Машини з проблемами
    
    log(f"Checking cycle times for {len(cycles)} machines...")
    for mname, c_list in cycles.items():
        machine_short = mname.split("_")[0] if "_" in mname else mname
        
        # Групуємо по програмах
        by_prog = {}
        for c in c_list:
            prog_name = c["program"] or "—"
            if prog_name not in by_prog:
                by_prog[prog_name] = []
            by_prog[prog_name].append(c)
        
        for prog, prog_cycles in by_prog.items():
            # Розраховуємо Calculated РОЗУМНО (пріоритет: MachiningResult → фільтровані → всі)
            calc_target = calculate_cycle_time_smart(mname, prog, prog_cycles, mr_data)
            
            if calc_target is None:
                continue
            
            # Визначаємо операцію
            op_num = get_operation_number(prog)
            prog_normalized = normalize_program_name(prog)
            
            # Шукаємо Excel Target
            excel_target = None
            for (excel_prog, excel_op, excel_machine), time_val in excel_targets.items():
                excel_prog_normalized = normalize_program_name(excel_prog)
                excel_machine_normalized = normalize_program_name(excel_machine)
                machine_normalized = normalize_program_name(machine_short)
                
                if (prog_normalized == excel_prog_normalized and 
                    op_num == excel_op and 
                    machine_normalized == excel_machine_normalized):
                    excel_target = time_val
                    break
            
            # Якщо є Target
            if excel_target:
                machines_checked.add(machine_short)  # Додаємо до перевірених
                diff_pct = ((calc_target - excel_target) / excel_target) * 100
                log(f"    {prog}: Calculated={calc_target}, Target={excel_target}, Diff={round(diff_pct, 1)}%")
                
                if abs(diff_pct) > 10:
                    # Унікальний ключ для target алертів (для логування)
                    target_key = f"target_{mname}_{prog}"
                    
                    log(f"    ✓ Adding target alert (difference >10%)")
                    # ЗАВЖДИ додаємо - відправляємо кожен раз незалежно від історії
                    target_alerts.append((machine_short, prog, calc_target, excel_target, diff_pct, target_key))
                    machines_with_issues.add(machine_short)  # Додаємо до проблемних
                else:
                    log(f"    ✗ Difference ≤10%")
            else:
                log(f"    {prog}: No target found in Excel")
    
    log(f"Found {len(target_alerts)} target alerts")
    
    # Підраховуємо статистику
    total_machines = len(machines_checked)
    machines_with_issues_count = len(machines_with_issues)
    machines_ok_count = total_machines - machines_with_issues_count
    
    # Формуємо повідомлення
    if downtime_alerts or target_alerts:
        # Є проблеми
        lines = [
            f"⚠️ <b>Factory Alert</b>  {period_to.strftime('%H:%M')}",
            f"🔗 <a href=\"{GITHUB_URL}\">Open report</a>\n"
        ]
        
        # Summary
        status_parts = []
        if machines_ok_count > 0:
            status_parts.append(f"✅ {machines_ok_count} OK")
        if machines_with_issues_count > 0:
            status_parts.append(f"🔴 {machines_with_issues_count} need attention")
        
        if total_machines > 0:
            lines.append(f"📊 <b>Status:</b> {' | '.join(status_parts)}")
            lines.append("")  # Порожня лінія
    else:
        # Все ОК - відправляємо позитивне повідомлення
        lines = [
            f"✅ <b>All Systems Normal</b>  {period_to.strftime('%H:%M')}",
            f"🔗 <a href=\"{GITHUB_URL}\">Open report</a>\n",
            f"📊 All {total_machines} machines within target cycle times"
        ]
    
    log(f"Preparing to send: {len(downtime_alerts)} downtime alerts, {len(target_alerts)} target alerts")
    
    # Додаємо алерти про простої
    if downtime_alerts:
        for mname, d, alert_key, is_repeat in downtime_alerts:
            short = mname.split("_")[0]
            end_s = d["end"].strftime("%H:%M") if d.get("end") else "ongoing"
            
            # Позначка: 🔴🔴 для повторних, 🔴 для нових
            marker = "🔴🔴" if is_repeat else "🔴"
            
            lines.append(
                f"\n{marker} <b>{short}</b>  {d['start'].strftime('%H:%M')}–{end_s}"
                f"  <b>{d['duration']} min</b>\n   Reason: {d['reason']}"
            )
            
            # Оновлюємо інформацію про алерт
            sent_alerts[alert_key] = {
                "machine": mname,
                "start": d['start'].strftime('%Y-%m-%d %H:%M'),
                "duration": d['duration'],  # Поточна тривалість
                "last_alert": current_time.isoformat()
            }
    
    # Додаємо алерти про перевищення Target
    if target_alerts:
        lines.append("\n\n⚙️ <b>Cycle Time Alerts (>10% difference):</b>")
        for machine, prog, calc, target, diff_pct, target_key in target_alerts:
            abs_diff = abs(diff_pct)
            if diff_pct > 0:
                # Повільніше - червоний
                status = f'<b>{round(abs_diff, 1)}% slower</b> 🔴'
            else:
                # Швидше - зелений
                status = f'<b>{round(abs_diff, 1)}% faster</b> 🟢'
            
            lines.append(
                f"\n📊 <b>{machine}</b> - {prog}"
                f"\n   Calculated: {calc} min | Target: {target} min"
                f"\n   {status}"
            )
            # НЕ зберігаємо target алерти - відправляємо завжди
    
    # Перевіряємо чи треба відправляти "All OK" повідомлення
    should_send = True
    if not downtime_alerts and not target_alerts:
        # Все ОК - перевіряємо коли востаннє відправляли таке повідомлення
        last_ok_alert = sent_alerts.get("_last_all_ok_alert")
        
        if last_ok_alert:
            last_ok_time = datetime.fromisoformat(last_ok_alert)
            hours_since_last = (current_time - last_ok_time).total_seconds() / 3600
            
            if hours_since_last < 4:
                # Менше 4 годин - не відправляємо
                log(f"All OK, but last message was {hours_since_last:.1f}h ago (< 4h) - skipping")
                should_send = False
            else:
                log(f"All OK, last message was {hours_since_last:.1f}h ago - sending")
        else:
            log("All OK - sending first time")
    
    # Відправляємо
    if should_send:
        send_telegram("\n".join(lines))
        
        # Якщо це було "All OK" повідомлення - запам'ятовуємо час
        if not downtime_alerts and not target_alerts:
            sent_alerts["_last_all_ok_alert"] = current_time.isoformat()
    
    # Зберігаємо оновлений список
    with open(sent_alerts_file, "w", encoding="utf-8") as f:
        json.dump({
            "last_reset": current_time.isoformat(),
            "alerts": sent_alerts
        }, f, indent=2)

# ── HTML generation ───────────────────────────────────────────────────────────
def fmt_time(dt):   return dt.strftime("%H:%M") if dt else "—"
def eff_color(pct): return "#22c55e" if pct >= 75 else ("#f59e0b" if pct >= 50 else "#ef4444")

def generate_html(cycles, downtimes, period_from, period_to, timeline_data, conn, excel_targets, mr_data, counter_markers=None):
    generated  = datetime.now().strftime("%d.%m.%Y %H:%M")
    period_str = f"{fmt_time(period_from)} – {fmt_time(period_to)}"
    today_str  = datetime.now().strftime("%Y-%m-%d")

    # ── Stats data ──────────────────────────────────────────────────────
    import json as _json
    from collections import defaultdict as _dd
    # Повний список: ALL_MACHINES з конфігу + всі що є в БД + поточні дані
    _all_known_machines = set(ALL_MACHINES) | set(cycles.keys()) | set(downtimes.keys())
    try:
        if conn:
            for _r in conn.execute("SELECT DISTINCT machine FROM daily_summary").fetchall():
                _all_known_machines.add(_r[0])
    except: pass
    _n_known = len(_all_known_machines) if _all_known_machines else 1

    # Годинні дані з поточних cycles/downtimes
    _hr_run   = _dd(lambda: _dd(float))
    _hr_total = _dd(lambda: _dd(float))
    for _mn, _cl in cycles.items():
        for _c in _cl:
            if _c.get("start") and _c.get("duration"):
                _h = _c["start"].hour
                _hr_run[_mn][_h]   += _c["duration"]
                _hr_total[_mn][_h] += _c["duration"]
    for _mn, _dd2 in downtimes.items():
        for _d in _dd2.get("downtimes", []):
            if _d.get("start") and _d.get("duration"):
                _h = _d["start"].hour
                _hr_total[_mn][_h] += _d["duration"]
    _all_h = set()
    for _m in _hr_total: _all_h |= set(_hr_total[_m].keys())
    _today_hdata = {}
    # Додаємо всі відомі машини — ті що без даних отримують 0 для всіх годин
    for _mn in _all_known_machines:
        _today_hdata[_mn] = {}
        for _h in _all_h:
            _t = _hr_total[_mn].get(_h, 0)
            _r = _hr_run[_mn].get(_h, 0)
            _today_hdata[_mn][str(_h)] = round(_r/_t*100) if _t else 0
    _today_hdata["SITE"] = {}
    for _h in _all_h:
        # SITE: знаменник = середній total активних × кількість всіх машин
        _active_tot = sum(_hr_total[_m].get(_h,0) for _m in _hr_total if _hr_total[_m].get(_h,0)>0)
        _n_active = sum(1 for _m in _hr_total if _hr_total[_m].get(_h,0)>0)
        _run2 = sum(_hr_run[_m].get(_h,0) for _m in _hr_run)
        _avg_t = (_active_tot / _n_active) if _n_active else 0
        _site_denom = _avg_t * _n_known
        _today_hdata["SITE"][str(_h)] = min(100, round(_run2/_site_denom*100)) if _site_denom else 0
    _hourly_js = _json.dumps({today_str: _today_hdata})
    # Денні дані з DB
    _all_daily = {}; _mk_set = set(_all_known_machines)
    try:
        if conn:
            for _r in conn.execute("SELECT date,machine,run_min,total_min,efficiency FROM daily_summary ORDER BY date").fetchall():
                _d2, _m2, _ru, _to, _ef = _r
                if _d2 not in _all_daily: _all_daily[_d2] = {}
                _all_daily[_d2][_m2] = round(_ef) if _ef else 0
                _mk_set.add(_m2)
                _all_daily[_d2].setdefault("__s__",{"r":0})
                _all_daily[_d2]["__s__"]["r"] += _ru or 0
    except: pass
    _n_machines = len(_mk_set)
    def _work_window_min(date_str):
        # Повертає кількість робочих хвилин для дня тижня
        # пн/пт: 07:00–19:00 = 720 хв
        # вт-чт: 06:30–00:30 наступного дня = 18 год = 1080 хв
        # сб/нд: 0 (динамічно)
        try:
            from datetime import date as _date
            wd = _date.fromisoformat(date_str).weekday()
            if wd in (0, 4): return 720
            if wd in (1, 2, 3): return 1080
        except: pass
        return 0
    for _d2 in _all_daily:
        for _m2 in _mk_set:
            if _m2 not in _all_daily[_d2]:
                _all_daily[_d2][_m2] = 0
        _sd = _all_daily[_d2].pop("__s__",{})
        _ww = _work_window_min(_d2)
        if _ww > 0:
            _site_t = _ww * _n_machines
            _all_daily[_d2]["SITE"] = min(100, round(_sd.get("r",0) / _site_t * 100))
        else:
            # сб/нд: тільки активні станки (робота у вихідні — бонус)
            _active_eff = [v for k,v in _all_daily[_d2].items() if k != "SITE" and v > 0]
            _all_daily[_d2]["SITE"] = round(sum(_active_eff) / len(_active_eff)) if _active_eff else 0
    _mk_list  = sorted(_mk_set) + ["SITE"]
    _sk_list  = [(_m.split("_")[0] if "_" in _m else _m) for _m in _mk_list[:-1]] + ["SITE"]
    _col_list = ["#3b82f6","#22c55e","#f59e0b","#ef4444","#a855f7","#06b6d4","#f97316","#ec4899"][:len(_mk_list)]
    _daily_js = _json.dumps(_all_daily)
    _mk_js    = _json.dumps([str(_m) for _m in _mk_list])
    _sk_js    = _json.dumps(_sk_list)
    _col_js   = _json.dumps(_col_list)
    # ────────────────────────────────────────────────────────────────────

    def timeline_bar(mname):
        segs         = timeline_data.get(mname, [])
        total_sec_tl = max((period_to - period_from).total_seconds(), 1)
        total_min    = total_sec_tl / 60
        VW, VH       = 10000, 44

        def to_x(pct):
            return round(pct / 100 * VW, 2)

        # ── сегменти ──────────────────────────────────────────────────
        rects = ""
        for s in segs:
            color  = "#22c55e" if s["state"] == "1" else "#ef4444"
            seg_id = s["id"]
            label  = s["label"].replace('"', "&quot;").replace("'", "&#39;")
            tip    = f'{s["start"]}\u2013{s["end"]} | {label}'
            x  = to_x(s["x"])
            w  = max(to_x(s["w"]), 0.5)
            rects += (
                f'<rect class="tl-seg" data-id="{seg_id}" data-tip="{tip}" '
                f'x="{x}" y="0" width="{w}" height="{VH}" fill="{color}" cursor="pointer"/>')



        markers_svg = ""
        if counter_markers:
            for ct in counter_markers.get(mname, []):
                ct_sec = (ct - period_from).total_seconds()
                pct = ct_sec / total_sec_tl * 100
                if 0 <= pct <= 100:
                    cx = to_x(pct)
                    markers_svg += (
                        f'<line x1="{cx}" y1="0" x2="{cx}" y2="{VH}" '
                        f'stroke="#a855f7" stroke-width="1" opacity="1" pointer-events="none"/>')

        # ── тіки — JSON масив для JS ───────────────────────────────────
        import json as _json
        ticks_json = []
        for i in range(0, int(total_min) + 1, 15):
            pct  = round(i / total_min * 100, 4)
            t    = (period_from + timedelta(minutes=i)).strftime("%H:%M")
            is30 = (i % 30 == 0)
            ticks_json.append({"p": pct, "t": t if is30 else "", "major": is30})

        short  = mname.split("_")[0] if "_" in mname else mname
        uid    = mname.replace(" ", "_").replace("-", "_")

        # SVG — тільки бари, початкова ширина 100%
        svg = (
            f'<svg class="tl-svg" id="svg_{uid}" data-machine="{short}" '
            f'viewBox="0 0 {VW} {VH}" preserveAspectRatio="none" '
            f'style="width:100%;height:{VH}px;display:block;background:#f1f5f9">'
            f'{rects}{markers_svg}'
            f'</svg>')

        # Canvas — тіки, завжди 100% ширини зовнішнього контейнера (не scroll-wrapper)
        # Canvas всередині scroll-wrapper, ширина = SVG ширина при zoom
        canvas = f'<canvas id="tc_{uid}" style="display:block;height:26px"></canvas>'

        ticks_js = (
            f'<script>(function(){{'
            f'var ticks={_json.dumps(ticks_json)};'
            f'var cv=document.getElementById("tc_{uid}");'
            f'var svg=document.getElementById("svg_{uid}");'
            f'function drawTicks(w){{'
            f'  var dpr=window.devicePixelRatio||1;'
            f'  cv.style.width=w+"px";'
            f'  cv.width=Math.round(w*dpr);cv.height=Math.round(26*dpr);'
            f'  var ctx=cv.getContext("2d");ctx.scale(dpr,dpr);'
            f'  ctx.clearRect(0,0,w,26);'
            f'  ticks.forEach(function(tk){{'
            f'    var x=tk.p/100*w;'
            f'    ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,tk.major?7:4);'
            f'    ctx.strokeStyle="#cbd5e1";ctx.lineWidth=tk.major?1.5:1;ctx.stroke();'
            f'    if(tk.t){{'
            f'      ctx.font="10px sans-serif";ctx.fillStyle="#64748b";'
            f'      ctx.textAlign="center";ctx.fillText(tk.t,x,18);'
            f'    }}'
            f'  }});'
            f'}}'
            f'var outer=cv.closest(".tl-outer-wrap");'
            f'if(outer) outer._redrawTicks=drawTicks;'
            f'drawTicks(svg.offsetWidth||outer.offsetWidth||800);'
            f'}})();</script>')

        return (
            f'<div class="tl-outer-wrap" style="width:100%">'
            f'<div class="tl-scroll-wrapper" data-machine="{short}" '
            f'style="overflow-x:auto;overflow-y:hidden;width:100%">'
            f'{svg}'
            f'{canvas}'
            f'</div>'
            f'{ticks_js}'
            f'</div>')

    def history_chart(mname):
        rows_h = load_history(conn, mname)
        if not rows_h:
            return ""
        cid    = mname.replace(" ", "_").replace("-", "_")
        short  = mname.split("_")[0] if "_" in mname else mname
        labels = json.dumps([f"{r[0][8:10]}.{r[0][5:7]}" for r in rows_h])
        eff    = json.dumps([r[1] if r[1] is not None else 0 for r in rows_h])
        return f'''
        <div class="section-title">📈 7-Day History</div>
        <div style="padding:8px 20px 16px">
          <div style="position:relative;height:140px">
            <canvas id="chart_{cid}"></canvas>
          </div>
        </div>
        <script>
        (function(){{
          new Chart(document.getElementById("chart_{cid}").getContext("2d"),{{
            type:"line",
            data:{{labels:{labels},datasets:[{{
              label:"{short}",
              data:{eff},
              borderColor:"#3b82f6",
              backgroundColor:"#3b82f622",
              tension:0.3,
              pointRadius:5,
              pointHoverRadius:7,
              borderWidth:2,
              fill:false,
              spanGaps:false
            }}]}},
            options:{{
              responsive:true,
              maintainAspectRatio:false,
              interaction:{{mode:"index",intersect:false}},
              plugins:{{
                legend:{{display:false}},
                tooltip:{{callbacks:{{label:function(c){{return c.dataset.label+": "+c.parsed.y+"%";}}}}}}
              }},
              scales:{{
                y:{{min:-5,max:100,
                   ticks:{{callback:function(v){{return v<0?"":v+"%";}}}},
                   title:{{display:true,text:"Efficiency"}}}},
                x:{{ticks:{{maxRotation:45}}}}
              }}
            }}
          }});
        }})();
        </script>'''

    def activity_section(c_list, d_list, mname):
        """Об'єднана таблиця циклів та простоїв, відсортована за часом"""
        segs = timeline_data.get(mname, [])
        
        # Функція пошуку ID для циклів
        def find_cycle_ids(cycle):
            from datetime import datetime
            c_start = cycle.get("start")
            if not c_start:
                return ""
            c_end = cycle.get("end") or datetime.now()
            ids = []
            for s in segs:
                if s["state"] != "1":
                    continue
                s_start = datetime.strptime(f"{c_start.strftime('%Y.%m.%d')} {s['start']}", "%Y.%m.%d %H:%M")
                s_end   = datetime.strptime(f"{c_start.strftime('%Y.%m.%d')} {s['end']}", "%Y.%m.%d %H:%M")
                # Округлюємо до хвилини (сегменти таймлайну %H:%M, підцикли до секунди)
                c_end_m   = c_end.replace(second=0, microsecond=0)
                c_start_m = c_start.replace(second=0, microsecond=0)
                if s_start < c_end_m and s_end > c_start_m:
                    ids.append(s["id"])
            return " ".join(ids) if ids else ""
        
        # Функція пошуку ID для простоїв
        def find_down_id(down):
            for s in segs:
                if s["state"] == "0" and s["start"] == down["start"].strftime("%H:%M"):
                    return s["id"]
                if s["state"] == "0" and s["start"] <= down["start"].strftime("%H:%M") <= s["end"]:
                    return s["id"]
            return ""
        
        # Маркери (фіолетові лінії) для цієї машини — відсортовані
        machine_markers = sorted(counter_markers.get(mname, []))
        # prog_markers: {datetime: prog} — для start-to-start маркерів
        prog_markers = counter_markers.get(f"__prog_{mname}", {})
        # green_durs: {datetime: float} — тривалість зеленого сектору після маркера
        green_durs = counter_markers.get(f"__green_{mname}", {})

        # Об'єднуємо всі події
        events = []

        # Додаємо цикли (COUNTER.MIN не показуємо)
        for c in c_list:
            if c.get("program", "").upper().startswith("COUNTER"):
                continue
            cycle_start = c["start"] if c.get("start") else datetime.now()
            events.append({
                "type": "cycle",
                "start": cycle_start,
                "end": c.get("end"),
                "duration": c["duration"],
                "cycle_time": c.get("cycle_time"),
                "program": c.get("program", "—"),
                "ongoing": c.get("ongoing", False),
                "ids": find_cycle_ids(c)
            })

        # Додаємо простої
        for d in d_list:
            events.append({
                "type": "downtime",
                "start": d["start"],
                "end": d.get("end"),
                "duration": d["duration"],
                "reason": d["reason"],
                "ongoing": d.get("ongoing", False),
                "ids": find_down_id(d)
            })

        # Сортуємо за часом
        events.sort(key=lambda e: e["start"])

        # Вставляємо маркери-розділювачі між подіями.
        # Маркер несе block_dur = час від ЦЬОГО маркера до НАСТУПНОГО (або до кінця останньої події).
        # Це дозволяє показати час циклу в блоці подій що ПІСЛЯ маркера.
        if machine_markers:
            # Права межа кожного блоку = наступний маркер або кінець останньої події
            last_event_end = None
            for e in reversed(events):
                if e.get("end"):
                    last_event_end = e["end"]
                    break
            # Для start-to-start маркерів block_dur = до наступного маркера тієї ж програми
            # Для COUNTER маркерів block_dur = до наступного маркера (будь-якого)
            marker_events = []
            for i, mk in enumerate(machine_markers):
                mk_prog = prog_markers.get(mk)  # None якщо це COUNTER маркер
                if mk_prog is not None:
                    # start-to-start: Cycle = тривалість зеленого сектору після цього маркера
                    blk_dur = green_durs.get(mk)  # green_dur збережений при генерації маркера
                else:
                    # COUNTER маркер: наступний маркер будь-якого типу
                    right = machine_markers[i+1] if i+1 < len(machine_markers) else last_event_end
                    blk_dur = round((right - mk).total_seconds() / 60, 2) if right and right > mk else None
                marker_events.append({
                    "type":        "marker",
                    "start":       mk,
                    "block_start": mk,
                    "block_dur":   blk_dur,
                    "block_prog":  mk_prog,
                })

            combined = []
            marker_idx = 0
            for e in events:
                while marker_idx < len(marker_events):
                    mk_ev = marker_events[marker_idx]
                    if mk_ev["start"] <= e["start"]:
                        combined.append(mk_ev)
                        marker_idx += 1
                    else:
                        break
                combined.append(e)
            while marker_idx < len(marker_events):
                combined.append(marker_events[marker_idx])
                marker_idx += 1
            events = combined

        if not events:
            return '<p class="empty">No activity detected</p>'
        
        # Генеруємо рядки таблиці
        # Групуємо події по блоках (між маркерами) для rowspan колонки Cycle.
        # Маркер = розділювач перед блоком. marker.block_dur = час цього блоку.
        blocks = []
        current_block = []
        current_marker = None
        for e in events:
            if e["type"] == "marker":
                # Зберігаємо попередній блок (без маркера або зі своїм маркером)
                if current_block:
                    blocks.append({"events": current_block, "marker": current_marker})
                current_block = []
                current_marker = e  # маркер для НАСТУПНОГО блоку
            else:
                current_block.append(e)
        if current_block or current_marker:
            blocks.append({"events": current_block, "marker": current_marker})

        # Якщо маркерів немає — один блок без маркера
        if not blocks:
            blocks = [{"events": events, "marker": None}]

        rows_html = ""
        for blk in blocks:
            blk_events = blk["events"]
            marker = blk["marker"]
            n = len(blk_events)

            # Фіолетовий горизонтальний розділювач перед кожним блоком
            if marker is not None:
                rows_html += (
                    f'<tr style="height:3px;padding:0;line-height:0;">'
                    f'<td colspan="6" style="height:3px;padding:0;background:#a855f7;border:none;"></td></tr>'
                )

            for idx, e in enumerate(blk_events):
                badge = ' <span class="badge ongoing">ongoing</span>' if e.get("ongoing") else ""

                if e["type"] == "cycle":
                    icon = "🟢"
                    detail = e["program"] or "—"
                    row_class = "activity-run"
                    start_s = fmt_time(e["start"])
                    end_s = "…" if not e.get("end") else fmt_time(e["end"])
                else:
                    icon = "🔴"
                    detail = e["reason"]
                    row_class = "activity-down"
                    start_s = fmt_time(e["start"])
                    end_s = "…" if not e.get("end") else fmt_time(e["end"])

                # Колонка Cycle з rowspan тільки для першого рядка блоку
                if idx == 0 and marker is not None and n > 0:
                    mk_prog = marker.get("block_prog")
                    first_prog = blk_events[0].get("program") if blk_events else None
                    first_cycle_time = blk_events[0].get("cycle_time") if blk_events else None
                    if mk_prog is not None:
                        # start-to-start: беремо cycle_time з першого зеленого рядка блоку
                        if mk_prog == first_prog and first_cycle_time is not None:
                            dur_text = f"{first_cycle_time} min"
                        else:
                            dur_text = None
                    else:
                        # COUNTER маркер: block_dur з маркера
                        dur_text = f"{marker['block_dur']} min" if marker.get("block_dur") is not None else "…"
                    if dur_text is not None:
                        cycle_td = (
                            f'<td rowspan="{n}" style="'
                            f'color:#7c3aed;font-weight:700;'
                            f'text-align:center;vertical-align:middle;'
                            f'white-space:nowrap;border-left:1px solid #000;">'
                            f'{dur_text}</td>'
                        )
                    else:
                        cycle_td = f'<td rowspan="{n}" style="border-left:1px solid #000;"></td>'
                elif idx == 0 and marker is None:
                    # Немає маркера — порожня колонка з rowspan
                    cycle_td = f'<td rowspan="{max(n,1)}" style="border-left:1px solid #000;"></td>'
                else:
                    cycle_td = ""  # вже зайнято rowspan

                rows_html += (
                    f'<tr class="tl-row {row_class}" data-id="{e["ids"]}">'
                    f'<td>{detail}</td>'
                    f'<td>{icon}</td>'
                    f'<td>{start_s}</td>'
                    f'<td>{end_s}{badge}</td>'
                    f'<td><strong>{e["duration"]} min</strong></td>'
                    f'{cycle_td}</tr>'
                )
        

        # Унікальний ID для цього блоку
        scroll_id = f"activity-scroll-{mname.replace('_', '-')}"
        return (
            f'<div class="resizable-section" id="rs-{scroll_id}" style="height:350px">'
            f'<div class="table-scroll-x" style="height:100%;overflow:hidden"><div class="scroll-table-wrap" style="height:100%">'
            f'<table class="scroll-table"><thead><tr><th>Details</th><th></th><th>Start</th><th>End</th><th>Duration</th><th style="color:#7c3aed;border-left:1px solid #000;text-align:center;">Cycle</th></tr></thead></table>'
            f'<div id="{scroll_id}" class="scroll-tbody-wrap" style="height:calc(100% - 40px);overflow-y:auto">'
            f'<table class="scroll-table"><tbody>{rows_html}</tbody></table>'
            f'</div></div></div></div>'
            f'<script>(function(){{'
            f'var wrap=document.getElementById("{scroll_id}");'
            f'var outer=document.getElementById("rs-{scroll_id}");'
            f'if(!wrap||!outer) return;'
            f'var inner=wrap.querySelector("table");'
            f'if(!inner) return;'
            f'var contentH=inner.offsetHeight+44;'  # 44px = thead
            f'if(contentH<350) outer.style.height=contentH+"px";'
            f'}})();</script>'
        )

    def cycles_section(c_list, mname, excel_targets, conn, mr_data, counter_markers=None):
        """Генерує Target Cycle Time з порівнянням з Excel"""
        if not c_list:
            return ""

        # green_durs для цієї машини: {datetime: float}
        green_durs = (counter_markers or {}).get(f"__green_{mname}", {})
        
        # DEBUG: Логуємо скільки targets завантажено
        log(f"cycles_section: machine={mname}, excel_targets count={len(excel_targets)}")
        
        # Витягуємо коротку назву станку (M1, M2 тощо)
        machine_short = mname.split("_")[0] if "_" in mname else mname
        
        # Групуємо цикли по програмах (COUNTER.MIN не показуємо)
        by_prog = defaultdict(list)
        for c in c_list:
            prog_name = c["program"] or "—"
            if prog_name.upper().startswith("COUNTER"):
                continue
            if prog_name not in by_prog:
                by_prog[prog_name] = []
            by_prog[prog_name].append(c)
        
        # Для кожної програми використовуємо нову логіку вибірки
        target_rows = []
        for prog, current_cycles in by_prog.items():
            # Визначаємо операцію
            op_num = get_operation_number(prog)
            
            # Calculated = KDE на фіолетових числах (cycle_time з кожного циклу)
            cycle_times = [c["cycle_time"] for c in current_cycles if c.get("cycle_time") and c["cycle_time"] > 0]

            if cycle_times:
                calc_target = calculate_real_cycle_time(cycle_times)
            else:
                cycle_durs = [c["duration"] for c in current_cycles if c.get("duration", 0) > 0]
                calc_target = calculate_real_cycle_time(cycle_durs) if cycle_durs else None

            
            if calc_target is None:
                # Недостатньо даних
                continue
            
            info_text = f"{len(current_cycles)} cycles today"
            
            # Шукаємо Excel Target з урахуванням станку та операції
            excel_target = None
            prog_normalized = normalize_program_name(prog)
            found_for_other_machine = None
            
            # Перебираємо всі ключі в excel_targets
            for (excel_prog, excel_op, excel_machine), time_val in excel_targets.items():
                excel_prog_normalized = normalize_program_name(excel_prog)
                excel_machine_normalized = normalize_program_name(excel_machine)
                machine_normalized = normalize_program_name(machine_short)
                
                # Порівнюємо: програма + операція + станок
                if (prog_normalized == excel_prog_normalized and 
                    op_num == excel_op and 
                    machine_normalized == excel_machine_normalized):
                    excel_target = time_val
                    break
                
                # Зберігаємо якщо знайшли для іншого станку
                if (prog_normalized == excel_prog_normalized and 
                    op_num == excel_op and 
                    machine_normalized != excel_machine_normalized):
                    found_for_other_machine = excel_machine
            
            # Порівняння
            if excel_target:
                diff = round(calc_target - excel_target, 2)  # Округлення до сотих
                diff_pct = round((diff / excel_target) * 100, 2) if excel_target else 0  # Також до сотих
                if diff > 0:
                    comparison = f'<span style="color:#ef4444">+{diff} min (+{diff_pct}%)</span>'
                elif diff < 0:
                    comparison = f'<span style="color:#22c55e">{diff} min ({diff_pct}%)</span>'
                else:
                    comparison = '<span style="color:#64748b">Match</span>'
                excel_col = f'{excel_target} min'
            else:
                # Якщо не знайшли для цього станку, але є для іншого
                if found_for_other_machine:
                    comparison = '<span style="color:#f59e0b">Wrong machine</span>'
                    excel_col = f'No norm for {machine_short}'
                else:
                    comparison = '<span style="color:#94a3b8">No data</span>'
                    excel_col = '—'
            
            target_rows.append(
                f'<tr><td><em>{prog}</em></td>'
                f'<td><span class="badge" style="background:#3F51B5;color:white;padding:2px 8px;border-radius:2px">OP{op_num}</span></td>'
                f'<td>{len(current_cycles)}</td>'
                f'<td><strong>{calc_target} min</strong></td>'
                f'<td>{excel_col}</td>'
                f'<td>{comparison}</td>'
                f'<td style="font-size:0.75rem;color:#64748b">{info_text}</td></tr>'
            )

        return (
            f'<div class="section-title" style="border-top:2px dashed #e2e8f0">🎯 Target Cycle Time</div>'
            f'<div class="table-scroll-x"><table><thead><tr><th>Program</th><th>OP</th><th>Total</th><th>Calculated</th><th>Target</th><th>Difference</th><th>Info</th></tr></thead>'
            f'<tbody>{"".join(target_rows)}</tbody></table></div>'
        )

    machines_html = ""
    machine_names = sorted(cycles.keys())
    nav_buttons = (
        '<button class="nav-btn nav-tab-btn active" id="tbtn-today" onclick="switchTab(\'today\')">Today</button>\n'
        '<button class="nav-btn nav-tab-btn" id="tbtn-stats" onclick="switchTab(\'stats\')">Statistics</button>\n'
        '<div style="border-top:1px solid #475569;margin:4px 0"></div>\n'
    ) + "".join(
        f'<a href="#machine-{mn.split("_")[0] if "_" in mn else mn}" class="nav-btn">{mn.split("_")[0] if "_" in mn else mn}</a>\n'
        for mn in machine_names
    )
    for mname in machine_names:
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
            f'<tr class="tl-row" data-id="{find_down_seg_id(d)}">'
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
        <div class="machine-card" id="machine-{short_name}">
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
          <div class="section-title">📋 Activity Log — {len(c_list)} cycles, {len(d_list)} downtimes ({total_down} min)</div>
          <div style="padding:0 0 4px">{activity_section(c_list, d_list, mname)}</div>
          {cycles_section(c_list, mname, excel_targets, conn, mr_data, counter_markers)}
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
  body{{font-family:'Roboto','Segoe UI',Arial,sans-serif;background:#ffffff;color:#212121;font-size:15px;margin:0}}

  /* ── Header ── */
  .header{{background:#1450CF;color:white;padding:10px 20px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;box-shadow:0 2px 4px rgba(0,0,0,.2)}}
  .header h1{{font-size:1.2rem;font-weight:500}}
  .header .meta{{font-size:.8rem;opacity:.9;text-align:right}}
  .header-tabs{{display:flex;gap:4px;align-items:center}}

  /* ── Layout ── */
  .container{{max-width:1100px;margin:16px auto;padding:0 12px}}

  /* ── Machine card ── */
  .machine-card{{background:#FFFFFF;border-radius:2px;box-shadow:0 2px 4px rgba(0,0,0,.3);margin-bottom:16px;overflow:hidden}}
  .machine-header{{background:#3DA9D7;color:white;padding:14px 16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}}
  .machine-title{{display:flex;flex-direction:column;gap:2px}}
  .machine-id{{font-size:1.1rem;font-weight:500}}
  .machine-full{{font-size:.7rem;opacity:.8;word-break:break-all}}
  .eff-badge{{padding:5px 12px;border-radius:2px;font-weight:500;font-size:.85rem;color:white;white-space:nowrap}}
  .eff-detail{{font-size:.72rem;font-weight:400;opacity:.9;margin-left:4px}}

  /* ── Section title ── */
  .section-title{{padding:10px 16px 5px;font-weight:500;font-size:.8rem;color:#757575;border-top:1px solid #E0E0E0;text-transform:uppercase;letter-spacing:.05em}}

  /* ── Tables (desktop) ── */
  table{{width:100%;border-collapse:collapse;font-size:.85rem}}
  th{{background:#F8F9FA;padding:8px 12px;text-align:left;font-weight:500;color:#495057;border-bottom:2px solid #DEE2E6;white-space:nowrap}}
  td{{padding:8px 12px;border-bottom:1px solid #DEE2E6;word-break:break-word}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#E9ECEF}}
  .tl-row{{cursor:pointer;transition:background-color 0.2s ease}}
  .tl-row:hover td{{background:#E3F2FD!important}}
  .activity-run td{{background:#E8F5E9}}
  .activity-down td{{background:#FFEBEE}}
  .activity-run:hover td{{background:#C8E6C9!important}}
  .activity-down:hover td{{background:#FFCDD2!important}}

  /* ── Scrollable tables ── */
  .scroll-table-wrap{{width:100%;border-bottom:1px solid #e2e8f0}}
  .scroll-table{{width:100%;border-collapse:collapse;table-layout:fixed;font-size:.85rem}}
  .scroll-table th,.scroll-table td{{padding:8px 12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .scroll-table thead th{{background:#F8F9FA;font-weight:500;color:#495057;border-bottom:2px solid #DEE2E6;position:sticky;top:0;z-index:1}}
  .scroll-table tbody tr:last-child td{{border-bottom:none}}
  .scroll-table tbody tr:hover td{{background:#E9ECEF}}
  .scroll-tbody-wrap{{display:block}}
  .scroll-tbody-wrap::-webkit-scrollbar{{width:5px}}
  .scroll-tbody-wrap::-webkit-scrollbar-track{{background:#F8F9FA}}
  .scroll-tbody-wrap::-webkit-scrollbar-thumb{{background:#ADB5BD;border-radius:2px}}
  
  /* ── Resizable Activity Log ── */
  .resizable-section{{
    position:relative;
    overflow:auto;
    border:1px solid #e2e8f0;
    border-radius:6px;
    background:#fff;
    resize:vertical;
    min-height:150px;
  }}
  .resizable-section::-webkit-resizer{{
    background:#cbd5e1;
    border-radius:3px;
  }}

  /* ── Timeline ── */
  .tl-scroll-wrapper{{overflow-x:auto;overflow-y:hidden;position:relative;margin:0 -12px;padding:0 12px}}
  .tl-scroll-wrapper::-webkit-scrollbar{{height:8px}}
  .tl-scroll-wrapper::-webkit-scrollbar-track{{background:#f1f5f9}}
  .tl-scroll-wrapper::-webkit-scrollbar-thumb{{background:#cbd5e1;border-radius:4px}}
  .tl-svg{{display:block;border-radius:4px;user-select:none;cursor:grab}}

  /* ── Legend ── */
  .legend{{display:flex;gap:14px;padding:4px 16px 10px;font-size:.78rem;flex-wrap:wrap;color:#FFFFFF}}
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
  .tl-seg.highlight{{opacity:1;filter:brightness(1.18)}}
  .tl-row{{transition:background-color 0.2s ease}}
  .tl-row.highlight td{{background:#fde047!important;font-weight:600}}
  .tl-row:hover td{{background:#f0f9ff!important}}

  /* ══════════════════════════════════════════
     MOBILE  ≤ 600px
  ══════════════════════════════════════════ */
  @media(max-width:600px){{
    body{{font-size:14px}}
    .header{{padding:10px 12px}}
    .header h1{{font-size:0.95rem}}
    .header-tabs{{gap:3px}}
    .tab-btn{{padding:4px 10px;font-size:0.75rem}}
    .container{{padding:0 6px;margin:8px auto}}
    .machine-card{{border-radius:6px;margin-bottom:10px}}
    /* machine-header — стак вертикально */
    .machine-header{{padding:10px 12px;flex-direction:column;align-items:flex-start;gap:6px}}
    .eff-badge{{font-size:.8rem;padding:4px 10px;align-self:stretch;text-align:center}}
    .eff-detail{{display:none}}
    .section-title{{padding:7px 12px 4px;font-size:.72rem}}
    /* Таблиці */
    .table-scroll-x{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
    table,.scroll-table{{font-size:.75rem;min-width:300px}}
    th,td,.scroll-table th,.scroll-table td{{padding:6px 8px}}
    /* Resizable — прибираємо resize handle на touch */
    .resizable-section{{resize:none}}
    /* Tooltip */
    #tl-tooltip{{
      position:fixed;bottom:16px;left:50%;transform:translateX(-50%);
      top:auto!important;max-width:92vw;text-align:center
    }}
    .legend{{padding:4px 10px 8px;font-size:.72rem;gap:8px}}
    .footer{{font-size:.7rem;padding:10px 10px 20px}}
    /* Stats */
    .chart-wrap{{height:220px}}
    .chart-panel{{padding:12px 10px}}
    .stats-controls{{gap:6px}}
    .stats-controls label{{font-size:0.78rem}}
    .stats-controls input[type=date]{{font-size:0.78rem;padding:4px 6px;max-width:130px}}
    .stats-controls button{{padding:5px 10px;font-size:0.75rem}}
    .stats-table th,.stats-table td{{padding:5px 8px;font-size:0.75rem}}
    .chart-legend{{gap:6px}}
    .leg-item{{font-size:0.72rem}}
    /* Scroll-top */
    #scroll-top{{right:12px;left:auto;bottom:20px;width:40px;height:40px;font-size:1.2rem}}
  }}
  /* Nav drawer (all screens) */
  .nav-sidebar{{
    position:fixed;top:0;left:0;bottom:0;width:fit-content;
    transform:translateX(-100%);
    transition:transform .25s ease;
    display:flex;flex-direction:column;
    background:transparent;padding:60px 8px 16px;gap:6px;
    z-index:1500;overflow-y:auto;
  }}
  .nav-sidebar.open{{transform:translateX(0)}}
  #nav-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:1400;}}
  #nav-overlay.open{{display:block}}
  #nav-toggle{{
    display:flex;position:fixed;left:0;top:50%;transform:translateY(-50%);
    width:26px;height:52px;background:#1e293b;color:#fff;
    border:none;border-radius:0 8px 8px 0;font-size:1rem;cursor:pointer;
    z-index:1600;align-items:center;justify-content:center;
    box-shadow:2px 0 8px rgba(0,0,0,.35);
  }}
  .nav-btn{{display:block;padding:8px 14px;font-size:0.82rem;white-space:nowrap;border-radius:6px;width:100%;text-align:left;background:#1e293b;color:#fff;font-weight:700;text-decoration:none;box-shadow:0 2px 6px rgba(0,0,0,0.3);transition:background .15s}}
  .nav-btn:hover{{background:#3b82f6}}
  .nav-tab-btn{{background:#1450CF!important;color:#fff!important}}
  .nav-tab-btn.active{{background:#3b82f6!important;box-shadow:0 0 0 2px #fff}}
  /* scroll top */
  #scroll-top{{position:fixed;bottom:24px;left:8px;width:44px;height:44px;border-radius:50%;background:#1e293b;color:#fff;border:none;font-size:1.4rem;cursor:pointer;box-shadow:0 4px 12px rgba(0,0,0,.3);display:none;align-items:center;justify-content:center;z-index:2000;transition:background .15s}}
  #scroll-top:hover{{background:#3b82f6}}
  #scroll-top.visible{{display:flex}}
  /* stats */
  .stats-controls{{display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:12px 0}}
  .stats-controls label{{font-size:0.85rem;color:#475569;display:flex;align-items:center;gap:6px}}
  .stats-controls input[type=date]{{padding:5px 8px;border:1px solid #cbd5e1;border-radius:6px;font-size:0.85rem}}
  .stats-controls button{{padding:6px 14px;background:#1e293b;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:0.82rem;font-weight:600}}
  .stats-controls button:hover{{background:#3b82f6}}
  .two-charts{{display:flex;flex-direction:column;gap:20px;margin-top:12px}}
  .chart-panel{{background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1);padding:16px}}
  .chart-title{{font-size:0.9rem;font-weight:700;color:#1e293b;margin-bottom:10px}}
  .chart-wrap{{position:relative;height:350px}}
  .chart-legend{{display:flex;flex-wrap:wrap;gap:10px;margin-top:8px}}
  .leg-item{{display:flex;align-items:center;gap:5px;font-size:0.8rem;color:#334155;font-weight:500}}
  .leg-dot{{width:11px;height:11px;border-radius:50%;display:inline-block}}
  .stats-table{{border-collapse:collapse;font-size:0.8rem;width:100%;margin-top:16px}}
  .stats-table th{{background:#1e293b;color:#fff;padding:6px 12px;text-align:center}}
  .stats-table td{{padding:5px 12px;text-align:center;border-bottom:1px solid #e2e8f0}}
  @media(max-width:900px){{.two-charts{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="header">
  <h1>📊 Machine Report</h1>
  <div class="meta">
    Period: {period_str}<br>
    Generated: {generated}
  </div>
</div>
<div id="tab-today">
<div class="container">
  <div class="legend">
    <span><span class="dot" style="background:#4CAF50"></span>Running</span>
    <span><span class="dot" style="background:#F44336"></span>Downtime</span>
  </div>
  {machines_html}
</div>
</div>
<div id="tab-stats" style="display:none">
  <div class="container">
    <div class="two-charts">
      <div class="chart-panel">
        <h3 class="chart-title">Today — Hourly Efficiency</h3>
        <div class="chart-wrap"><canvas id="effChartToday"></canvas></div>
        <div class="chart-legend" id="legend-today"></div>
      </div>
      <div class="chart-panel">
        <h3 class="chart-title">Period Trend</h3>
        <div class="stats-controls">
          <label>From: <input type="date" id="stat-from" value="{(datetime.now()-timedelta(days=6)).strftime('%Y-%m-%d')}"></label>
          <label>To: <input type="date" id="stat-to" value="{today_str}"></label>
          <button onclick="updatePeriodChart()">Apply</button>
          <button onclick="setRange(7)">7d</button>
          <button onclick="setRange(30)">30d</button>
          <button onclick="setRange(90)">90d</button>
        </div>
        <div class="chart-wrap"><canvas id="effChartPeriod"></canvas></div>
        <div class="chart-legend" id="legend-period"></div>
      </div>
    </div>
    <div id="avg-table-wrap"></div>
  </div>
</div>
<div id="nav-overlay"></div>
<button id="nav-toggle" title="Навігація">&#9776;</button>
<div class="nav-sidebar" id="nav-sidebar">
{nav_buttons}
</div>
<button id="scroll-top" onclick="window.scrollTo({{top:0,behavior:'smooth'}})" title="↑">↑</button>
<div class="footer">Source: {CSV_FILE} &nbsp;|&nbsp; DB: {DB_FILE}</div>
<div id="tl-tooltip"></div>
<script>
(function(){{
  var tip = document.getElementById("tl-tooltip");

  // ── Tooltip + highlight при наведенні на сегмент таймлайну ────────────────
  document.querySelectorAll(".tl-seg").forEach(function(seg){{
    seg.addEventListener("mouseenter", function(e){{
      var id=seg.dataset.id, txt=seg.dataset.tip;
      tip.textContent=txt; tip.style.display="block";
      var svg=seg.closest("svg");
      svg.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.add("dim"); }});
      seg.classList.remove("dim"); seg.classList.add("highlight");
      if(id) document.querySelectorAll('.tl-row').forEach(function(r){{
        var ids=r.dataset.id?r.dataset.id.split(' '):[];
        if(ids.indexOf(id)!==-1) r.classList.add("highlight");
      }});
    }});
    seg.addEventListener("mousemove", function(e){{
      tip.style.left=(e.clientX+14)+"px"; tip.style.top=(e.clientY-32)+"px";
    }});
    seg.addEventListener("mouseleave", function(){{
      tip.style.display="none";
      var svg=seg.closest("svg");
      svg.querySelectorAll(".tl-seg").forEach(function(s){{ s.classList.remove("dim","highlight"); }});
      var id=seg.dataset.id;
      if(id) document.querySelectorAll('.tl-row').forEach(function(r){{
        var ids=r.dataset.id?r.dataset.id.split(' '):[];
        if(ids.indexOf(id)!==-1) r.classList.remove("highlight");
      }});
    }});
    seg.addEventListener("click", function(){{
      var id=seg.dataset.id; if(!id) return;
      var targetRow=null;
      document.querySelectorAll('.tl-row').forEach(function(r){{
        var ids=r.dataset.id?r.dataset.id.split(' '):[];
        if(ids.indexOf(id)!==-1) targetRow=r;
      }});
      if(!targetRow) return;
      var sc=targetRow.closest(".scroll-tbody-wrap");
      if(sc){{ var rr=targetRow.getBoundingClientRect(),cr=sc.getBoundingClientRect(); sc.scrollTo({{top:sc.scrollTop+(rr.top-cr.top)-(cr.height/2)+(rr.height/2),behavior:'smooth'}}); }}
      else {{ window.scrollTo({{top:targetRow.getBoundingClientRect().top+window.scrollY-window.innerHeight/2,behavior:'smooth'}}); }}
      targetRow.classList.add("highlight");
      setTimeout(function(){{ targetRow.classList.remove("highlight"); }},1500);
    }});
    var _tt,_tm=false;
    seg.addEventListener("touchstart",function(e){{
      _tt=Date.now();_tm=false;e.preventDefault();
      tip.textContent=seg.dataset.tip;tip.style.display="block";
      var svg=seg.closest("svg");
      svg.querySelectorAll(".tl-seg").forEach(function(s){{s.classList.add("dim");}});
      seg.classList.remove("dim");seg.classList.add("highlight");
      var id=seg.dataset.id;
      if(id) document.querySelectorAll('.tl-row').forEach(function(r){{
        var ids=r.dataset.id?r.dataset.id.split(' '):[];
        if(ids.indexOf(id)!==-1) r.classList.add("highlight");
      }});
    }},{{passive:false}});
    seg.addEventListener("touchmove",function(){{_tm=true;}});
    seg.addEventListener("touchend",function(){{
      var id=seg.dataset.id;
      if(!_tm&&(Date.now()-_tt)<300&&id){{
        var tr=null;
        document.querySelectorAll('.tl-row').forEach(function(r){{
          var ids=r.dataset.id?r.dataset.id.split(' '):[];
          if(ids.indexOf(id)!==-1) tr=r;
        }});
        if(tr){{
          var sc=tr.closest(".scroll-tbody-wrap");
          if(sc){{var rr=tr.getBoundingClientRect(),cr=sc.getBoundingClientRect();sc.scrollTo({{top:sc.scrollTop+(rr.top-cr.top)-(cr.height/2),behavior:'smooth'}});}}
          else{{window.scrollTo({{top:tr.getBoundingClientRect().top+window.scrollY-window.innerHeight/2,behavior:'smooth'}});}}
          tr.classList.add("highlight");setTimeout(function(){{tr.classList.remove("highlight");}},1500);
        }}
      }}
      setTimeout(function(){{
        tip.style.display="none";
        var svg=seg.closest("svg");
        svg.querySelectorAll(".tl-seg").forEach(function(s){{s.classList.remove("dim","highlight");}});
        if(id) document.querySelectorAll('.tl-row').forEach(function(r){{
          var ids=r.dataset.id?r.dataset.id.split(' '):[];
          if(ids.indexOf(id)!==-1) r.classList.remove("highlight");
        }});
      }},1400);
    }});
  }});

  // ── Highlight при наведенні на рядок таблиці ──────────────────────
  document.querySelectorAll(".tl-row").forEach(function(row){{
    row.addEventListener("mouseenter",function(){{
      var ids=row.dataset.id?row.dataset.id.split(' '):[];
      if(!ids.length) return;
      var svg=null,segs=[];
      ids.forEach(function(id){{
        var seg=document.querySelector('.tl-seg[data-id="'+id+'"]');
        if(seg){{if(!svg)svg=seg.closest("svg");segs.push(seg);}}
      }});
      if(!segs.length) return;
      svg.querySelectorAll(".tl-seg").forEach(function(s){{s.classList.add("dim");}});
      segs.forEach(function(s){{s.classList.remove("dim");s.classList.add("highlight");}});
      tip.textContent=segs[0].dataset.tip;tip.style.display="block";
      tip.style.left=(row.getBoundingClientRect().right+10)+"px";
      tip.style.top=(row.getBoundingClientRect().top+window.scrollY)+"px";
    }});
    row.addEventListener("mouseleave",function(){{
      var ids=row.dataset.id?row.dataset.id.split(' '):[];
      ids.forEach(function(id){{
        var seg=document.querySelector('.tl-seg[data-id="'+id+'"]');
        if(seg){{var svg=seg.closest("svg");svg.querySelectorAll(".tl-seg").forEach(function(s){{s.classList.remove("dim","highlight");}});}}
      }});
      tip.style.display="none";
    }});
    row.addEventListener("click",function(){{
      var ids=row.dataset.id?row.dataset.id.split(' '):[];
      if(!ids.length) return;
      var fs=document.querySelector('.tl-seg[data-id="'+ids[0]+'"]');
      if(!fs) return;
      var wrapper=fs.closest(".tl-scroll-wrapper");
      if(!wrapper) return;
      var sr=fs.getBoundingClientRect(),wr=wrapper.getBoundingClientRect();
      wrapper.scrollTo({{left:wrapper.scrollLeft+(sr.left-wr.left)-(wr.width/2)+(sr.width/2),behavior:'smooth'}});
    }});
  }});

  // ── Drag to scroll ─────────────────────────────────────────────────
  document.querySelectorAll(".tl-scroll-wrapper").forEach(function(wrapper){{
    var svg=wrapper.querySelector("svg");if(!svg) return;
    var isDown=false,startX,slStart,isDragging=false;
    svg.addEventListener("mousedown",function(e){{isDown=true;isDragging=false;startX=e.pageX;slStart=wrapper.scrollLeft;wrapper.style.cursor="grabbing";}});
    document.addEventListener("mousemove",function(e){{if(!isDown) return;e.preventDefault();isDragging=true;wrapper.scrollLeft=slStart+(startX-e.pageX)*2;}});
    document.addEventListener("mouseup",function(){{if(isDown){{isDown=false;wrapper.style.cursor="";setTimeout(function(){{isDragging=false;}},50);}}}});
    svg.addEventListener("click",function(e){{if(isDragging){{e.stopPropagation();e.preventDefault();}}}},true);
    var txStart,tsLeft;
    svg.addEventListener("touchstart",function(e){{txStart=e.touches[0].pageX;tsLeft=wrapper.scrollLeft;}},{{passive:true}});
    svg.addEventListener("touchmove",function(e){{if(!txStart) return;wrapper.scrollLeft=tsLeft+(txStart-e.touches[0].pageX)*2;}},{{passive:true}});
    svg.addEventListener("touchend",function(){{txStart=null;}});
  }});

  // ── Ctrl+Wheel zoom ─────────────────────────────────────────────────
  // Zoom: SVG розтягується (бари), canvas перемальовується (тіки завжди чіткі)
  document.querySelectorAll(".tl-outer-wrap").forEach(function(outer){{
    var wrapper=outer.querySelector(".tl-scroll-wrapper");
    var svg=wrapper?wrapper.querySelector("svg"):null;
    if(!svg) return;
    var BASE=wrapper.offsetWidth||800,cur=BASE,MIN=BASE,MAX=BASE*20;
    wrapper.addEventListener("wheel",function(e){{
      if(!e.ctrlKey) return;e.preventDefault();
      var rect=svg.getBoundingClientRect();
      var nw=Math.round(Math.min(MAX,Math.max(MIN,cur*(e.deltaY<0?1.15:1/1.15))));
      if(nw===cur) return;
      var ratio=(e.clientX-rect.left+wrapper.scrollLeft)/cur;
      cur=nw;svg.style.width=nw+"px";
      wrapper.scrollLeft=Math.round(ratio*nw-(e.clientX-rect.left));
      // Canvas тіки: передаємо точну ширину SVG
      if(outer._redrawTicks) outer._redrawTicks(nw);
    }},{{passive:false}});
  }});
}})();

// ── Stats charts ──────────────────────────────────────────────────
(function(){{
  var ALL   = {_daily_js};
  var HDATA = {_hourly_js};
  var MK    = {_mk_js};
  var SK    = {_sk_js};
  var COLS  = {_col_js};
  var tCh=null, pCh=null;

  function ds(labels,getFn){{
    return MK.map(function(k,i){{
      return {{label:SK[i],data:labels.map(function(l){{var v=getFn(k,l);return v!=null?v:0;}}),
        borderColor:COLS[i],backgroundColor:COLS[i]+'22',tension:0.3,
        pointRadius:5,pointHoverRadius:7,borderWidth:k==='SITE'?4:1.5,
        borderDash:k==='SITE'?[8,4]:[],
        fill:false,spanGaps:false}};
    }});
  }}
  function opts(){{return{{responsive:true,maintainAspectRatio:false,
    interaction:{{mode:'index',intersect:false}},
    plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label:function(c){{return c.dataset.label+': '+(c.parsed.y!=null?c.parsed.y+'%':'—');}}}}}}  }},
    scales:{{y:{{min:-5,max:100,ticks:{{callback:function(v){{return v<0?'':v+'%';}}}},title:{{display:true,text:'Efficiency'}}}},x:{{ticks:{{maxRotation:45}}}}}}}};}}
  function leg(id,ds2){{
    var el=document.getElementById(id);if(!el)return;
    el.innerHTML=ds2.map(function(d,i){{return '<span class="leg-item"><span class="leg-dot" style="background:'+COLS[i]+'"></span>'+d.label+'</span>';}}).join('');
  }}

  function initToday(){{
    var tod=new Date().toISOString().slice(0,10);
    var hd=HDATA[tod]||{{}};
    var hs=new Set();
    Object.values(hd).forEach(function(m){{Object.keys(m).forEach(function(h){{hs.add(parseInt(h));}});}});
    var hours=Array.from(hs).sort(function(a,b){{return a-b;}});
    var labels=hours.map(function(h){{return (h<10?'0':'')+h+':00';}});
    var d2=ds(labels,function(k,lbl){{var h=parseInt(lbl);return (hd[k]&&hd[k][String(h)]!=null)?hd[k][String(h)]:0;}});
    if(tCh)tCh.destroy();
    var ctx=document.getElementById('effChartToday');if(!ctx)return;
    tCh=new Chart(ctx.getContext('2d'),{{type:'line',data:{{labels:labels,datasets:d2}},options:opts()}});
    requestAnimationFrame(function(){{tCh.resize();}});
    leg('legend-today',d2);
  }}

  function updatePeriodChart(){{
    var from=document.getElementById('stat-from').value;
    var to=document.getElementById('stat-to').value;
    var dates=Object.keys(ALL).sort().filter(function(d){{return d>=from&&d<=to;}});
    var d2=ds(dates,function(k,d){{return ALL[d]?ALL[d][k]:null;}});
    if(pCh)pCh.destroy();
    var ctx=document.getElementById('effChartPeriod');if(!ctx)return;
    pCh=new Chart(ctx.getContext('2d'),{{type:'line',data:{{labels:dates,datasets:d2}},options:opts()}});
    leg('legend-period',d2);
    var tb=document.getElementById('avg-table-wrap');
    if(tb&&dates.length){{
      var hdr=MK.map(function(k,i){{return '<th>'+SK[i]+'</th>';}}).join('');
      var cells=MK.map(function(k,i){{
        var vals=dates.map(function(d){{return ALL[d]&&ALL[d][k]!=null?ALL[d][k]:0;}});
        var avg=Math.round(vals.reduce(function(a,b){{return a+b;}},0)/vals.length);
        var col=avg>=75?'#22c55e':avg>=50?'#f59e0b':'#ef4444';
        return '<td><b style="color:'+col+'">'+avg+'%</b></td>';
      }}).join('');
      tb.innerHTML='<table class="stats-table"><thead><tr><th>Avg</th>'+hdr+'</tr></thead><tbody><tr><td><b>'+from+(from!==to?' – '+to:'')+'</b></td>'+cells+'</tr></tbody></table>';
    }}
  }}

  window.initToday=initToday;
  window.updatePeriodChart=updatePeriodChart;
  window.setRange=function(n){{
    var to=new Date(),from=new Date();from.setDate(to.getDate()-n+1);
    document.getElementById('stat-from').value=from.toISOString().slice(0,10);
    document.getElementById('stat-to').value=to.toISOString().slice(0,10);
    updatePeriodChart();
  }};
  // Scroll-to-top
  var stBtn=document.getElementById('scroll-top');
  if(stBtn) window.addEventListener('scroll',function(){{stBtn.classList.toggle('visible',window.scrollY>400);}});
}})();

// ── Nav drawer (mobile) ───────────────────────────────────────────
(function(){{
  var toggle=document.getElementById('nav-toggle');
  var sidebar=document.getElementById('nav-sidebar');
  var overlay=document.getElementById('nav-overlay');
  if(!toggle||!sidebar||!overlay) return;
  function openNav(){{sidebar.classList.add('open');overlay.classList.add('open');}}
  function closeNav(){{sidebar.classList.remove('open');overlay.classList.remove('open');}}
  toggle.addEventListener('click',function(){{
    sidebar.classList.contains('open')?closeNav():openNav();
  }});
  overlay.addEventListener('click',closeNav);
  sidebar.querySelectorAll('.nav-btn').forEach(function(btn){{
    btn.addEventListener('click',closeNav);
  }});
}})();

function switchTab(name){{
  document.getElementById('tab-today').style.display=name==='today'?'':'none';
  document.getElementById('tab-stats').style.display=name==='stats'?'':'none';
  var t1=document.getElementById('tbtn-today');
  var t2=document.getElementById('tbtn-stats');
  if(t1) t1.classList.toggle('active',name==='today');
  if(t2) t2.classList.toggle('active',name==='stats');
  if(name==='stats'){{
    setTimeout(function(){{requestAnimationFrame(function(){{
      if(window.initToday) window.initToday();
      if(window.setRange)  window.setRange(7);
    }});}},80);
  }}
  // close drawer after tab switch
  var nav=document.getElementById('nav-sidebar');
  var overlay=document.getElementById('nav-overlay');
  if(nav) nav.classList.remove('open');
  if(overlay) overlay.classList.remove('open');
}}
</script>
</body>
</html>"""
# =============================================================================
def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Step 1+2 — download both files
    log("=" * 60)
    log("FACTORY MONITOR START")
    log("=" * 60)
    if not download_both_files():
        log("Download failed — aborting.")
        sys.exit(1)

    # Step 3 — analyze
    log("── Step 3: Analyzing data ──")
    rows = load_csv()
    log(f"Rows loaded: {len(rows)}")
    _found_machines = sorted(set(r["MachineName"] for r in rows if r.get("MachineName")))
    log(f"Machines in CSV: {_found_machines}")

    filtered, period_from, period_to = filter_last_hours(rows, HOURS_BACK)
    date_str = period_to.strftime("%Y-%m-%d")
    log(f"Period: {period_from.strftime('%H:%M')} – {period_to.strftime('%H:%M')} ({len(filtered)} rows)")

    cycles        = analyze_cycles(filtered)

    # Load mr_data for cycle time calculation and COUNTER markers
    mr_data = []
    if os.path.exists(MACHINING_RESULT):
        with open(MACHINING_RESULT, encoding="utf-8") as f:
            mr_data = list(csv.DictReader(f))
        log(f"Loaded {len(mr_data)} records from MachiningResult")
    else:
        log("MachiningResult file not found, mr_data is empty")

    counter_markers = get_counter_markers(mr_data, cycles)
    counter_machines = set(counter_markers.keys())  # машини що реально мають COUNTER
    cycles = split_cycles_by_counter(cycles, counter_markers)
    # Для машин без COUNTER — перераховуємо цикли як start-to-start
    # і генеруємо маркери на межах між циклами
    cycles, counter_markers = apply_start_to_start_cycles(cycles, counter_markers, mr_data)
    # Для машин з COUNTER — додаємо маркери на межах RunState 1↔0
    counter_markers = add_runstate_boundary_markers(counter_markers, filtered, counter_machines)

    downtimes     = analyze_downtime(filtered)
    timeline_data = build_timeline_data(filtered, period_from, period_to)
    timeline_data = split_timeline_by_counter(timeline_data, counter_markers, period_from, period_to)

    conn = init_db()
    save_to_db(conn, date_str, cycles, downtimes)
    log("History saved to DB")

    # Step 3.5 — load Excel target times (перед алертами!)
    log("── Step 3.5: Loading Excel target times ──")
    excel_targets = load_target_times()

    # Step 3.6 — check and send alerts
    check_and_alert(downtimes, period_to, cycles, excel_targets, mr_data)

    # Step 4 — report
    log("── Step 4: Generating report ──")
    try:
        html = generate_html(cycles, downtimes, period_from, period_to, timeline_data, conn, excel_targets, mr_data, counter_markers)
    except Exception as e:
        log(f"✗ Error generating HTML: {e}")
        import traceback
        log(traceback.format_exc())
        raise
    
    conn.close()

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"Report saved: {OUTPUT_HTML}")

    # Step 5 — publish to GitHub Pages
    log("── Step 5: Publishing to GitHub Pages ──")
    publish_to_github(html)
    
    log("=" * 60)
    log("FACTORY MONITOR COMPLETE")
    log("=" * 60)

if __name__ == "__main__":
    main()