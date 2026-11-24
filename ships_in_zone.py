#!/usr/bin/env python3
"""
Суда в заданной акватории -> таблицы CSV (открываются в Excel).

Источники (переменная SOURCE):
  gfw        Global Fishing Watch — заходы в порты/на якорные стоянки, бесплатно
             для некоммерческого использования, данные с задержкой ~3 суток
             (таблица port_visits.csv)
  mst        MyShipTracking API — запрос «суда в зоне» (по умолчанию; видит Калининград)
  aisstream  aisstream.io — бесплатный поток (в районе Калининграда покрытия нет)

Два режима:
  python ships_in_zone.py           разовый запуск: слушает LISTEN_SECONDS и
                                    пишет таблицы (для ПК по расписанию и GitHub Actions)
  python ships_in_zone.py --daemon  непрерывная работа (для сервера): слушает
                                    поток постоянно, пишет таблицы раз в FLUSH_MINUTES,
                                    сам переподключается при обрывах

Файлы (в папке DATA_DIR, по умолчанию рядом со скриптом):
  visits.csv        заходы: одна строка на каждый заход судна (вход / выход)
  observations.csv  журнал: позиция каждого судна на каждой записи
  state.json        служебный файл, не удалять
"""

import asyncio
import csv
import json
import math
import os
import sys
import time
from datetime import datetime, timezone, timedelta

# ======================= НАСТРОЙКИ =======================

# Источник данных: "gfw" (Global Fishing Watch), "mst" (MyShipTracking) или "aisstream".
SOURCE = os.environ.get("SOURCE", "gfw").strip().lower() or "gfw"

# Ключи. Лучше задавать переменными окружения (в GitHub — секретами).
API_KEY = os.environ.get("AISSTREAM_API_KEY", "ВСТАВЬТЕ_КЛЮЧ_СЮДА")   # aisstream.io
MST_API_KEY = os.environ.get("MST_API_KEY", "")                         # MyShipTracking
GFW_API_KEY = os.environ.get("GFW_API_KEY", "")                         # Global Fishing Watch

# Global Fishing Watch: за сколько последних дней перезапрашивать заходы при каждом
# запуске (данные приходят с задержкой ~3 суток, поэтому окно с запасом).
GFW_LOOKBACK_DAYS = int(os.environ.get("GFW_LOOKBACK_DAYS", 14))

# Как часто запускается скрипт, минут. Для MyShipTracking это же окно «свежести»:
# берутся суда, чья последняя позиция в зоне не старше этого числа минут.
INTERVAL_MINUTES = int(os.environ.get("INTERVAL_MINUTES", 180))

# MyShipTracking: "simple" = 1 монета за судно; "extended" = 3 монеты, но с
# пунктом назначения, флагом, портами, дедвейтом и т.д.
MST_RESPONSE = os.environ.get("MST_RESPONSE", "simple").strip() or "simple"

# Общая область подписки: [[широта, долгота], [широта, долгота]] — два угла.
# Калининградская область: от Балтийской косы до Куршской косы, с подходами.
# Захватывает и небольшие приграничные польские воды у Балтийской косы.
MAIN_AREA = [[54.35, 19.20], [55.30, 21.30]]
# Для проверки можно временно задать другую область переменной окружения AREA,
# например AREA="53.5,9.5,66,30.5" (вся Балтика) — широта1,долгота1,широта2,долгота2.
if os.environ.get("AREA", "").strip():
    _a = [float(x) for x in os.environ["AREA"].replace(" ", "").split(",")]
    MAIN_AREA = [[_a[0], _a[1]], [_a[2], _a[3]]]

# Подзоны для столбца «Зона». Проверяются сверху вниз, берётся первая подходящая.
# Границы примерные — при необходимости поправьте.
ZONES = [
    ("Порт Калининград",           [[54.66, 20.30], [54.73, 20.56]]),
    ("Порт Балтийск",              [[54.62, 19.86], [54.67, 19.93]]),
    ("Калининградский морской канал", [[54.62, 19.93], [54.71, 20.30]]),
    ("Рейд и подходы к Балтийску", [[54.55, 19.55], [54.75, 19.86]]),
    ("Порт Пионерский",            [[54.94, 20.18], [54.98, 20.27]]),
    ("Калининградский залив",      [[54.35, 19.60], [54.66, 20.35]]),
    ("Куршский залив",             [[54.80, 20.75], [55.30, 21.30]]),
]
OTHER_ZONE = "Балтийское море (акватория)"

LISTEN_SECONDS = int(os.environ.get("LISTEN_SECONDS", 240))  # разовый режим
FLUSH_MINUTES = 10           # режим --daemon: как часто записывать таблицы
# Сколько минут без сигнала = судно ушло из зоны (два пропущенных запуска + запас).
EXIT_AFTER_MINUTES = max(90, INTERVAL_MINUTES * 2 + 30)
TZ_OFFSET_HOURS = 2          # время в таблицах: 2 = Калининград (UTC+2)
CSV_DELIMITER = ";"          # ';' — чтобы русский Excel сразу разбил по столбцам
SPOOF_SPEED_KNOTS = 50       # скачок быстрее этого = подозрительная позиция

# =========================================================

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", HERE)
OBS_FILE = os.path.join(DATA_DIR, "observations.csv")
VISITS_FILE = os.path.join(DATA_DIR, "visits.csv")
STATE_FILE = os.path.join(DATA_DIR, "state.json")
AIS_URL = "wss://stream.aisstream.io/v0/stream"
LOCAL_TZ = timezone(timedelta(hours=TZ_OFFSET_HOURS))
TFMT = "%Y-%m-%d %H:%M"

OBS_COLUMNS = [
    "Время записи", "MMSI", "Название", "IMO", "Позывной", "Тип судна", "Флаг (MID)",
    "Зона", "Широта", "Долгота", "Скорость, уз", "Курс", "Статус",
    "Пункт назначения", "Длина, м", "Последний сигнал (UTC)", "Примечание",
]
VISIT_COLUMNS = [
    "MMSI", "Название", "IMO", "Тип судна", "Пункт назначения",
    "Зона входа", "Последняя зона", "Вход в зону", "Последний сигнал",
    "Выход из зоны", "В зоне сейчас", "Примечание",
]

NAV_STATUS = {
    0: "На ходу (двигатель)", 1: "На якоре", 2: "Не управляется",
    3: "Ограничен в маневре", 4: "Стеснён осадкой", 5: "Ошвартован",
    6: "На мели", 7: "Ведёт лов", 8: "На ходу (паруса)",
}

# Первые три цифры MMSI = страна регистрации (неполный список самых частых).
MID = {
    "273": "Россия", "261": "Польша", "277": "Литва", "275": "Латвия", "276": "Эстония",
    "230": "Финляндия", "265": "Швеция", "266": "Швеция", "219": "Дания", "220": "Дания",
    "211": "Германия", "218": "Германия", "244": "Нидерланды", "245": "Нидерланды",
    "257": "Норвегия", "258": "Норвегия", "259": "Норвегия", "209": "Кипр", "210": "Кипр",
    "212": "Кипр", "248": "Мальта", "249": "Мальта", "256": "Мальта", "215": "Мальта",
    "229": "Мальта", "636": "Либерия", "637": "Либерия", "351": "Панама", "352": "Панама",
    "353": "Панама", "354": "Панама", "355": "Панама", "356": "Панама", "357": "Панама",
    "370": "Панама", "371": "Панама", "372": "Панама", "373": "Панама", "374": "Панама",
    "538": "Маршалловы о-ва", "304": "Антигуа и Барбуда", "305": "Антигуа и Барбуда",
    "314": "Барбадос", "341": "Сент-Китс и Невис", "620": "Коморы", "626": "Габон",
    "613": "Камерун", "511": "Палау", "572": "Тувалу", "677": "Танзания",
    "671": "Того", "667": "Сьерра-Леоне", "312": "Белиз", "457": "Монголия",
    "271": "Турция", "240": "Греция", "241": "Греция", "237": "Греция",
    "232": "Великобритания", "233": "Великобритания", "235": "Великобритания",
    "477": "Гонконг", "563": "Сингапур", "564": "Сингапур", "565": "Сингапур",
    "566": "Сингапур", "412": "Китай", "413": "Китай", "414": "Китай",
    "470": "ОАЭ", "471": "ОАЭ", "422": "Иран",
}


def ship_type_name(code):
    if not code:
        return ""
    specific = {
        30: "Рыболовное", 31: "Буксир (буксировка)", 32: "Буксир (буксировка)",
        33: "Дноуглубительное", 34: "Водолазное", 35: "Военное",
        36: "Парусное", 37: "Прогулочное", 50: "Лоцманское", 51: "Спасательное",
        52: "Буксир", 53: "Портовое вспомогательное", 54: "Противозагрязнительное",
        55: "Правоохранительное", 58: "Медицинское",
    }
    if code in specific:
        return specific[code]
    groups = {2: "Экраноплан", 4: "Скоростное", 6: "Пассажирское",
              7: "Грузовое", 8: "Танкер", 9: "Прочее"}
    return groups.get(code // 10, f"Код {code}")


def type_of(s):
    if s.get("type_text"):
        return s["type_text"]
    if s.get("type"):
        return ship_type_name(s["type"])
    if s.get("vtype") not in (None, ""):
        return f"Группа {s['vtype']}"
    return ""


def in_box(lat, lon, box):
    (a, b), (c, d) = box
    return min(a, c) <= lat <= max(a, c) and min(b, d) <= lon <= max(b, d)


def zone_of(lat, lon):
    if lat == "" or lon == "":
        return ""
    for name, box in ZONES:
        if in_box(lat, lon, box):
            return name
    return OTHER_ZONE


def dist_nm(lat1, lon1, lat2, lon2):
    r = 3440.065  # радиус Земли в морских милях
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def now_local():
    return datetime.now(LOCAL_TZ)


def clean(s):
    return (s or "").replace("@", "").strip()


def log(text):
    print(f"[{now_local().strftime(TFMT)}] {text}", flush=True)


# ---------------- разбор сообщений ----------------

def process_message(msg, seen, static_cache):
    mtype = msg.get("MessageType")
    meta = msg.get("MetaData", {}) or {}
    body = (msg.get("Message", {}) or {}).get(mtype, {}) or {}
    mmsi = meta.get("MMSI") or body.get("UserID")
    if not mmsi:
        return
    mmsi = str(mmsi)

    if mtype == "ShipStaticData":
        dim = body.get("Dimension") or {}
        length = (dim.get("A") or 0) + (dim.get("B") or 0)
        static_cache[mmsi] = {
            "name": clean(body.get("Name")),
            "imo": str(body["ImoNumber"]) if body.get("ImoNumber") else "",
            "callsign": clean(body.get("CallSign")),
            "type": body.get("Type"),
            "destination": clean(body.get("Destination")),
            "length": length or "",
        }
        return

    if mtype in ("PositionReport", "StandardClassBPositionReport",
                 "ExtendedClassBPositionReport"):
        lat = meta.get("latitude", body.get("Latitude"))
        lon = meta.get("longitude", body.get("Longitude"))
        if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
            return
        rec = seen.setdefault(mmsi, {})
        rec.update({
            "lat": round(lat, 5), "lon": round(lon, 5),
            "sog": body.get("Sog", ""), "cog": body.get("Cog", ""),
            "status": NAV_STATUS.get(body.get("NavigationalStatus"), "")
                      if mtype == "PositionReport" else "",
            "time": clean(str(meta.get("time_utc", "")))[:19],
        })
        name = clean(meta.get("ShipName"))
        if name:
            rec["name"] = name


def subscription():
    return {
        "APIKey": API_KEY,
        "BoundingBoxes": [MAIN_AREA],
        "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport",
                               "ExtendedClassBPositionReport", "ShipStaticData"],
    }


async def listen_once(seconds):
    import websockets  # pip install websockets
    seen, static_new = {}, {}
    counts = {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    async with websockets.connect(AIS_URL, open_timeout=60) as ws:
        await ws.send(json.dumps(subscription()))  # отправить сразу после подключения
        while (left := deadline - loop.time()) > 0:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=left)
            except asyncio.TimeoutError:
                break
            msg = json.loads(raw)
            if "error" in msg:
                raise RuntimeError(f"aisstream: {msg['error']}")
            mt = msg.get("MessageType", "?")
            counts[mt] = counts.get(mt, 0) + 1
            process_message(msg, seen, static_new)
    log(f"Область {MAIN_AREA}: получено сообщений {sum(counts.values())} {counts or ''}")
    return seen, static_new


# ---------------- MyShipTracking ----------------

MST_URL = "https://api.myshiptracking.com/api/v2/vessel/zone"
MST_NAV = {  # в ответе MyShipTracking статус приходит числом (как в AIS)
    **NAV_STATUS, 15: "",
}


def mst_request(minutes_back):
    import urllib.request, urllib.parse, urllib.error
    (a, b), (c, d) = MAIN_AREA
    q = urllib.parse.urlencode({
        "minlat": min(a, c), "maxlat": max(a, c),
        "minlon": min(b, d), "maxlon": max(b, d),
        "response": MST_RESPONSE, "minutesBack": minutes_back,
    })
    req = urllib.request.Request(f"{MST_URL}?{q}", headers={
        "Authorization": f"Bearer {MST_API_KEY}", "Accept": "application/json",
        "User-Agent": "ships-in-zone/1.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            body, charged = json.loads(r.read().decode("utf-8")), r.headers.get("X-Credit-Charged")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            raise RuntimeError(f"MyShipTracking: HTTP {e.code}")
        charged = None
    if body.get("status") != "success":
        raise RuntimeError(f"MyShipTracking: {body.get('code')} {body.get('message')}")
    return body.get("data") or [], charged


def fetch_mst():
    try:
        data, charged = mst_request(INTERVAL_MINUTES)
    except RuntimeError as e:
        if "ERR_TRIAL_LIMIT" not in str(e) or INTERVAL_MINUTES <= 60:
            raise
        log(f"Пробный ключ не даёт окно {INTERVAL_MINUTES} мин, беру 60 мин.")
        data, charged = mst_request(60)
    seen, static_new = {}, {}
    for v in data:
        mmsi = str(v.get("mmsi") or "")
        lat, lon = v.get("lat"), v.get("lng")
        if not mmsi or lat is None or lon is None:
            continue
        ns = v.get("nav_status")
        try:
            status = MST_NAV.get(int(ns), "")
        except (TypeError, ValueError):
            status = str(ns or "")
        seen[mmsi] = {
            "lat": round(float(lat), 5), "lon": round(float(lon), 5),
            "sog": v.get("speed", ""), "cog": v.get("course", ""), "status": status,
            "time": str(v.get("received") or "")[:19].replace("T", " "),
            "name": clean(v.get("vessel_name")),
        }
        st = {"name": clean(v.get("vessel_name")),
              "imo": str(v["imo"]) if v.get("imo") else ""}
        if v.get("ais_type"):
            st["type"] = v.get("ais_type")
        elif v.get("vtype") is not None:
            st["vtype"] = v.get("vtype")
        if v.get("vessel_type"):
            st["type_text"] = clean(v.get("vessel_type"))
        for k_src, k_dst in (("callsign", "callsign"), ("destination", "destination"),
                             ("flag", "flag")):
            if v.get(k_src):
                st[k_dst] = clean(str(v[k_src]))
        if v.get("size_a") is not None and v.get("size_b") is not None:
            st["length"] = (v.get("size_a") or 0) + (v.get("size_b") or 0) or ""
        static_new[mmsi] = st
    log(f"MyShipTracking: судов в ответе {len(seen)}, списано монет: {charged or 'н/д'}")
    return seen, static_new


# ---------------- Global Fishing Watch ----------------

GFW_URL = "https://gateway.api.globalfishingwatch.org/v3/events"
PORT_VISITS_FILE = os.path.join(DATA_DIR, "port_visits.csv")
PV_COLUMNS = [
    "Начало захода", "Окончание", "Длительность, ч", "Название", "MMSI", "Флаг",
    "Тип судна", "Место (якорная стоянка / порт)", "У причала", "Зона",
    "Широта", "Долгота", "Достоверность (2-4)", "ID события",
]
GFW_TYPES = {
    "CARGO": "Грузовое", "TANKER": "Танкер", "FISHING": "Рыболовное",
    "PASSENGER": "Пассажирское", "CARRIER": "Рефрижератор/перевозчик",
    "SUPPORT": "Вспомогательное", "BUNKER": "Бункеровщик", "GEAR": "Орудие лова",
    "SEISMIC_VESSEL": "Сейсморазведка", "OTHER": "Прочее", "DISCREPANCY": "Не определён",
}
ISO3 = {
    "RUS": "Россия", "POL": "Польша", "LTU": "Литва", "LVA": "Латвия", "EST": "Эстония",
    "FIN": "Финляндия", "SWE": "Швеция", "DNK": "Дания", "DEU": "Германия",
    "NLD": "Нидерланды", "NOR": "Норвегия", "CYP": "Кипр", "MLT": "Мальта",
    "LBR": "Либерия", "PAN": "Панама", "MHL": "Маршалловы о-ва", "ATG": "Антигуа и Барбуда",
    "COM": "Коморы", "GAB": "Габон", "CMR": "Камерун", "PLW": "Палау", "TUV": "Тувалу",
    "TZA": "Танзания", "TGO": "Того", "SLE": "Сьерра-Леоне", "BLZ": "Белиз",
    "MNG": "Монголия", "TUR": "Турция", "GRC": "Греция", "GBR": "Великобритания",
    "HKG": "Гонконг", "SGP": "Сингапур", "CHN": "Китай", "ARE": "ОАЭ", "IRN": "Иран",
    "KNA": "Сент-Китс и Невис", "BRB": "Барбадос", "VCT": "Сент-Винсент и Гренадины",
}


def gfw_page(start, end, offset, limit=100):
    import urllib.request, urllib.error
    (a, b), (c, d) = MAIN_AREA
    lat1, lat2, lon1, lon2 = min(a, c), max(a, c), min(b, d), max(b, d)
    body = {
        "datasets": ["public-global-port-visits-events:latest"],
        "startDate": start, "endDate": end,
        "geometry": {"type": "Polygon", "coordinates": [[
            [lon1, lat1], [lon2, lat1], [lon2, lat2], [lon1, lat2], [lon1, lat1]]]},
    }
    req = urllib.request.Request(
        f"{GFW_URL}?offset={offset}&limit={limit}",
        data=json.dumps(body).encode("utf-8"), method="POST",
        headers={"Authorization": f"Bearer {GFW_API_KEY}",
                 "Content-Type": "application/json", "User-Agent": "ships-in-zone/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", "replace")[:300]
        raise RuntimeError(f"Global Fishing Watch: HTTP {e.code} {text}")


def gfw_time(iso):
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.astimezone(LOCAL_TZ).strftime(TFMT)
    except ValueError:
        return iso[:16]


def run_gfw():
    state = load_state()
    events = state.setdefault("gfw_events", {})
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=GFW_LOOKBACK_DAYS)).isoformat()
    end = (today + timedelta(days=1)).isoformat()      # конец не включается
    offset, got, new = 0, 0, 0
    while True:
        page = gfw_page(start, end, offset)
        entries = page.get("entries") or []
        for e in entries:
            pv = e.get("port_visit") or {}
            anc = pv.get("intermediateAnchorage") or pv.get("startAnchorage") or {}
            v = e.get("vessel") or {}
            pos = e.get("position") or {}
            lat = anc.get("lat", pos.get("lat"))
            lon = anc.get("lon", pos.get("lon"))
            try:
                lat, lon = round(float(lat), 5), round(float(lon), 5)
            except (TypeError, ValueError):
                lat = lon = ""
            place = anc.get("name") or anc.get("topDestination") or ""
            top = anc.get("topDestination") or ""
            if top and place and top != place:
                place = f"{place} ({top})"
            eid = e.get("id")
            if eid not in events:
                new += 1
            events[eid] = {
                "start": gfw_time(e.get("start")), "end": gfw_time(e.get("end")),
                "hours": round(pv.get("durationHrs") or 0, 1),
                "name": clean(v.get("name")), "mmsi": str(v.get("ssvid") or ""),
                "flag": ISO3.get(v.get("flag") or "", v.get("flag") or ""),
                "type": GFW_TYPES.get(str(v.get("type") or "").upper(), v.get("type") or ""),
                "place": place, "dock": "да" if anc.get("atDock") else "",
                "zone": zone_of(lat, lon) if lat != "" else "",
                "lat": lat, "lon": lon, "conf": pv.get("confidence", ""),
            }
        got += len(entries)
        nxt = page.get("nextOffset")
        if not entries or nxt is None or nxt <= offset or got >= (page.get("total") or 0):
            break
        offset = nxt

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PORT_VISITS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=CSV_DELIMITER)
        w.writerow(PV_COLUMNS)
        for eid, r in sorted(events.items(), key=lambda x: x[1]["start"], reverse=True):
            w.writerow([r["start"], r["end"], r["hours"], r["name"], r["mmsi"], r["flag"],
                        r["type"], r["place"], r["dock"], r["zone"], r["lat"], r["lon"],
                        r["conf"], eid])
    save_state(state)
    log(f"Global Fishing Watch: заходов за {GFW_LOOKBACK_DAYS} дн. — {got}, новых — {new}, "
        f"всего в таблице — {len(events)}.")


# ---------------- состояние и таблицы ----------------

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            st = json.load(f)
    else:
        st = {}
    st.setdefault("static", {})
    st.setdefault("visits", [])
    st.setdefault("last_pos", {})
    return st


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


def check_jump(state, mmsi, r, t):
    """Помечает физически невозможный скачок позиции (помехи/подмена GPS)."""
    prev = state["last_pos"].get(mmsi)
    state["last_pos"][mmsi] = {"lat": r["lat"], "lon": r["lon"], "t": t.isoformat()}
    if not prev:
        return ""
    hours = (t - datetime.fromisoformat(prev["t"])).total_seconds() / 3600
    d = dist_nm(prev["lat"], prev["lon"], r["lat"], r["lon"])
    if d > 5 and hours > 0 and d / hours > SPOOF_SPEED_KNOTS:
        return f"Скачок позиции {d:.0f} миль — возможна подмена GPS"
    return ""


def flush(state, seen, static_new, run_time):
    """Записывает собранное в таблицы и обновляет заходы."""
    for mmsi, s in static_new.items():
        old = state["static"].get(mmsi, {})
        state["static"][mmsi] = {**old, **{k: v for k, v in s.items() if v}}
    static = state["static"]
    t = run_time.strftime(TFMT)

    rows, notes = [], {}
    for mmsi, r in seen.items():
        s = static.get(mmsi, {})
        if r.get("name") and not s.get("name"):
            s["name"] = r["name"]
            static[mmsi] = s
        zone = zone_of(r["lat"], r["lon"])
        r["zone"] = zone
        note = check_jump(state, mmsi, r, run_time)
        notes[mmsi] = note
        rows.append([
            t, mmsi, r.get("name") or s.get("name", ""), s.get("imo", ""),
            s.get("callsign", ""), type_of(s),
            s.get("flag") or MID.get(mmsi[:3], ""), zone, r["lat"], r["lon"], r.get("sog", ""),
            r.get("cog", ""), r.get("status", ""), s.get("destination", ""),
            s.get("length", ""), r.get("time", ""), note,
        ])
    rows.sort(key=lambda x: (x[7], x[2]))

    os.makedirs(DATA_DIR, exist_ok=True)
    new_file = not os.path.exists(OBS_FILE)
    with open(OBS_FILE, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=CSV_DELIMITER)
        if new_file:
            w.writerow(OBS_COLUMNS)
        w.writerows(rows)

    # заходы
    open_visits = {v["mmsi"]: v for v in state["visits"] if not v.get("exit")}
    for mmsi, r in seen.items():
        v = open_visits.get(mmsi)
        if v is None:
            v = {"mmsi": mmsi, "enter": t, "zone_in": r["zone"], "exit": "", "note": ""}
            state["visits"].append(v)
            open_visits[mmsi] = v
        v["last"] = t
        v["zone_last"] = r["zone"]
        if notes.get(mmsi):
            v["note"] = "Были скачки позиции (возможна подмена GPS)"
    limit = run_time - timedelta(minutes=EXIT_AFTER_MINUTES)
    for mmsi, v in open_visits.items():
        if mmsi not in seen:
            last = datetime.strptime(v["last"], TFMT).replace(tzinfo=LOCAL_TZ)
            if last < limit:
                v["exit"] = v["last"]

    with open(VISITS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=CSV_DELIMITER)
        w.writerow(VISIT_COLUMNS)
        for v in sorted(state["visits"], key=lambda v: v["enter"], reverse=True):
            s = static.get(v["mmsi"], {})
            w.writerow([
                v["mmsi"], s.get("name", ""), s.get("imo", ""), type_of(s),
                s.get("destination", ""), v.get("zone_in", ""), v.get("zone_last", ""),
                v["enter"], v["last"], v["exit"], "" if v["exit"] else "да", v.get("note", ""),
            ])
    save_state(state)
    log(f"Записано судов: {len(seen)}. Сейчас в зоне: "
        f"{sum(1 for v in state['visits'] if not v['exit'])}.")


# ---------------- режимы ----------------

def run_once(attempts=4, pause=20):
    state = load_state()
    if SOURCE == "mst":
        seen, static_new = fetch_mst()
        flush(state, seen, static_new, now_local())
        return
    for i in range(1, attempts + 1):
        try:
            seen, static_new = asyncio.run(listen_once(LISTEN_SECONDS))
            break
        except Exception as e:
            if "aisstream:" in str(e) or i == attempts:  # ошибку ключа повторять бессмысленно
                raise
            log(f"Попытка {i} не удалась ({e}). Повтор через {pause} с.")
            time.sleep(pause)
    flush(state, seen, static_new, now_local())


async def run_daemon():
    import websockets
    state = load_state()
    seen, static_new = {}, {}
    next_flush = time.monotonic() + FLUSH_MINUTES * 60
    backoff = 10
    log("Непрерывный режим запущен.")
    while True:
        try:
            async with websockets.connect(AIS_URL, open_timeout=60,
                                          ping_interval=30) as ws:
                await ws.send(json.dumps(subscription()))
                log("Подключено к aisstream.io")
                backoff = 10
                while True:
                    left = next_flush - time.monotonic()
                    if left <= 0:
                        flush(state, seen, static_new, now_local())
                        seen, static_new = {}, {}
                        next_flush = time.monotonic() + FLUSH_MINUTES * 60
                        continue
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=left)
                    except asyncio.TimeoutError:
                        continue
                    msg = json.loads(raw)
                    if "error" in msg:
                        raise RuntimeError(f"aisstream: {msg['error']}")
                    process_message(msg, seen, static_new)
        except Exception as e:  # обрыв связи, ошибка сервиса и т.п.
            log(f"Связь потеряна ({e}). Повтор через {backoff} с.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


def main():
    if SOURCE == "gfw":
        if not GFW_API_KEY:
            sys.exit("Укажите ключ Global Fishing Watch: переменная окружения GFW_API_KEY.")
        try:
            run_gfw()
        except Exception as e:
            sys.exit(f"Не удалось получить данные: {e}")
        return
    if SOURCE == "mst":
        if not MST_API_KEY:
            sys.exit("Укажите ключ MyShipTracking: переменная окружения MST_API_KEY.")
    elif not API_KEY or "ВСТАВЬТЕ" in API_KEY:
        sys.exit("Укажите ключ: переменная окружения AISSTREAM_API_KEY или API_KEY в скрипте.")
    if "--daemon" in sys.argv:
        if SOURCE == "mst":
            sys.exit("Непрерывный режим только для aisstream. Для MyShipTracking — запуск по расписанию.")
        asyncio.run(run_daemon())
    else:
        try:
            run_once()
        except Exception as e:
            sys.exit(f"Не удалось получить данные: {e}")


if __name__ == "__main__":
    main()
