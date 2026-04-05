"""
PEI Infrastructure Intelligence — Scraper  v3
==============================================
Mirrors TII data structure:
  { sectors: { <sector>: { tiers: { tier1: { indicators:[...] } } } } }

New in v3 (on top of v2):
  - CHS IWLS API — Charlottetown harbour real-time water level + storm surge
  - CWFIS NRCan — Fire Weather Index (FWI) for PEI, daily CSV
  - NOAA CoastWatch ERDDAP — Gulf of St. Lawrence SST anomaly
  - AAFC Canadian Drought Monitor — GeoJSON, PEI polygon classification

Run:   python pii_scraper.py
Output: pii_data_YYYYMMDD.json  +  pii_data_latest.json
"""

import json
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Constants ────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PEI-Infrastructure-Intelligence/2.0; "
        "+https://github.com/Sargjones/pei-infrastructure)"
    )
}
TIMEOUT        = 18
TODAY          = datetime.now(timezone.utc)
DATESTAMP      = TODAY.strftime("%Y%m%d")
GENERATED      = TODAY.strftime("%Y-%m-%d %H:%M UTC")
CABLE_CAP_MW   = 300
PEAK_RECORD_MW = 400
CHARLOTTETOWN_LAT, CHARLOTTETOWN_LON = 46.2382, -63.1311
BRIDGE_LAT,        BRIDGE_LON        = 46.2500, -63.6800

# ── GPEI ER Wait Times — Radware session cookies ──────────────────────────────
# Obtained from browser DevTools after passing Radware JS challenge.
# Refresh: visit QEH wait times page in browser → DevTools → Network →
#   click ERWaitTimes_QEH → Cookies tab → copy all values into string below.
# Expiry: ~182 days from capture date.
# Captured: 2026-04-05 from Sarah's browser session.
GPEI_ER_COOKIES = (
    "_gcl_au=1.1.810639456.1775399009; "
    "_ga=GA1.1.878020187.1775399009; "
    "__uzma=946ecce4-cfd6-4114-8307-aee1f9336791; "
    "__uzmb=1775399010; "
    "__uzme=0151; "
    "uzmxj=7f9000054d41eb-aadd-4efd-8e5b-99641120bf4f1-17753990076992478439-5f83be724ce8628f19; "
    "__uzmc=226752533841; "
    "__uzmd=1775401486; "
    "__uzmf=7f9000946ecce4-cfd6-4114-8307-aee1f93367911-17753990102442476131-0031e1883db84bc332625; "
    "uzmx=7f9000054d41eb-aadd-4efd-8e5b-99641120bf4f1-17753990007552479320-7fbfff7d6c0ff13758; "
    "_ga_7DEKGJT4LV=GS2.1.s1775401485$o2$g1$t1775401486$j59$l0$h211841725; "
    "_ga_HLNHEB3NTC=GS2.1.s1775401485$o2$g1$t1775401486$j59$l0$h0"
)

WMO_DESC = {
    0:"Clear", 1:"Mainly clear", 2:"Partly cloudy", 3:"Overcast",
    45:"Fog", 48:"Icy fog",
    51:"Light drizzle", 53:"Moderate drizzle", 55:"Heavy drizzle",
    61:"Light rain", 63:"Moderate rain", 65:"Heavy rain",
    71:"Light snow", 73:"Moderate snow", 75:"Heavy snow", 77:"Snow grains",
    80:"Light showers", 81:"Showers", 82:"Heavy showers",
    85:"Snow showers", 86:"Heavy snow showers",
    95:"Thunderstorm", 96:"Thunderstorm w/ hail", 99:"Severe thunderstorm",
}


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def get(url, **kw):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, **kw)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"  [WARN] GET {url[:80]} — {e}", file=sys.stderr)
        return None

def post_json(url, body):
    try:
        r = requests.post(url, json=body,
                          headers={**HEADERS, "Content-Type": "application/json"},
                          timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"  [WARN] POST {url[:80]} — {e}", file=sys.stderr)
        return None

def soup(url, **kw):
    r = get(url, **kw)
    return BeautifulSoup(r.text, "html.parser") if r else None

def indicator(label, value, unit="", status="ok", note="", context="",
              date="", source="", tier=1, muted=False, banner=""):
    return {k: v for k, v in {
        "label":   label,
        "value":   str(value) if value is not None else "—",
        "unit":    unit,
        "status":  status,
        "note":    note,
        "context": context,
        "date":    date,
        "source":  source,
        "tier":    tier,
        "muted":   muted or False,
        "banner":  banner,
    }.items() if v or v == 0}

def manual(label, value, source="", tier=1, note=""):
    return indicator(label, value, status="manual",
                     source=source, tier=tier, note=note)


# ── Shared data fetchers ──────────────────────────────────────────────────────

def fetch_open_meteo(lat, lon, params="temperature_2m,wind_speed_10m,wind_gusts_10m,weather_code,precipitation,relative_humidity_2m"):
    url = (f"https://api.open-meteo.com/v1/forecast"
           f"?latitude={lat}&longitude={lon}&current={params}"
           f"&timezone=America%2FHalifax&forecast_days=1")
    r = get(url)
    if not r:
        return None
    try:
        return r.json().get("current", {})
    except Exception:
        return None

def fetch_boc_rate(series="FXUSDCAD"):
    url = f"https://www.bankofcanada.ca/valet/observations/{series}/json?recent=1"
    r   = get(url)
    if not r:
        return None, None
    try:
        obs = r.json().get("observations", [])
        if obs:
            latest = obs[-1]
            return float(latest[series]["v"]), latest.get("d", "")
    except Exception:
        pass
    return None, None

def fetch_aqhi_pei():
    url = "https://dd.weather.gc.ca/air_quality/aqhi/atl/observation/realtime/xml/AQ_OBS_PE_CURRENT.xml"
    r   = get(url)
    if not r:
        return None
    try:
        s = BeautifulSoup(r.text, "xml")
        for tag in ["aqhi", "AQHI", "airQualityHealthIndex"]:
            el = s.find(tag)
            if el and el.text.strip():
                return float(el.text.strip())
    except Exception:
        pass
    return None

def fetch_wx_alerts_pei():
    url   = "https://weather.gc.ca/rss/warning/pe_e.xml"
    r     = get(url)
    count, status, text = 0, "ok", "None active"
    if not r:
        return count, status, text
    try:
        s       = BeautifulSoup(r.text, "xml")
        entries = s.find_all("entry") or s.find_all("item")
        active  = [e for e in entries if e.find("title") and
                   any(kw in (e.find("title").text or "").lower()
                       for kw in ["warning","watch","statement","advisory"])]
        count   = len(active)
        if count > 0:
            status = "warn"
            first  = active[0].find("title").text.strip()
            text   = first[:70] + ("…" if len(first) > 70 else "")
        if any("warning" in (e.find("title").text or "").lower() for e in active):
            status = "alert"
    except Exception:
        pass
    return count, status, text

def fetch_metar(icao="CYYG"):
    url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=json"
    r   = get(url)
    if not r:
        return None
    try:
        data = r.json()
        return data[0] if data else None
    except Exception:
        return None

def fetch_statcan_cpi_pei():
    """PEI All-items CPI — StatCan getSeriesDataFromCubePidCoordAndLatestNPeriods"""
    url = ("https://www150.statcan.gc.ca/t1/tbl1/en/dtbl/"
           "getSeriesDataFromCubePidCoordAndLatestNPeriods/json"
           "?pid=18100004&coord=1.9&latestN=2")
    r = get(url)
    if not r:
        return None, None, None
    try:
        series = r.json().get("object", [])
        if len(series) < 1:
            return None, None, None
        latest   = series[-1]
        prev     = series[-2] if len(series) >= 2 else None
        val      = float(latest.get("value", 0))
        ref      = latest.get("refPer", "")
        yoy      = None
        if prev:
            pv = float(prev.get("value", 0))
            if pv:
                yoy = round(((val - pv) / pv) * 100, 1)
        return val, ref, yoy
    except Exception:
        return None, None, None

def fetch_cmhc_vacancy():
    """Charlottetown rental vacancy rate from CMHC HMiP API."""
    url    = "https://api.cmhc-schl.gc.ca/housing/indicators/vacancy-rate"
    params = {"geo_uid": "105", "year": TODAY.year}
    r      = get(url, params=params)
    if not r:
        return None, None
    try:
        data    = r.json()
        results = data.get("data", data.get("results", []))
        if results:
            latest = results[-1] if isinstance(results, list) else results
            rate   = latest.get("vacancy_rate", latest.get("value"))
            period = latest.get("year", latest.get("period", ""))
            return float(rate), str(period)
    except Exception:
        pass
    return None, None


def fetch_charlottetown_water_level():
    """
    CHS IWLS API — Charlottetown harbour real-time water level.
    Station code: 01700  API: https://api-iwls.dfo-mpo.gc.ca/api/v1
    Returns dict: water_level (m), predicted (m), anomaly (m), updated — or None.
    Anomaly > 0.5m = elevated storm surge concern.
    """
    import datetime as dt
    base         = "https://api-iwls.dfo-mpo.gc.ca/api/v1"
    station_id   = None
    # Look up station ID
    try:
        r = get(f"{base}/stations", params={"code": "01700"})
        if r:
            data = r.json()
            if isinstance(data, list) and data:
                station_id = data[0].get("id")
    except Exception as e:
        print(f"  [WARN] CHS station lookup — {e}", file=sys.stderr)
    if not station_id:
        station_id = "5cebf1e03d0f4a073c4bbdbe"   # known Charlottetown ID

    time_end = TODAY.strftime("%Y-%m-%dT%H:00:00Z")
    time_beg = (TODAY - dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:00:00Z")

    wlo = wlp = None
    for code, var in [("wlo", "wlo"), ("wlp", "wlp")]:
        try:
            r2 = get(f"{base}/stations/{station_id}/data",
                     params={"time-series-code": code, "from": time_beg, "to": time_end})
            if r2:
                rows = r2.json()
                if rows and isinstance(rows, list):
                    val = rows[-1].get("value")
                    if code == "wlo": wlo = float(val)
                    else:             wlp = float(val)
        except Exception as e:
            print(f"  [WARN] CHS {code} — {e}", file=sys.stderr)

    if wlo is None:
        return None
    anomaly = round(wlo - wlp, 3) if wlp is not None else None
    return {"water_level": round(wlo, 3), "predicted": round(wlp, 3) if wlp else None,
            "anomaly": anomaly, "updated": GENERATED}


def fetch_fwi_pei():
    """
    CWFIS NRCan — Fire Weather Index for PEI, daily CSV.
    URL pattern: https://cwfis.cfs.nrcan.gc.ca/downloads/fwi_obs/YYYYMMDD_fwi.csv
    PEI province code: PE  |  FWI scale: 0-5 Low, 5-12 Moderate, 12-20 High,
                                          20-30 Very High, 30+ Extreme
    """
    import datetime as dt
    for offset in [0, 1, 2]:
        date_str = (TODAY - dt.timedelta(days=offset)).strftime("%Y%m%d")
        url = f"https://cwfis.cfs.nrcan.gc.ca/downloads/fwi_obs/{date_str}_fwi.csv"
        r   = get(url)
        if not r:
            continue
        try:
            lines  = r.text.strip().split("\n")
            if len(lines) < 2:
                continue
            header = [h.strip().strip('"').lower() for h in lines[0].split(",")]
            cols   = {h: i for i, h in enumerate(header)}
            prov_i = cols.get("prov", cols.get("province", -1))
            lat_i  = cols.get("lat",  cols.get("latitude",  -1))
            lon_i  = cols.get("lon",  cols.get("longitude", -1))
            fwi_i  = cols.get("fwi",  -1)
            if fwi_i < 0:
                continue

            fwi_vals = []
            for line in lines[1:]:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) <= fwi_i:
                    continue
                # Filter by province or lat/lon bounding box
                in_pei = False
                if prov_i >= 0 and prov_i < len(parts):
                    in_pei = parts[prov_i].upper() in ("PE", "PEI")
                elif lat_i >= 0 and lon_i >= 0:
                    try:
                        lat = float(parts[lat_i]); lon = float(parts[lon_i])
                        in_pei = (45.9 <= lat <= 47.1 and -64.5 <= lon <= -61.9)
                    except (ValueError, IndexError):
                        pass
                if not in_pei:
                    continue
                try:
                    fwi_vals.append(float(parts[fwi_i]))
                except ValueError:
                    pass

            if fwi_vals:
                return {"fwi":      round(sum(fwi_vals)/len(fwi_vals), 1),
                        "fwi_max":  round(max(fwi_vals), 1),
                        "stations": len(fwi_vals),
                        "date":     date_str,
                        "season":   TODAY.month in range(4, 11)}
        except Exception as e:
            print(f"  [WARN] CWFIS FWI ({date_str}) — {e}", file=sys.stderr)
    return None


def fetch_gulf_sst_anomaly():
    """
    NOAA CoastWatch ERDDAP — Gulf of St. Lawrence / Northumberland Strait SST anomaly.
    Dataset: noaacrwsstanomalyDaily (NOAA Coral Reef Watch, 5km daily)
    Point: ~46.5°N, 63.5°W (central Northumberland Strait)
    Returns dict: anomaly (°C), date — or None.
    Positive = warmer than average = amplified storm/erosion risk for PEI.
    """
    import datetime as dt
    yesterday    = (TODAY - dt.timedelta(days=1)).strftime("%Y-%m-%dT12:00:00Z")
    two_days_ago = (TODAY - dt.timedelta(days=2)).strftime("%Y-%m-%dT12:00:00Z")
    lat, lon     = 46.5, -63.5
    url = (
        "https://coastwatch.pfeg.noaa.gov/erddap/griddap/"
        "noaacrwsstanomalyDaily.csv"
        f"?sea_surface_temperature_anomaly"
        f"[({two_days_ago}):1:({yesterday})]"
        f"[({lat-0.05:.2f}):1:({lat+0.05:.2f})]"
        f"[({lon-0.05:.2f}):1:({lon+0.05:.2f})]"
    )
    r = get(url)
    if not r:
        return None
    try:
        lines = [l.strip() for l in r.text.strip().split("\n") if l.strip()]
        if len(lines) < 3:
            return None
        # ERDDAP CSV: row 0=col names, row 1=units, row 2+=data
        data_lines = [l for l in lines[2:] if l]
        if not data_lines:
            return None
        parts   = data_lines[-1].split(",")
        anomaly = float(parts[-1].strip())
        date_str = parts[0].strip()[:10]
        return {"anomaly": round(anomaly, 2), "date": date_str}
    except Exception as e:
        print(f"  [WARN] NOAA SST anomaly — {e}", file=sys.stderr)
        return None


def fetch_aafc_drought_pei():
    """
    AAFC Canadian Drought Monitor — monthly GeoJSON for PEI.
    Drought categories: D0=Abnormally Dry, D1=Moderate, D2=Severe,
                        D3=Extreme, D4=Exceptional, None=No drought
    Returns dict: category, label, date — or None.
    """
    import datetime as dt
    # Try current month and previous month
    for offset in [0, 1, 2]:
        d    = (TODAY.replace(day=1) - dt.timedelta(days=offset * 28))
        yr   = d.strftime("%Y")
        mo   = d.strftime("%m")
        url  = (
            f"https://agriculture.canada.ca/atlas/data_donnees/"
            f"canadianDroughtMonitor/data_donnees/geoJSON/"
            f"CDM_{yr}_{mo}_bilingual.geojson"
        )
        r = get(url)
        if not r:
            continue
        try:
            gj       = r.json()
            features = gj.get("features", [])
            PEI_LAT  = (45.9, 47.1)
            PEI_LON  = (-64.5, -61.9)
            cats     = []

            def in_pei_bbox(coords):
                """Recursively check if any coord is within PEI bounding box."""
                if not coords:
                    return False
                if isinstance(coords[0], (int, float)):
                    return (PEI_LAT[0] <= coords[1] <= PEI_LAT[1] and
                            PEI_LON[0] <= coords[0] <= PEI_LON[1])
                return any(in_pei_bbox(c) for c in coords)

            for feat in features:
                props = feat.get("properties", {})
                # Province check
                prov = " ".join(str(v) for v in props.values()).upper()
                geom = feat.get("geometry", {})
                if "EDWARD" not in prov and "PEI" not in prov:
                    if not in_pei_bbox(geom.get("coordinates", [])):
                        continue
                # Extract category
                for key in ["DCAT_EN","drought_cat","category","CAT","CLASS","DCAT"]:
                    val = str(props.get(key, "")).strip()
                    if val:
                        cats.append(val)
                        break

            if not cats:
                continue

            order  = ["D4","D3","D2","D1","D0","None",""]
            labels = {"D4":"Exceptional Drought","D3":"Extreme Drought",
                      "D2":"Severe Drought","D1":"Moderate Drought",
                      "D0":"Abnormally Dry","None":"No Drought","":"No Drought"}
            worst  = ""
            for c in order:
                if any(c in cat for cat in cats):
                    worst = c; break

            return {"category": worst, "label": labels.get(worst, worst),
                    "date": f"{yr}-{mo}"}
        except Exception as e:
            print(f"  [WARN] AAFC drought GeoJSON ({yr}-{mo}) — {e}", file=sys.stderr)
    return None



def fetch_nrcan_furnace_oil_charlottetown():
    """
    NRCan weekly furnace oil price for Charlottetown.
    URL: www2.nrcan.gc.ca/eneene/sources/pripri/prices_bycity_e.cfm
         ?PriceYear=0&ProductID=7&LocationID=66  (66 = Charlottetown)
    Parses the HTML price table — most recent week.
    Returns dict: price_cpl (cents/litre), week_ending, change_cpl — or None.
    """
    url = (
        "http://www2.nrcan.gc.ca/eneene/sources/pripri/prices_bycity_e.cfm"
        "?PriceYear=0&ProductID=7&LocationID=66"
    )
    r = get(url)
    if not r:
        return None
    try:
        s = BeautifulSoup(r.text, "html.parser")
        # Find the prices table — look for table with "Week Ending" header
        tables = s.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not any("week" in h for h in headers):
                continue
            rows = table.find_all("tr")
            data_rows = []
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 2 and cells[0] and cells[1]:
                    # Look for date pattern YYYY-MM-DD and a numeric price
                    import re as _re
                    if _re.match(r"\d{4}-\d{2}-\d{2}", cells[0]):
                        try:
                            price = float(cells[1])
                            data_rows.append((cells[0], price))
                        except ValueError:
                            pass
            if data_rows:
                # Most recent row is last
                latest = data_rows[-1]
                prev   = data_rows[-2] if len(data_rows) >= 2 else None
                change = round(latest[1] - prev[1], 1) if prev else None
                return {
                    "price_cpl":   latest[1],
                    "week_ending": latest[0],
                    "change_cpl":  change,
                }
    except Exception as e:
        print(f"  [WARN] NRCan furnace oil — {e}", file=sys.stderr)
    return None


def fetch_statcan_food_cpi_pei():
    """
    Statistics Canada table 18-10-0004-01 — PEI Food CPI.
    Uses the getSeriesDataFromCubePidCoordAndLatestNPeriods JSON endpoint.
    PEI = province coord 9, Food = product coord 2
    Coordinate string: "9.2" for PEI Food purchased from stores
    Returns dict: index, ref_period, yoy_pct — or None.
    """
    # Coord 9.2 = PEI, Food purchased from stores
    # Coord 9.1 = PEI, All-items (for context)
    results = {}
    for label, coord in [("food", "9.2"), ("all_items", "9.1")]:
        url = (
            "https://www150.statcan.gc.ca/t1/tbl1/en/dtbl/"
            "getSeriesDataFromCubePidCoordAndLatestNPeriods/json"
            f"?pid=18100004&coord={coord}&latestN=13"
        )
        r = get(url)
        if not r:
            continue
        try:
            series = r.json().get("object", [])
            if len(series) < 2:
                continue
            # Most recent is last
            latest = series[-1]
            year_ago = series[-13] if len(series) >= 13 else series[0]
            val   = float(latest.get("value", 0))
            ref   = latest.get("refPer", "")
            ya    = float(year_ago.get("value", 0))
            yoy   = round(((val - ya) / ya) * 100, 1) if ya else None
            results[label] = {"index": val, "ref_period": ref, "yoy_pct": yoy}
        except Exception as e:
            print(f"  [WARN] StatCan food CPI ({label}) — {e}", file=sys.stderr)
    return results if results else None


def fetch_statcan_gasoline_charlottetown():
    """
    Statistics Canada table 18-10-0001-01 — Monthly average retail gasoline prices.
    Charlottetown (Summerside) = geo coord for PEI city.
    Uses the same getSeriesData endpoint.
    Returns dict: price_cpl, ref_period, change_cpl — or None.
    Coord for Charlottetown gasoline: table 18-10-0001, coord TBD — fallback to scraping
    """
    # StatCan table 18-10-0001-01 Monthly avg retail prices gasoline & fuel oil
    # Charlottetown regular gasoline
    url = (
        "https://www150.statcan.gc.ca/t1/tbl1/en/dtbl/"
        "getSeriesDataFromCubePidCoordAndLatestNPeriods/json"
        "?pid=18100001&coord=8.1.1&latestN=2"  # coord 8=PEI city, 1.1=regular gasoline
    )
    r = get(url)
    if not r:
        return None
    try:
        series = r.json().get("object", [])
        if len(series) < 1:
            return None
        latest = series[-1]
        prev   = series[-2] if len(series) >= 2 else None
        val    = float(latest.get("value", 0))
        ref    = latest.get("refPer", "")
        change = round(val - float(prev.get("value", 0)), 1) if prev else None
        return {"price_cpl": val, "ref_period": ref, "change_cpl": change}
    except Exception as e:
        print(f"  [WARN] StatCan gasoline — {e}", file=sys.stderr)
    return None

def fetch_maritime_electric_energy():
    """
    Maritime Electric own API — live generation mix.
    Discovered April 2026 via JS bundle analysis of grid-status page.

    Endpoint: https://graph.api.maritimeelectric.com/v1/EnergyPurchase/LoadData
    Auth: Cloudflare allows requests with Origin: graph-ui.api.maritimeelectric.com
          OR with no User-Agent (server-to-server style).
    Updates every 15 minutes.

    Fields:
      load       — total island demand MW (integer)
      wind       — wind generation MW
      solar      — solar + battery MW
      fossil     — combustion turbine MW
      imported   — NB cable import MW
      deficit    — generation shortfall MW (0 = no deficit)
      peakValue  — all-time system peak MW (403 MW, Jan 25 2026)
      peakTime   — ISO timestamp of all-time peak
      updateTime — ISO timestamp of this reading
    """
    ME_HEADERS = {
        "Origin":  "https://graph-ui.api.maritimeelectric.com",
        "Referer": "https://graph-ui.api.maritimeelectric.com/LoadData",
        "Accept":  "application/json",
    }
    try:
        r = requests.get(
            "https://graph.api.maritimeelectric.com/v1/EnergyPurchase/LoadData",
            headers=ME_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"  [WARN] ME LoadData — {e}", file=sys.stderr)
        r = None
    if not r:
        return None
    try:
        d    = r.json()
        load = float(d.get("load", 0))
        if load <= 0:
            return None
        result = {
            "load":       load,
            "wind":       float(d.get("wind",     0)),
            "solar":      float(d.get("solar",    0)),
            "fossil":     float(d.get("fossil",   0)),
            "imported":   float(d.get("imported", 0)),
            "deficit":    float(d.get("deficit",  0)),
            "peak_value": float(d.get("peakValue", 0)),
            "peak_time":  d.get("peakTime", ""),
            "update_time": d.get("updateTime", GENERATED),
            "source":     "Maritime Electric API (graph.api.maritimeelectric.com)",
        }
        print(f"  [ME API] Load={load} Wind={result['wind']} Solar={result['solar']} "
              f"Fossil={result['fossil']} Import={result['imported']} "
              f"Deficit={result['deficit']}", file=sys.stderr)
        return result
    except Exception as e:
        print(f"  [WARN] Maritime Electric energy API — {e}", file=sys.stderr)
        return None


def fetch_gpei_energy():
    """
    Maritime Electric generation data — Maritime Electric API primary,
    Rukavina proxy fallback.

    Primary:  graph.api.maritimeelectric.com  (ME's own API, confirmed April 2026)
    Fallback: energy.reinvented.net           (Rukavina proxy — has wind export split)
    """
    # ── Primary: Maritime Electric's own API ─────────────────────────────────
    result = fetch_maritime_electric_energy()
    if result:
        return result

    # ── Fallback: Rukavina deliver endpoint ──────────────────────────────────
    print("  [Energy] ME API failed — trying Rukavina proxy", file=sys.stderr)
    r = get("https://energy.reinvented.net/pei-energy/govpeca/deliver-govpeca-data.php?format=json")
    if r:
        try:
            d    = r.json()
            cur  = d.get("current", {})
            prev = d.get("previous", {})
            peaks = {
                "load":     d.get("peak",        {}).get("peak"),
                "wind":     d.get("peakwind",     {}).get("peak"),
                "imported": d.get("peakimported", {}).get("peak"),
                "fossil":   d.get("peakfossil",   {}).get("peak"),
            }
            load = float(cur.get("on-island-load", 0))
            if load > 0:
                solar = max(0.0, float(cur.get("on-island-solar", 0)))
                result = {
                    "load":        load,
                    "wind":        float(cur.get("on-island-wind",   0)),
                    "solar":       solar,
                    "fossil":      float(cur.get("on-island-fossil", 0)),
                    "imported":    float(cur.get("imported",         0)),
                    "wind_used":   float(cur.get("wind-local",       0)),
                    "wind_export": float(cur.get("wind-export",      0)),
                    "pct_wind":    float(cur.get("percentage-wind",  0)),
                    "update_time": cur.get("updatetime", GENERATED),
                    "source":      "Maritime Electric via reinvented.net",
                    "peaks":       peaks,
                }
                if prev:
                    prev_load = float(prev.get("on-island-load", 0))
                    if prev_load > 0:
                        result["load_trend"] = round(load - prev_load, 1)
                return result
        except Exception as e:
            print(f"  [WARN] Rukavina fallback — {e}", file=sys.stderr)

    print("  [WARN] All energy sources failed", file=sys.stderr)
    return None


def fetch_maritime_electric_grid_status():
    """
    Maritime Electric Grid Status Index — from their own API.
    Discovered April 2026 via JS bundle analysis.

    Endpoint: https://graph.api.maritimeelectric.com/v1/EnergyPurchase/LoadShedding
    Returns:
      imageURL            — "../images/LoadShedding/NN.png"
      timePeriodDescription — active alert text (empty when Normal)
      updateTime          — ISO timestamp

    Image numbers (confirmed April 2026 from downloaded PNGs):
      01 = Normal      (green  — needle pointing left)
      02 = Watch       (yellow — higher demand forecast within 72h)
      03 = Warning     (orange — approaching max capacity within 24h)
      04 = Load Shedding (red  — rotating outages active)

    Falls back to page scrape if API unavailable.
    """
    import re as _re

    ME_HEADERS = {
        "Origin":  "https://graph-ui.api.maritimeelectric.com",
        "Referer": "https://graph-ui.api.maritimeelectric.com/LoadShedding",
        "Accept":  "application/json",
    }
    try:
        r = requests.get(
            "https://graph.api.maritimeelectric.com/v1/EnergyPurchase/LoadShedding",
            headers=ME_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"  [WARN] ME LoadShedding — {e}", file=sys.stderr)
        r = None
    if r:
        try:
            d         = r.json()
            image_url = d.get("imageURL", "")
            desc      = d.get("timePeriodDescription", "").strip()
            upd       = d.get("updateTime", GENERATED)

            # Extract image number from URL e.g. "../images/LoadShedding/02.png" → "02"
            m = _re.search(r"(\d+)\.png", image_url)
            if m:
                num = int(m.group(1))
                level_map = {
                    1: {"level": "normal",       "label": "Normal",
                        "colour": "green",  "status": "ok"},
                    2: {"level": "watch",        "label": "Watch",
                        "colour": "yellow", "status": "warn"},
                    3: {"level": "warning",      "label": "Warning",
                        "colour": "orange", "status": "warn"},
                    4: {"level": "load_shedding","label": "Load Shedding",
                        "colour": "red",    "status": "alert"},
                }
                result = level_map.get(num, level_map[1]).copy()
                result["description"] = desc
                result["update_time"] = upd
                print(f"  [ME GSI] Level={num} ({result['label']}) desc='{desc}'",
                      file=sys.stderr)
                return result
        except Exception as e:
            print(f"  [WARN] ME grid status API — {e}", file=sys.stderr)

    # ── Fallback: scrape the page directly ───────────────────────────────────
    print("  [GSI] API failed — falling back to page scrape", file=sys.stderr)
    url = "https://www.maritimeelectric.com/outages/outages/grid-status/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml",
        "Accept-Language": "en-CA,en;q=0.9",
        "Referer":         "https://www.maritimeelectric.com/",
    }
    try:
        r2 = requests.get(url, headers=headers, timeout=TIMEOUT)
        if not r2 or r2.status_code != 200:
            return None
        raw   = r2.text
        # Strip FAQ section before scanning
        for marker in ["what is load shedding", "frequently asked",
                       "load shedding, or rotating outages, is a last resort"]:
            idx = raw.lower().find(marker)
            if idx > 500:
                raw = raw[:idx]
                break
        raw_lower = raw.lower()
        if any(p in raw_lower for p in ['class="red active"', "status-red active",
                                         'data-level="red"']):
            return {"level": "load_shedding", "label": "Load Shedding",
                    "colour": "red", "status": "alert"}
        if any(p in raw_lower for p in ['class="orange active"', "status-orange active",
                                         'data-level="orange"']):
            return {"level": "warning", "label": "Warning",
                    "colour": "orange", "status": "warn"}
        if any(p in raw_lower for p in ['class="yellow active"', "status-yellow active",
                                         'data-level="yellow"']):
            return {"level": "watch", "label": "Watch",
                    "colour": "yellow", "status": "warn"}
        return {"level": "normal", "label": "Normal",
                "colour": "green", "status": "ok"}
    except Exception as e:
        print(f"  [WARN] GSI page scrape fallback — {e}", file=sys.stderr)
        return None


# ── SECTOR: ENERGY ───────────────────────────────────────────────────────────

def scrape_energy():
    t1, t2, t3 = [], [], []

    grid = fetch_gpei_energy()
    if grid and "load" in grid:
        load   = grid.get("load",     0.0)
        wind   = grid.get("wind",     0.0)
        solar  = grid.get("solar",    0.0)
        fossil = grid.get("fossil",   0.0)
        # Use directly-reported imported MW if available, else derive it
        nb     = grid.get("imported") or max(0.0, load - wind - solar - fossil)
        util   = round((nb / CABLE_CAP_MW) * 100, 1)
        upd    = grid.get("update_time", GENERATED)
        peaks  = grid.get("peaks", {})

        # Load trend arrow from previous 15-min reading
        trend      = grid.get("load_trend")
        trend_str  = f"  ({trend:+.1f} MW vs 15 min ago)" if trend is not None else ""
        peak_load  = peaks.get("load")
        peak_str   = f"  ·  Today's peak: {peak_load} MW" if peak_load else ""

        t1.append(indicator("Island System Load", f"{load:.1f}", unit="MW",
            status="alert" if load >= 380 else "warn" if load >= 300 else "ok",
            note=(f"Extreme demand — load shedding risk. Record ~{PEAK_RECORD_MW} MW" if load >= 380
                  else "Demand at or above cable import cap — on-island generation required" if load >= 300
                  else ""),
            context=f"Peak record: ~{PEAK_RECORD_MW} MW (Jan/Feb){trend_str}{peak_str}",
            source="Maritime Electric via reinvented.net", tier=1, date=upd))

        peak_import = peaks.get("imported")
        peak_imp_str = f"  ·  Today's peak import: {peak_import} MW" if peak_import else ""
        t1.append(indicator("NB Cable Utilization", f"{util}",
            unit=f"% of {CABLE_CAP_MW} MW cap  ({nb:.0f} MW imported)",
            status="alert" if util >= 95 else "warn" if util >= 80 else "ok",
            note=("Cables at capacity — load shedding imminent" if util >= 95
                  else "High cable utilization — grid stress elevated" if util >= 80 else ""),
            context=f"4 subsea cables — hard physical limit{peak_imp_str}",
            source="Maritime Electric via reinvented.net", tier=1, date=upd,
            banner=f"NB cable import at {util}% — load shedding risk" if util >= 95 else ""))

        wpct = round((wind / load * 100), 1) if load > 0 else 0
        t1.append(indicator("Wind Generation", f"{wind:.1f}",
            unit=f"MW  ({wpct}% of load)",
            context="~204 MW installed wind capacity on PEI",
            source="Maritime Electric via GPEI", tier=1, date=upd))

        t2.append(indicator("Solar Generation", f"{solar:.1f}", unit="MW",
            context="Slemon Park, Sunbank + batteries",
            source="Maritime Electric via GPEI", tier=2, date=upd))
        t2.append(indicator("On-Island Fossil Fuel", f"{fossil:.1f}", unit="MW",
            status="alert" if fossil > 60 else "warn" if fossil > 20 else "ok",
            note=("Heavy fossil dispatch — near peak capacity event" if fossil > 60
                  else "Combustion turbines running — grid under pressure" if fossil > 20 else ""),
            context="Combustion turbines — last resort backup",
            source="Maritime Electric via GPEI", tier=2, date=upd))
        t2.append(indicator("NB Import", f"{nb:.0f}",
            unit=f"MW  (cap: {CABLE_CAP_MW} MW)",
            status="alert" if nb >= 285 else "warn" if nb >= 240 else "ok",
            source="Maritime Electric via GPEI", tier=2, date=upd))

        chart = dict(load=round(load,1), wind=round(wind,1), solar=round(solar,1),
                     fossil=round(fossil,1), nb_import=round(nb,1),
                     cable_cap=CABLE_CAP_MW, peak_record=PEAK_RECORD_MW, updated=upd)
    else:
        for lbl in ["Island System Load", "NB Cable Utilization", "Wind Generation"]:
            t1.append(manual(lbl, "unavailable", source="Maritime Electric via GPEI", tier=1,
                note="GPEI API unavailable — check maritimeelectric.com"))
        chart = None

    # ── Grid Status Index — Maritime Electric dedicated fetcher ──────────────
    gsi = fetch_maritime_electric_grid_status()
    if gsi:
        gsi_val    = gsi["label"]
        gsi_status = gsi["status"]
        gsi_note   = {
            "load_shedding": "Rotating outages active — reduce all non-essential consumption now",
            "warning":       "Approaching max capacity within 24h — turn off high-draw appliances",
            "watch":         "Higher demand forecast within 72h — prepare to conserve if asked",
            "normal":        "",
        }.get(gsi.get("level", "normal"), "")

        # ── MW sanity check — override page scrape if live data contradicts it ──
        # Load shedding requires: load near/above 300 MW AND fossil fuel running
        # AND/OR outage customers > 0. If load is low and fossil is 0, it's Normal.
        if grid and gsi.get("level") == "load_shedding":
            load_mw   = grid.get("load",   0)
            fossil_mw = grid.get("fossil", 0)
            if load_mw < 340 and fossil_mw < 5:
                print(f"  [GSI] Overriding false alert: load={load_mw} fossil={fossil_mw}",
                      file=sys.stderr)
                gsi_val, gsi_status, gsi_note = "Normal", "ok", ""
                gsi["level"] = "normal"

        # Warning also requires load >= 270 MW to be plausible
        if grid and gsi.get("level") == "warning":
            if grid.get("load", 0) < 250:
                gsi_val, gsi_status, gsi_note = "Normal", "ok", ""
                gsi["level"] = "normal"

    else:
        # Fallback: derive status entirely from live MW load data
        if grid and grid.get("load", 0) >= 380:
            gsi_val, gsi_status = "Critical Load", "alert"
            gsi_note = f"Load {grid['load']:.0f} MW — load shedding risk"
        elif grid and grid.get("load", 0) >= 300:
            gsi_val, gsi_status = "High Demand", "warn"
            gsi_note = f"Load {grid['load']:.0f} MW — at cable import cap"
        else:
            gsi_val, gsi_status, gsi_note = "Normal", "ok", ""

    t1.append(indicator("Grid Status Index", gsi_val, status=gsi_status, note=gsi_note,
        context="maritimeelectric.com/outages/outages/grid-status",
        source="Maritime Electric", tier=1,
        banner="LOAD SHEDDING ACTIVE — rotating outages in progress on PEI"
               if gsi_status == "alert" and "load" in gsi_val.lower() else ""))

    # Outage customers
    s2    = soup("https://poweroutage.com/ca/utility/1370")
    o_val, o_status = None, "ok"
    if s2:
        m2 = re.search(r"([\d,]+)\s+(?:customers?|cust)", s2.get_text(" ", strip=True), re.I)
        if m2:
            o_val   = m2.group(1)
            n       = int(o_val.replace(",", ""))
            o_status = "alert" if n > 5000 else "warn" if n > 500 else "ok"

    t1.append(indicator("Outage Customers", o_val or "check map",
        unit="customers affected" if o_val else "",
        status=o_status if o_val else "manual",
        source="poweroutage.com / Maritime Electric", tier=1, date=GENERATED))

    t2.append(manual("Summerside Utility Status", "see summerside.ca/electric",
        source="City of Summerside Electric Utility", tier=2))
    # ── NRCan Furnace Oil Price — Charlottetown ─────────────────────────────
    furn = fetch_nrcan_furnace_oil_charlottetown()
    if furn:
        p      = furn["price_cpl"]
        chg    = furn["change_cpl"]
        f_stat = "warn" if p > 175 else "ok"
        chg_str = f"  ({chg:+.1f} vs prev week)" if chg is not None else ""
        t3.append(indicator("Heating Oil — Charlottetown", f"{p:.1f}",
            unit=f"¢/L incl. tax  ·  {furn['week_ending']}{chg_str}",
            status=f_stat,
            note="Above 175¢/L — elevated heating cost burden on island households" if f_stat == "warn" else "",
            context="~35% of PEI households rely on fuel oil for heat",
            source="Natural Resources Canada — weekly retail prices", tier=3, date=GENERATED))
    else:
        t3.append(manual("Heating Oil Price", "check nrcan.gc.ca/energy/prices",
            source="NRCan / local suppliers", tier=3,
            note="~35% of PEI households rely on heating oil"))
    t3.append(manual("Net Zero Initiatives", "see netzeronavigatorpei.com",
        source="GPEI Environment, Energy and Climate Action", tier=3))

    return {"tiers": {
        "tier1": {"label": "Critical Infrastructure", "indicators": t1},
        "tier2": {"label": "Secondary Systems",       "indicators": t2},
        "tier3": {"label": "Contextual Signals",      "indicators": t3},
    }, "chart_data": chart}


# ── SECTOR: WATER ─────────────────────────────────────────────────────────────

def scrape_water():
    t1, t2, t3 = [], [], []

    s = soup("https://www.princeedwardisland.ca/en/feature/boil-water-advisories")
    count, bw_status, bw_note = 0, "ok", ""
    if s:
        text  = s.get_text(" ", strip=True)
        m     = re.search(r"(\d+)\s+(?:active|current)\s+(?:boil.water|advisory)", text, re.I)
        count = int(m.group(1)) if m else len([li for li in s.find_all("li")
                    if "boil" in li.get_text().lower() or "advisory" in li.get_text().lower()])
        if count > 0:
            bw_status = "alert" if count > 5 else "warn"
            bw_note   = f"{count} active advisory/advisories — check GPEI site"

    t1.append(indicator("Boil Water Advisories", str(count),
        unit="active", status=bw_status, note=bw_note,
        context="100% groundwater province — unique in Canada",
        source="GPEI Environment, Energy and Climate Action", tier=1, date=GENERATED))

    t2.append(manual("Central Water Systems", "see GPEI EECA",
        source="GPEI Environment, Energy and Climate Action", tier=2,
        note="~50% of residents on central water; remainder on private wells"))
    t3.append(manual("Nitrate Risk (Agricultural)", "ongoing monitoring",
        source="GPEI Environment, Energy and Climate Action", tier=3,
        note="Potato agriculture — primary source of nitrate contamination in aquifers"))
    t3.append(manual("Coastal Water Quality", "see PEI Open Data",
        source="data.princeedwardisland.ca", tier=3))

    return {"tiers": {
        "tier1": {"label": "Critical Infrastructure", "indicators": t1},
        "tier2": {"label": "Secondary Systems",       "indicators": t2},
        "tier3": {"label": "Contextual Signals",      "indicators": t3},
    }}


# ── SECTOR: HEALTH ────────────────────────────────────────────────────────────


def fetch_gpei_er_wait_times(feature_name, cookie_string=None):
    """
    GPEI /api/workflow — Emergency Department wait times.
    Cracked April 5 2026 via browser DevTools network inspection.

    Endpoint:  POST https://wdf.princeedwardisland.ca/api/workflow
    Auth:      Radware session cookies (solved by real browser JS challenge)
               Cookies stored in GPEI_COOKIES constant below.
               Bound to IP+UA fingerprint — may need refresh periodically.

    Request body (214 bytes — discovered via Payload tab):
      {
        "appName":    "<featureName>",
        "featureName": "<featureName>",
        "metaVars":   {"service_id": null, "save_location": null},
        "queryName":  "<featureName>",
        "queryVars":  {"service": "<featureName>", "activity": "<featureName>"}
      }

    Response structure:
      data[0] = TableV2 with rows:
        "Patients in the Waiting Room" → count, (no wait time — they're waiting)
          children: triage rows with Div.data.value = triage level name
            "Most Urgent (Level 2)"        → count, wait text e.g. "2-3 hours"
            "Urgent (Level 3)"             → count, wait text e.g. "> 10 hours"
            "Less than Urgent (Level 4&5)" → count, wait text
        "Patients being treated by a Physician" → count
        "Total Patients in Emergency Department" → count
      data[1] = Paragraph: "Last updated April 5, 2026 12:10 PM"

      KCMH when closed returns a single Heading with closure message.

    Feature names:
      ERWaitTimes_QEH  — Queen Elizabeth Hospital, Charlottetown
      ERWaitTimes_PCH  — Prince County Hospital, Summerside
      ERWaitTimes_WH   — Western Hospital
      ERWaitTimes_KCMH — Kings County Memorial Hospital
    """
    # ── Radware session cookies ───────────────────────────────────────────────
    # Obtained from browser DevTools after passing JS challenge.
    # These are HttpOnly so only visible in DevTools — not readable by JS.
    # Refresh procedure: visit QEH page in browser, DevTools → Network →
    #   click ERWaitTimes_QEH → Cookies tab → copy all cookie values.
    # Expiry: ~182 days (Max-Age=15724800)
    cookies_str = cookie_string or GPEI_ER_COOKIES
    if not cookies_str:
        print(f"  [WARN] No GPEI ER cookies configured — {feature_name} stays manual",
              file=sys.stderr)
        return None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Mobile Safari/537.36"
        ),
        "Accept":           "application/json",
        "Accept-Language":  "en",
        "Content-Type":     "application/json",
        "Origin":           "https://www.princeedwardisland.ca",
        "Referer":          "https://www.princeedwardisland.ca/",
        "Client-Show-Status": "true",
        "Sec-Ch-Ua":        '"Chromium";v="146", "Not-A-Brand";v="24", "Google Chrome";v="146"',
        "Sec-Ch-Ua-Mobile": "?1",
        "Sec-Ch-Ua-Platform": '"Android"',
        "Sec-Fetch-Dest":   "empty",
        "Sec-Fetch-Mode":   "cors",
        "Sec-Fetch-Site":   "same-site",
        "Cookie":           cookies_str,
    }
    body = {
        "appName":    feature_name,
        "featureName": feature_name,
        "metaVars":   {"service_id": None, "save_location": None},
        "queryName":  feature_name,
        "queryVars":  {"service": feature_name, "activity": feature_name},
    }

    try:
        r = requests.post(
            "https://wdf.princeedwardisland.ca/api/workflow",
            json=body, headers=headers, timeout=TIMEOUT
        )
        if not r.ok:
            print(f"  [WARN] GPEI ER {feature_name} HTTP {r.status_code}",
                  file=sys.stderr)
            return None
        if 'captcha' in r.text.lower():
            print(f"  [WARN] GPEI ER {feature_name} — Radware blocked (cookies expired?)",
                  file=sys.stderr)
            return None

        d    = r.json()
        data = d.get('data')
        if not data:
            return None

        result = {"feature": feature_name, "raw": d}

        # ── KCMH-style: single Heading = closure/special message ─────────────
        if isinstance(data, dict) and data.get('type') == 'Heading':
            result['closed']  = True
            result['message'] = data.get('data', {}).get('text', '').strip()
            print(f"  [ER] {feature_name}: CLOSED — {result['message'][:60]}",
                  file=sys.stderr)
            return result

        # ── Normal: list with TableV2 + Paragraph ────────────────────────────
        if not isinstance(data, list):
            return None

        # Extract timestamp from Paragraph element
        for item in data:
            if item.get('type') == 'Paragraph':
                result['updated'] = item.get('data', {}).get('text', '').strip()

        # Parse the TableV2
        table = next((x for x in data if x.get('type') == 'TableV2'), None)
        if not table:
            return None

        rows = [c for c in table.get('children', [])
                if c.get('type') == 'TableV2Row']

        def cell_text(row, idx):
            """Get text from the Nth cell in a row."""
            cells = [c for c in row.get('children', [])
                     if c.get('type') in ('TableV2Cell', 'TableV2Header')]
            if idx < len(cells):
                return (cells[idx].get('data', {}).get('text') or '').strip()
            return ''

        def row_label(row):
            """Get row label — from TableV2Header text or Div child value."""
            for child in row.get('children', []):
                # Direct header text
                if child.get('type') == 'TableV2Header':
                    t = (child.get('data', {}).get('text') or '').strip()
                    if t:
                        return t
                # Div child inside a cell
                if child.get('type') == 'TableV2Cell':
                    for gc in child.get('children', []):
                        if gc.get('type') == 'Div':
                            v = (gc.get('data', {}).get('value') or '').strip()
                            if v:
                                import html as _html
                                return _html.unescape(v)
            return ''

        for row in rows:
            label = row_label(row)
            if not label:
                continue
            count_str = cell_text(row, 1)
            wait_str  = cell_text(row, 2)
            try:
                count = int(count_str) if count_str else None
            except ValueError:
                count = None

            label_lower = label.lower()
            if 'waiting room' in label_lower:
                result['waiting_room'] = count
            elif 'most urgent' in label_lower or 'level 2' in label_lower:
                result['level2_count'] = count
                result['level2_wait']  = wait_str
            elif 'urgent' in label_lower and 'level 3' in label_lower:
                result['level3_count'] = count
                result['level3_wait']  = wait_str
            elif 'less than urgent' in label_lower or 'level 4' in label_lower:
                result['level4_count'] = count
                result['level4_wait']  = wait_str
            elif 'physician' in label_lower:
                result['with_physician'] = count
            elif 'total' in label_lower:
                result['total'] = count

        result['closed'] = False
        print(f"  [ER] {feature_name}: total={result.get('total')} "
              f"waiting={result.get('waiting_room')} "
              f"L3_wait={result.get('level3_wait')} "
              f"updated={result.get('updated', '')[-20:]}",
              file=sys.stderr)
        return result

    except Exception as e:
        print(f"  [WARN] GPEI ER {feature_name} — {e}", file=sys.stderr)
        return None


def scrape_health():
    t1, t2, t3 = [], [], []

    aqhi = fetch_aqhi_pei()
    if aqhi is not None:
        t1.append(indicator("AQHI — Charlottetown", f"{aqhi:.0f}", unit="/10",
            status="alert" if aqhi >= 7 else "warn" if aqhi >= 4 else "ok",
            note=("High risk — reduce outdoor exertion" if aqhi >= 7
                  else "Moderate risk for sensitive groups" if aqhi >= 4 else ""),
            source="Environment and Climate Change Canada", tier=1, date=GENERATED))
    else:
        t1.append(manual("AQHI — Charlottetown", "see weather.gc.ca",
            source="Environment and Climate Change Canada", tier=1))

    # ── QEH Emergency Department — GPEI workflow API ────────────────────────
    # ── QEH — Queen Elizabeth Hospital, Charlottetown ───────────────────────
    # Uses GPEI /api/workflow with Radware session cookies.
    # Cracked April 5 2026 — see fetch_gpei_er_wait_times() for full details.
    def _er_indicator(name, hosp_data, tier, is_tier1=False):
        """Build indicator cards from parsed ER wait time data."""
        if not hosp_data:
            return None
        upd = hosp_data.get("updated", GENERATED)

        if hosp_data.get("closed"):
            msg = hosp_data.get("message", "ED closed")
            ind = indicator(name, "Closed", status="alert",
                note=msg[:120] if msg else "ED temporarily closed",
                source="Health PEI / GPEI", tier=tier, date=upd,
                banner=f"{name} — {msg[:80]}" if is_tier1 else "")
            return ind

        total   = hosp_data.get("total")
        waiting = hosp_data.get("waiting_room")
        l2c     = hosp_data.get("level2_count")
        l3c     = hosp_data.get("level3_count")
        l3w     = hosp_data.get("level3_wait", "")
        phys    = hosp_data.get("with_physician")

        # Status based on total and worst triage wait
        status = "ok"
        if total and total >= 60:       status = "alert"
        elif total and total >= 40:     status = "warn"
        if "> 10" in (l3w or ""):       status = max(status, "warn",
                                            key=["ok","warn","alert"].index)

        ctx_parts = []
        if waiting is not None: ctx_parts.append(f"Waiting: {waiting}")
        if l2c:                 ctx_parts.append(f"Level 2: {l2c} ({hosp_data.get('level2_wait','')})")
        if l3c:                 ctx_parts.append(f"Level 3: {l3c} ({l3w})")
        if phys is not None:    ctx_parts.append(f"With physician: {phys}")

        note = ""
        if "> 10" in (l3w or "") and l3c:
            note = f"Level 3 (Urgent) wait >10 hours — {l3c} patients affected"
        elif total and total >= 60:
            note = f"ED at high capacity — {total} total patients"

        return indicator(name, str(total) if total else "—",
            unit="total patients in ED",
            status=status,
            note=note,
            context=" · ".join(ctx_parts),
            source="Health PEI (GPEI /api/workflow)",
            tier=tier, date=upd)

    qeh = fetch_gpei_er_wait_times("ERWaitTimes_QEH")
    if qeh:
        ind = _er_indicator("QEH Emergency Department", qeh, tier=1, is_tier1=True)
        if ind: t1.append(ind)
    else:
        t1.append(manual("QEH ED Wait Time", "see princeedwardisland.ca",
            source="Health PEI — cookies may need refresh", tier=1,
            note="princeedwardisland.ca/en/feature/emergency-department-wait-times-queen-elizabeth-hospital-qeh"))

    # ── PCH — Prince County Hospital, Summerside ─────────────────────────────
    pch = fetch_gpei_er_wait_times("ERWaitTimes_PCH")
    if pch:
        ind = _er_indicator("Prince County Hospital ED", pch, tier=2)
        if ind: t2.append(ind)
    else:
        t2.append(manual("Prince County Hospital ED", "see healthpei.ca",
            source="Health PEI — Summerside", tier=2))

    # ── WH — Western Hospital ────────────────────────────────────────────────
    wh = fetch_gpei_er_wait_times("ERWaitTimes_WH")
    if wh:
        ind = _er_indicator("Western Hospital ED", wh, tier=2)
        if ind: t2.append(ind)
    else:
        t2.append(manual("Western Hospital ED", "see healthpei.ca",
            source="Health PEI — Western PEI", tier=2))

    # ── KCMH — Kings County Memorial Hospital ────────────────────────────────
    kcmh = fetch_gpei_er_wait_times("ERWaitTimes_KCMH")
    if kcmh:
        ind = _er_indicator("Kings County Memorial Hospital ED", kcmh, tier=2)
        if ind: t2.append(ind)
    else:
        t2.append(manual("Kings County Memorial ED", "see healthpei.ca",
            source="Health PEI — Montague", tier=2))

    # ── Respiratory illness surveillance ─────────────────────────────────────
    s = soup("https://www.princeedwardisland.ca/en/information/health-pei/respiratory-illness-surveillance")
    flu_val, flu_status, flu_note = "Normal", "ok", ""
    if s:
        text = s.get_text(" ", strip=True).lower()
        if "elevated" in text or "increased activity" in text:
            flu_val, flu_status, flu_note = "Elevated", "warn", "Respiratory illness above seasonal baseline"
        elif "high" in text and "activity" in text:
            flu_val, flu_status, flu_note = "High", "alert", "High respiratory illness — increased system pressure"

    t2.append(indicator("Respiratory Illness Activity", flu_val,
        status=flu_status, note=flu_note,
        source="Health PEI", tier=2, date=GENERATED))

    t3.append(manual("Patients Without a Doctor", "see Health PEI / CFPC",
        source="Health PEI", tier=3,
        note="PEI has ongoing primary care challenges — lowest physician ratio in Canada"))

    return {"tiers": {
        "tier1": {"label": "Critical Infrastructure", "indicators": t1},
        "tier2": {"label": "Secondary Systems",       "indicators": t2},
        "tier3": {"label": "Contextual Signals",      "indicators": t3},
    }}


# ── SECTOR: TRANSPORT ─────────────────────────────────────────────────────────

def scrape_transport():
    t1, t2, t3 = [], [], []

    s = soup("https://www.confederationbridge.com/bridge-conditions")
    br_val, br_status, br_note = "Open", "ok", ""
    if s:
        text = s.get_text(" ", strip=True).lower()
        if "closed" in text:
            br_val, br_status, br_note = "CLOSED", "alert", "Bridge closed — check confederationbridge.com"
        elif any(w in text for w in ["restriction","caution","commercial","one-lane","one lane"]):
            br_val, br_status, br_note = "Restrictions", "warn", "Vehicle restrictions — check confederationbridge.com"

    t1.append(indicator("Confederation Bridge", br_val, status=br_status, note=br_note,
        context="12.9 km fixed link — primary supply corridor",
        source="confederationbridge.com", tier=1, date=GENERATED,
        banner="Confederation Bridge CLOSED — primary land supply route affected" if br_status == "alert" else ""))

    wx_br = fetch_open_meteo(BRIDGE_LAT, BRIDGE_LON,
                             params="wind_speed_10m,wind_gusts_10m,temperature_2m")
    if wx_br and wx_br.get("wind_gusts_10m") is not None:
        gust = wx_br["wind_gusts_10m"]
        spd  = wx_br.get("wind_speed_10m", 0)
        t2.append(indicator("Wind Gust (Bridge Area)", f"{gust:.0f}",
            unit="km/h",
            status="alert" if gust > 110 else "warn" if gust > 90 else "ok",
            note=("Bridge may be closed to all traffic" if gust > 110
                  else "High-sided vehicle restrictions likely" if gust > 90 else ""),
            context=f"Sustained: {spd:.0f} km/h",
            source="Open-Meteo (Borden-Carleton)", tier=2, date=GENERATED))
    else:
        t2.append(manual("Wind Gust (Bridge Area)", "see weather.gc.ca",
            source="Environment Canada", tier=2))

    s2 = soup("https://www.ferries.ca/northumberland/schedule/")
    f_val, f_status = ("In Season" if TODAY.month in range(5,13) else "Off Season"), \
                      ("ok"        if TODAY.month in range(5,13) else "manual")
    if s2 and f_status == "ok":
        ft = s2.get_text(" ", strip=True).lower()
        if "cancel" in ft or "suspend" in ft:
            f_val, f_status = "Disruption", "warn"

    t2.append(indicator("Northumberland Ferry", f_val,
        unit="Wood Islands ↔ Pictou",
        status=f_status,
        context="Seasonal alternate crossing (May–Dec) · 75 min",
        source="ferries.ca", tier=2, date=GENERATED))

    metar = fetch_metar("CYYG")
    if metar:
        fcat = metar.get("fltcat", metar.get("flightCategory", "VFR"))
        vis  = metar.get("visib")
        ceil = metar.get("ceil")
        raw  = metar.get("rawOb", "")
        m_status = ("alert" if fcat == "LIFR" else "warn" if fcat in ("IFR","MVFR") else "ok")
        t2.append(indicator("Charlottetown Airport (YYG)", fcat or "VFR",
            status=m_status,
            note=(f"Low visibility — delays likely. Vis: {vis}SM  Ceil: {ceil}ft"
                  if m_status in ("alert","warn") else ""),
            context=f"METAR: {raw[:55]}" if raw else "",
            source="AviationWeather.gov METAR", tier=2, date=GENERATED))
    else:
        t2.append(manual("Charlottetown Airport (YYG)", "see flightaware.com/CYYG",
            source="Charlottetown Airport Authority", tier=2))

    s3 = soup("https://www.princeedwardisland.ca/en/feature/road-conditions")
    r_val, r_status = "Normal", "ok"
    if s3:
        rt = s3.get_text(" ", strip=True).lower()
        if "closed" in rt:
            r_val, r_status = "Closures reported", "warn"
        elif any(w in rt for w in ["ice","snow","blowing","slippery"]):
            r_val, r_status = "Winter conditions", "warn"

    t2.append(indicator("Island Road Conditions", r_val, status=r_status,
        source="GPEI Transportation and Infrastructure", tier=2, date=GENERATED))

    t3.append(manual("Bridge Commercial Traffic", "see confederationbridge.com",
        source="Strait Crossing Bridge Ltd", tier=3,
        note="~1.5M vehicles/year — primary supply corridor for food, fuel, goods"))

    return {"tiers": {
        "tier1": {"label": "Critical Infrastructure", "indicators": t1},
        "tier2": {"label": "Secondary Systems",       "indicators": t2},
        "tier3": {"label": "Contextual Signals",      "indicators": t3},
    }}


# ── SECTOR: FINANCIAL ─────────────────────────────────────────────────────────

def scrape_financial():
    t1, t2, t3 = [], [], []

    cad, fx_date = fetch_boc_rate("FXUSDCAD")
    if cad:
        t1.append(indicator("CAD/USD", f"{cad:.4f}", unit="CAD per USD",
            status="alert" if cad > 1.50 else "warn" if cad > 1.45 else "ok",
            note=("Very weak loonie — significant import cost pressure" if cad > 1.50
                  else "Weak loonie — imported goods more expensive on PEI" if cad > 1.45 else ""),
            context=f"BoC as of {fx_date}" if fx_date else "",
            source="Bank of Canada Valet API", tier=1, date=GENERATED))
    else:
        t1.append(manual("CAD/USD", "unavailable", source="Bank of Canada", tier=1))

    cpi_val, cpi_ref, cpi_yoy = fetch_statcan_cpi_pei()
    if cpi_val and cpi_yoy is not None:
        t2.append(indicator("PEI CPI (YoY)",
            f"+{cpi_yoy}%" if cpi_yoy >= 0 else f"{cpi_yoy}%",
            unit=f"All-items · index: {cpi_val:.1f}",
            status="alert" if cpi_yoy > 4.0 else "warn" if cpi_yoy > 2.5 else "ok",
            note=("High inflation — elevated cost pressure on island households" if cpi_yoy > 4.0
                  else "Above Bank of Canada 2% target" if cpi_yoy > 2.5 else ""),
            context=f"Reference: {cpi_ref}" if cpi_ref else "",
            source="Statistics Canada table 18-10-0004", tier=2, date=GENERATED))
    else:
        t2.append(manual("PEI CPI (YoY)", "see statcan.gc.ca/18-10-0004",
            source="Statistics Canada", tier=2,
            note="Monthly release — PEI-specific price levels"))

    vacancy, v_period = fetch_cmhc_vacancy()
    if vacancy is not None:
        t2.append(indicator("Charlottetown Vacancy Rate", f"{vacancy:.1f}",
            unit=f"%  ({v_period})",
            status="alert" if vacancy < 1.0 else "warn" if vacancy < 2.0 else "ok",
            note=("Critical housing shortage — vacancy below 1%" if vacancy < 1.0
                  else "Very low vacancy — Charlottetown housing stress" if vacancy < 2.0 else ""),
            source="CMHC Housing Market Information Portal", tier=2, date=GENERATED))
    else:
        t2.append(manual("Housing Vacancy Rate", "see CMHC Housing Market",
            source="CMHC", tier=2,
            note="Charlottetown among lowest vacancy rates in Atlantic Canada"))

    t3.append(manual("Lobster Ex-Vessel Price", "see GPEI Fisheries",
        source="GPEI Fisheries and Communities", tier=3,
        note="PEI lobster — primary export commodity; spring/fall seasons"))
    t3.append(manual("Tourism Activity", "see tourismpei.com",
        source="Tourism PEI", tier=3,
        note="~$500M annually; highly seasonal (June–Sept)"))

    return {"tiers": {
        "tier1": {"label": "Critical Infrastructure", "indicators": t1},
        "tier2": {"label": "Secondary Systems",       "indicators": t2},
        "tier3": {"label": "Contextual Signals",      "indicators": t3},
    }}


# ── SECTOR: PUBLIC SAFETY ──────────────────────────────────────────────────────

def scrape_public_safety():
    t1, t2, t3 = [], [], []

    count, wx_status, wx_text = fetch_wx_alerts_pei()
    t1.append(indicator("Weather Alerts (PEI)", str(count),
        unit="active alerts", status=wx_status,
        note=wx_text if count > 0 else "",
        context="ECCC public alerting — Alert Ready",
        source="Environment and Climate Change Canada", tier=1, date=GENERATED,
        banner=f"Weather warning active: {wx_text}" if wx_status == "alert" else ""))

    s = soup("https://www.princeedwardisland.ca/en/topic/emergency-management")
    emo_val, emo_status, emo_note = "Normal", "ok", ""
    if s:
        text = s.get_text(" ", strip=True).lower()
        if "state of emergency" in text:
            emo_val, emo_status, emo_note = "State of Emergency", "alert", "Provincial SOE declared"
        elif "elevated" in text:
            emo_val, emo_status = "Elevated", "warn"

    t1.append(indicator("PEI EMO Status", emo_val, status=emo_status, note=emo_note,
        source="PEI Emergency Measures Organization", tier=1, date=GENERATED))

    # ── Charlottetown Harbour Water Level — CHS IWLS API ────────────────────
    wl = fetch_charlottetown_water_level()
    if wl is not None:
        level   = wl["water_level"]
        anomaly = wl.get("anomaly")
        pred    = wl.get("predicted")
        wl_status = "ok"
        wl_note   = ""
        if anomaly is not None:
            if anomaly >= 1.0:
                wl_status = "alert"
                wl_note   = f"Significant storm surge +{anomaly:.2f}m above predicted — coastal flood risk"
            elif anomaly >= 0.5:
                wl_status = "warn"
                wl_note   = f"Elevated water level +{anomaly:.2f}m above predicted — monitor low-lying areas"
        context_str = (f"Predicted: {pred:.3f}m  |  Anomaly: {anomaly:+.3f}m" if pred and anomaly is not None
                       else f"Predicted: {pred:.3f}m" if pred else "")
        t2.append(indicator("Charlottetown Harbour Level", f"{level:.3f}",
            unit="m (chart datum)",
            status=wl_status,
            note=wl_note,
            context=context_str,
            source="CHS IWLS API (DFO) — station 01700",
            tier=2, date=GENERATED))
        # Storm surge card (Tier 3 contextual unless active)
        if anomaly is not None:
            surge_status = "alert" if anomaly >= 1.0 else "warn" if anomaly >= 0.5 else "ok"
            t3.append(indicator("Storm Surge (Charlottetown)", f"{anomaly:+.3f}",
                unit="m above predicted tide",
                status=surge_status,
                note=wl_note if surge_status != "ok" else "",
                context="Positive = water above predicted tide level",
                source="CHS IWLS API (DFO)", tier=3, date=GENERATED))
        else:
            t3.append(manual("Storm Surge Risk", "see wateroffice.ec.gc.ca",
                source="Environment Canada / Water Survey of Canada", tier=3,
                note="Low-lying coastal areas at risk; Charlottetown harbour tide gauge"))
    else:
        t2.append(manual("Charlottetown Harbour Level", "see tides.gc.ca",
            source="Canadian Hydrographic Service (DFO)", tier=2,
            note="CHS IWLS API station 01700 — real-time water level"))
        t3.append(manual("Storm Surge Risk", "see wateroffice.ec.gc.ca",
            source="Environment Canada / Water Survey of Canada", tier=3,
            note="Low-lying coastal areas at risk; Charlottetown harbour tide gauge"))

    t2.append(manual("Coast Guard (Maritime Region)", "see dfo-mpo.gc.ca",
        source="Canadian Coast Guard — Maritimes Region", tier=2,
        note="PEI surrounded by Northumberland Strait, Gulf of St. Lawrence"))
    t2.append(manual("RCMP PEI — Public Notices", "see rcmp-grc.gc.ca/pe",
        source="RCMP L Division", tier=2))
    t3.append(manual("Coastal Erosion Status", "see GPEI Environment",
        source="GPEI Environment, Energy and Climate Action", tier=3,
        note="PEI: highest coastal erosion rates in Canada (~0.3m/yr avg). Storm surge risk."))

    return {"tiers": {
        "tier1": {"label": "Critical Infrastructure", "indicators": t1},
        "tier2": {"label": "Secondary Systems",       "indicators": t2},
        "tier3": {"label": "Contextual Signals",      "indicators": t3},
    }}


# ── SECTOR: ENVIRONMENT ───────────────────────────────────────────────────────

def scrape_environment():
    t1, t2, t3 = [], [], []

    wx = fetch_open_meteo(CHARLOTTETOWN_LAT, CHARLOTTETOWN_LON)
    if wx:
        temp   = wx.get("temperature_2m")
        gust   = wx.get("wind_gusts_10m")
        spd    = wx.get("wind_speed_10m")
        precip = wx.get("precipitation")
        hum    = wx.get("relative_humidity_2m")
        desc   = WMO_DESC.get(wx.get("weather_code", 0), "")

        t_status = ("alert" if temp is not None and temp <= -25 else
                    "warn"  if temp is not None and (temp <= -15 or temp >= 32) else "ok")
        t1.append(indicator("Temperature (Charlottetown)",
            f"{temp:.1f}" if temp is not None else "—", unit="°C",
            status=t_status,
            note=("Extreme cold — elevated heating demand and exposure risk" if temp is not None and temp <= -25
                  else "Cold snap — elevated heating demand" if temp is not None and temp <= -15
                  else "Heat advisory conditions" if temp is not None and temp >= 32 else ""),
            context=(f"{desc}  ·  Wind {spd:.0f} km/h  ·  Humidity {hum}%"
                     if spd and hum else desc),
            source="Open-Meteo (Charlottetown)", tier=1, date=GENERATED))

        if gust is not None:
            t1.append(indicator("Wind Gusts (Charlottetown)", f"{gust:.0f}",
                unit="km/h",
                status="alert" if gust > 100 else "warn" if gust > 70 else "ok",
                note=("Damaging gusts — infrastructure risk" if gust > 100
                      else "Strong gusts — elevated bridge and coastal risk" if gust > 70 else ""),
                context=f"Sustained: {spd:.0f} km/h" if spd else "",
                source="Open-Meteo (Charlottetown)", tier=1, date=GENERATED))

        if precip is not None and precip > 0:
            t2.append(indicator("Precipitation (Current Hour)", f"{precip:.1f}",
                unit="mm",
                status="warn" if precip > 10 else "ok",
                note="Heavy precipitation — road and drainage impacts" if precip > 10 else "",
                source="Open-Meteo", tier=2, date=GENERATED))
    else:
        t1.append(manual("Temperature (Charlottetown)", "see weather.gc.ca",
            source="Environment Canada", tier=1))

    # ── Gulf SST Anomaly — NOAA CoastWatch ERDDAP ────────────────────────────
    sst = fetch_gulf_sst_anomaly()
    if sst is not None:
        a      = sst["anomaly"]
        s_stat = "alert" if a >= 2.0 else "warn" if a >= 1.0 else "ok"
        t2.append(indicator("Gulf SST Anomaly", f"{a:+.2f}",
            unit=f"deg-C  ({sst['date']})",
            status=s_stat,
            note=("Strongly above-average Gulf temps — amplified storm and erosion risk" if a >= 2.0 else
                  "Above-average Gulf temperatures — elevated storm potential" if a >= 1.0 else
                  "Below-average Gulf temps — reduced storm intensification risk" if a <= -1.0 else ""),
            context="Northumberland Strait · +ve = warmer than avg = amplified risk",
            source="NOAA Coral Reef Watch (CoastWatch ERDDAP)", tier=2, date=GENERATED))
    else:
        t2.append(manual("Gulf SST Anomaly", "see dfo-mpo.gc.ca",
            source="NOAA CoastWatch / DFO Bedford Institute", tier=2,
            note="Warm Gulf SST amplifies storm systems and coastal erosion"))

    # ── Canadian Drought Monitor — AAFC GeoJSON ──────────────────────────────
    drought = fetch_aafc_drought_pei()
    if drought:
        dc     = drought["category"]
        d_stat = ("alert" if dc in ("D3","D4") else
                  "warn"  if dc in ("D0","D1","D2") else "ok")
        t2.append(indicator("Drought Conditions (PEI)", drought["label"],
            unit=f"CDM · {drought['date']}",
            status=d_stat,
            note=("Extreme drought — significant potato and agriculture stress" if dc in ("D3","D4") else
                  "Moderate to Severe drought — potato yields and groundwater at risk" if dc in ("D1","D2") else
                  "Abnormally dry — monitor soil moisture and well levels" if dc == "D0" else ""),
            context="Canadian Drought Monitor (AAFC) — monthly assessment",
            source="Agriculture and Agri-Food Canada", tier=2, date=GENERATED))
    else:
        t2.append(manual("Drought Conditions", "see agr.gc.ca/drought-watch",
            source="Agriculture and Agri-Food Canada", tier=2,
            note="Drought affects potato crop yields — PEI's primary commodity"))

    # ── Fire Weather Index — CWFIS NRCan ─────────────────────────────────────
    fwi = fetch_fwi_pei()
    if fwi and fwi.get("season"):
        f      = fwi["fwi"]
        fmax   = fwi["fwi_max"]
        f_stat = "alert" if f >= 30 else "warn" if f >= 12 else "ok"
        f_cls  = ("Extreme" if f >= 30 else "Very High" if f >= 20 else
                  "High" if f >= 12 else "Moderate" if f >= 5 else "Low")
        t3.append(indicator("Fire Weather Index (PEI)", f"{f:.1f}",
            unit=f"avg · max {fmax:.1f}  ({fwi['date']})",
            status=f_stat,
            note=("Extreme fire danger — open burn ban likely in effect" if f >= 30 else
                  "Very high fire danger — open burning restricted" if f >= 20 else
                  "High fire danger — use caution with open flames" if f >= 12 else ""),
            context=f"Danger class: {f_cls} · {fwi['stations']} PEI stations",
            source="CWFIS / Natural Resources Canada", tier=3, date=GENERATED))
    elif fwi and not fwi.get("season"):
        t3.append(indicator("Fire Weather Index (PEI)", "Off Season",
            unit="Active: Apr-Oct",
            status="ok",
            context="FWI calculated during fire season only",
            source="CWFIS / Natural Resources Canada", tier=3, date=GENERATED))
    else:
        t3.append(manual("Fire Weather Index (PEI)", "see cwfis.cfs.nrcan.gc.ca",
            source="Canadian Wildland Fire Information System", tier=3))

    return {"tiers": {
        "tier1": {"label": "Critical Infrastructure", "indicators": t1},
        "tier2": {"label": "Secondary Systems",       "indicators": t2},
        "tier3": {"label": "Contextual Signals",      "indicators": t3},
    }}


# ── SECTOR: FOOD & PRICES ─────────────────────────────────────────────────────

def scrape_food():
    t2, t3 = [], []
    # ── StatCan Food CPI — PEI ──────────────────────────────────────────────
    food_cpi = fetch_statcan_food_cpi_pei()
    if food_cpi and "food" in food_cpi:
        fd = food_cpi["food"]
        yoy = fd.get("yoy_pct")
        f_stat = "alert" if yoy and yoy > 6.0 else "warn" if yoy and yoy > 3.5 else "ok"
        yoy_str = f"+{yoy}%" if yoy is not None and yoy >= 0 else f"{yoy}%" if yoy is not None else "n/a"
        t2.append(indicator("Food Prices (PEI)", yoy_str,
            unit=f"YoY  ·  index: {fd['index']:.1f}  ·  {fd['ref_period']}",
            status=f_stat,
            note=("Food inflation well above national target — significant pressure on island households"
                  if f_stat == "alert" else
                  "Food inflation above Bank of Canada 2% target" if f_stat == "warn" else ""),
            context="Island logistics premium: all non-local food crosses the Confederation Bridge",
            source="Statistics Canada table 18-10-0004", tier=2, date=GENERATED))
        # Also add gasoline price if available
        gas = fetch_statcan_gasoline_charlottetown()
        if gas:
            g_stat = "warn" if gas["price_cpl"] > 180 else "ok"
            chg_str = f"  ({gas['change_cpl']:+.1f} vs prev month)" if gas.get("change_cpl") else ""
            t2.append(indicator("Gasoline — Charlottetown", f"{gas['price_cpl']:.1f}",
                unit=f"¢/L  ·  {gas['ref_period']}{chg_str}",
                status=g_stat,
                note="High gasoline price — elevated cost for island households and supply chains" if g_stat == "warn" else "",
                source="Statistics Canada table 18-10-0001", tier=2, date=GENERATED))
    else:
        t2.append(manual("Grocery Price Index (PEI)", "see statcan.gc.ca",
            source="Statistics Canada", tier=2,
            note="Island logistics premium: all non-local food crosses the Confederation Bridge"))
    t2.append(manual("Lobster Landings", "see GPEI Fisheries",
        source="GPEI Fisheries and Communities", tier=2,
        note="Spring: late April–June. Fall: August–October."))
    t2.append(manual("Potato Crop Status", "see GPEI Agriculture",
        source="GPEI Agriculture", tier=2,
        note="~100,000 acres annually — PEI's dominant crop"))
    t3.append(manual("Food Bank Usage (PEI)", "see foodbankspei.ca",
        source="Food Banks PEI", tier=3))
    return {"tiers": {
        "tier2": {"label": "Secondary Systems",  "indicators": t2},
        "tier3": {"label": "Contextual Signals", "indicators": t3},
    }}


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    print(f"[PII Scraper v2] {GENERATED}")
    sectors  = {}
    scrapers = [
        ("energy",             "Energy",               scrape_energy),
        ("water",              "Water",                scrape_water),
        ("health",             "Health",               scrape_health),
        ("transport_logistics","Transport & Logistics", scrape_transport),
        ("financial",          "Financial",            scrape_financial),
        ("public_safety",      "Public Safety",        scrape_public_safety),
        ("environment",        "Environment",          scrape_environment),
        ("food",               "Food & Prices",        scrape_food),
    ]
    for key, label, fn in scrapers:
        print(f"  Scraping {label}…")
        try:
            sectors[key]          = fn()
            sectors[key]["label"] = label
        except Exception:
            print(f"  [ERROR] {label}:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            sectors[key] = {"label": label, "tiers": {"tier1": {
                "label": "Critical Infrastructure",
                "indicators": [indicator(f"{label} data", "scraper error",
                               status="error", tier=1, note="Scraper error — check logs")]
            }}}

    output = {"generated": GENERATED, "timestamp": GENERATED,
              "source": "PEI Infrastructure Intelligence v2", "sectors": sectors}

    for p in [Path(f"pii_data_{DATESTAMP}.json"), Path("pii_data_latest.json")]:
        p.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Wrote {p}")
    print("[PII Scraper v2] Done.")


if __name__ == "__main__":
    run()
