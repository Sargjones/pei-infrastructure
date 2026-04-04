"""
PEI Infrastructure Intelligence — Scraper  v2
==============================================
Mirrors TII data structure:
  { sectors: { <sector>: { tiers: { tier1: { indicators:[...] } } } } }

New in v2:
  - GPEI/Maritime Electric live MW feed (energy generation mix)
  - Open-Meteo for Charlottetown weather + bridge-area wind gusts (no API key)
  - Bank of Canada Valet API for CAD/USD
  - AviationWeather.gov METAR for Charlottetown Airport (CYYG)
  - Statistics Canada Valet API for PEI CPI
  - CMHC API for Charlottetown housing vacancy
  - Fixed Grid Status false-positive (keyword to live status extraction)
  - Environment Canada AQHI XML feed
  - ECCC weather alerts RSS for PEI

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

def fetch_gpei_energy():
    """POST to GPEI workflow API — Maritime Electric 15-min feed."""
    r = post_json("https://wdf.princeedwardisland.ca/prod/workflow",
                  {"featureName": "WindEnergy"})
    if not r:
        return None
    try:
        data   = r.json()
        comps  = data.get("components", [])
        result = {}
        for comp in comps:
            d = comp.get("data") or comp.get("fields") or comp
            if not isinstance(d, dict):
                continue
            for key, val in d.items():
                lk = key.lower()
                try:
                    fval = float(str(val).replace(",", ""))
                except (ValueError, TypeError):
                    if "time" in lk or "update" in lk:
                        result["update_time"] = str(val)
                    continue
                if   "load" in lk and "on" in lk:              result["load"]   = fval
                elif "wind" in lk and ("gen" in lk or "total" in lk): result["wind"] = fval
                elif "solar" in lk and "gen" in lk:            result["solar"]  = fval
                elif "fossil" in lk or ("fuel" in lk and "fossil" in lk): result["fossil"] = fval
                elif "time" in lk or "update" in lk:           result["update_time"] = str(val)
        # flat fallback
        if not result:
            for key, val in (data if isinstance(data, dict) else {}).items():
                lk = key.lower()
                try:
                    fval = float(str(val).replace(",", ""))
                except (ValueError, TypeError):
                    continue
                if   "load"   in lk: result["load"]   = fval
                elif "wind"   in lk: result["wind"]   = fval
                elif "solar"  in lk: result["solar"]  = fval
                elif "fossil" in lk: result["fossil"] = fval
        return result or None
    except Exception as e:
        print(f"  [WARN] GPEI parse — {e}", file=sys.stderr)
        return None


# ── SECTOR: ENERGY ───────────────────────────────────────────────────────────

def scrape_energy():
    t1, t2, t3 = [], [], []

    grid = fetch_gpei_energy()
    if grid and "load" in grid:
        load   = grid.get("load",   0.0)
        wind   = grid.get("wind",   0.0)
        solar  = grid.get("solar",  0.0)
        fossil = grid.get("fossil", 0.0)
        nb     = max(0.0, load - wind - solar - fossil)
        util   = round((nb / CABLE_CAP_MW) * 100, 1)
        upd    = grid.get("update_time", GENERATED)

        t1.append(indicator("Island System Load", f"{load:.1f}", unit="MW",
            status="alert" if load >= 380 else "warn" if load >= 300 else "ok",
            note=(f"Extreme demand — load shedding risk. Record ~{PEAK_RECORD_MW} MW" if load >= 380
                  else "Demand at or above cable import cap — on-island generation required" if load >= 300
                  else ""),
            context=f"Peak record: ~{PEAK_RECORD_MW} MW (Jan/Feb)",
            source="Maritime Electric via GPEI", tier=1, date=upd))

        t1.append(indicator("NB Cable Utilization", f"{util}",
            unit=f"% of {CABLE_CAP_MW} MW cap  ({nb:.0f} MW imported)",
            status="alert" if util >= 95 else "warn" if util >= 80 else "ok",
            note=("Cables at capacity — load shedding imminent" if util >= 95
                  else "High cable utilization — grid stress elevated" if util >= 80 else ""),
            context="4 subsea cables — hard physical limit",
            source="Maritime Electric via GPEI", tier=1, date=upd,
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

    # Grid Status Index (fixed: live status text only)
    s = soup("https://www.maritimeelectric.com/outages/outages/grid-status/")
    gsi_val, gsi_status, gsi_note = "Normal", "ok", ""
    if s:
        page = s.get_text(" ", strip=True)
        m = re.search(
            r"(?:current\s+status|grid\s+status\s+index)[:\s]+([A-Za-z][A-Za-z\s]{2,40}?)(?:\.|,|\n|$)",
            page, re.I)
        if m:
            live = m.group(1).strip().lower()
            if "load shed" in live or "rotating" in live:
                gsi_val, gsi_status = "Load Shedding", "alert"
                gsi_note = "Rotating outages active — reduce non-essential consumption now"
            elif "elevated" in live:
                gsi_val, gsi_status = "Elevated Concern", "warn"
                gsi_note = "Grid under stress — conserve energy, especially 6–10am and 4–9pm"
            elif "conservation" in live:
                gsi_val, gsi_status = "Conservation Request", "warn"
                gsi_note = "Maritime Electric requesting voluntary conservation"
        elif grid and grid.get("load", 0) >= 380:
            gsi_val, gsi_status, gsi_note = "Critical Load", "alert", f"Load {grid['load']:.0f} MW"
        elif grid and grid.get("load", 0) >= 300:
            gsi_val, gsi_status, gsi_note = "High Demand", "warn", f"Load {grid['load']:.0f} MW at cable cap"

    t1.append(indicator("Grid Status Index", gsi_val, status=gsi_status, note=gsi_note,
        context="maritimeelectric.com/outages/outages/grid-status",
        source="Maritime Electric", tier=1,
        banner="LOAD SHEDDING ACTIVE — rotating outages in progress" if gsi_status == "alert" else ""))

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

    t1.append(manual("QEH ED Wait Time", "see healthpei.ca",
        source="Health PEI — Queen Elizabeth Hospital", tier=1,
        note="No public real-time ED feed — Health PEI does not publish wait times"))

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
    t2.append(manual("QEH Capacity", "see healthpei.ca", source="Health PEI", tier=2))
    t2.append(manual("Prince County Hospital", "see healthpei.ca",
        source="Health PEI — Summerside", tier=2))
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

    t2.append(manual("Coast Guard (Maritime Region)", "see dfo-mpo.gc.ca",
        source="Canadian Coast Guard — Maritimes Region", tier=2,
        note="PEI surrounded by Northumberland Strait, Gulf of St. Lawrence"))
    t2.append(manual("RCMP PEI — Public Notices", "see rcmp-grc.gc.ca/pe",
        source="RCMP L Division", tier=2))
    t3.append(manual("Coastal Erosion Status", "see GPEI Environment",
        source="GPEI Environment, Energy and Climate Action", tier=3,
        note="PEI: highest coastal erosion rates in Canada (~0.3m/yr avg). Storm surge risk."))
    t3.append(manual("Storm Surge Risk", "see wateroffice.ec.gc.ca",
        source="Environment Canada / Water Survey of Canada", tier=3,
        note="Low-lying coastal areas at risk; Charlottetown harbour tide gauge"))

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

    t2.append(manual("Gulf SST Anomaly", "see dfo-mpo.gc.ca",
        source="DFO / Bedford Institute of Oceanography", tier=2,
        note="Warm Gulf SST amplifies storm systems and coastal erosion"))
    t2.append(manual("Drought Conditions", "see agr.gc.ca/drought-watch",
        source="Agriculture and Agri-Food Canada", tier=2,
        note="Drought affects potato crop yields — PEI's primary commodity"))
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
