"""
PEI Infrastructure Intelligence — Scraper
==========================================
Mirrors the TII (Toronto Infrastructure Intelligence) data structure:
  { sectors: { <sector>: { tiers: { tier1: { indicators: [...] } } } } }

Sectors: energy, water, health, transport_logistics, financial, public_safety, environment, food
Tiers:
  tier1 — critical infrastructure (real-time / authoritative)
  tier2 — secondary systems (near real-time / official feeds)
  tier3 — contextual signals (weekly / manually curated)

PEI-specific data sources:
  - Maritime Electric (grid status, outages)
  - Environment and Climate Change Canada (weather, air quality)
  - Health PEI / CIHI
  - Confederation Bridge / Northumberland Ferries
  - PEI Open Data Portal
  - Statistics Canada
  - poweroutage.com (outage aggregation)

Run:   python pii_scraper.py
Output: pii_data_YYYYMMDD.json  +  pii_data_latest.json
"""

import json
import re
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PEI-Infrastructure-Intelligence/1.0; "
        "+https://github.com/Sargjones/pei-infrastructure)"
    )
}
TIMEOUT = 18
TODAY   = datetime.now(timezone.utc)
DATESTAMP = TODAY.strftime("%Y%m%d")
GENERATED = TODAY.strftime("%Y-%m-%d %H:%M UTC")

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def get(url, **kw):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, **kw)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"  [WARN] GET {url} — {e}", file=sys.stderr)
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
    return indicator(label, value, status="manual", source=source, tier=tier, note=note)


# ---------------------------------------------------------------------------
# SECTOR: ENERGY
# ---------------------------------------------------------------------------

def scrape_energy():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── Maritime Electric Grid Status ────────────────────────────────────
    # maritimeelectric.com/outages/outages/grid-status/ has a public status page.
    # The page uses dynamic JS widgets; we scrape the static text signals.
    grid_url = "https://www.maritimeelectric.com/outages/outages/grid-status/"
    s = soup(grid_url)
    grid_status = "ok"
    grid_note   = ""
    grid_value  = "Normal"
    if s:
        text = s.get_text(" ", strip=True).lower()
        if "load shedding" in text or "rotating outage" in text:
            grid_status = "alert"
            grid_value  = "Load Shedding"
            grid_note   = "Rotating outages active — reduce non-essential consumption"
        elif "conservation" in text or "conservation request" in text:
            grid_status = "warn"
            grid_value  = "Conservation"
            grid_note   = "Maritime Electric requesting voluntary conservation"
        elif "caution" in text or "elevated" in text:
            grid_status = "warn"
            grid_value  = "Elevated demand"
    indicators_t1.append(indicator(
        "Grid Status Index", grid_value, status=grid_status,
        note=grid_note,
        context="maritimeelectric.com/outages/outages/grid-status",
        source="Maritime Electric",
        tier=1
    ))

    # ── Outage customers ────────────────────────────────────────────────
    # poweroutage.com aggregates Maritime Electric outage data
    outage_url = "https://poweroutage.com/ca/utility/1370"
    s2 = soup(outage_url)
    outage_customers = None
    outage_status = "ok"
    if s2:
        # Look for customer count pattern
        text2 = s2.get_text(" ", strip=True)
        m = re.search(r"([\d,]+)\s+(?:customers?|cust)", text2, re.I)
        if m:
            outage_customers = m.group(1)
            num = int(outage_customers.replace(",", ""))
            if num > 5000:  outage_status = "alert"
            elif num > 500: outage_status = "warn"
    if outage_customers is None:
        indicators_t1.append(manual(
            "Outage Customers", "check maritimeelectric.com",
            source="Maritime Electric outage map", tier=1
        ))
    else:
        indicators_t1.append(indicator(
            "Outage Customers", outage_customers,
            unit="customers affected",
            status=outage_status,
            source="poweroutage.com / Maritime Electric",
            tier=1,
            date=GENERATED,
        ))

    # ── Summerside Electric Utility ──────────────────────────────────────
    # Summerside has its own municipal utility (~9,000 customers).
    # No public real-time API — manual tier
    indicators_t2.append(manual(
        "Summerside Utility Status", "see summerside.ca/electric",
        source="City of Summerside Electric Utility", tier=2
    ))

    # ── Wind Generation (PEI has significant installed wind) ─────────────
    # No real-time PEI-specific wind API; proxy via IESO-like data is not
    # available for Maritimes. Use Environment Canada wind speed at North Cape
    # as a generation proxy.
    wx_url = "https://dd.weather.gc.ca/observations/xml/PE/hourly/PE_hourly_e.xml"
    r_wx = get(wx_url)
    wind_speed = None
    wind_station = "North Cape"
    if r_wx:
        try:
            wx_soup = BeautifulSoup(r_wx.text, "xml")
            # Find North Cape or Tignish station
            for stn in wx_soup.find_all("station"):
                name = (stn.get("name") or "").lower()
                if "north cape" in name or "tignish" in name or "summerside" in name:
                    ws = stn.find("wind_speed")
                    if ws and ws.text.strip():
                        wind_speed = float(ws.text.strip())
                        wind_station = stn.get("name", "PEI station")
                        break
        except Exception:
            pass

    if wind_speed is not None:
        wind_note = ""
        wind_status = "ok"
        if wind_speed > 90:
            wind_status = "warn"
            wind_note = "Bridge closure threshold may be reached"
        elif wind_speed > 120:
            wind_status = "alert"
            wind_note = "Extreme winds — bridge likely closed"
        indicators_t2.append(indicator(
            "Wind Speed (NW PEI)", f"{wind_speed:.0f}",
            unit="km/h", status=wind_status, note=wind_note,
            context=f"Station: {wind_station}",
            source="Environment Canada", tier=2, date=GENERATED
        ))
    else:
        indicators_t2.append(manual(
            "Wind Speed (NW PEI)", "unavailable",
            source="Environment Canada", tier=2
        ))

    # ── Heating oil / propane context ────────────────────────────────────
    # PEI has high proportion of oil and propane heat — energy poverty indicator.
    # No real-time PEI-specific price feed; use weekly manual tier.
    indicators_t3.append(manual(
        "Heating Oil Price", "check nrcan.gc.ca/energy/prices",
        source="NRCan / local suppliers", tier=3,
        note="PEI relies heavily on heating oil (~35% of households)"
    ))

    indicators_t3.append(manual(
        "Net Zero Initiatives", "see netzeronavigatorpei.com",
        source="GPEI Environment, Energy and Climate Action", tier=3
    ))

    return {
        "tiers": {
            "tier1": {"label": "Critical Infrastructure", "indicators": indicators_t1},
            "tier2": {"label": "Secondary Systems",       "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals",      "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# SECTOR: WATER
# ---------------------------------------------------------------------------

def scrape_water():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── PEI groundwater notes ────────────────────────────────────────────
    # PEI is unique in Canada — 100% groundwater for drinking water.
    # No real-time public API for aquifer levels.
    # Check PEI Open Data for any water-related datasets.
    boil_url = "https://www.princeedwardisland.ca/en/feature/boil-water-advisories"
    s = soup(boil_url)
    boil_count  = 0
    boil_status = "ok"
    boil_note   = ""
    if s:
        text = s.get_text(" ", strip=True)
        m = re.search(r"(\d+)\s+(?:active|current)\s+(?:boil.water|advisory)", text, re.I)
        if m:
            boil_count  = int(m.group(1))
            if boil_count > 0:
                boil_status = "warn"
                boil_note   = f"{boil_count} active advisory/advisories — check GPEI site"
            if boil_count > 5:
                boil_status = "alert"
        # Also look for any list items
        items = s.find_all("li")
        if not m and items:
            advisory_items = [i for i in items if "boil" in i.get_text().lower() or "advisory" in i.get_text().lower()]
            if advisory_items:
                boil_count  = len(advisory_items)
                boil_status = "warn"
                boil_note   = f"{boil_count} advisory location(s)"

    indicators_t1.append(indicator(
        "Boil Water Advisories", str(boil_count) if boil_count else "0",
        unit="active", status=boil_status, note=boil_note,
        source="GPEI Environment, Energy and Climate Action",
        context="100% groundwater province",
        tier=1, date=GENERATED
    ))

    # ── Wastewater / central water systems ──────────────────────────────
    indicators_t2.append(manual(
        "Central Water Systems", "see GPEI EECA",
        source="GPEI Environment, Energy and Climate Action", tier=2,
        note="~50% of residents on central water; remainder on private wells"
    ))

    # ── Nitrate contamination risk ────────────────────────────────────────
    # Agricultural runoff (potato farming) is PEI's primary water quality threat.
    indicators_t3.append(manual(
        "Nitrate Risk (Agricultural)", "ongoing monitoring",
        source="GPEI Environment, Energy and Climate Action", tier=3,
        note="Potato agriculture — primary source of nitrate contamination in aquifers"
    ))

    indicators_t3.append(manual(
        "Coastal Water Quality", "see PEI Open Data",
        source="data.princeedwardisland.ca", tier=3
    ))

    return {
        "tiers": {
            "tier1": {"label": "Critical Infrastructure", "indicators": indicators_t1},
            "tier2": {"label": "Secondary Systems",       "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals",      "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# SECTOR: HEALTH
# ---------------------------------------------------------------------------

def scrape_health():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── Environment Canada AQHI ──────────────────────────────────────────
    aqhi_url = "https://dd.weather.gc.ca/air_quality/aqhi/atl/observation/realtime/xml/AQ_OBS_PE_CURRENT.xml"
    r = get(aqhi_url)
    aqhi_val    = None
    aqhi_status = "ok"
    aqhi_note   = ""
    if r:
        try:
            aq_soup = BeautifulSoup(r.text, "xml")
            val_tag = aq_soup.find("aqhi") or aq_soup.find("AQHI")
            if val_tag:
                aqhi_val = float(val_tag.text.strip())
                if aqhi_val >= 7:
                    aqhi_status = "alert"
                    aqhi_note   = "High risk — reduce outdoor exertion"
                elif aqhi_val >= 4:
                    aqhi_status = "warn"
                    aqhi_note   = "Moderate risk for sensitive groups"
        except Exception:
            pass

    if aqhi_val is not None:
        indicators_t1.append(indicator(
            "AQHI — Charlottetown", f"{aqhi_val:.0f}",
            unit="/10", status=aqhi_status, note=aqhi_note,
            source="Environment and Climate Change Canada",
            tier=1, date=GENERATED
        ))
    else:
        indicators_t1.append(manual(
            "AQHI — Charlottetown", "see weather.gc.ca",
            source="Environment and Climate Change Canada", tier=1
        ))

    # ── QEH Emergency department ──────────────────────────────────────────
    # Health PEI does not publish real-time ED wait times publicly.
    indicators_t1.append(manual(
        "QEH ED Wait Time", "see Health PEI",
        source="Health PEI — Queen Elizabeth Hospital", tier=1,
        note="No public real-time ED feed — check healthpei.ca"
    ))

    # ── Hospital capacity ────────────────────────────────────────────────
    indicators_t2.append(manual(
        "QEH Capacity", "see healthpei.ca",
        source="Health PEI", tier=2
    ))

    indicators_t2.append(manual(
        "Prince County Hospital", "see healthpei.ca",
        source="Health PEI — Summerside", tier=2
    ))

    # ── Respiratory illness / flu season ────────────────────────────────
    flu_url = "https://www.princeedwardisland.ca/en/information/health-pei/respiratory-illness-surveillance"
    s = soup(flu_url)
    flu_status = "ok"
    flu_note   = ""
    flu_value  = "Normal"
    if s:
        text = s.get_text(" ", strip=True).lower()
        if "elevated" in text or "increased activity" in text:
            flu_status = "warn"
            flu_value  = "Elevated activity"
            flu_note   = "Respiratory illness activity above seasonal baseline"
        elif "high" in text and "activity" in text:
            flu_status = "alert"
            flu_value  = "High activity"

    indicators_t2.append(indicator(
        "Respiratory Illness Activity", flu_value,
        status=flu_status, note=flu_note,
        source="Health PEI",
        tier=2, date=GENERATED
    ))

    # ── Physician access ─────────────────────────────────────────────────
    indicators_t3.append(manual(
        "Patients Without a Doctor", "see Health PEI / CFPC",
        source="Health PEI", tier=3,
        note="PEI has ongoing primary care access challenges — lowest physician ratio in Canada"
    ))

    return {
        "tiers": {
            "tier1": {"label": "Critical Infrastructure", "indicators": indicators_t1},
            "tier2": {"label": "Secondary Systems",       "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals",      "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# SECTOR: TRANSPORT & LOGISTICS
# ---------------------------------------------------------------------------

def scrape_transport():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── Confederation Bridge Status ──────────────────────────────────────
    bridge_url = "https://www.confederationbridge.com/bridge-conditions"
    s = soup(bridge_url)
    bridge_status = "ok"
    bridge_value  = "Open"
    bridge_note   = ""
    if s:
        text = s.get_text(" ", strip=True).lower()
        if "closed" in text:
            bridge_status = "alert"
            bridge_value  = "CLOSED"
            bridge_note   = "Confederation Bridge closed — check confederationbridge.com"
        elif "restriction" in text or "caution" in text or "commercial" in text:
            bridge_status = "warn"
            bridge_value  = "Restrictions"
            bridge_note   = "Vehicle restrictions in effect — check confederationbridge.com"
        elif "one-lane" in text or "one lane" in text:
            bridge_status = "warn"
            bridge_value  = "One-lane"

    indicators_t1.append(indicator(
        "Confederation Bridge", bridge_value,
        status=bridge_status, note=bridge_note,
        context="12.9 km fixed link to NB",
        source="confederationbridge.com",
        tier=1, date=GENERATED,
        banner="Confederation Bridge closed — primary land supply route affected" if bridge_status == "alert" else ""
    ))

    # ── Environment Canada wind at bridge ────────────────────────────────
    # Bridge closure threshold is typically wind gusts > 90 km/h for high-sided vehicles
    # We use the Borden-Carleton station (bridge approach) from ECCC hourly obs
    wx_url = "https://dd.weather.gc.ca/observations/xml/PE/hourly/PE_hourly_e.xml"
    r_wx = get(wx_url)
    borden_wind = None
    if r_wx:
        try:
            wx_soup = BeautifulSoup(r_wx.text, "xml")
            for stn in wx_soup.find_all("station"):
                name = (stn.get("name") or "").lower()
                if "borden" in name or "carleton" in name or "charlottetown" in name:
                    ws = stn.find("wind_gust") or stn.find("wind_speed")
                    if ws and ws.text.strip():
                        borden_wind = float(ws.text.strip())
                        break
        except Exception:
            pass

    if borden_wind is not None:
        gust_status = "ok"
        gust_note   = ""
        if borden_wind > 90:
            gust_status = "warn"
            gust_note   = "High-sided vehicle restrictions may apply"
        if borden_wind > 110:
            gust_status = "alert"
            gust_note   = "Bridge may be closed to all traffic"
        indicators_t2.append(indicator(
            "Wind Gust (Bridge Area)", f"{borden_wind:.0f}",
            unit="km/h", status=gust_status, note=gust_note,
            source="Environment Canada", tier=2, date=GENERATED
        ))
    else:
        indicators_t2.append(manual(
            "Wind Gust (Bridge Area)", "see weather.gc.ca",
            source="Environment Canada", tier=2
        ))

    # ── Northumberland Ferries ───────────────────────────────────────────
    ferry_url = "https://www.ferries.ca/northumberland/schedule/"
    s2 = soup(ferry_url)
    ferry_status = "ok"
    ferry_value  = "Check schedule"
    if s2:
        text2 = s2.get_text(" ", strip=True).lower()
        month_name = TODAY.strftime("%B").lower()
        # Ferries run May–December typically
        if TODAY.month in range(5, 13):
            ferry_value  = "In Season"
            ferry_status = "ok"
            if "cancel" in text2 or "suspend" in text2:
                ferry_status = "warn"
                ferry_value  = "Disruption"
        else:
            ferry_value  = "Off Season"
            ferry_status = "manual"

    indicators_t2.append(indicator(
        "Northumberland Ferry", ferry_value,
        unit="Wood Islands ↔ Pictou",
        status=ferry_status,
        context="Seasonal alternate crossing (May–Dec)",
        source="ferries.ca", tier=2, date=GENERATED
    ))

    # ── Charlottetown Airport ────────────────────────────────────────────
    # No real-time PEI airport delay API; use FlightAware status via manual
    indicators_t2.append(manual(
        "Charlottetown Airport (YYG)", "see flightaware.com/CYYG",
        source="Charlottetown Airport Authority", tier=2
    ))

    # ── Road conditions ──────────────────────────────────────────────────
    road_url = "https://www.princeedwardisland.ca/en/feature/road-conditions"
    s3 = soup(road_url)
    road_status = "ok"
    road_value  = "Normal"
    if s3:
        text3 = s3.get_text(" ", strip=True).lower()
        if "closed" in text3:
            road_status = "warn"
            road_value  = "Closures reported"
        elif "ice" in text3 or "snow" in text3 or "blowing" in text3:
            road_status = "warn"
            road_value  = "Winter conditions"

    indicators_t2.append(indicator(
        "Island Road Conditions", road_value,
        status=road_status,
        source="GPEI Transportation and Infrastructure",
        tier=2, date=GENERATED
    ))

    # ── Supply chain context ─────────────────────────────────────────────
    indicators_t3.append(manual(
        "Bridge Commercial Traffic", "see confederationbridge.com",
        source="Strait Crossing Bridge Ltd", tier=3,
        note="~1.5M vehicles annually — primary supply corridor for food, fuel, goods"
    ))

    return {
        "tiers": {
            "tier1": {"label": "Critical Infrastructure", "indicators": indicators_t1},
            "tier2": {"label": "Secondary Systems",       "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals",      "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# SECTOR: FINANCIAL
# ---------------------------------------------------------------------------

def scrape_financial():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── CAD/USD exchange rate ─────────────────────────────────────────────
    fx_url = "https://api.exchangerate-api.com/v4/latest/USD"
    r = get(fx_url)
    cad_rate    = None
    cad_status  = "ok"
    cad_note    = ""
    if r:
        try:
            data = r.json()
            cad_rate = data["rates"].get("CAD")
        except Exception:
            pass

    if cad_rate:
        if cad_rate > 1.45:
            cad_status = "warn"
            cad_note   = "Weak loonie — imported goods more expensive"
        if cad_rate > 1.50:
            cad_status = "alert"
            cad_note   = "Very weak loonie — significant import cost pressure"
        indicators_t1.append(indicator(
            "CAD/USD", f"{cad_rate:.4f}",
            unit="CAD per USD", status=cad_status, note=cad_note,
            source="exchangerate-api.com", tier=1, date=GENERATED
        ))
    else:
        indicators_t1.append(manual(
            "CAD/USD", "unavailable", source="Bank of Canada", tier=1
        ))

    # ── Inflation (CPI) ──────────────────────────────────────────────────
    # Statistics Canada CPI — no real-time API, monthly release.
    indicators_t2.append(manual(
        "PEI CPI (YoY)", "see statcan.gc.ca table 18-10-0004",
        source="Statistics Canada", tier=2,
        note="Monthly release — PEI CPI tracks island-specific price levels"
    ))

    # ── PEI housing market ───────────────────────────────────────────────
    indicators_t2.append(manual(
        "Housing Vacancy Rate", "see CMHC Housing Market",
        source="CMHC", tier=2,
        note="Charlottetown among lowest vacancy rates in Atlantic Canada"
    ))

    # ── Lobster price ────────────────────────────────────────────────────
    # Lobster is PEI's primary export commodity — a leading economic indicator.
    indicators_t3.append(manual(
        "Lobster Ex-Vessel Price", "see PEI Fisheries",
        source="GPEI Fisheries and Communities", tier=3,
        note="PEI lobster — primary export commodity; spring/fall seasons"
    ))

    # ── Tourism indicators ───────────────────────────────────────────────
    indicators_t3.append(manual(
        "Tourism Activity", "see tourismpei.com",
        source="Tourism PEI", tier=3,
        note="PEI tourism contributes ~$500M annually; highly seasonal (June–Sept)"
    ))

    return {
        "tiers": {
            "tier1": {"label": "Critical Infrastructure", "indicators": indicators_t1},
            "tier2": {"label": "Secondary Systems",       "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals",      "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# SECTOR: PUBLIC SAFETY
# ---------------------------------------------------------------------------

def scrape_public_safety():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── Environment Canada weather alerts ────────────────────────────────
    alert_url = "https://dd.weather.gc.ca/alerts/cap/today/ATLANTIC_CAP.tar"
    # Use the ATOM feed for PEI instead (more accessible)
    atom_url = "https://weather.gc.ca/rss/warning/pe_e.xml"
    r = get(atom_url)
    wx_alert_count  = 0
    wx_alert_status = "ok"
    wx_alert_text   = "None active"
    if r:
        try:
            atom_soup = BeautifulSoup(r.text, "xml")
            entries   = atom_soup.find_all("entry")
            active    = [e for e in entries if e.find("title") and
                         any(kw in (e.find("title").text or "").lower()
                             for kw in ["warning","watch","statement","advisory"])]
            wx_alert_count = len(active)
            if wx_alert_count > 0:
                wx_alert_status = "warn"
                first = active[0].find("title").text.strip()
                wx_alert_text   = first[:60] + ("…" if len(first) > 60 else "")
            if any("warning" in (e.find("title").text or "").lower() for e in active):
                wx_alert_status = "alert"
        except Exception:
            pass

    indicators_t1.append(indicator(
        "Weather Alerts (PEI)", str(wx_alert_count) if wx_alert_count else "0",
        unit="active alerts",
        status=wx_alert_status,
        note=wx_alert_text if wx_alert_count > 0 else "",
        context="ECCC public alerting",
        source="Environment and Climate Change Canada",
        tier=1, date=GENERATED,
        banner=f"Weather warning active: {wx_alert_text}" if wx_alert_status == "alert" else ""
    ))

    # ── PEI EMO ──────────────────────────────────────────────────────────
    emo_url = "https://www.princeedwardisland.ca/en/topic/emergency-management"
    s = soup(emo_url)
    emo_status = "ok"
    emo_value  = "Normal"
    emo_note   = ""
    if s:
        text = s.get_text(" ", strip=True).lower()
        if "state of emergency" in text:
            emo_status = "alert"
            emo_value  = "State of Emergency"
            emo_note   = "Provincial state of emergency declared"
        elif "level 2" in text or "elevated" in text:
            emo_status = "warn"
            emo_value  = "Elevated"

    indicators_t1.append(indicator(
        "PEI EMO Status", emo_value,
        status=emo_status, note=emo_note,
        source="PEI Emergency Measures Organization",
        tier=1, date=GENERATED
    ))

    # ── Coast Guard / Search and Rescue ──────────────────────────────────
    indicators_t2.append(manual(
        "Coast Guard (Maritime Region)", "see dfo-mpo.gc.ca",
        source="Canadian Coast Guard — Maritimes Region", tier=2,
        note="PEI surrounded by Northumberland Strait, Gulf of St. Lawrence, Gulf waters"
    ))

    # ── RCMP PEI ─────────────────────────────────────────────────────────
    indicators_t2.append(manual(
        "RCMP PEI — Public Notices", "see rcmp-grc.gc.ca/pe",
        source="RCMP L Division", tier=2
    ))

    # ── Coastal erosion / flood risk ─────────────────────────────────────
    indicators_t3.append(manual(
        "Coastal Erosion Status", "see GPEI Environment",
        source="GPEI Environment, Energy and Climate Action", tier=3,
        note="PEI: highest coastal erosion rates in Canada (~0.3m/yr avg). Storm surge risk."
    ))

    # ── Storm surge ──────────────────────────────────────────────────────
    indicators_t3.append(manual(
        "Storm Surge Risk", "see wateroffice.ec.gc.ca",
        source="Environment Canada / Water Survey of Canada", tier=3,
        note="Low-lying coastal areas at risk; Charlottetown harbour tide gauge"
    ))

    return {
        "tiers": {
            "tier1": {"label": "Critical Infrastructure", "indicators": indicators_t1},
            "tier2": {"label": "Secondary Systems",       "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals",      "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# SECTOR: ENVIRONMENT
# ---------------------------------------------------------------------------

def scrape_environment():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── Current conditions — Charlottetown ───────────────────────────────
    wx_url = "https://dd.weather.gc.ca/observations/xml/PE/hourly/PE_hourly_e.xml"
    r = get(wx_url)
    temp = None
    conditions = None
    wx_station = "Charlottetown"
    if r:
        try:
            wx_soup = BeautifulSoup(r.text, "xml")
            for stn in wx_soup.find_all("station"):
                name = (stn.get("name") or "").lower()
                if "charlottetown" in name:
                    t = stn.find("temperature")
                    c = stn.find("condition") or stn.find("presentWeather")
                    if t and t.text.strip():
                        temp = float(t.text.strip())
                    if c and c.text.strip():
                        conditions = c.text.strip()
                    break
        except Exception:
            pass

    if temp is not None:
        temp_status = "ok"
        temp_note   = ""
        if temp <= -20:
            temp_status = "warn"
            temp_note   = "Extreme cold — elevated heating demand and exposure risk"
        elif temp >= 32:
            temp_status = "warn"
            temp_note   = "Heat advisory conditions"
        indicators_t1.append(indicator(
            "Temperature (Charlottetown)", f"{temp:.1f}",
            unit="°C", status=temp_status, note=temp_note,
            context=conditions or "",
            source="Environment Canada",
            tier=1, date=GENERATED
        ))
    else:
        indicators_t1.append(manual(
            "Temperature (Charlottetown)", "see weather.gc.ca",
            source="Environment Canada", tier=1
        ))

    # ── Gulf of St. Lawrence sea surface temperature ──────────────────────
    # Warmer Gulf waters intensify Atlantic storms affecting PEI
    indicators_t2.append(manual(
        "Gulf SST Anomaly", "see dfo-mpo.gc.ca",
        source="DFO / Bedford Institute of Oceanography", tier=2,
        note="Warm Gulf SST amplifies storm systems and contributes to coastal erosion"
    ))

    # ── Snowpack / drought ───────────────────────────────────────────────
    indicators_t2.append(manual(
        "Drought Conditions", "see agr.gc.ca/drought-watch",
        source="Agriculture and Agri-Food Canada", tier=2,
        note="Drought affects potato crop yields — PEI's primary agricultural commodity"
    ))

    # ── Wildfire risk ────────────────────────────────────────────────────
    indicators_t3.append(manual(
        "Fire Weather Index (PEI)", "see cwfis.cfs.nrcan.gc.ca",
        source="Canadian Wildland Fire Information System", tier=3
    ))

    return {
        "tiers": {
            "tier1": {"label": "Critical Infrastructure", "indicators": indicators_t1},
            "tier2": {"label": "Secondary Systems",       "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals",      "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# SECTOR: FOOD & PRICES
# ---------------------------------------------------------------------------

def scrape_food():
    indicators_t1, indicators_t2, indicators_t3 = [], [], []

    # ── Grocery price index ───────────────────────────────────────────────
    # No real-time PEI-specific grocery API; use manual
    indicators_t2.append(manual(
        "Grocery Price Index (PEI)", "see statcan.gc.ca / GPEI",
        source="Statistics Canada", tier=2,
        note="Island logistics premium: all non-local food crosses the Confederation Bridge"
    ))

    # ── Lobster / seafood landings ────────────────────────────────────────
    indicators_t2.append(manual(
        "Lobster Landings", "see GPEI Fisheries",
        source="GPEI Fisheries and Communities", tier=2,
        note="Spring season: late April–June. Fall season: August–October. Key economic driver."
    ))

    # ── Potato crop status ───────────────────────────────────────────────
    indicators_t2.append(manual(
        "Potato Crop Status", "see GPEI Agriculture",
        source="GPEI Agriculture", tier=2,
        note="~100,000 acres of potato production annually — PEI's dominant crop"
    ))

    # ── Food bank demand ─────────────────────────────────────────────────
    indicators_t3.append(manual(
        "Food Bank Usage (PEI)", "see foodbankspei.ca",
        source="Food Banks PEI", tier=3
    ))

    return {
        "tiers": {
            "tier2": {"label": "Secondary Systems",  "indicators": indicators_t2},
            "tier3": {"label": "Contextual Signals", "indicators": indicators_t3},
        }
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    print(f"[PII Scraper] {GENERATED}")

    sectors = {}

    scrapers = [
        ("energy",             "Energy",              scrape_energy),
        ("water",              "Water",               scrape_water),
        ("health",             "Health",              scrape_health),
        ("transport_logistics","Transport & Logistics",scrape_transport),
        ("financial",          "Financial",           scrape_financial),
        ("public_safety",      "Public Safety",       scrape_public_safety),
        ("environment",        "Environment",         scrape_environment),
        ("food",               "Food & Prices",       scrape_food),
    ]

    for key, label, fn in scrapers:
        print(f"  Scraping {label}…")
        try:
            sectors[key] = fn()
            sectors[key]["label"] = label
        except Exception:
            print(f"  [ERROR] {label} sector failed:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            sectors[key] = {
                "label": label,
                "tiers": {
                    "tier1": {"label": "Critical Infrastructure", "indicators": [
                        indicator(f"{label} data", "scraper error",
                                  status="error", tier=1,
                                  note="Scraper encountered an error — check logs")
                    ]}
                }
            }

    output = {
        "generated":  GENERATED,
        "timestamp":  GENERATED,
        "source":     "PEI Infrastructure Intelligence",
        "sectors":    sectors
    }

    dated_path  = Path(f"pii_data_{DATESTAMP}.json")
    latest_path = Path("pii_data_latest.json")

    for p in [dated_path, latest_path]:
        p.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Wrote {p}")

    print("[PII Scraper] Done.")


if __name__ == "__main__":
    run()
