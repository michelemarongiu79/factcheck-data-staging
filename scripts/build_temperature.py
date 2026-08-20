#!/usr/bin/env python3
"""
Temi: Temperature e Precipitazioni in Italia (1940-2024), una linea per regione.
Dati MENSILI: il sito calcola la media sui mesi selezionati dall'utente.

Un'unica chiamata giornaliera per regione a Open-Meteo restituisce sia le
temperature (media/max/min) sia la pioggia giornaliera: le precipitazioni
sono quindi "gratis" in termini di richieste HTTP (stesso numero di
chiamate, stesso rischio di rate-limit di prima) — per questo i due temi
condividono questo script invece di duplicare il fetch.

Fonte: Open-Meteo — archivio storico (rianalisi ERA5, dal 1940), senza API key.
Cache dei dati grezzi in data/raw/temp_daily/ (non persiste tra i run in CI).

Esecuzione:  python scripts/build_temperature.py
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "site" / "data" / "temperature_regioni.json"
OUTPUT_PRECIP = ROOT / "site" / "data" / "precipitazioni_regioni.json"
# Cache dei dati GREZZI giornalieri (una volta scaricati, non si riscarica mai piu':
# qualsiasi calcolo — media, picco, minimo, stagionali... — si deriva da qui in locale).
RAW = ROOT / "data" / "raw" / "temp_daily"

ANNO_INIZIO, ANNO_FINE = 1940, 2024
ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; factcheck/1.0)"}
# Variabili GIORNALIERE grezze scaricate da Open-Meteo (una per chiave).
VARIABILI = {
    "media": "temperature_2m_mean", "max": "temperature_2m_max", "min": "temperature_2m_min",
    "precipitazione": "precipitation_sum",
}
# Serie MENSILI derivate: chiave pubblica -> (variabile grezza, aggregazione sui giorni del mese).
# Per le temperature ogni variante ha una propria variabile grezza; per le
# precipitazioni media e picco derivano invece dalla STESSA serie giornaliera
# (il totale piovuto in un giorno), solo aggregata in modo diverso.
SERIE_MENSILI = {
    "media": ("media", "mean"), "max": ("max", "max"), "min": ("min", "min"),
    "precip_media": ("precipitazione", "mean"), "precip_picco": ("precipitazione", "max"),
}

REGIONI = [
    ("Piemonte", 45.07, 7.69, "#1e3a8a"), ("Valle d'Aosta", 45.74, 7.32, "#1d4ed8"),
    ("Lombardia", 45.46, 9.19, "#2563eb"), ("Trentino-Alto Adige", 46.07, 11.12, "#3b82f6"),
    ("Veneto", 45.44, 12.32, "#60a5fa"), ("Friuli-Venezia Giulia", 45.65, 13.78, "#0891b2"),
    ("Liguria", 44.41, 8.93, "#0e7490"), ("Emilia-Romagna", 44.49, 11.34, "#155e75"),
    ("Toscana", 43.77, 11.25, "#059669"), ("Umbria", 43.11, 12.39, "#10b981"),
    ("Marche", 43.62, 13.51, "#84cc16"), ("Lazio", 41.89, 12.48, "#f59e0b"),
    ("Abruzzo", 42.35, 13.40, "#f97316"), ("Molise", 41.56, 14.66, "#ea580c"),
    ("Campania", 40.85, 14.27, "#fb7185"), ("Puglia", 41.12, 16.87, "#f43f5e"),
    ("Basilicata", 40.64, 15.81, "#e11d48"), ("Calabria", 38.91, 16.59, "#be123c"),
    ("Sicilia", 38.12, 13.36, "#9f1239"), ("Sardegna", 39.22, 9.12, "#7f1d1d"),
]


def scarica_daily(lat: float, lon: float) -> dict:
    """Scarica i dati GIORNALIERI grezzi (media/max/min) da Open-Meteo."""
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": f"{ANNO_INIZIO}-01-01", "end_date": f"{ANNO_FINE}-12-31",
        "daily": ",".join(VARIABILI.values()), "timezone": "Europe/Rome",
    }
    resp = None
    for tentativo in range(6):
        resp = requests.get(ARCHIVE, params=params, headers=HEADERS, timeout=180)
        if resp.status_code == 429:
            attesa = 20 * (tentativo + 1)
            print(f"      429, attendo {attesa}s...")
            time.sleep(attesa)
            continue
        break
    resp.raise_for_status()
    d = resp.json()["daily"]
    return {"time": d["time"], **{var: d[col] for var, col in VARIABILI.items()}}


def daily_cache(nome: str, lat: float, lon: float) -> dict:
    """Dati giornalieri grezzi dalla cache; scarica solo se non ci sono ancora."""
    cf = RAW / f"{nome}.json"
    if cf.exists():
        return json.loads(cf.read_text(encoding="utf-8"))
    d = scarica_daily(lat, lon)
    RAW.mkdir(parents=True, exist_ok=True)
    cf.write_text(json.dumps(d), encoding="utf-8")
    time.sleep(8)   # spaziatura anti rate-limit solo dopo un download reale
    return d


def mensili_da_daily(daily: dict) -> dict:
    """Deriva {chiave: {anno(str): [12 valori]}} dai dati giornalieri grezzi, una
    chiave per ogni voce di SERIE_MENSILI (media/max/min temperatura, media/picco
    precipitazione). Per le chiavi con aggregazione max/min salva anche il GIORNO
    esatto in cui si e' verificato il valore, sotto '<chiave>_giorno' (stessa forma,
    ma stringhe YYYY-MM-DD invece di numeri) — permette di rispondere a "quando
    esattamente e' successo il picco?" cliccando sul grafico."""
    df = pd.DataFrame({"date": pd.to_datetime(daily["time"])})
    for var in VARIABILI:
        df[var] = daily[var]
    df["y"] = df["date"].dt.year
    df["m"] = df["date"].dt.month
    out = {}
    for chiave, (var_grezza, agg) in SERIE_MENSILI.items():
        sotto = df.dropna(subset=[var_grezza])
        g = sotto.groupby(["y", "m"])[var_grezza].agg(agg)
        per_anno: dict[str, list] = {}
        for (y, m), v in g.items():
            per_anno.setdefault(str(int(y)), [None] * 12)[int(m) - 1] = round(float(v), 2)
        out[chiave] = per_anno

        if agg in ("max", "min"):
            idx_fn = "idxmax" if agg == "max" else "idxmin"
            gi = sotto.groupby(["y", "m"])[var_grezza].agg(idx_fn)
            giorni: dict[str, list] = {}
            for (y, m), riga in gi.items():
                giorni.setdefault(str(int(y)), [None] * 12)[int(m) - 1] = sotto.loc[riga, "date"].strftime("%Y-%m-%d")
            out[chiave + "_giorno"] = giorni
    return out


def main() -> int:
    print(f">>> Temperature + precipitazioni MENSILI {ANNO_INIZIO}-{ANNO_FINE} per {len(REGIONI)} regioni...")
    anni = list(range(ANNO_INIZIO, ANNO_FINE + 1))

    risultati: dict[str, dict] = {}
    da_fare = list(REGIONI)
    for passata in range(6):
        ancora = []
        for reg in da_fare:
            nome, lat, lon, _ = reg
            try:
                risultati[nome] = mensili_da_daily(daily_cache(nome, lat, lon))
                print(f"    {nome} ok")
            except Exception as e:
                print(f"    {nome} rimandato ({type(e).__name__})")
                ancora.append(reg)
        da_fare = ancora
        if not da_fare:
            break
        print(f">>> passata {passata + 1}: mancano {len(da_fare)}, attendo 60s...")
        time.sleep(60)

    if da_fare:
        print("ERRORE: regioni non completate:", [r[0] for r in da_fare], file=sys.stderr)
        return 1

    scrivi(risultati, anni)
    print(">>> FATTO.")
    return 0


def _serie_regioni(risultati: dict, anni: list, chiavi: dict, chiavi_giorno: dict | None = None) -> list:
    """Costruisce l'array 'serie' (una voce per regione) leggendo da risultati[nome]
    solo le chiavi indicate. chiavi = {chiave_pubblica: chiave_in_risultati}.
    chiavi_giorno (opzionale) = stesse chiavi pubbliche ma per il giorno esatto del
    picco (solo per le varianti con aggregazione max/min, vedi mensili_da_daily)."""
    serie = []
    for nome, lat, lon, colore in REGIONI:
        if nome not in risultati:
            continue   # regione non ancora disponibile (anteprima parziale)
        data = risultati[nome]
        mensili = {pub: [data[interna].get(str(a), [None] * 12) for a in anni] for pub, interna in chiavi.items()}
        voce = {"key": nome, "label": nome, "colore": colore, "mensili": mensili}
        if chiavi_giorno:
            voce["mensili_giorni"] = {
                pub: [data[interna + "_giorno"].get(str(a), [None] * 12) for a in anni]
                for pub, interna in chiavi_giorno.items()
            }
        serie.append(voce)
    return serie


def scrivi(risultati: dict, anni: list) -> None:
    scrivi_temperature(risultati, anni)
    scrivi_precipitazioni(risultati, anni)


def scrivi_temperature(risultati: dict, anni: list) -> None:
    serie = _serie_regioni(risultati, anni, {"media": "media", "max": "max", "min": "min"},
                            chiavi_giorno={"max": "max", "min": "min"})

    meta = {
        "id": "temperature",
        "titolo": f"Temperature in Italia: il trend regione per regione ({ANNO_INIZIO}-{ANNO_FINE})",
        "descrizione": (f"La temperatura media annua dal {ANNO_INIZIO} al {ANNO_FINE} in ogni regione "
                        "italiana (capoluogo). Scegli media/massima/minima e i mesi; usa la legenda, "
                        "la mappa o il pulsante per isolare le regioni."),
        "nota": ("Dati di rianalisi meteo (ERA5), riferiti al capoluogo di regione e non alla rete di "
                 "stazioni ufficiale: ottimi per l'andamento generale, non per il singolo valore."),
        "fonti": [{"nome": f"Temperatura giornaliera {ANNO_INIZIO}-{ANNO_FINE} (media/max/min, capoluoghi)",
                   "ente": "Open-Meteo — archivio storico (rianalisi ERA5 di ECMWF)",
                   "serie": "temperature_2m_mean / _max / _min", "url": "https://open-meteo.com/"}],
        "trasformazioni": ["Per ogni mese: Media = media giornaliera; Massima = picco (massimo assoluto) del mese; Minima = minimo assoluto del mese.",
                           "Sui mesi selezionati si applica la stessa logica (media dei mesi, o picco/minimo tra i mesi).",
                           "Fonte di rianalisi meteo (ERA5), riferita al capoluogo di regione."],
        "generato_il": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }
    config = {
        "viste": ["reali"], "asse0_label": "°C", "freq": "A",
        "periodo_default": 0, "decimali": 1, "legenda_verticale": True,
        "mappa": "regioni_italia", "mesi": True,
        # agg = come si combinano i mesi selezionati: media=media, max=picco, min=minimo
        "varianti": [{"key": "media", "label": "Media", "agg": "mean"},
                     {"key": "max", "label": "Massima (picco)", "agg": "max", "cerca": "ondata di caldo"},
                     {"key": "min", "label": "Minima", "agg": "min", "cerca": "gelo ondata di freddo"}],
        "variante_default": "media", "caption": {"tipo": "media_trend"},
        "contesto": {
            "titolo": "Perché l'andamento non è lineare (1940-2024)",
            "testo": (
                "Le temperature massime italiane non salgono in linea retta: restano relativamente "
                "stabili dal 1940 al 1980 (media regionale: da 22,3°C a 21,1°C, quindi in leggero "
                "calo), per poi salire con decisione fino a 25,6°C nel 2024. Le temperature minime, "
                "nello stesso periodo 1940-1980, si muovono nella direzione opposta (da 2,4°C a "
                "4,1°C, in aumento).\n\n"
                "Questa asimmetria giorno/notte è la firma di un fenomeno documentato a livello "
                "globale: fino agli anni '70-'80 la combustione di carburanti fossili ad alto "
                "contenuto di zolfo, senza filtri, ha immesso in atmosfera grandi quantità di "
                "aerosol solfatici, che riflettono la luce solare e rendono le nubi più riflettenti "
                "(«global dimming»), abbassando soprattutto le temperature massime diurne — mentre "
                "l'accumulo di gas serra continuava comunque a scaldare le notti.\n\n"
                "Con le normative anti-inquinamento degli anni '70-'80 (piogge acide, qualità "
                "dell'aria), gli aerosol sono diminuiti, l'atmosfera si è «schiarita» e l'effetto "
                "riscaldante dei gas serra è emerso pienamente anche di giorno: da qui la risalita "
                "ripida delle massime dagli anni '80 a oggi."
            ),
            "fonti": [
                {"nome": "Aerosol pollution caused decades of \"global dimming\"",
                 "ente": "American Geophysical Union (AGU)",
                 "url": "https://news.agu.org/press-release/aerosol-pollution-caused-decades-of-global-dimming/"},
                {"nome": "Sixth Assessment Report (AR6), Working Group I — Physical Science Basis",
                 "ente": "IPCC — Intergovernmental Panel on Climate Change",
                 "url": "https://www.ipcc.ch/report/ar6/wg1/"},
            ],
        },
    }
    payload = {"meta": meta, "config": config,
               "date": [f"{a}-01-01" for a in anni], "serie": serie}
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f">>> Scritto {OUTPUT.name} ({len(anni)} anni, {len(serie)} regioni, mensile)")


def scrivi_precipitazioni(risultati: dict, anni: list) -> None:
    serie = _serie_regioni(risultati, anni, {"media": "precip_media", "picco": "precip_picco"},
                            chiavi_giorno={"picco": "precip_picco"})

    meta = {
        "id": "precipitazioni",
        "titolo": f"Piogge in Italia: quanto piove, regione per regione ({ANNO_INIZIO}-{ANNO_FINE})",
        "descrizione": (f"Precipitazioni giornaliere dal {ANNO_INIZIO} al {ANNO_FINE} in ogni regione "
                        "italiana (capoluogo): la media giornaliera racconta quanto piove di solito, "
                        "il picco (il giorno più piovoso) quanto sono intense le piogge estreme "
                        "(«bombe d'acqua»). Scegli la variante e i mesi; usa la legenda o la mappa "
                        "per isolare le regioni."),
        "nota": ("Dati di rianalisi meteo (ERA5), riferiti al capoluogo di regione e non alla rete di "
                 "stazioni ufficiale: ottimi per l'andamento generale, non per il singolo evento locale "
                 "(un nubifragio puo' colpire un punto della regione lontano dal capoluogo)."),
        "fonti": [{"nome": f"Precipitazione giornaliera {ANNO_INIZIO}-{ANNO_FINE} (capoluoghi)",
                   "ente": "Open-Meteo — archivio storico (rianalisi ERA5 di ECMWF)",
                   "serie": "precipitation_sum", "url": "https://open-meteo.com/"}],
        "trasformazioni": ["Per ogni mese: Media = media della pioggia giornaliera; Picco = il giorno più piovoso del mese.",
                           "Sui mesi selezionati si applica la stessa logica (media dei mesi, o picco tra i mesi).",
                           "Fonte di rianalisi meteo (ERA5), riferita al capoluogo di regione."],
        "generato_il": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }
    config = {
        "viste": ["reali"], "asse0_label": "mm", "freq": "A",
        "periodo_default": 0, "decimali": 1, "legenda_verticale": True,
        "mappa": "regioni_italia", "mesi": True,
        "varianti": [{"key": "media", "label": "Media giornaliera", "agg": "mean"},
                     {"key": "picco", "label": "Picco (giorno più piovoso)", "agg": "max",
                      "cerca": "alluvione nubifragio maltempo"}],
        "variante_default": "media", "caption": {"tipo": "precip_estremi"},
    }
    payload = {"meta": meta, "config": config,
               "date": [f"{a}-01-01" for a in anni], "serie": serie}
    OUTPUT_PRECIP.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PRECIP.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f">>> Scritto {OUTPUT_PRECIP.name} ({len(anni)} anni, {len(serie)} regioni, mensile)")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except requests.RequestException as e:
        print(f"ERRORE di rete: {e}", file=sys.stderr)
        sys.exit(1)
