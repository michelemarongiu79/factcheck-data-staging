#!/usr/bin/env python3
"""
Tema: Prezzi di benzina e gasolio per regione, storico dal 2015.

Fonte: MIMIT, archivio storico dei prezzi comunicati dai singoli distributori
("prezzo_alle_8", un CSV al giorno per trimestre, dal 2015). Non esiste un dato
regionale gia' pronto e storico: lo ricaviamo noi, per ogni giorno, facendo la
media dei prezzi self-service di benzina/gasolio degli impianti di ogni regione
(impianto -> provincia dall'anagrafica ATTUALE -> regione da una tabella fissa),
poi riportiamo il tutto a cadenza settimanale (come il tema "Petrolio vs
carburanti alla pompa", che pero' e' un dato nazionale).

Limite noto: usiamo l'anagrafica ATTUALE (impianti oggi attivi) anche per gli
anni passati, quindi gli impianti chiusi nel frattempo non vengono geolocalizzati
e le loro righe di prezzo storiche sono scartate. Per una media regionale questo
e' un'approssimazione accettabile (dichiarata nella nota del tema), non un errore
sistematico in una direzione particolare.

Peso dei dati: ogni trimestre e' un tar.gz di ~40-110 MB con ~90 CSV giornalieri
(uno al giorno). Scaricare ed elaborare tutta la storia (2015-oggi, ~46 trimestri)
in un colpo solo rischia di essere troppo lento per un singolo run della pipeline:
lo script quindi elabora i trimestri in ordine cronologico entro un budget di
tempo per esecuzione, salva il progresso (dati grezzi giorno/regione/carburante)
in data/carburanti_regioni_progress.json (committato, NON in data/raw/) e riprende
da li' al lancio successivo. Rilancia piu' volte la pipeline finche' il tema non
copre tutta la storia.

Esecuzione:  python scripts/build_carburanti_regioni.py
"""

from __future__ import annotations

import io
import json
import re
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "site" / "data" / "carburanti_regioni.json"
PROGRESS = ROOT / "data" / "carburanti_regioni_progress.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; factcheck/1.0)"}

PAGINA_ARCHIVIO = "https://www.mimit.gov.it/it/open-data/elenco-dataset/carburanti-archivio-prezzi"
ANAGRAFICA_URL = "https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv"

FUEL_OK = {"Benzina", "Gasolio"}
BUDGET_SECONDI = 18 * 60   # tempo massimo di download/elaborazione per lancio

# Provincia (sigla) -> regione. Include anche le sigle sarde pre-riforma 2016
# (CI, VS, OG, OT), che possono comparire nei dati piu' vecchi.
PROVINCIA_REGIONE: dict[str, str] = {}
_MAPPA = {
    "Piemonte": "TO VC NO CN AT AL BI VB",
    "Valle d'Aosta": "AO",
    "Lombardia": "MI BG BS PV CR MN LC LO SO VA CO MB",
    "Trentino-Alto Adige": "TN BZ",
    "Veneto": "VR VI BL TV VE PD RO",
    "Friuli-Venezia Giulia": "UD GO TS PN",
    "Liguria": "GE IM SP SV",
    "Emilia-Romagna": "PC PR RE MO BO FE RA FC RN",
    "Toscana": "MS LU PT FI LI PI AR SI GR PO",
    "Umbria": "PG TR",
    "Marche": "PU AN MC AP FM",
    "Lazio": "VT RI RM LT FR",
    "Abruzzo": "AQ TE PE CH",
    "Molise": "CB IS",
    "Campania": "CE BN NA AV SA",
    "Puglia": "FG BA TA BR LE BT",
    "Basilicata": "PZ MT",
    "Calabria": "CS CZ RC KR VV",
    "Sicilia": "TP PA ME AG CL EN CT RG SR",
    "Sardegna": "SS NU CA OR SU CI VS OG OT",
}
for _regione, _sigle in _MAPPA.items():
    for _s in _sigle.split():
        PROVINCIA_REGIONE[_s] = _regione


def _get(url: str, **kw) -> requests.Response:
    return requests.get(url, headers=HEADERS, timeout=180, **kw)


def _chiave_trimestre(url: str) -> tuple[int, int]:
    m = re.search(r"/(\d{4})_([1-4])_tr\.tar\.gz$", url)
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)


def trova_trimestri() -> list[str]:
    """URL dei trimestri 'prezzo_alle_8', in ordine cronologico crescente."""
    r = _get(PAGINA_ARCHIVIO)
    r.raise_for_status()
    href = re.findall(r'href="([^"]+)"', r.text, flags=re.I)
    link = [h for h in href if "/prezzo_alle_8/" in h and h.lower().endswith(".tar.gz")]
    assoluti = {h if h.startswith("http") else "https://www.mimit.gov.it" + h for h in link}
    return sorted(assoluti, key=_chiave_trimestre)


def carica_anagrafica() -> dict[int, str]:
    """idImpianto -> sigla provincia, dall'anagrafica ATTUALE (vedi limite in testa al file)."""
    r = _get(ANAGRAFICA_URL)
    r.raise_for_status()
    testo = r.content.decode("utf-8", errors="replace")
    righe = testo.splitlines()
    if righe and righe[0].lower().startswith("estrazione"):
        righe = righe[1:]
    df = pd.read_csv(io.StringIO("\n".join(righe)), sep="|", dtype=str, on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]
    df = df.dropna(subset=["idImpianto", "Provincia"])
    df["idImpianto"] = pd.to_numeric(df["idImpianto"], errors="coerce")
    df = df.dropna(subset=["idImpianto"])
    return dict(zip(df["idImpianto"].astype(int), df["Provincia"].str.strip().str.upper()))


def _nome_a_data(nome: str) -> str | None:
    m = re.search(r"(\d{4})(\d{2})(\d{2})", nome)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def _data_a_trimestre(data_iso: str) -> tuple[int, int]:
    anno, mese = int(data_iso[:4]), int(data_iso[5:7])
    return (anno, (mese - 1) // 3 + 1)


def _parse_prezzi(testo: str) -> pd.DataFrame | None:
    righe = testo.splitlines()
    if righe and righe[0].lower().startswith("estrazione"):
        righe = righe[1:]
    if not righe:
        return None
    header = righe[0]
    sep = ";" if ";" in header else ("|" if "|" in header else ",")
    try:
        df = pd.read_csv(io.StringIO("\n".join(righe)), sep=sep, dtype=str, on_bad_lines="skip")
    except Exception:
        return None
    df.columns = [c.strip() for c in df.columns]
    if not {"idImpianto", "descCarburante", "prezzo", "isSelf"}.issubset(df.columns):
        return None
    df = df[df["descCarburante"].isin(FUEL_OK) & (df["isSelf"].astype(str).str.strip() == "1")].copy()
    if df.empty:
        return None
    df["idImpianto"] = pd.to_numeric(df["idImpianto"], errors="coerce")
    df["prezzo"] = pd.to_numeric(df["prezzo"], errors="coerce")
    return df.dropna(subset=["idImpianto", "prezzo"])[["idImpianto", "descCarburante", "prezzo"]]


def elabora_trimestre(url: str, provincia_di: dict[int, str]) -> list[dict]:
    """Scarica in streaming un trimestre e ne ricava righe {data, regione, fuel, prezzo, n}."""
    righe_out: list[dict] = []
    t0 = time.time()
    resp = _get(url, stream=True)
    resp.raise_for_status()
    tf = tarfile.open(fileobj=resp.raw, mode="r|gz")
    n_file = 0
    for member in tf:
        if not member.isfile():
            continue
        data = _nome_a_data(member.name)
        if not data:
            continue
        f = tf.extractfile(member)
        if f is None:
            continue
        raw = f.read()
        try:
            testo = raw.decode("utf-8")
        except UnicodeDecodeError:
            testo = raw.decode("latin-1", errors="replace")
        df = _parse_prezzi(testo)
        n_file += 1
        if n_file % 15 == 0:
            print(f"      ...{n_file} giorni, {time.time() - t0:.0f}s trascorsi", flush=True)
        if df is None or df.empty:
            continue
        df["provincia"] = df["idImpianto"].map(provincia_di)
        df = df.dropna(subset=["provincia"])
        df["regione"] = df["provincia"].map(PROVINCIA_REGIONE)
        df = df.dropna(subset=["regione"])
        if df.empty:
            continue
        g = df.groupby(["regione", "descCarburante"])["prezzo"].agg(["mean", "count"])
        for (regione, fuel), row in g.iterrows():
            righe_out.append({"data": data, "regione": regione, "fuel": fuel,
                               "prezzo": round(float(row["mean"]), 4), "n": int(row["count"])})
    tf.close()
    resp.close()
    print(f"    {url.rsplit('/', 1)[-1]}: {n_file} giorni, {len(righe_out)} righe regione/carburante")
    return righe_out


def carica_progresso() -> dict:
    if PROGRESS.exists():
        return json.loads(PROGRESS.read_text(encoding="utf-8"))
    return {"ultimo_trimestre": None, "righe": []}


def salva_progresso(stato: dict) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(stato, ensure_ascii=False), encoding="utf-8")


def scrivi_json(righe: list[dict]) -> None:
    """Da righe lunghe {data,regione,fuel,prezzo} a JSON del tema: una serie per
    regione, con valori settimanali per variante (benzina/gasolio)."""
    df = pd.DataFrame(righe)
    df["data"] = pd.to_datetime(df["data"])

    regioni = sorted(df["regione"].unique())
    colori = [
        "#1e3a8a", "#1d4ed8", "#2563eb", "#3b82f6", "#60a5fa", "#0891b2", "#0e7490",
        "#155e75", "#059669", "#10b981", "#84cc16", "#f59e0b", "#f97316", "#ea580c",
        "#fb7185", "#f43f5e", "#e11d48", "#be123c", "#9f1239", "#7f1d1d",
    ]

    date_settimanali = None
    serie = []
    for i, regione in enumerate(regioni):
        sotto = df[df["regione"] == regione]
        varianti_valori: dict[str, list] = {}
        for fuel_key, fuel_label in (("benzina", "Benzina"), ("gasolio", "Gasolio")):
            s = sotto[sotto["fuel"] == fuel_label].set_index("data")["prezzo"]
            if s.empty:
                continue
            settimanale = s.resample("W-MON", label="left", closed="left").mean()
            if date_settimanali is None:
                date_settimanali = settimanale.index
            else:
                settimanale = settimanale.reindex(date_settimanali)
            varianti_valori[fuel_key] = [None if pd.isna(v) else round(float(v), 4) for v in settimanale]
        if varianti_valori:
            serie.append({"key": regione, "label": regione, "colore": colori[i % len(colori)],
                          "valori_varianti": varianti_valori})

    if date_settimanali is None:
        raise RuntimeError("Nessun dato aggregabile: controlla le righe grezze.")

    meta = {
        "id": "carburanti_regioni",
        "titolo": "Benzina e gasolio: quanto costano in ogni regione (dal 2015)",
        "descrizione": ("Prezzo medio settimanale di benzina e gasolio (self-service) in ogni "
                        "regione italiana, dal 2015 a oggi, ricavato dai prezzi comunicati dai "
                        "singoli distributori al MIMIT."),
        "nota": ("Media calcolata sugli impianti oggi attivi (anagrafica più recente), applicata "
                 "anche agli anni passati: gli impianti chiusi nel frattempo non sono conteggiati "
                 "nella loro regione per gli anni in cui erano attivi. Solo prezzo self-service."),
        "fonti": [{"nome": "Prezzi carburanti praticati dai distributori (archivio storico dal 2015)",
                   "ente": "MIMIT — Osservatorio Prezzi Carburanti",
                   "serie": "prezzo_alle_8 (self-service, Benzina/Gasolio)",
                   "url": "https://www.mimit.gov.it/it/open-data/elenco-dataset/carburanti-archivio-prezzi"}],
        "trasformazioni": [
            "Ogni impianto è assegnato a una regione tramite provincia (anagrafica attuale).",
            "Per ogni giorno: media dei prezzi self-service degli impianti di ogni regione.",
            "Riportato a cadenza settimanale (lunedì): media dei giorni disponibili della settimana.",
        ],
    }
    config = {
        "viste": ["reali"], "asse0_label": "€ / litro", "freq": "W",
        "periodo_default": 3, "decimali": 3, "legenda_verticale": True,
        "varianti": [{"key": "benzina", "label": "Benzina"}, {"key": "gasolio", "label": "Gasolio"}],
        "variante_default": "benzina",
        "caption": {"tipo": "regioni_estremi"},
    }
    payload = {"meta": meta, "config": config,
               "date": [d.strftime("%Y-%m-%d") for d in date_settimanali], "serie": serie}
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f">>> Scritto {OUTPUT.name} ({len(date_settimanali)} settimane, {len(serie)} regioni)")


def main() -> int:
    inizio = time.time()
    print(">>> Trovo i trimestri disponibili...")
    trimestri = trova_trimestri()
    if not trimestri:
        print("ERRORE: nessun trimestre trovato (pagina cambiata?).", file=sys.stderr)
        return 1
    print(f">>> {len(trimestri)} trimestri, dal {trimestri[0].rsplit('/', 1)[-1]} "
          f"al {trimestri[-1].rsplit('/', 1)[-1]}")

    stato = carica_progresso()
    ultimo = tuple(stato["ultimo_trimestre"]) if stato.get("ultimo_trimestre") else None
    da_fare = [u for u in trimestri if ultimo is None or _chiave_trimestre(u) >= ultimo]
    print(f">>> Da processare: {len(da_fare)} trimestri (ripresa da "
          f"{stato.get('ultimo_trimestre') or 'inizio storia'})")

    if ultimo is not None:
        # Il trimestre di ripresa viene rifatto (potrebbe essere stato interrotto
        # a meta'): scartiamo le sue righe gia' salvate prima di rielaborarlo.
        stato["righe"] = [r for r in stato["righe"] if _data_a_trimestre(r["data"]) < ultimo]

    print(">>> Carico l'anagrafica impianti -> provincia...")
    provincia_di = carica_anagrafica()
    print(f">>> {len(provincia_di)} impianti mappati a provincia")

    processati = 0
    for url in da_fare:
        if time.time() - inizio > BUDGET_SECONDI and processati > 0:
            print(f">>> Budget di tempo esaurito, mi fermo qui (ripartirò dal prossimo trimestre).")
            break
        chiave = _chiave_trimestre(url)
        print(f">>> Trimestre {chiave[0]}_T{chiave[1]}...")
        righe_trimestre = elabora_trimestre(url, provincia_di)
        stato["righe"].extend(righe_trimestre)
        stato["ultimo_trimestre"] = list(chiave)
        salva_progresso(stato)
        processati += 1

    print(f">>> Trimestri processati in questo run: {processati}")
    if not stato["righe"]:
        print("ERRORE: nessuna riga raccolta.", file=sys.stderr)
        return 1

    scrivi_json(stato["righe"])
    completo = da_fare and processati == len(da_fare)
    print(">>> STORICO COMPLETO." if completo else ">>> Storico parziale: rilancia la pipeline per proseguire.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except requests.RequestException as e:
        print(f"ERRORE di rete: {e}", file=sys.stderr)
        sys.exit(1)
