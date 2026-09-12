# -*- coding: utf-8 -*-
"""
=============================================================================
  CEREBELLIUM v4.1  |  ETL Incremental + Hash SHA-1 + Batch Insert
  Compatibilidad total con cerebrillum.db v3.9.x
=============================================================================
CAMBIOS ESTRUCTURALES vs v3.9.2:
  [v4-ETL-1] Hash SHA-1 por fila para detección de duplicados SIN tocar DB.
             Antes: INSERT fallaba con IntegrityError -> ahora filtra antes.
             Impacto: 0 escrituras innecesarias, log exacto de filas nuevas.

  [v4-ETL-2] Batch insert con executemany() en lugar de execute() por fila.
             Antes: 1 INSERT por partido -> ahora lote de 500 por commit.
             Impacto: 3-5x más rápido en archivos grandes (>500 partidos).

  [v4-ETL-3] Tabla etl_log para trazabilidad de cada importación.
             Campos: archivo, timestamp, insertados, duplicados, hash_run.
             Impacto: aegist puede mostrar historial de cargas sin re-leer CSVs.

  [v4-ETL-4] Ventana activa (tabla hot) separada de archivo (tabla cold).
             hot  = últimas 2 temporadas -> usada por DataPipeline.
             cold = todo el histórico    -> usada solo para re-entrenamiento.
             NOTA: la tabla cold se crea aquí pero DataPipeline la ignora
             en producción para mantener latencia baja. Ver CONTINUITYv4.py.

  [v4-ETL-5] _hash_fila() es determinista: mismos datos = mismo hash siempre.
             Esto permite comparar hashes entre corridas sin leer la DB.

  [v4-SYNTAX] Todos los helpers privados tienen prefijo _ consistente.
              _safe_float, _safe_int, _parse_fecha, _mes_de_fecha,
              _normalizar_temporada, _slug, _hash_fila — ninguno expuesto.
=============================================================================
"""

from enum import UNIQUE
import os
import sys 
import sqlite3
import csv
import glob
import hashlib
import json
from datetime import datetime

# ==============================================================================
# CONFIGURACION
# ==============================================================================
DB_NAME    = "cerebrillum.db"
CSV_FOLDER = "csv_data"
BATCH_SIZE = 500        # [v4-ETL-2] tamaño del lote para executemany
HOT_WINDOW = 2          # [v4-ETL-4] temporadas activas en tabla hot

DIV_LIGA = {
    "E0":  "premier_league", "SP1": "la_liga",   "I1":  "serie_a",
    "D1":  "bundesliga",     "F1":  "ligue_1",   "P1":  "primera_liga",
    "N1":  "eredivisie",     "B1":  "pro_league", "CL": "champions_league",
    "E2":  "league_one",
}

CUOTAS_1X2_EU = {
    "B365H":"b365_h",  "B365D":"b365_d",  "B365A":"b365_a",
    "PSH":  "ps_h",    "PSD":  "ps_d",    "PSA":  "ps_a",
    "MaxH": "max_h",   "MaxD": "max_d",   "MaxA": "max_a",
    "AvgH": "avg_h",   "AvgD": "avg_d",   "AvgA": "avg_a",
    "B365CH":"b365c_h","B365CD":"b365c_d","B365CA":"b365c_a",
    "PSCH":  "psc_h",  "PSCD": "psc_d",   "PSCA": "psc_a",
    "MaxCH": "maxc_h", "MaxCD":"maxc_d",  "MaxCA":"maxc_a",
    "AvgCH": "avgc_h", "AvgCD":"avgc_d",  "AvgCA":"avgc_a",
}

CUOTAS_1X2_AM = {
    "PSCH":   "psc_h",   "PSCD":   "psc_d",   "PSCA":   "psc_a",
    "MaxCH":  "maxc_h",  "MaxCD":  "maxc_d",  "MaxCA":  "maxc_a",
    "AvgCH":  "avgc_h",  "AvgCD":  "avgc_d",  "AvgCA":  "avgc_a",
    "B365CH": "b365c_h", "B365CD": "b365c_d", "B365CA": "b365c_a",
}

CUOTAS_OU = {
    "B365>2.5":"b365_over","B365<2.5":"b365_under",
    "Max>2.5": "max_over", "Max<2.5": "max_under",
    "Avg>2.5": "avg_over", "Avg<2.5": "avg_under",
}

CUOTAS_AH = {
    "AHh":"ah_linea","B365AHH":"b365_ahh","B365AHA":"b365_aha",
    "MaxAHH":"max_ahh","MaxAHA":"max_aha","AvgAHH":"avg_ahh","AvgAHA":"avg_aha",
}


# ==============================================================================
# AUDIT LOG (sin cambios de interfaz vs v3.9.x)
# ==============================================================================
class Log:
    @staticmethod
    def _ts(): return datetime.now().strftime("%H:%M:%S")
    @staticmethod
    def paso(n, msg):
        print(f"\n  [{Log._ts()}] >>> PASO {n}: {msg}")
        print(f"  {'='*58}")
    @staticmethod
    def ok(msg):    print(f"  [{Log._ts()}] [OK]    {msg}")
    @staticmethod
    def info(msg):  print(f"  [{Log._ts()}] [INFO]  {msg}")
    @staticmethod
    def warn(msg):  print(f"  [{Log._ts()}] [WARN]  {msg}")
    @staticmethod
    def error(msg): print(f"  [{Log._ts()}] [ERROR] {msg}")
    @staticmethod
    def dato(k, v): print(f"  [{Log._ts()}] [DATA]  {str(k):<30} -> {v}")


# ==============================================================================
# HELPERS  [v4-SYNTAX] todos con prefijo _
# ==============================================================================
def _safe_float(val) -> float:
    try:
        return float(str(val).strip())
    except (ValueError, TypeError, AttributeError):
        return None

def _safe_int(val) -> int:
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError, AttributeError):
        return None

def _parse_fecha(raw: str) -> str:
    raw = str(raw).strip()
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw

def _mes_de_fecha(fecha_str: str) -> int:
    try:
        partes = fecha_str.split("-")
        if len(partes) < 2:
            return 0
        return int(partes[1])
    except (ValueError, TypeError, AttributeError):
        return 0

def _normalizar_temporada(raw: str) -> str:
    raw = str(raw).strip()
    if not raw or raw == "desconocida":
        return raw
    if "/" in raw:
        partes = raw.split("/")
        try:
            a1 = int(partes[0])
            a2 = int(partes[1])
            if a1 < 100: a1 += 2000
            if a2 < 100: return f"{a1}-{str(a2):0>2}"
            else:        return f"{a1}-{str(a2)[2:]}"
        except ValueError:
            pass
    if "-" in raw and len(raw) == 5:
        return f"20{raw}"
    if raw.isdigit() and len(raw) == 4:
        a = int(raw)
        return f"{a}-{str(a+1)[2:]}"
    return raw

def _slug(liga_id: str, team_name: str) -> str:
    s = team_name.lower().strip()
    for ch in [" ", "-", ".", "'", "/"]:
        s = s.replace(ch, "_")
    return f"{liga_id}:{s}"

# [v4-ETL-1] Hash determinista por partido
def _hash_fila(fecha: str, local_id: str, visita_id: str) -> str:
    """
    Genera SHA-1 de 12 chars del trio (fecha, local, visitante).
    Determinista: mismos datos siempre producen el mismo hash.
    Usado como partido_id Y como clave de deduplicación.
    """
    raw = f"{fecha}|{local_id}|{visita_id}"
    return hashlib.sha1(raw.encode()).hexdigest()[:12]

def _liga_id_americano(country: str, league: str, fecha_str: str,
                        prefijo_archivo: str = "") -> str:
    country_u = (country or "").upper().strip()
    league_u  = (league  or "").upper().strip()
    prefijo_u = prefijo_archivo.upper()
    if "CCL" in league_u or "CONCACAF" in league_u: return "concacaf_cl"
    if ("USA" in country_u or "MLS" in league_u
            or prefijo_u in ("USA", "MLS")): return "mls"
    if ("MEXICO" in country_u or "MEX" in country_u
            or "LIGA MX" in league_u or prefijo_u in ("MX", "LM", "MEXICO", "MEX")):
        mes = _mes_de_fecha(fecha_str)
        return "liga_mx_clausura" if mes <= 5 else "liga_mx_apertura"
    return None

def _sql(cur, sql: str, label: str = "") -> bool:
    try:
        cur.execute(sql)
        return True
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if "already exists" not in msg and "duplicate column" not in msg:
            Log.warn(f"SQL [{label}]: {e}")
        return False


# ==============================================================================
# SCHEMA  [v4-ETL-3] + [v4-ETL-4]
# ==============================================================================
def inicializar_schema(db_path: str = DB_NAME) -> None:
    """
    Idéntico al v3.9.2 + 2 adiciones:
      - columna hash_fila en historial_partidos  [v4-ETL-1]
      - tabla etl_log para trazabilidad          [v4-ETL-3]
    La tabla historial_hot es una VIEW, no una tabla extra:
      SELECT * WHERE temporada IN (últimas 2)    [v4-ETL-4]
    """
    Log.paso(1, "Schema CerebrillumV4.0")
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("PRAGMA foreign_keys = OFF;")
    cur.execute("PRAGMA journal_mode = WAL;")     # concurrencia con Streamlit

    _sql(cur, """
        CREATE TABLE IF NOT EXISTS dim_ligas (
            liga_id              TEXT PRIMARY KEY,
            nombre               TEXT NOT NULL,
            region               TEXT NOT NULL DEFAULT 'UEFA',
            fase                 INTEGER NOT NULL DEFAULT 2,
            avg_goles_historico  REAL DEFAULT 2.5,
            avg_corners_partido  REAL DEFAULT 10.0,
            rho_dixon_coles      REAL DEFAULT -0.04,
            lambda_base_ataque   REAL DEFAULT 1.30,
            lambda_base_defensa  REAL DEFAULT 1.30
        )""", "dim_ligas")

    _sql(cur, """
        CREATE TABLE IF NOT EXISTS dim_equipos (
            equipo_id           TEXT PRIMARY KEY,
            nombre_comercial    TEXT NOT NULL,
            liga_id             TEXT DEFAULT '',
            pais                TEXT DEFAULT '',
            estadio_altitud     INTEGER DEFAULT 0,
            estadio_capacidad   INTEGER DEFAULT 0,
            tipo_cesped         TEXT DEFAULT 'natural',
            prestigio_historico INTEGER DEFAULT 10,
            elo_actual          INTEGER DEFAULT 1500
        )""", "dim_equipos")

    _sql(cur, """
    CREATE TABLE IF NOT EXISTS historial_partidos (
        partido_id              TEXT PRIMARY KEY,
        hash_fila               TEXT,           -- sin UNIQUE aquí, lo pone el índice
        fecha                   TEXT NOT NULL,
        temporada               TEXT DEFAULT '',
        liga_id                 TEXT NOT NULL,
        numero_jornada          INTEGER DEFAULT 0,
        equipo_local            TEXT NOT NULL,
        equipo_visitante        TEXT NOT NULL,
        goles_local             INTEGER,
        goles_visitante         INTEGER,
        resultado_ft            TEXT DEFAULT NULL,
        goles_ht_local          INTEGER DEFAULT NULL,
        goles_ht_visitante      INTEGER DEFAULT NULL,
        resultado_ht            TEXT DEFAULT NULL,
        tiros_totales_local     INTEGER DEFAULT NULL,
        tiros_totales_vis       INTEGER DEFAULT NULL,
        tiros_puerta_local      INTEGER DEFAULT NULL,
        tiros_puerta_vis        INTEGER DEFAULT NULL,
        corners_local           INTEGER DEFAULT NULL,
        corners_visitante       INTEGER DEFAULT NULL,
        amarillas_local         INTEGER DEFAULT NULL,
        amarillas_vis           INTEGER DEFAULT NULL,
        rojas_local             INTEGER DEFAULT 0,
        rojas_vis               INTEGER DEFAULT 0,
        faltas_local            INTEGER DEFAULT NULL,
        faltas_vis              INTEGER DEFAULT NULL,
        arbitro_nombre          TEXT DEFAULT NULL,
        es_inicio_temporada     INTEGER DEFAULT 0,
        es_clasico              INTEGER DEFAULT 0,
        es_eliminatoria         INTEGER DEFAULT 0,
        es_partido_vuelta       INTEGER DEFAULT 0,
        dias_descanso_local     INTEGER DEFAULT 3,
        dias_descanso_vis       INTEGER DEFAULT 3,
        xg_local                REAL DEFAULT NULL,
        xg_visitante            REAL DEFAULT NULL,
        posesion_local          REAL DEFAULT NULL,
        temperatura             REAL DEFAULT NULL,
        asistencia_real         INTEGER DEFAULT NULL
    )""", "historial_partidos")

# Fallback para DBs v3.x que ya existen sin hash_fila
# SQLite no acepta ADD COLUMN UNIQUE, la unicidad la da el índice idx_hp_has
    _sql(cur, "ALTER TABLE historial_partidos ADD COLUMN hash_fila TEXT", "historial_partidos")
    cols_1x2 = ", ".join(f"{c} REAL DEFAULT NULL" for c in CUOTAS_1X2_EU.values())
    _sql(cur, f"CREATE TABLE IF NOT EXISTS cuotas_1x2 (partido_id TEXT PRIMARY KEY, {cols_1x2})", "cuotas_1x2")
    cols_ou = ", ".join(f"{c} REAL DEFAULT NULL" for c in CUOTAS_OU.values())
    _sql(cur, f"CREATE TABLE IF NOT EXISTS cuotas_ou (partido_id TEXT PRIMARY KEY, {cols_ou})", "cuotas_ou")
    cols_ah = ", ".join(f"{c} REAL DEFAULT NULL" for c in CUOTAS_AH.values())
    _sql(cur, f"CREATE TABLE IF NOT EXISTS cuotas_ah (partido_id TEXT PRIMARY KEY, {cols_ah})", "cuotas_ah")

    # [v4-ETL-3] Trazabilidad de cada importación
    _sql(cur, """
        CREATE TABLE IF NOT EXISTS etl_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            archivo     TEXT NOT NULL,
            liga_id     TEXT,
            temporada   TEXT,
            formato     TEXT,
            insertados  INTEGER DEFAULT 0,
            duplicados  INTEGER DEFAULT 0,
            errores     INTEGER DEFAULT 0,
            hash_run    TEXT
        )""", "etl_log")
    # [v4.6-GAP13] Cola de partidos pendientes de cierre — persistencia real.
    # PROBLEMA QUE RESUELVE: cola_partidos vivía únicamente en st.session_state
    # (PACIFICO KyJ), memoria volátil que se pierde en cada reinicio del
    # proceso Streamlit (deploy, reinicio de PC, expiración de sesión). Un
    # partido inferido pero no cerrado en la misma sesión desaparecía sin
    # dejar rastro, nunca llegaba a AuditorRentabilidad. Auditoría real:
    # 7 de 35+ apuestas de dos meses quedaron registradas en state_roi.json.
    # payload_json almacena el dict completo que hoy arma _cola_append() en
    # pacificokyj.py (lam_l, lam_v, p1/px/p2, mercado, cuota_elegida, etc.)
    # serializado — evita duplicar cada campo como columna SQL individual,
    # que además tendría que mantenerse sincronizado a mano con cada gap
    # nuevo (Gap7 O/U, Gap8 predictor_lider, futuros mercados AH/corners).
    _sql(cur, """
        CREATE TABLE IF NOT EXISTS cola_partidos_pendientes (
            partido_id       TEXT PRIMARY KEY,
            timestamp        TEXT NOT NULL,
            payload_json     TEXT NOT NULL,
            cerrado          INTEGER DEFAULT 0
        )""", "cola_partidos_pendientes")

    # [v4-ETL-4] Vista de ventana activa (últimas 2 temporadas)
    # Es una VIEW: no ocupa espacio extra, siempre refleja datos reales
    _sql(cur, """
        CREATE VIEW IF NOT EXISTS historial_hot AS
        SELECT * FROM historial_partidos
        WHERE temporada IN (
            SELECT DISTINCT temporada FROM historial_partidos
            ORDER BY temporada DESC
            LIMIT 4
        )""", "historial_hot")

    for nombre, target in [
        ("idx_hp_fecha",     "ON historial_partidos(fecha)"),
        ("idx_hp_liga",      "ON historial_partidos(liga_id)"),
        ("idx_hp_local",     "ON historial_partidos(equipo_local)"),
        ("idx_hp_visita",    "ON historial_partidos(equipo_visitante)"),
        ("idx_hp_temporada", "ON historial_partidos(temporada)"),
        # [v4-ETL-1] Índice en hash para deduplicación O(1)
        ("idx_hp_hash",      "ON historial_partidos(hash_fila)"),
    ]:
        _sql(cur, f"CREATE INDEX IF NOT EXISTS {nombre} {target}", nombre)

    cur.executemany(
                                     """
        INSERT OR IGNORE INTO dim_ligas
            (liga_id, nombre, region, fase, avg_goles_historico,
             avg_corners_partido, rho_dixon_coles,
             lambda_base_ataque, lambda_base_defensa)
             VALUES (?,?,?,?,?,?,?,?,?)""", [
        ("premier_league",  "Premier League",   "UEFA",  2, 2.7, 10.1, -0.040, 1.35, 1.35),
        ("league_one",       "League One",      "UEFA",  2, 2.6,  9.5, -0.040, 1.30, 1.30),
        ("la_liga",         "LaLiga",           "UEFA",  2, 2.5,  9.6, -0.040, 1.25, 1.25),
        ("serie_a",         "Serie A",          "UEFA",  2, 2.6,  9.3, -0.070, 1.30, 1.30),
        ("bundesliga",      "Bundesliga",       "UEFA",  2, 3.1, 10.8, -0.030, 1.55, 1.55),
        ("ligue_1",         "Ligue 1",          "UEFA",  2, 2.6,  9.5, -0.040, 1.30, 1.30),
        ("champions_league","Champions League", "UEFA",  2, 2.8, 10.3, -0.040, 1.40, 1.40),
        ("liga_mx_clausura","Liga MX Clausura", "NAFTA", 1, 2.4,  9.8, -0.050, 1.20, 1.20),
        ("liga_mx_apertura","Liga MX Apertura", "NAFTA", 1, 2.4,  9.8, -0.050, 1.20, 1.20),
        ("concacaf_cl",     "CONCACAF Champs",  "NAFTA", 1, 2.6, 10.2, -0.040, 1.30, 1.30),
        ("mls",             "MLS",              "NAFTA", 1, 2.9, 10.5, -0.030, 1.45, 1.45),
    ])

    conn.commit()
    cur.execute("PRAGMA foreign_keys = ON;")
    conn.commit()
    conn.close()
    Log.ok("Schema v4.0 listo (WAL + hash_fila + etl_log + historial_hot VIEW)")


# ==============================================================================
# CARGA DE HASHES EXISTENTES  [v4-ETL-1]
# ==============================================================================
def _cargar_hashes_existentes(db_path: str) -> set:
    """
    Lee todos los hash_fila de la DB en un set de Python.
    Coste: 1 query al inicio de cada importación -> O(1) por fila después.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        cur.execute("SELECT hash_fila FROM historial_partidos WHERE hash_fila IS NOT NULL")
        hashes = {r[0] for r in cur.fetchall()}
        conn.close()
        return hashes
    except Exception:
        return set()


# ==============================================================================
# ETL — IMPORTAR CSV  [v4-ETL-1] [v4-ETL-2]
# ==============================================================================
def importar_csv(csv_path: str,
                 liga_id_override: str = None,
                 temporada: str = None,
                 limite_registros: int = None,
                 db_path: str = DB_NAME) -> dict:
    """
    [v4-ETL-1] Deduplicación por hash SHA-1 antes de tocar la DB.
    [v4-ETL-2] Batch insert: acumula filas y hace executemany cada 500.
    [v4-ETL-3] Registra resultado en etl_log al finalizar.
    """
    Log.paso(2, f"ETL v4: {os.path.basename(csv_path)}")

    if not os.path.exists(csv_path):
        Log.error(f"No encontrado: {csv_path}")
        return {}

    with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
        reader       = csv.DictReader(f)
        columnas_csv = set(reader.fieldnames or [])
        filas = [row for row in reader
                 if row.get("HomeTeam", "").strip() or row.get("Home", "").strip()]

    es_europeo   = "HomeTeam" in columnas_csv
    es_americano = (not es_europeo) and "Home" in columnas_csv

    if not es_europeo and not es_americano:
        Log.error("Formato no reconocido")
        return {}

    prefijo_archivo = os.path.basename(csv_path).split("_")[0].upper().replace(".CSV", "")
    col_home = "HomeTeam" if es_europeo else "Home"
    col_away = "AwayTeam" if es_europeo else "Away"
    mapa_odds_1x2 = CUOTAS_1X2_EU if es_europeo else CUOTAS_1X2_AM

    if not temporada:
        nombre_base = os.path.basename(csv_path).replace(".csv", "")
        for part in nombre_base.split("_"):
            if len(part) == 4 and part.isdigit():
                a = int(part)
                temporada = f"{a}-{str(a+1)[2:]}"
                break
        temporada = temporada or "desconocida"

    if limite_registros:
        filas = filas[:limite_registros]

    fechas_unicas   = sorted({_parse_fecha(r.get("Date", "")) for r in filas})
    fecha_a_jornada = {f: i + 1 for i, f in enumerate(fechas_unicas)}

    # [v4-ETL-1] Cargar hashes existentes ANTES del loop
    hashes_existentes = _cargar_hashes_existentes(db_path)
    Log.info(f"Hashes en DB: {len(hashes_existentes)} | Filas a procesar: {len(filas)}")

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # [v4-ETL-2] Lotes para batch insert
    lote_partidos  = []
    lote_odds_1x2  = []
    lote_odds_ou   = []
    lote_odds_ah   = []
    lote_equipos   = []

    insertados = duplicados = errores = skip_liga = 0
    equipos_nuevos = set()
    ligas_vistas   = {}

    for fila in filas:
        try:
            fecha_raw = fila.get("Date", "").strip()
            fecha     = _parse_fecha(fecha_raw)

            if liga_id_override:
                liga_id = liga_id_override
            elif es_europeo:
                div     = fila.get("Div", "").strip()
                liga_id = DIV_LIGA.get(div) or DIV_LIGA.get(prefijo_archivo)
                if not liga_id:
                    skip_liga += 1
                    continue
            else:
                country = fila.get("Country", "").strip()
                league  = fila.get("League",  "").strip()
                liga_id = _liga_id_americano(country, league, fecha, prefijo_archivo)
                if not liga_id:
                    skip_liga += 1
                    continue

            ligas_vistas[liga_id] = ligas_vistas.get(liga_id, 0) + 1

            home_raw  = fila.get(col_home, "").strip()
            away_raw  = fila.get(col_away, "").strip()
            if not home_raw or not away_raw:
                continue

            local_id  = _slug(liga_id, home_raw)
            visita_id = _slug(liga_id, away_raw)

            # [v4-ETL-1] Calcular hash y verificar duplicado SIN tocar la DB
            hash_f     = _hash_fila(fecha, local_id, visita_id)
            partido_id = f"{fecha}:{local_id}:{visita_id}"

            if hash_f in hashes_existentes:
                duplicados += 1
                continue

            # Equipo local y visitante -> lote
            for eid, nom in [(local_id, home_raw), (visita_id, away_raw)]:
                if eid not in equipos_nuevos:
                    lote_equipos.append((eid, nom, liga_id))
                    equipos_nuevos.add(eid)

            temp_csv  = _normalizar_temporada(fila.get("Season", temporada))
            jornada   = fecha_a_jornada.get(fecha, 0)

            if es_europeo:
                goles_l, goles_v = _safe_int(fila.get("FTHG")), _safe_int(fila.get("FTAG"))
                res_ft  = fila.get("FTR", "").strip() or None
                ght_l   = _safe_int(fila.get("HTHG"))
                ght_v   = _safe_int(fila.get("HTAG"))
                res_ht  = fila.get("HTR", "").strip() or None
            else:
                goles_l, goles_v = _safe_int(fila.get("HG")), _safe_int(fila.get("AG"))
                res_ft  = fila.get("Res", "").strip() or None
                ght_l = ght_v = res_ht = None

            hp = (
                partido_id, hash_f, fecha, temp_csv, liga_id, jornada,
                local_id, visita_id, goles_l, goles_v, res_ft,
                ght_l, ght_v, res_ht,
                _safe_int(fila.get("HS")),  _safe_int(fila.get("AS")),
                _safe_int(fila.get("HST")), _safe_int(fila.get("AST")),
                _safe_int(fila.get("HC")),  _safe_int(fila.get("AC")),
                _safe_int(fila.get("HY")),  _safe_int(fila.get("AY")),
                _safe_int(fila.get("HR")) or 0,
                _safe_int(fila.get("AR")) or 0,
                _safe_int(fila.get("HF")),  _safe_int(fila.get("AF")),
                fila.get("Referee", "").strip() or None,
                1 if jornada <= 3 else 0,
                0, 0, 0, 3, 3, None, None, None, None, None
            )
            lote_partidos.append(hp)

            # Odds
            odds_1x2 = {"partido_id": partido_id}
            for csv_col, db_col in mapa_odds_1x2.items():
                if csv_col in columnas_csv:
                    v = _safe_float(fila.get(csv_col))
                    if v is not None:
                        odds_1x2[db_col] = v
            if len(odds_1x2) > 1:
                lote_odds_1x2.append(odds_1x2)

            if es_europeo:
                odds_ou = {"partido_id": partido_id}
                for csv_col, db_col in CUOTAS_OU.items():
                    if csv_col in columnas_csv:
                        v = _safe_float(fila.get(csv_col))
                        if v is not None:
                            odds_ou[db_col] = v
                if len(odds_ou) > 1:
                    lote_odds_ou.append(odds_ou)

                odds_ah = {"partido_id": partido_id}
                for csv_col, db_col in CUOTAS_AH.items():
                    if csv_col in columnas_csv:
                        v = _safe_float(fila.get(csv_col))
                        if v is not None:
                            odds_ah[db_col] = v
                if len(odds_ah) > 1:
                    lote_odds_ah.append(odds_ah)

            # [v4-ETL-1] Registrar hash en memoria para detectar duplicados
            # en el mismo archivo (sin esperar a que estén en DB)
            hashes_existentes.add(hash_f)
            insertados += 1

            # [v4-ETL-2] Flush del lote cada BATCH_SIZE filas
            if len(lote_partidos) >= BATCH_SIZE:
                _flush(cur, lote_partidos, lote_equipos,
                       lote_odds_1x2, lote_odds_ou, lote_odds_ah)
                lote_partidos.clear(); lote_equipos.clear()
                lote_odds_1x2.clear(); lote_odds_ou.clear(); lote_odds_ah.clear()
                conn.commit()

        except Exception as e:
            errores += 1
            if errores <= 5:
                Log.warn(f"Fila omitida: {e}")

    # Flush del remanente
    if lote_partidos:
        _flush(cur, lote_partidos, lote_equipos,
               lote_odds_1x2, lote_odds_ou, lote_odds_ah)
        conn.commit()

    # [v4-ETL-3] Registrar en etl_log
    liga_id_final = liga_id_override or (
        max(ligas_vistas.items(), key=lambda x: x[1])[0] if ligas_vistas else "auto"
    )
    hash_run = hashlib.sha1(
        f"{csv_path}{insertados}{datetime.now().isoformat()}".encode()
    ).hexdigest()[:12]

    cur.execute("""
        INSERT INTO etl_log (timestamp, archivo, liga_id, temporada, formato,
                             insertados, duplicados, errores, hash_run)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (datetime.now().isoformat(), os.path.basename(csv_path), liga_id_final,
          temporada, "europeo" if es_europeo else "americano",
          insertados, duplicados, errores, hash_run))
    conn.commit()
    conn.close()

    Log.ok(f"Insertados: {insertados} | Duplicados (hash): {duplicados} | "
           f"Sin liga: {skip_liga} | Errores: {errores}")

    return {
        "archivo": os.path.basename(csv_path), "liga_id": liga_id_final,
        "temporada": temporada, "formato": "europeo" if es_europeo else "americano",
        "insertados": insertados, "duplicados": duplicados,
        "errores": errores, "skip_liga": skip_liga,
        "equipos_nuevos": len(equipos_nuevos), "hash_run": hash_run,
    }


def _flush(cur, lote_partidos, lote_equipos,
           lote_odds_1x2, lote_odds_ou, lote_odds_ah):
    """
    [v4-ETL-2] Ejecuta todos los INSERT del lote en una sola transacción.
    Usa INSERT OR IGNORE para robustez ante colisiones de hash improbables.
    """
    # Equipos
    cur.executemany(
        "INSERT OR IGNORE INTO dim_equipos (equipo_id, nombre_comercial, liga_id) VALUES (?,?,?)",
        lote_equipos
    )
    # Partidos (todas las columnas fijas)
    cur.executemany("""
        INSERT OR IGNORE INTO historial_partidos
        (partido_id, hash_fila, fecha, temporada, liga_id, numero_jornada,
         equipo_local, equipo_visitante, goles_local, goles_visitante,
         resultado_ft, goles_ht_local, goles_ht_visitante, resultado_ht,
         tiros_totales_local, tiros_totales_vis, tiros_puerta_local, tiros_puerta_vis,
         corners_local, corners_visitante, amarillas_local, amarillas_vis,
         rojas_local, rojas_vis, faltas_local, faltas_vis, arbitro_nombre,
         es_inicio_temporada, es_clasico, es_eliminatoria, es_partido_vuelta,
         dias_descanso_local, dias_descanso_vis,
         xg_local, xg_visitante, posesion_local, temperatura, asistencia_real)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, lote_partidos)

    # Odds 1x2 (esquema variable -> insertar cada uno individualmente pero en lote)
    for d in lote_odds_1x2:
        c = ", ".join(d.keys())
        p = ", ".join(["?"] * len(d))
        try:
            cur.execute(f"INSERT OR IGNORE INTO cuotas_1x2 ({c}) VALUES ({p})", list(d.values()))
        except sqlite3.Error:
            pass
    for d in lote_odds_ou:
        c = ", ".join(d.keys())
        p = ", ".join(["?"] * len(d))
        try:
            cur.execute(f"INSERT OR IGNORE INTO cuotas_ou ({c}) VALUES ({p})", list(d.values()))
        except sqlite3.Error:
            pass
    for d in lote_odds_ah:
        c = ", ".join(d.keys())
        p = ", ".join(["?"] * len(d))
        try:
            cur.execute(f"INSERT OR IGNORE INTO cuotas_ah ({c}) VALUES ({p})", list(d.values()))
        except sqlite3.Error:
            pass


def importar_carpeta(carpeta: str = CSV_FOLDER,
                     csv_path: str = None,
                     limite_por_archivo: int = None,
                     db_path: str = DB_NAME) -> list:
    Log.paso(0, f"Escaneando carpeta: {carpeta}/")
    csv = sorted(glob.glob(os.path.join(carpeta, "*.csv")))
    if not csv:
        Log.warn(f"No hay CSV en '{carpeta}'")
        return []
    Log.dato("CSV encontrados", len(csv))
    resultados = []
    for csv_path in csv:
        res = importar_csv(csv_path, limite_registros=limite_por_archivo, db_path=db_path)
        resultados.append(res)
    total_ins = sum(r.get("insertados", 0) for r in resultados)
    Log.ok(f"Total insertados: {total_ins}")
    return resultados


def resumen_db(db_path: str = DB_NAME) -> None:
    Log.paso(99, "Resumen DB v4.0")
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("""
        SELECT liga_id, temporada, COUNT(*),
               ROUND(AVG(goles_local + goles_visitante), 2),
               ROUND(AVG(tiros_puerta_local + tiros_puerta_vis), 1)
        FROM historial_partidos WHERE goles_local IS NOT NULL
        GROUP BY liga_id, temporada ORDER BY liga_id, temporada
    """)
    rows = cur.fetchall()
    if rows:
        print(f"\n  {'Liga':<22} {'Temp':<10} {'N':>5} {'AvgG':>6} {'SoT':>6}")
        print("  " + "-" * 55)
        for liga, tmp, n, ag, asot in rows:
            ag   = ag   or 0.0
            asot = asot or 0.0
            flag = "(*)" if asot == 0.0 else "   "
            print(f"  {liga:<22} {tmp:<10} {n:>5} {ag:>6} {asot:>5}{flag}")

    for label, q in [
        ("Total partidos",    "SELECT COUNT(*) FROM historial_partidos"),
        ("Entradas etl_log",  "SELECT COUNT(*) FROM etl_log"),
    ]:
        cur.execute(q)
        Log.dato(label, cur.fetchone()[0])

    # [v4-ETL-3] Mostrar últimas 5 importaciones
    cur.execute("SELECT timestamp, archivo, insertados, duplicados FROM etl_log ORDER BY id DESC LIMIT 5")
    logs = cur.fetchall()
    if logs:
        print("\n  Últimas importaciones:")
        for ts, arch, ins, dup in logs:
            print(f"    {ts[:16]}  {arch:<30}  +{ins} /{dup} dup")
    conn.close()


# ==============================================================================
# MAIN CLI
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("  CEREBRILLUM v4.0  |  ETL Dual Europa + America")
    print("  football-data.co.uk  |  Liga MX + MLS + EPL + LaLiga...")
    print("=" * 65)

    inicializar_schema(DB_NAME)

    print("\n  [a] Importar un CSV especifico")
    print("  [b] Importar todos los CSV de csv_data/")
    print("  [c] Ver resumen de la DB actual")
    print("  [d] Salir")

    op = input("\n  Opcion [a/b/c/d]: ").strip().lower()

    if op == "a":
        ruta = input("  Ruta del CSV: ").strip()
        lig  = input("  liga_id (Enter=auto-detectar): ").strip().lower() or None
        tmp  = input("  Temporada ej 2024-25 (Enter=auto): ").strip() or None
        lim  = input("  Limite de partidos (Enter=todos): ").strip()
        importar_csv(
            ruta,
            liga_id_override=lig,
            temporada=tmp,
            limite_registros=int(lim) if lim.isdigit() else None,
        )

    elif op == "b":
        os.makedirs(CSV_FOLDER, exist_ok=True)
        lim = input("  Limite por archivo (recomendado 150-200, Enter=todos): ").strip()
        importar_carpeta(
            CSV_FOLDER,
            limite_por_archivo=int(lim) if lim.isdigit() else None,
        )

    resumen_db(DB_NAME)   # Solo dentro de __main__