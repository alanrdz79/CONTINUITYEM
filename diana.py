# -*- coding: utf-8 -*-
"""
=============================================================================
  DIANA v1.0  |  CAPA 1b — CAPTURADOR AUTOMÁTICO DE CSVs
  Fuente: football-data.co.uk  |  Destino: csv_data/ (misma carpeta que lee AEGIST)
  Precede a CEREBELLIUM (Capa 2 — ETL). No la reemplaza ni la modifica.
=============================================================================
QUÉ RESUELVE:
  Hasta ahora, alimentar cerebrillum.db exigía entrar manualmente al sitio,
  ubicar cada uno de los 8+ enlaces, descargarlos y llevarlos a csv_data/.
  Con 8 ligas automatizables y una sola persona operando el proyecto, esa
  tarea es un cuello de botella de tiempo que no aporta valor analítico.

  DIANA reemplaza SOLO el paso de "ir a buscar el archivo" (Capa 1).
  El paso de "decidir qué se importa a la DB" (Capa 2, CEREBELLIUM) sigue
  intacto: por defecto DIANA únicamente descarga y coloca el CSV en
  csv_data/ — el flujo manual de ETL (CLI de CEREBELLIUM o módulo ETL de
  AEGIST) sigue funcionando exactamente igual que hoy, sin ningún cambio.
  La ingesta automática es opcional y explícita (--importar).

LIGAS CUBIERTAS (verificado en vivo contra football-data.co.uk, no por
memoria — ver notas de auditoría):
  Formato "main" (un archivo por temporada, mmz4281/{temporada}/{DIV}.csv):
    E0=premier_league, SP1=la_liga, I1=serie_a, D1=bundesliga, F1=ligue_1,
    N1=eredivisie, E2=league_one
  Formato "extra" (un archivo único con todo el histórico, new/{PAIS}.csv):
    MEX=liga_mx (Clausura/Apertura se separan por mes, ya lo hace
                 _liga_id_americano() en CEREBELLIUM, sin tocar nada),
    USA=mls

LIGAS EXCLUIDAS INTENCIONALMENTE (no dañar lo que no se puede automatizar):
  champions_league, concacaf_cl — confirmado que football-data.co.uk NO
  las distribuye (cubre 11 ligas domésticas "main" + 16 países "extra";
  ninguna sección incluye competiciones de confederación/copa continental).
  Se dejan exactamente como hoy: carga manual desde la fuente que uses.

GAP DETECTADO Y NO CORREGIDO AQUÍ (requiere tocar CEREBELLIUMv4.py):
  DIV_LIGA en CEREBELLIUMv4.py no incluye "E2" (League One). Sin ese
  parche de 1 línea, los CSV de E2 que DIANA descargue caerán en
  skip_liga silenciosamente al importarlos. Parche sugerido (no aplicado
  automáticamente por disciplina de "no tocar sin permiso explícito"):

      DIV_LIGA = {
          "E0":  "premier_league", "SP1": "la_liga",   "I1":  "serie_a",
          "D1":  "bundesliga",     "F1":  "ligue_1",   "P1":  "primera_liga",
          "N1":  "eredivisie",     "B1":  "pro_league", "CL": "champions_league",
          "E2":  "league_one",   # <- única línea nueva
      }

DEPENDENCIAS: únicamente 'requests', ya presente en requirements.txt.
  Cero librerías nuevas que mantener.

USO:
  python diana.py                    -> descarga solo temporada vigente
  python diana.py --historico 2      -> + 2 temporadas anteriores (backfill)
  python diana.py --forzar           -> ignora cache TTL, re-descarga todo
  python diana.py --importar         -> tras descargar, ingiere a la DB
                                         automáticamente vía CEREBELLIUM
  python diana.py --carpeta otra/    -> carpeta destino distinta a csv_data/

PROGRAMACIÓN (Windows, ejecución diaria automática):
  schtasks /create /tn "DIANA_Diario" /tr "python C:\\ruta\\diana.py --importar" ^
           /sc daily /st 07:00
=============================================================================
"""

import os
import sys
import time
import argparse
from datetime import datetime

try:
    import requests
except ImportError:
    print("[ERROR] Falta 'requests'. Ya está en requirements.txt: "
          "pip install -r requirements.txt")
    sys.exit(1)

# ==============================================================================
# IMPORTACIÓN SEGURA DE CEREBELLIUM — mismo patrón que usa AEGISTv501.py
# DIANA debe poder ejecutarse aunque CEREBELLIUM no esté en el path (modo
# "solo descarga"), y debe integrarse sin fricción cuando sí lo está.
# ==============================================================================
CEREBELLIUM_DISPONIBLE = False
try:
    from CEREBELLIUM import (
        Log, importar_csv, importar_carpeta, inicializar_schema, DB_NAME,
    )
    CEREBELLIUM_DISPONIBLE = True
except Exception as _e:
    class Log:  # Fallback mínimo — mismo formato de salida que CEREBELLIUM
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
    DB_NAME = "cerebrillum.db"
    _CEREBELLIUM_ERROR = f"{type(_e).__name__}: {_e}"


# ==============================================================================
# CONFIGURACIÓN — misma variable de entorno que usa AEGISTv501.py (DATA_DIR),
# así ambos scripts apuntan siempre a la misma carpeta sin configurarlo dos veces.
# ==============================================================================
DATA_DIR       = os.environ.get("AEGIST_DATA_DIR", "csv_data")
TTL_HORAS       = float(os.environ.get("DIANA_TTL_HORAS", "6"))
TIMEOUT_SEG     = 20
BASE_URL_MAIN   = "https://www.football-data.co.uk/mmz4281"
BASE_URL_EXTRA  = "https://www.football-data.co.uk/new"
HEADERS         = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "DianaBot/1.0 (uso personal, no comercial, proyecto)"
}

# [DIANA-1] Ligas formato "main": un archivo por temporada
LIGAS_EUROPEAS_MMZ = {
    "E0":  "premier_league",
    "SP1": "la_liga",
    "I1":  "serie_a",
    "D1":  "bundesliga",
    "F1":  "ligue_1",
    "N1":  "eredivisie",
    "E2":  "league_one",   # requiere el parche de 1 línea en DIV_LIGA (ver docstring)
}

# [DIANA-2] Ligas formato "extra": un solo archivo con todo el histórico
LIGAS_EXTRA_NEW = {
    "MEX": "liga_mx",
    "USA": "mls",
}

# [DIANA-3] Ligas del proyecto que NO existen en football-data.co.uk.
# Se documentan aquí (no se intentan descargar) para que quede explícito
# por qué faltan, en vez de fallar en silencio.
LIGAS_EXCLUIDAS = {
    "champions_league": "football-data.co.uk no distribuye competiciones "
                         "de confederación/copa continental.",
    "concacaf_cl":       "football-data.co.uk no distribuye competiciones "
                         "de confederación/copa continental.",
}


# ==============================================================================
# HELPERS — TEMPORADA
# ==============================================================================
def _temporada_actual() -> tuple:
    """
    Devuelve (año_inicio, código_sitio) de la temporada europea vigente.
    Convención de football-data.co.uk: temporada arranca jul/ago.
    Ej.: hoy=2026-08-02 -> (2026, "2627"); hoy=2026-03-10 -> (2025, "2526").
    """
    hoy = datetime.now()
    inicio = hoy.year if hoy.month >= 7 else hoy.year - 1
    fin = inicio + 1
    codigo = f"{str(inicio)[2:]}{str(fin)[2:]}"
    return inicio, codigo


def _temporadas_a_descargar(inicio_vigente: int, n_atras: int) -> list:
    """[(año_inicio, código_sitio), ...] para la vigente (ya resuelta contra
    el sitio, ver _resolver_temporada_vigente) + n_atras anteriores."""
    out = []
    for i in range(max(0, n_atras) + 1):
        a = inicio_vigente - i
        codigo = f"{str(a)[2:]}{str(a + 1)[2:]}"
        out.append((a, codigo))
    return out


def _resolver_temporada_vigente(session):
    """
    [DIANA-4] La temporada 'vigente' NO se decide solo por calendario.
    PROBLEMA QUE RESUELVE: un corte fijo por mes se rompe cada año durante
    la ventana entre julio y la fecha real en que football-data.co.uk
    publica los archivos de la temporada entrante (normalmente cuando
    arrancan los partidos, no antes). Confirmado en produccion: con mes=8
    el sitio aun servia la 2025/26, no la 2026/27 -> 404 en 7 ligas.

    SOLUCION: usar E0 (Premier League) como referencia, verificando con
    una peticion real. Si la temporada calculada por calendario no existe
    todavia, se retrocede una temporada automaticamente. Sin fecha de
    corte que reajustar a mano cada año.
    """
    inicio_calc, _ = _temporada_actual()
    for intento in range(2):
        a = inicio_calc - intento
        codigo = f"{str(a)[2:]}{str(a + 1)[2:]}"
        url = f"{BASE_URL_MAIN}/{codigo}/E0.csv"
        try:
            resp = session.get(url, timeout=TIMEOUT_SEG, headers=HEADERS)
            if resp.status_code == 200 and len(resp.content) >= 200:
                if intento > 0:
                    Log.info(f"Temporada 'main' ajustada automaticamente: "
                             f"el sitio aun no publica {inicio_calc}-{str(inicio_calc+1)[2:]}, "
                             f"usando {a}-{str(a+1)[2:]}")
                return a, codigo
        except requests.exceptions.RequestException:
            continue
    Log.warn("No se pudo confirmar la temporada 'main' vigente contra el sitio; "
             "se usara el calculo por calendario y se reportara error por liga si falla.")
    return inicio_calc, f"{str(inicio_calc)[2:]}{str(inicio_calc + 1)[2:]}"


# ==============================================================================
# DESCARGA — FORMATO "MAIN" (europeo, por temporada)
# ==============================================================================
def _descargar_liga_europea(div: str, año_inicio: int, codigo_temporada: str,
                            carpeta: str, session: "requests.Session",
                            es_vigente: bool, forzar: bool = False) -> dict:
    """
    Nombre de archivo: {DIV}_{año_inicio}_auto.csv  (NO se usa el código del
    sitio como "2526" en el nombre: el parser de temporada de CEREBELLIUM
    interpretaría "2526" como el año 2526. Se usa el año real, ej. 2025).
    """
    url          = f"{BASE_URL_MAIN}/{codigo_temporada}/{div}.csv"
    nombre       = f"{div}_{año_inicio}_auto.csv"
    destino      = os.path.join(carpeta, nombre)
    temporada_id = f"{año_inicio}-{str(año_inicio + 1)[2:]}"

    if not forzar and os.path.exists(destino):
        if not es_vigente:
            return {"div": div, "estado": "omitido", "archivo": nombre,
                     "temporada": temporada_id, "motivo": "temporada cerrada, ya en cache"}
        edad_h = (time.time() - os.path.getmtime(destino)) / 3600.0
        if edad_h < TTL_HORAS:
            return {"div": div, "estado": "omitido", "archivo": nombre,
                     "temporada": temporada_id, "motivo": f"cache reciente ({edad_h:.1f}h < {TTL_HORAS}h)"}

    try:
        resp = session.get(url, timeout=TIMEOUT_SEG, headers=HEADERS)
        resp.raise_for_status()
        contenido = resp.content
        if len(contenido) < 200:
            return {"div": div, "estado": "vacio", "temporada": temporada_id,
                     "motivo": f"{len(contenido)} bytes — temporada sin partidos aún"}
        with open(destino, "wb") as f:
            f.write(contenido)
        return {"div": div, "estado": "ok", "archivo": nombre,
                 "temporada": temporada_id, "bytes": len(contenido)}
    except requests.exceptions.HTTPError as e:
        codigo_http = e.response.status_code if e.response is not None else "?"
        if codigo_http == 404:
            return {"div": div, "estado": "no_disponible", "temporada": temporada_id,
                     "motivo": f"el sitio no tiene archivo para {temporada_id} en {div}"}
        return {"div": div, "estado": "error", "temporada": temporada_id, "motivo": str(e)}
    except requests.exceptions.RequestException as e:
        return {"div": div, "estado": "error", "temporada": temporada_id, "motivo": str(e)}


# ==============================================================================
# DESCARGA — FORMATO "EXTRA" (americano, histórico completo en un archivo)
# ==============================================================================
def _descargar_liga_extra(codigo_pais: str, carpeta: str,
                          session: "requests.Session", forzar: bool = False) -> dict:
    url     = f"{BASE_URL_EXTRA}/{codigo_pais}.csv"
    nombre  = f"{codigo_pais}_auto.csv"
    destino = os.path.join(carpeta, nombre)

    # Formato "extra" es un único archivo acumulativo: siempre puede tener
    # partidos nuevos, así que respeta el TTL igual que la temporada vigente.
    if not forzar and os.path.exists(destino):
        edad_h = (time.time() - os.path.getmtime(destino)) / 3600.0
        if edad_h < TTL_HORAS:
            return {"pais": codigo_pais, "estado": "omitido", "archivo": nombre,
                     "motivo": f"cache reciente ({edad_h:.1f}h < {TTL_HORAS}h)"}

    try:
        resp = session.get(url, timeout=TIMEOUT_SEG, headers=HEADERS)
        resp.raise_for_status()
        contenido = resp.content
        if len(contenido) < 200:
            return {"pais": codigo_pais, "estado": "vacio", "motivo": f"{len(contenido)} bytes"}
        with open(destino, "wb") as f:
            f.write(contenido)
        return {"pais": codigo_pais, "estado": "ok", "archivo": nombre, "bytes": len(contenido)}
    except requests.exceptions.RequestException as e:
        return {"pais": codigo_pais, "estado": "error", "motivo": str(e)}


# ==============================================================================
# LOG DE RESULTADO — formato uniforme para ambas ramas
# ==============================================================================
def _log_resultado(r: dict, etiqueta: str) -> None:
    estado = r.get("estado")
    if estado == "ok":
        Log.ok(f"{etiqueta}: {r.get('archivo')} ({r.get('bytes', 0):,} bytes)")
    elif estado == "omitido":
        Log.info(f"{etiqueta}: omitido — {r.get('motivo')}")
    elif estado == "vacio":
        Log.warn(f"{etiqueta}: sin datos — {r.get('motivo')}")
    elif estado == "no_disponible":
        Log.info(f"{etiqueta}: no publicado por el sitio — {r.get('motivo')}")
    else:
        Log.error(f"{etiqueta}: FALLÓ — {r.get('motivo')}")


# ==============================================================================
# ORQUESTADOR PRINCIPAL
# ==============================================================================
def ejecutar_diana(carpeta: str = None, historico_temporadas: int = 0,
                   forzar: bool = False, auto_importar: bool = False,
                   db_path: str = None) -> dict:
    """
    Punto de entrada único. Pensado para usarse tanto desde CLI (__main__)
    como desde un futuro botón en AEGIST (mismo patrón de integración de
    3 líneas que ya usaste para ARGUS: import + llamada + mostrar resultado).
    """
    carpeta = carpeta or DATA_DIR
    os.makedirs(carpeta, exist_ok=True)
    db_path = db_path or DB_NAME

    session = requests.Session()
    Log.paso(0, "DIANA — Resolviendo temporada 'main' vigente contra el sitio")
    inicio_actual, _ = _resolver_temporada_vigente(session)
    temporadas = _temporadas_a_descargar(inicio_actual, historico_temporadas)

    resultados = {
        "europeas":  [],
        "extra":     [],
        "excluidas": dict(LIGAS_EXCLUIDAS),
        "carpeta":   os.path.abspath(carpeta),
    }

    Log.paso(1, f"DIANA — {len(LIGAS_EUROPEAS_MMZ)} ligas 'main' x "
                f"{len(temporadas)} temporada(s)")
    for div, liga_id in LIGAS_EUROPEAS_MMZ.items():
        for año_inicio, codigo in temporadas:
            es_vigente = (año_inicio == inicio_actual)
            r = _descargar_liga_europea(div, año_inicio, codigo, carpeta,
                                        session, es_vigente, forzar)
            r["liga_id"] = liga_id
            resultados["europeas"].append(r)
            _log_resultado(r, f"{liga_id} {r.get('temporada','')}")

    Log.paso(2, f"DIANA — {len(LIGAS_EXTRA_NEW)} ligas 'extra' (histórico único)")
    for codigo_pais, liga_id in LIGAS_EXTRA_NEW.items():
        r = _descargar_liga_extra(codigo_pais, carpeta, session, forzar)
        r["liga_id"] = liga_id
        resultados["extra"].append(r)
        _log_resultado(r, liga_id)

    todos = resultados["europeas"] + resultados["extra"]
    n_ok      = sum(1 for r in todos if r["estado"] == "ok")
    n_omit    = sum(1 for r in todos if r["estado"] == "omitido")
    n_error   = sum(1 for r in todos if r["estado"] == "error")
    Log.ok(f"Descarga completa: {n_ok} nuevo(s)/actualizado(s), "
           f"{n_omit} en cache, {n_error} con error.")

    if LIGAS_EXCLUIDAS:
        Log.info("Excluidas (no distribuidas por la fuente, sin cambios): "
                 + ", ".join(LIGAS_EXCLUIDAS.keys()))

    if auto_importar:
        if not CEREBELLIUM_DISPONIBLE:
            Log.error(f"--importar solicitado pero CEREBELLIUM no está disponible: "
                       f"{_CEREBELLIUM_ERROR}")
            resultados["importacion"] = None
        else:
            Log.paso(3, "DIANA — Ingesta automática vía CEREBELLIUM")
            inicializar_schema(db_path)
            resumen = importar_carpeta(carpeta, db_path=db_path)
            resultados["importacion"] = resumen
            total_ins = sum(r.get("insertados", 0) for r in resumen)
            Log.ok(f"Ingesta completa: {total_ins} partido(s) nuevo(s) en {db_path}")

    return resultados


# ==============================================================================
# CLI
# ==============================================================================
def _parsear_args():
    p = argparse.ArgumentParser(
        description="DIANA — Capturador automático de CSVs football-data.co.uk"
    )
    p.add_argument("--historico", type=int, default=0,
                   help="Temporadas adicionales hacia atrás a descargar (0=solo vigente)")
    p.add_argument("--forzar", action="store_true",
                   help="Ignora el cache TTL y re-descarga todo")
    p.add_argument("--importar", action="store_true",
                   help="Tras descargar, ejecuta la ingesta a la DB automáticamente")
    p.add_argument("--carpeta", type=str, default=None,
                   help="Carpeta destino (por defecto csv_data/ o AEGIST_DATA_DIR)")
    return p.parse_args()


def main():
    args = _parsear_args()
    print("\n" + "=" * 65)
    print("  DIANA v1.0  |  Capturador automático — football-data.co.uk")
    print(f"  Destino: {os.path.abspath(args.carpeta or DATA_DIR)}")
    print("=" * 65)

    resultado = ejecutar_diana(
        carpeta=args.carpeta,
        historico_temporadas=args.historico,
        forzar=args.forzar,
        auto_importar=args.importar,
    )

    print("\n" + "-" * 65)
    print("  RESUMEN")
    print("-" * 65)
    for r in resultado["europeas"] + resultado["extra"]:
        etiqueta = r.get("liga_id", "?")
        detalle  = r.get("archivo") or r.get("motivo", "")
        print(f"  {etiqueta:<18} {r.get('estado','?'):<8} {detalle}")
    if resultado["excluidas"]:
        print(f"\n  Excluidas: {', '.join(resultado['excluidas'].keys())}")
    print("-" * 65 + "\n")


if __name__ == "__main__":
    main()