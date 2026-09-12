# -*- coding: utf-8 -*-
"""
==============================================================================
  PACIFICO KyJ v5.1  |  PACIFICO KyJ CONTROL PANEL
  CONTINUITY v4.5 (10 gaps) + CEREBELLIUM v4.0
  Sistema Adaptativo de Predicción de Alta Frecuencia
==============================================================================

CAMBIOS v5.1 vs v5.0.1 — SINCRONIZACIÓN CON CONTINUITY v4.5 (Gap 1-10):

  [v51-BRAND-1]  Renombrado de marca en toda la interfaz visual:
                 "AEGIST MATRIX" -> "PACIFICO KyJ". page_title, header del
                 sidebar y footer actualizados. Las clases CSS internas
                 (prefijo "am-") se mantienen sin cambio: son identificadores
                 técnicos invisibles al usuario, no texto de marca — renombrarlas
                 no aporta valor y sí introduce riesgo de romper referencias
                 en cientos de puntos del archivo sin beneficio visible.

  [v51-BUG-1]    Corregido bug de wiring pre-existente en el sidebar: las
                 longitudes de menu_labels y _MENU_KEYS ya coincidían (8=8,
                 no había IndexError), pero la etiqueta en la posición 8 decía
                 "MUNDIAL 2026" mientras _MENU_KEYS[7] mapeaba a la clave
                 "RENTABILIDAD" — que además no tenía módulo ni entrada en
                 dispatch. Resultado real: clic en "MUNDIAL 2026" activaba
                 modulo_activo="RENTABILIDAD", dispatch.get() devolvía None,
                 y la app caía silenciosamente en RADAR sin avisar del error.
                 Corregido en dos frentes: (1) menu_labels ahora se construye
                 desde _MENU_LABELS_BASE (única fuente de verdad, ya existía
                 pero no se usaba), eliminando la posibilidad de que etiqueta
                 y clave se desincronicen otra vez; (2) se implementó el
                 módulo RENTABILIDAD real (ver v51-GAP10) en vez de dejar la
                 clave huérfana.

  [v51-GAP3]     modulo_inferencia(): el vector de inferencia ya NO hardcodea
                 forma_pts_local_5=7.5, h2h_win_rate_local=0.45, tsr_local=0.5,
                 dif_prestigio/altitud/elo=0. Usa
                 DataPipeline.construir_vector_inferencia() — mismo cálculo
                 que construir_features() usa en entrenamiento, eliminando el
                 desajuste de distribución entre train e inferencia.

  [v51-GAP6]     modulo_inferencia(): ajuste post-blend por posición en tabla
                 vía DataPipeline.calcular_tabla_posiciones() +
                 AjustadorCuantico.factor_tabla_posiciones().

  [v51-GAP7]     modulo_inferencia(): mercado Over/Under 2.5 vía
                 TransmutadorEstocastico.calcular_over_under() +
                 EscudoFinanciero.evaluar_binario(). Cuotas O/U opcionales
                 en el formulario (mismo patrón que CONTINUITY main() CLI).

  [v51-GAP5+8]   modulo_feedback() (CIERRE): además de RegistradorFeedback
                 (error de lambda), ahora registra el resultado MONETARIO
                 real en AuditorRentabilidad — determinando "gano" contra el
                 mercado realmente apostado (no solo victoria local) y
                 etiquetando predictor_lider (RF/Online) para habilitar
                 BlendAdaptativo.ajustar_por_roi().

  [v51-GAP9]     modulo_inferencia(): consulta AuditorRentabilidad.racha_actual()
                 antes de evaluar y usa EscudoFinanciero.evaluar_con_racha()
                 en lugar de evaluar_triple() directo — penaliza el stake
                 sugerido tras rachas de derrotas consecutivas.

  [v51-GAP10]    Nuevo módulo RENTABILIDAD (modulo_rentabilidad): expone
                 AuditorRentabilidad.resumen_global()/resumen_liga(),
                 BlendAdaptativo.ajustar_por_roi() y Backtester.ejecutar()
                 bajo demanda — capacidades que existían en CONTINUITY pero
                 no tenían ningún punto de acceso desde la UI.

CAMBIOS v5.2 vs v5.1 — SINCRONIZACIÓN CON CEREBELLIUM v4.1 (GAP13):

  [v52-GAP13]    CORRECCIÓN ESTRUCTURAL — Persistencia real de cola_partidos.
                 PROBLEMA QUE RESUELVE: cola_partidos vivía únicamente en
                 st.session_state (memoria volátil del proceso Streamlit).
                 Cualquier partido inferido en INFERENCIA pero no cerrado
                 en la MISMA sesión desaparecía sin dejar rastro ante un
                 reinicio del proceso (deploy, reinicio de PC, expiración
                 de sesión) — nunca llegaba a AuditorRentabilidad. Auditoría
                 real sobre uso en producción: solo 7 de 35+ apuestas de dos
                 meses de operación quedaron registradas en state_roi.json.
                 CORRECCIÓN: _cola_get/_cola_append/_cola_cerrar/_cola_descartar
                 ahora leen y escriben la tabla cola_partidos_pendientes de
                 CEREBELLIUM v4.1 (creada en inicializar_schema, ver
                 CEREBELLIUM.py GAP13), en vez de session_state. Un partido
                 pendiente de cierre ahora sobrevive a cualquier reinicio del
                 proceso, sin importar cuánto tiempo pase antes de cerrarlo.
                 Distinción cerrar vs. descartar:
                   - _cola_cerrar()   -> UPDATE cerrado=1 (soft-close, usado
                     en CIERRE tras registrar el resultado real; preserva el
                     registro para trazabilidad/auditoría histórica).
                   - _cola_descartar() -> DELETE real (usado en "BORRAR
                     ÚLTIMA" de INFERENCIA; la inferencia se descarta antes
                     de convertirse en una apuesta real, no tiene valor de
                     auditoría conservarla).
                 COMPATIBILIDAD: payload_json conserva exactamente el mismo
                 dict que ya armaba _cola_append() en v5.1 (lam_l, lam_v,
                 p1/px/p2, mercado, cuota_elegida, predictor_lider, etc.)
                 serializado — cero cambios a la estructura de datos interna
                 de un partido en cola, cero cambios a CONTINUITY.py.
                 IMPACTO: todas las funciones que antes llamaban
                 _cola_get()/_cola_append(p)/_cola_remove(pid) ahora reciben
                 conn como primer argumento — cambio mecánico de firma,
                 sin alterar ninguna lógica de negocio existente.
                 main() ahora llama inicializar_schema(DB_NAME) al arrancar
                 (idempotente, solo CREATE TABLE IF NOT EXISTS) para
                 garantizar que cola_partidos_pendientes exista incluso si
                 nunca se corrió CEREBELLIUM.py ni diana.py --importar antes.

EJECUCIÓN:
streamlit run pacificokyj.py
streamlit cache clear
==============================================================================
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
import time
import json
import os
import sys
import traceback
import hashlib

# ==============================================================================
# CONFIGURACIÓN — PRIMERA INSTRUCCIÓN STREAMLIT
# ==============================================================================
st.set_page_config(
    page_title="PACIFICO KyJ",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==============================================================================
# CONSTANTES DE SISTEMA
# ==============================================================================
DB_NAME           = "cerebrillum.db"
STATE_FILE        = "state.json"
STATE_ONLINE      = "state_online.json"
STATE_BLEND       = "state_blend.json"
FEATURE_STORE_DIR = "feature_store"
VERSION           = "v5.2"

# [v501-ETL-1] DATA_DIR sobreescribible por variable de entorno
DATA_DIR = os.environ.get("AEGIST_DATA_DIR", "csv_data")

# Límites de mantenimiento
_LIMITE_EQUIPOS_JSON  = 500
_LIMITE_PARQUET_HRS   = 24
_TABLAS_REQUERIDAS    = frozenset({
    "dim_ligas", "dim_equipos", "historial_partidos",
    "cuotas_1x2", "cuotas_ou", "cuotas_ah", "etl_log",
    "cola_partidos_pendientes",
})
_VISTAS_REQUERIDAS    = frozenset({"historial_hot"})
_TABLAS_PERMITIDAS    = _TABLAS_REQUERIDAS | _VISTAS_REQUERIDAS | {"historial_partidos"}
# ==============================================================================
# SYS.PATH
# ==============================================================================
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

# ==============================================================================
# IMPORTACIÓN SEGURA CONTINUITY v4.1
# ==============================================================================
MOTOR_DISPONIBLE = False
MOTOR_ERROR_MSG  = ""
MODELOS_DIR      = "modelos_rf"

try:
    from continuitis import (
        DataPipeline, EstadoAdaptativo, MotorInferenciaIA, MotorOnline,
        BlendAdaptativo, FeatureStore, AjustadorCuantico,
        TransmutadorEstocastico, EscudoFinanciero, RegistradorFeedback,
        RhoDinamicoCalculator, AuditorRentabilidad, Backtester,
        ALL_LIGAS, FASE_1_NAFTA, FASE_2_EUROPA, PARON_FIFA_MESES,
        MODELOS_DIR, EV_THRESHOLD, MIN_EDGE_REQUERIDO,
        FEATURE_STORE_DIR, FEATURE_COLUMNS,
        FEATURE_BASE, FEATURE_DINAMICAS,
        MERCADOS_LABELS,
    )
    MOTOR_DISPONIBLE = True
except Exception as _e:
    MOTOR_ERROR_MSG   = f"{type(_e).__name__}: {_e}"
    PARON_FIFA_MESES  = {3, 6, 9, 10, 11}
    EV_THRESHOLD      = 0.05
    MIN_EDGE_REQUERIDO = 0.02
    FEATURE_STORE_DIR = "feature_store"
    FEATURE_COLUMNS   = []
    FEATURE_BASE      = []
    FEATURE_DINAMICAS = []
    FASE_1_NAFTA = {
        "liga_mx_clausura": {"nombre": "Liga MX Clausura", "rango": [1, 5],  "region": "NAFTA", "avg_goles": 2.4, "rho_dc": -0.05},
        "liga_mx_apertura": {"nombre": "Liga MX Apertura", "rango": [7, 12], "region": "NAFTA", "avg_goles": 2.4, "rho_dc": -0.05},
        "concacaf_cl":      {"nombre": "CONCACAF CL",      "rango": [2, 4],  "region": "NAFTA", "avg_goles": 2.6, "rho_dc": -0.04},
        "mls":              {"nombre": "MLS",               "rango": [3, 12], "region": "NAFTA", "avg_goles": 2.9, "rho_dc": -0.03},
    }
    FASE_2_EUROPA = {
        "premier_league":   {"nombre": "Premier League",  "rango": [8, 5], "region": "UEFA", "avg_goles": 2.7, "rho_dc": -0.04},
        "la_liga":          {"nombre": "LaLiga",          "rango": [8, 5], "region": "UEFA", "avg_goles": 2.5, "rho_dc": -0.04},
        "serie_a":          {"nombre": "Serie A",         "rango": [8, 5], "region": "UEFA", "avg_goles": 2.6, "rho_dc": -0.07},
        "eredivisie":       {"nombre": "Eredivisie",      "rango": [8, 5], "region": "UEFA", "avg_goles": 3.2, "rho_dc": -0.02},
        "bundesliga":       {"nombre": "Bundesliga",      "rango": [8, 5], "region": "UEFA", "avg_goles": 3.1, "rho_dc": -0.03},
        "ligue_1":          {"nombre": "Ligue 1",         "rango": [8, 5], "region": "UEFA", "avg_goles": 2.6, "rho_dc": -0.04},
        "champions_league": {"nombre": "Champions League","rango": [9, 5], "region": "UEFA", "avg_goles": 2.8, "rho_dc": -0.04},
        "league_one":       {"nombre": "League One",      "rango": [8, 5], "region": "UEFA", "avg_goles": 2.5, "rho_dc": -0.04},
    }
    ALL_LIGAS = {**FASE_1_NAFTA, **FASE_2_EUROPA}
    
    # [v502-IMPORT] Fallback cuando CONTINUITY no está disponible
    MERCADOS_LABELS = {
      "1": "Victoria Local",
       "X": "Empate",
       "2": "Victoria Visitante",
    }


# ==============================================================================
# IMPORTACIÓN SEGURA CEREBELLIUM v4.1
# ==============================================================================
ETL_DISPONIBLE = False
ETL_ERROR_MSG  = ""
try:
    from CEREBELLIUM import inicializar_schema, importar_csv, resumen_db, importar_carpeta
    ETL_DISPONIBLE = True
except Exception as _e2:
    ETL_ERROR_MSG = f"{type(_e2).__name__}: {_e2}"


# ==============================================================================
# IMPORTACIÓN SEGURA DIANA v1.0 + PURGA v1.0  [OPERACION-1]
# Ambos se importan "en caliente" para que el módulo OPERACION pueda invocar
# sus funciones directamente sin salir de Streamlit ni abrir una terminal.
# ==============================================================================
DIANA_DISPONIBLE = False
DIANA_ERROR_MSG  = ""
try:
    from diana import ejecutar_diana as _diana_ejecutar
    DIANA_DISPONIBLE = True
except Exception as _e3:
    DIANA_ERROR_MSG = f"{type(_e3).__name__}: {_e3}"

PURGA_DISPONIBLE = False
PURGA_ERROR_MSG  = ""
try:
    import PURGA as _purga_mod
    PURGA_DISPONIBLE = True
except Exception as _e4:
    PURGA_ERROR_MSG = f"{type(_e4).__name__}: {_e4}"


# ==============================================================================
# CSS — AEGIST MATRIX v5.0.1  [v501-CSS-1]
# Cambios respecto a v5.0:
#   - Tipografía: Geist Mono + Outfit (mejor legibilidad a tamaños pequeños)
#   - line-height unificado 1.5 en body; 1.7 en cards de datos
#   - letter-spacing reducido en labels (-0.02em → 0.08em) para evitar
#     solapamiento en viewports estrechos
#   - margin/padding estandarizado: base 8px, múltiplos de 4
#   - am-node-detail: white-space: pre-line + overflow: hidden
#   - am-partido-teams: white-space: nowrap + text-overflow: ellipsis
#   - Removed conflicting am-node.ok/.warn/.err that fought with base .am-node
# ==============================================================================
_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700;800&family=Geist+Mono:wght@300;400;500;600&display=swap');

/* ── Reset base ── */
html, body, [class*="css"] {
    font-family: 'Geist Mono', 'IBM Plex Mono', monospace;
    background-color: #0A0F1E;
    color: #B8C4D8;
    line-height: 1.5;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #060B15 !important;
    border-right: 1px solid #1A2642 !important;
}
[data-testid="stSidebar"] * {
    font-family: 'Geist Mono', monospace;
    line-height: 1.5;
}

/* ── Display titles ── */
.am-title {
    font-family: 'Outfit', sans-serif;
    font-weight: 800;
    letter-spacing: -0.02em;
    color: #FFFFFF;
    line-height: 1.2;
    margin: 0;
    padding: 0;
}
.am-accent { color: #C8FF00; }
.am-muted {
    color: #4A5568;
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    line-height: 1.4;
    margin: 0;
    padding: 0;
}

/* ── Section header ── */
.am-section-header {
    margin: 20px 0 16px 0;
    padding: 0;
    border-bottom: 1px solid #1A2642;
    padding-bottom: 12px;
}

/* ── Métricas ── */
div[data-testid="stMetricValue"] {
    font-family: 'Geist Mono', monospace !important;
    font-size: 1.35rem !important;
    font-weight: 600 !important;
    color: #FFFFFF !important;
    line-height: 1.3 !important;
}
div[data-testid="stMetricLabel"] {
    font-family: 'Geist Mono', monospace !important;
    font-size: 0.62rem !important;
    color: #4A5568 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.10em !important;
    line-height: 1.4 !important;
}
div[data-testid="stMetricDelta"] {
    font-family: 'Geist Mono', monospace !important;
    font-size: 0.68rem !important;
    line-height: 1.4 !important;
}
[data-testid="stMetric"] {
    background: #0D1526 !important;
    border: 1px solid #1A2642 !important;
    border-radius: 6px !important;
    padding: 12px 16px !important;
}

/* ── Botones ── */
.stButton > button[kind="primary"] {
    background: #C8FF00 !important;
    color: #0A0F1E !important;
    border: none !important;
    font-family: 'Outfit', sans-serif !important;
    font-weight: 700 !important;
    font-size: 12px !important;
    letter-spacing: 0.06em !important;
    border-radius: 4px !important;
    padding: 8px 16px !important;
    line-height: 1.4 !important;
}
.stButton > button[kind="primary"]:hover {
    background: #D4FF33 !important;
    box-shadow: 0 0 14px rgba(200,255,0,0.2) !important;
}
.stButton > button[kind="secondary"] {
    background: transparent !important;
    border: 1px solid #1A2642 !important;
    color: #6B7A99 !important;
    font-family: 'Geist Mono', monospace !important;
    font-size: 10px !important;
    border-radius: 4px !important;
    line-height: 1.4 !important;
}

/* ── Formularios ── */
[data-testid="stForm"] {
    background: #0D1526 !important;
    border: 1px solid #1A2642 !important;
    border-radius: 8px !important;
    padding: 20px 20px 16px !important;
}
.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stSelectbox > div > div {
    background: #060B15 !important;
    border: 1px solid #1E2D5E !important;
    border-radius: 4px !important;
    color: #B8C4D8 !important;
    font-family: 'Geist Mono', monospace !important;
    font-size: 11px !important;
    line-height: 1.5 !important;
}
/* Evita que labels de inputs se solapen con campos */
.stTextInput label, .stNumberInput label,
.stSelectbox label, .stSlider label,
.stCheckbox label {
    font-family: 'Geist Mono', monospace !important;
    font-size: 10px !important;
    color: #6B7A99 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    line-height: 1.6 !important;
    margin-bottom: 4px !important;
}

/* ── DataFrames ── */
[data-testid="stDataFrame"] {
    border: 1px solid #1A2642 !important;
    border-radius: 6px !important;
}
.dvn-scroller { background: #0D1526 !important; }

/* ── Alertas ── */
[data-testid="stAlert"] {
    border-radius: 4px !important;
    border-left-width: 3px !important;
    font-family: 'Geist Mono', monospace !important;
    font-size: 11px !important;
    line-height: 1.5 !important;
}

/* ── Radio sidebar ── */
[data-testid="stSidebar"] .stRadio label {
    font-family: 'Geist Mono', monospace !important;
    font-size: 10px !important;
    color: #6B7A99 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    padding: 3px 0 !important;
    line-height: 1.6 !important;
}
[data-testid="stSidebar"] .stRadio label:hover { color: #C8FF00 !important; }
[data-testid="stSidebar"] [aria-checked="true"] + span { color: #C8FF00 !important; }

/* ── Divider ── */
hr { border-color: #1A2642 !important; margin: 16px 0 !important; }

/* ── Progress bar ── */
.stProgress > div > div {
    background: linear-gradient(90deg, #1E2D5E, #C8FF00) !important;
    border-radius: 2px !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #060B15; }
::-webkit-scrollbar-thumb { background: #1E2D5E; border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: #C8FF00; }

/* ── Pulso animado ── */
@keyframes pulse-line {
    0%   { opacity: 0.4; }
    50%  { opacity: 1.0; }
    100% { opacity: 0.4; }
}
.am-pulse { animation: pulse-line 2s ease-in-out infinite; }

/* ── Cards de pipeline  [v501-CSS-1] border-left solo color, no clase extra ── */
.am-node {
    background: #0D1526;
    border: 1px solid #1A2642;
    border-left: 3px solid #1E2D5E;
    border-radius: 6px;
    padding: 10px 14px;
    margin: 4px 0;
    font-family: 'Geist Mono', monospace;
    font-size: 11px;
    line-height: 1.5;
}
/* Estado como atributo data, no clase adicional */
.am-node[data-estado="ok"]     { border-left-color: #00D4AA; }
.am-node[data-estado="warn"]   { border-left-color: #F59E0B; }
.am-node[data-estado="err"]    { border-left-color: #FF3B3B; }
.am-node[data-estado="db"]     { border-left-color: #7C3AED; }
.am-node[data-estado="radar"]  { border-left-color: #3B82F6; }

.am-node-title {
    font-weight: 600;
    font-size: 11px;
    color: #E2E8F0;
    font-family: 'Outfit', sans-serif;
    line-height: 1.4;
    display: block;
    margin-bottom: 2px;
}
.am-node-detail {
    color: #4A5568;
    font-size: 10px;
    line-height: 1.7;
    white-space: pre-line;
    overflow: hidden;
    text-overflow: ellipsis;
    margin-top: 2px;
}
.am-node-badge {
    float: right;
    font-size: 9px;
    letter-spacing: 0.1em;
    line-height: 1.4;
}
.badge-ok   { color: #00D4AA; }
.badge-warn { color: #F59E0B; }
.badge-err  { color: #FF3B3B; }

/* ── Cards liga/radar ── */
.am-liga-card {
    background: #0D1526;
    border: 1px solid #1A2642;
    border-radius: 6px;
    padding: 10px 14px 14px;
    margin: 4px 0;
    font-family: 'Geist Mono', monospace;
    font-size: 11px;
    line-height: 1.5;
    position: relative;
    overflow: hidden;
}
.am-liga-name {
    font-family: 'Outfit', sans-serif;
    font-weight: 700;
    font-size: 13px;
    color: #E2E8F0;
    display: block;
    margin-bottom: 2px;
}
.am-liga-fase {
    font-size: 10px;
    color: #6B7A99;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    line-height: 1.5;
    display: block;
    margin-bottom: 2px;
}
.am-liga-estado {
    font-size: 9px;
    float: right;
    font-weight: 600;
    letter-spacing: 0.12em;
    line-height: 1.4;
}
.am-liga-bar {
    position: absolute;
    bottom: 0;
    left: 0;
    height: 2px;
    border-radius: 0 0 6px 6px;
}

/* ── Partido en cola card  [v501-CSS-1] nowrap en teams ── */
.am-partido-card {
    background: #0D1526;
    border: 1px solid #1E2D5E;
    border-radius: 6px;
    padding: 12px 16px;
    margin: 6px 0;
    font-family: 'Geist Mono', monospace;
    font-size: 11px;
    border-left: 3px solid #3B82F6;
    line-height: 1.5;
    overflow: hidden;
}
.am-partido-id    { color: #3B82F6; font-size: 10px; letter-spacing: 0.1em; display: block; }
.am-partido-teams {
    font-family: 'Outfit', sans-serif;
    font-weight: 700;
    font-size: 14px;
    color: #FFFFFF;
    margin: 4px 0;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
}
.am-partido-meta  { color: #4A5568; font-size: 10px; line-height: 1.7; display: block; }

/* ── Badge cola ── */
.am-queue-badge {
    background: rgba(255,59,59,0.10);
    border: 1px solid #FF3B3B;
    border-radius: 4px;
    padding: 6px 10px;
    margin: 6px 0;
    font-size: 10px;
    color: #FF3B3B;
    font-family: 'Geist Mono', monospace;
    line-height: 1.5;
    display: block;
}

/* ── Estado global widget  [v501-EMPTY-1] ── */
.am-status-bar {
    background: #060B15;
    border: 1px solid #1A2642;
    border-radius: 6px;
    padding: 8px 14px;
    margin-bottom: 12px;
    font-family: 'Geist Mono', monospace;
    font-size: 10px;
    line-height: 1.8;
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    align-items: center;
}
.am-status-item { display: inline-flex; gap: 6px; align-items: center; }
.am-status-dot  { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }

/* ── Señal operar/no operar ── */
.signal-op   { color: #C8FF00; font-weight: 700; }
.signal-noop { color: #FF3B3B; font-weight: 700; }

/* ── Footer ── */
.am-footer {
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    background: #060B15;
    border-top: 1px solid #1A2642;
    padding: 6px 24px;
    font-family: 'Geist Mono', monospace;
    font-size: 9px;
    color: #1E2D5E;
    letter-spacing: 0.18em;
    text-align: center;
    z-index: 9999;
    line-height: 1.5;
}
.am-footer span { color: #C8FF00; }
</style>
"""


# ==============================================================================
# HELPERS — NORMALIZACIÓN Y VALIDACIÓN
# ==============================================================================
import difflib

def _normalizar_equipo_id(raw: str, liga_id: str, conn=None) -> tuple:
    raw = raw.strip().lower()
    if not raw:
        return "", "El ID de equipo no puede estar vacío."
    
    # Normalización básica original
    if raw.startswith(f"{liga_id}:"):
        slug = raw[len(liga_id)+1:]
        if not slug:
            return "", f"El nombre tras '{liga_id}:' está vacío."
        id_buscado = raw
    elif ":" not in raw:
        slug = raw.replace(" ", "_").replace("-", "_")
        id_buscado = f"{liga_id}:{slug}"
    else:
        otra_liga = raw.split(":")[0]
        if otra_liga != liga_id:
            return "", (
                f"El equipo '{raw}' pertenece a '{otra_liga}' "
                f"pero la liga seleccionada es '{liga_id}'. "
                f"Usa el formato '{liga_id}:nombre_equipo'."
            )
        id_buscado = raw

    # Validar y autocorregir (fuzzy matching) si hay conexión a la base de datos
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT equipo_id FROM dim_equipos WHERE liga_id=?", (liga_id,))
            equipos_validos = [row[0] for row in cur.fetchall()]
            
            if equipos_validos:
                # 1. Coincidencia exacta
                if id_buscado in equipos_validos:
                    return id_buscado, ""
                
                # 2. Búsqueda fuzzy por id completo (ej. liga_mx_apertura:america)
                matches = difflib.get_close_matches(id_buscado, equipos_validos, n=1, cutoff=0.6)
                if matches:
                    return matches[0], ""
                
                # 3. Búsqueda fuzzy solo por slug de nombre de equipo
                slug_buscado = id_buscado.split(":")[1] if ":" in id_buscado else id_buscado
                slugs_validos = {eq.split(":")[1]: eq for eq in equipos_validos if ":" in eq}
                
                matches_slug = difflib.get_close_matches(slug_buscado, list(slugs_validos.keys()), n=1, cutoff=0.5)
                if matches_slug:
                    return slugs_validos[matches_slug[0]], ""
                
                return "", f"Equipo '{raw}' no encontrado. ¿Quisiste decir alguno de estos? {', '.join([e.split(':')[1] for e in equipos_validos[:5]])}..."
        except Exception:
            pass # Falla silenciosa de DB, retrocedemos al comportamiento original

    return id_buscado, ""



def _safe_float(val) -> float:
    try:
        return float(str(val).strip())
    except Exception:
        return 0.0


def _safe_int(val) -> int:
    try:
        return int(float(str(val).strip()))
    except Exception:
        return 0


# ==============================================================================
# HELPERS — COLA DE PARTIDOS  [v52-GAP13]
# ------------------------------------------------------------------------------
# CORRECCIÓN ESTRUCTURAL: antes vivían en st.session_state (memoria volátil,
# se perdía en cada reinicio del proceso Streamlit). Ahora leen y escriben
# la tabla cola_partidos_pendientes de CEREBELLIUM v4.1 — un partido
# pendiente de cierre sobrevive a cualquier reinicio del proceso, sin
# importar cuánto tiempo pase entre INFERENCIA y CIERRE.
#
# Todas las funciones son defensivas (try/except -> degradan a lista vacía
# o no-op con aviso en UI) siguiendo el mismo patrón ya usado por
# query_df/tabla_existe/contar_tabla en este archivo, para que un problema
# de DB no tumbe la app completa.
# ==============================================================================
def _cola_get(conn) -> list:
    """Retorna la lista de partidos pendientes de cierre (cerrado=0), en
    orden de inserción (rowid ascendente == orden cronológico real)."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT payload_json FROM cola_partidos_pendientes "
            "WHERE cerrado=0 ORDER BY rowid ASC"
        )
        rows = cur.fetchall()
        return [json.loads(r[0]) for r in rows]
    except Exception as e:
        st.session_state["_cola_error"] = str(e)
        return []


def _cola_append(conn, partido: dict) -> None:
    """Inserta un nuevo partido en la cola persistente. partido_id es la
    clave primaria — INSERT OR REPLACE es seguro porque _gen_partido_id()
    ya garantiza unicidad práctica (liga + hora + milisegundos)."""
    try:
        conn.execute(
            "INSERT OR REPLACE INTO cola_partidos_pendientes "
            "(partido_id, timestamp, payload_json, cerrado) VALUES (?, ?, ?, 0)",
            (partido["partido_id"], partido.get("timestamp", ""), json.dumps(partido)),
        )
        conn.commit()
    except Exception as e:
        st.session_state["_cola_error"] = str(e)


def _cola_cerrar(conn, partido_id: str) -> None:
    """Marca un partido como cerrado (soft-close). Se usa tras registrar
    exitosamente el resultado real en CIERRE — preserva el registro en la
    tabla para trazabilidad/auditoría histórica, solo deja de contar como
    'pendiente'."""
    try:
        conn.execute(
            "UPDATE cola_partidos_pendientes SET cerrado=1 WHERE partido_id=?",
            (partido_id,),
        )
        conn.commit()
    except Exception as e:
        st.session_state["_cola_error"] = str(e)


def _cola_descartar(conn, partido_id: str) -> None:
    """Elimina físicamente un partido de la cola (hard delete). Se usa en
    'BORRAR ÚLTIMA' de INFERENCIA: la inferencia se descarta ANTES de
    convertirse en una apuesta real, no tiene valor de auditoría
    conservarla — a diferencia de _cola_cerrar()."""
    try:
        conn.execute(
            "DELETE FROM cola_partidos_pendientes WHERE partido_id=?",
            (partido_id,),
        )
        conn.commit()
    except Exception as e:
        st.session_state["_cola_error"] = str(e)


def _cola_ligas_duplicadas(conn) -> set:
    ligas = [p["liga_id"] for p in _cola_get(conn)]
    return {l for l in ligas if ligas.count(l) > 1}


def _gen_partido_id(liga_id: str) -> str:
    ahora = datetime.now()
    abrev = liga_id.upper().replace("_", "")[:8]
    ts    = ahora.strftime("%H%M%S")
    ms    = str(ahora.microsecond)[:3]
    return f"{abrev}-{ts}-{ms}"


# ==============================================================================
# HELPERS — ESTADO DE MÓDULO
# ==============================================================================
_MENU_KEYS   = ["OPERACION", "RADAR", "INFERENCIA", "CIERRE", "DASHBOARD",
                "AUDITORIA", "PIPELINE", "RENTABILIDAD"]
_MENU_LABELS_BASE = ["OPERACION", "RADAR", "INFERENCIA", "CIERRE", "DASHBOARD",
                     "AUDITORIA", "PIPELINE", "RENTABILIDAD"]


def _set_modulo(key: str):
    st.session_state["modulo_activo"] = key


def _get_modulo() -> str:
    return st.session_state.get("modulo_activo", "OPERACION")


# ==============================================================================
# CAPA DB — WAL + thread-safe
# ==============================================================================
@st.cache_resource
def conectar_cerebrillum():
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA cache_size = -8000;")
        return conn
    except sqlite3.Error as e:
        st.error(f"[DB CRÍTICO] {e}")
        st.stop()


def query_df(conn, sql: str, params=()) -> pd.DataFrame:
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


def tabla_existe(conn, nombre: str) -> bool:
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=?",
            (nombre,)
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def contar_tabla(conn, tabla: str) -> int:
    if tabla not in _TABLAS_PERMITIDAS:
        return -1
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {tabla}")
        return cur.fetchone()[0]
    except Exception:
        return -1


# ==============================================================================
# DIAGNÓSTICO DE SISTEMA
# ==============================================================================
@st.cache_data(ttl=300)
def _diagnostico_sistema() -> list:
    checks = []
    ahora  = datetime.now()

    checks.append({"nivel": "CRITICAL" if not MOTOR_DISPONIBLE else "INFO",
                   "msg":   "CONTINUITYv41 no importado" if not MOTOR_DISPONIBLE else "Motor CONTINUITYv41 activo",
                   "detalle": MOTOR_ERROR_MSG[:120] if not MOTOR_DISPONIBLE else ""})

    checks.append({"nivel": "WARN" if not ETL_DISPONIBLE else "INFO",
                   "msg":   "CEREBELLIUMv4 no importado" if not ETL_DISPONIBLE else "ETL CEREBELLIUM v4.1 activo",
                   "detalle": ETL_ERROR_MSG[:120] if not ETL_DISPONIBLE else ""})

    for path, label in [(STATE_FILE, "state.json"), (STATE_ONLINE, "state_online.json"), (STATE_BLEND, "state_blend.json")]:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    d = json.load(f)
                n = len(d) if isinstance(d, dict) else len(d.get("estado", {}))
                nivel = "WARN" if n > _LIMITE_EQUIPOS_JSON else "INFO"
                checks.append({"nivel": nivel, "msg": f"{label}: {n} entradas", "detalle": ""})
            except Exception as e:
                checks.append({"nivel": "CRITICAL", "msg": f"{label} corrupto", "detalle": str(e)[:80]})
        else:
            checks.append({"nivel": "INFO", "msg": f"{label}: aún no creado", "detalle": ""})

    if os.path.exists(FEATURE_STORE_DIR):
        pqs      = [f for f in os.listdir(FEATURE_STORE_DIR) if f.endswith(".parquet")]
        exp      = [p for p in pqs if (ahora - datetime.fromtimestamp(
                        os.path.getmtime(os.path.join(FEATURE_STORE_DIR, p))
                    )).total_seconds() > _LIMITE_PARQUET_HRS * 3600]
        nivel    = "WARN" if exp else "INFO"
        checks.append({"nivel": nivel,
                        "msg":   f"Feature Store: {len(pqs)} parquets, {len(exp)} expirados",
                        "detalle": ""})
    else:
        checks.append({"nivel": "INFO", "msg": "Feature Store: no creado", "detalle": ""})

    pkl_path = os.path.join(MODELOS_DIR, "motor_ia.pkl")
    if os.path.exists(pkl_path):
        age_d = (ahora - datetime.fromtimestamp(os.path.getmtime(pkl_path))).days
        sz_mb = os.path.getsize(pkl_path) / (1024 * 1024)
        checks.append({"nivel": "WARN" if age_d > 30 else "INFO",
                        "msg":   f"motor_ia.pkl: {age_d}d | {sz_mb:.1f}MB", "detalle": ""})
    else:
        checks.append({"nivel": "WARN", "msg": "motor_ia.pkl: sin caché", "detalle": ""})

    mes = ahora.month
    if mes in PARON_FIFA_MESES:
        checks.append({"nivel": "WARN", "msg": f"PARON FIFA MES {mes} — reducir stake ~20%", "detalle": ""})
    if ahora.year == 2026 and mes in [6, 7]:
        checks.append({"nivel": "INFO", "msg": "WORLD CUP 2026 ACTIVO", "detalle": ""})

    return checks


# ==============================================================================
# COMPONENTES VISUALES
# ==============================================================================
def _render_estado_bar(valor: float, maximo: float = 5.0,
                       color: str = "#C8FF00", altura: int = 4) -> str:
    pct = min(100.0, max(0.0, (valor / max(maximo, 0.001)) * 100.0))
    return (
        f"<div style='background:#1A2642;border-radius:2px;height:{altura}px;margin:4px 0;'>"
        f"<div style='background:{color};height:{altura}px;width:{pct:.1f}%;border-radius:2px;'></div></div>"
        f"<span style='font-size:9px;color:#4A5568;'>{valor:.3f}</span>"
    )


def _render_seccion_titulo(texto: str, sub: str = "") -> None:
    """[v501-CSS-1] Separación explícita header/contenido para evitar solapamiento."""
    sub_html = f"<div class='am-muted' style='margin-top:4px;'>{sub}</div>" if sub else ""
    st.markdown(
        f"<div class='am-section-header'>"
        f"<div class='am-title' style='font-size:18px;'>{texto}</div>"
        f"{sub_html}"
        f"</div>",
        unsafe_allow_html=True,
    )


def _render_partido_card(p: dict) -> None:
    loc_abrev = p["local"].split(":")[-1]  if ":" in p["local"]  else p["local"]
    vis_abrev = p["visita"].split(":")[-1] if ":" in p["visita"] else p["visita"]
    signal    = '<span class="signal-op">OPERAR</span>' if p.get("operar") else '<span class="signal-noop">NO OPERAR</span>'
    st.markdown(
        f"<div class='am-partido-card'>"
        f"<span class='am-partido-id'>{p['partido_id']}</span>"
        f"<span style='float:right;font-size:9px;color:#6B7A99;'>{p.get('timestamp','')}</span>"
        f"<span class='am-partido-teams'>{loc_abrev.upper()} vs {vis_abrev.upper()}</span>"
        f"<span class='am-partido-meta'>"
        f"λL={p['lam_l']:.3f} | λV={p['lam_v']:.3f} | "
        f"P1={p['p1']*100:.1f}% PX={p['px']*100:.1f}% P2={p['p2']*100:.1f}% | "
        f"EV={p['ev']*100:+.2f}% | {signal}"
        f"</span></div>",
        unsafe_allow_html=True,
    )


# ==============================================================================
# ESTADO GLOBAL WIDGET  [v501-EMPTY-1]
# Visible en todos los módulos: vincula objetos reales cargados.
# [v52-GAP13] n_cola ahora viene de la tabla persistente, no de session_state.
# ==============================================================================
def _render_estado_sistema_global(conn) -> None:
    """Barra persistente con estado real de todos los objetos del sistema."""
    motor_c  = "#00D4AA" if MOTOR_DISPONIBLE else "#FF3B3B"
    etl_c    = "#00D4AA" if ETL_DISPONIBLE   else "#F59E0B"
    db_ok    = os.path.exists(DB_NAME)
    db_c     = "#00D4AA" if db_ok else "#FF3B3B"
    state_c  = "#00D4AA" if os.path.exists(STATE_FILE)   else "#4A5568"
    on_c     = "#00D4AA" if os.path.exists(STATE_ONLINE) else "#4A5568"
    pkl_c    = "#00D4AA" if os.path.exists(os.path.join(MODELOS_DIR, "motor_ia.pkl")) else "#F59E0B"
    n_cola   = len(_cola_get(conn))
    cola_c   = "#3B82F6" if n_cola > 0 else "#4A5568"

    items = [
        (motor_c, f"MOTOR {'ON' if MOTOR_DISPONIBLE else 'OFF'}"),
        (etl_c,   f"ETL {'ON' if ETL_DISPONIBLE else 'OFF'}"),
        (db_c,    f"DB {'OK' if db_ok else 'FALTA'}"),
        (state_c, "STATE"),
        (on_c,    "ONLINE"),
        (pkl_c,   "PKL RF"),
        (cola_c,  f"COLA {n_cola}"),
    ]
    dots = "".join(
        f"<span class='am-status-item'>"
        f"<span class='am-status-dot' style='background:{c};'></span>"
        f"<span style='color:{c};'>{label}</span>"
        f"</span>"
        for c, label in items
    )
    st.markdown(
        f"<div class='am-status-bar'>{dots}</div>",
        unsafe_allow_html=True,
    )
    if st.session_state.get("_cola_error"):
        st.markdown(
            f"<div style='font-size:9px;color:#FF3B3B;margin:-8px 0 8px;'>"
            f"[COLA] error de persistencia: {st.session_state['_cola_error'][:160]}</div>",
            unsafe_allow_html=True,
        )


# ==============================================================================
# RADAR DATA
# ==============================================================================
def _radar_data() -> dict:
    ahora    = datetime.now()
    mes, ano = ahora.month, ahora.year

    def _activa(rango):
        ini, fin = rango
        return (mes >= ini or mes <= fin) if ini > fin else (ini <= mes <= fin)

    def _fase(lid, activa):
        if not activa: return "RECESO", "off"
        if lid in ("liga_mx_clausura", "liga_mx_apertura"):
            if mes in [11,12]: return "LIGUILLA", "hot"
            if mes == 10:      return "REPECHAJE", "warm"
            if mes in [1,7]:   return "APERTURA", "start"
        if lid == "champions_league":
            for ms_set, val in [
                ({9,10,11,12}, ("GRUPOS", "warm")),
                ({2,3},        ("OCTAVOS", "warm")),
                ({4},          ("CUARTOS/SEMIS", "hot")),
                ({5,6},        ("FINAL", "hot")),
            ]:
                if mes in ms_set: return val
        if lid == "mls" and mes in [10,11,12]: return "PLAYOFFS", "hot"
        if mes in [4,5]:  return "RECTA FINAL", "hot"
        if mes in [8,9]:  return "APERTURA", "start"
        if mes == 12:     return "RECESO", "off"
        return "REGULAR", "active"

    ligas = []
    for lid, meta in ALL_LIGAS.items():
        activa     = _activa(meta["rango"])
        fase, tipo = _fase(lid, activa)
        ligas.append({
            "liga_id":   lid, "nombre": meta["nombre"], "region": meta["region"],
            "activa":    activa, "fase": fase, "tipo": tipo,
            "avg_goles": meta.get("avg_goles", 2.5),
        })
    return {
        "timestamp":  ahora.strftime("%Y-%m-%d %H:%M"),
        "paron":      mes in PARON_FIFA_MESES,
        "mes":        mes,
        "copa_mundo": (ano == 2026 and mes in [6, 7]),
        "ligas":      ligas,
    }


# ==============================================================================
# MÓDULO 1 — RADAR DE MERCADOS
# ==============================================================================
def modulo_radar(conn):
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("RADAR DE MERCADOS", "SEÑALES ACTIVAS POR REGIÓN")
    data = _radar_data()

    COLOR_MAP = {
        "hot":    ("#FF3B3B", "ALTA"),
        "warm":   ("#F59E0B", "MEDIA"),
        "active": ("#00D4AA", "ACTIVA"),
        "start":  ("#3B82F6", "APERTURA"),
        "off":    ("#2D3748", "RECESO"),
    }

    if data["paron"]:
        st.markdown(
            f"<div style='background:rgba(245,158,11,0.08);border:1px solid #F59E0B;"
            f"border-radius:6px;padding:10px 16px;margin:8px 0;font-size:11px;color:#F59E0B;'>"
            f"PARON FIFA — MES {data['mes']} &nbsp;|&nbsp; Reducir exposure ~20%"
            "</div>", unsafe_allow_html=True
        )
    if data["copa_mundo"]:
        st.markdown(
            "<div style='background:rgba(0,212,170,0.08);border:1px solid #00D4AA;"
            "border-radius:6px;padding:10px 16px;margin:8px 0;font-size:11px;color:#00D4AA;'>"
            "WORLD CUP 2026 ACTIVO — Módulo predictivo disponible en MUNDIAL 2026"
            "</div>", unsafe_allow_html=True
        )

    st.markdown(
        f"<div class='am-muted' style='margin-bottom:16px;'>PULSO {data['timestamp']}</div>",
        unsafe_allow_html=True
    )

    col_nafta, col_sep, col_uefa = st.columns([1, 0.05, 1])
    for col, region in [(col_nafta, "NAFTA"), (col_uefa, "UEFA")]:
        with col:
            st.markdown(
                f"<div class='am-muted' style='margin-bottom:10px;'>"
                f"{'[ NAFTA / CONCACAF ]' if region=='NAFTA' else '[ UEFA / EUROPA ]'}"
                f"</div>", unsafe_allow_html=True
            )
            for liga in [l for l in data["ligas"] if l["region"] == region]:
                color_hex, nivel = COLOR_MAP.get(liga["tipo"], ("#4A5568", "—"))
                activa_txt       = "ACTIVA" if liga["activa"] else "RECESO"
                avg_g_pct        = min(100, int((liga["avg_goles"] / 4.0) * 100))
                st.markdown(
                    f"<div class='am-liga-card'>"
                    f"<span class='am-liga-estado' style='color:{color_hex};'>{nivel}</span>"
                    f"<span class='am-liga-name'>{liga['nombre']}</span>"
                    f"<span class='am-liga-fase'>{liga['fase']} · {activa_txt} · AVG {liga['avg_goles']} GOL</span>"
                    f"<div class='am-liga-bar' style='background:{color_hex};width:{avg_g_pct}%;'></div>"
                    f"</div>", unsafe_allow_html=True
                )
    with col_sep:
        st.markdown(
            "<div style='background:#1A2642;width:1px;min-height:400px;margin:24px auto;'></div>",
            unsafe_allow_html=True
        )

    st.divider()
    st.markdown("<div class='am-muted' style='margin-bottom:12px;'>TEMPERATURA DE MERCADO</div>", unsafe_allow_html=True)
    hot_ligas = [l for l in data["ligas"] if l["tipo"] == "hot"   and l["activa"]]
    act_ligas = [l for l in data["ligas"] if l["tipo"] in ("active","warm") and l["activa"]]
    off_ligas = [l for l in data["ligas"] if not l["activa"]]
    c1, c2, c3 = st.columns(3)
    c1.metric("ALTA PRIORIDAD", len(hot_ligas),  delta=f"{len(hot_ligas)} ligas críticas")
    c2.metric("ACTIVAS",        len(act_ligas),  delta="temporada regular")
    c3.metric("RECESO",         len(off_ligas),  delta="monitoreo pasivo")


# ==============================================================================
# MÓDULO 2 — MOTOR DE INFERENCIA v5.0.2
# [v501-INF-1] Botón BORRAR ÚLTIMA fuera del form
# [v501-INF-2] Modificadores dinámicos FINAL y MLS
# [v52-GAP13] Cola persistente — ver _cola_append/_cola_descartar
# ==============================================================================
def modulo_inferencia(conn):
    """
    Motor de inferencia -- cableado real contra CONTINUITY v4.5 (10 gaps).

      - [v502-GAP1a] Evaluación triple 1X2 (EscudoFinanciero.evaluar_triple),
        overround descontado, formulario con cuota_1/cuota_x/cuota_2.
      - [v51-GAP3]  Vector de inferencia vía
        DataPipeline.construir_vector_inferencia() -- mismo cálculo que usa
        construir_features() en entrenamiento (forma, h2h, tsr,
        diferenciales de equipo), no constantes hardcodeadas.
      - [v51-RHO]   Rho dinámico (RhoDinamicoCalculator) según MAE reciente
        del MotorOnline y apertura ofensiva del partido.
      - [v51-GAP6]  Ajuste post-blend por posición real en tabla
        (AjustadorCuantico.factor_tabla_posiciones).
      - [v51-GAP7]  Mercado Over/Under 2.5 opcional (misma matriz Poisson,
        EscudoFinanciero.evaluar_binario).
      - [v51-GAP9]  Penalización de stake por racha adversa
        (AuditorRentabilidad.racha_actual + EscudoFinanciero.evaluar_con_racha).
      - [v51-GAP8]  predictor_lider (RF/ONLINE) persistido en la cola para
        que CIERRE lo use al registrar el resultado real en AuditorRentabilidad.
      - [v52-GAP13] La cola vive en cola_partidos_pendientes (CEREBELLIUM
        v4.1), no en session_state -- sobrevive a reinicios del proceso.
    """
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("MOTOR DE INFERENCIA", "DIXON-COLES · POISSON ADAPTATIVO · KELLY 25% · 3 MERCADOS")

    if not MOTOR_DISPONIBLE:
        st.markdown(
            f"<div style='background:rgba(255,59,59,0.08);border:1px solid #FF3B3B;"
            f"border-radius:6px;padding:16px;font-size:12px;color:#FF3B3B;line-height:1.6;'>"
            f"MOTOR OFFLINE — {MOTOR_ERROR_MSG[:200]}"
            "</div>", unsafe_allow_html=True
        )
        return

    cola = _cola_get(conn)
    if cola:
        st.markdown(
            f"<div style='background:rgba(59,130,246,0.08);border:1px solid #3B82F6;"
            f"border-radius:6px;padding:8px 14px;font-size:11px;color:#3B82F6;margin-bottom:12px;line-height:1.6;'>"
            f"{len(cola)} PARTIDO(S) EN COLA — ve a CIERRE para registrar resultados"
            "</div>", unsafe_allow_html=True
        )

    # [v501-INF-1] Botón BORRAR ÚLTIMA — fuera del form
    # [v52-GAP13] descarta (DELETE real) en vez de solo remover de una lista
    # en memoria -- la fila desaparece también de cola_partidos_pendientes.
    if cola:
        col_b1, col_b2, _ = st.columns([1, 1, 4])
        with col_b1:
            if st.button("BORRAR ÚLTIMA", type="secondary", key="btn_borrar_ultima"):
                st.session_state["confirm_borrar_ultima"] = True
        with col_b2:
            if st.session_state.get("confirm_borrar_ultima"):
                if st.button("CONFIRMAR", type="primary", key="btn_confirm_borrar"):
                    ultimo = cola[-1]
                    _cola_descartar(conn, ultimo["partido_id"])
                    hist_i = st.session_state.get("historial_partidos_sesion", [])
                    if hist_i:
                        st.session_state["historial_partidos_sesion"] = hist_i[:-1]
                    st.session_state["confirm_borrar_ultima"] = False
                    st.rerun()

    ligas_disponibles = list(ALL_LIGAS.keys())

    # [v501-INF-2] Pre-leer si es final de torneo o MLS para deshabilitar jornada
    liga_sel_key = st.session_state.get("inf_liga_sel", ligas_disponibles[0])
    es_final_key = st.session_state.get("inf_es_final", False)
    deshabilitar_jornada = es_final_key or (liga_sel_key == "mls")

    with st.form("form_inferencia_v502"):
        st.markdown(
            "<div class='am-muted' style='margin-bottom:10px;'>CONTEXTO DEL PARTIDO</div>",
            unsafe_allow_html=True
        )
        c1, c2, c3 = st.columns(3)
        with c1:
            liga_id = st.selectbox(
                "LIGA", ligas_disponibles,
                index=ligas_disponibles.index(liga_sel_key) if liga_sel_key in ligas_disponibles else 0,
            )
        with c2:
            local  = st.text_input("EQUIPO LOCAL",     placeholder="nombre_equipo")
        with c3:
            visita = st.text_input("EQUIPO VISITANTE", placeholder="nombre_equipo")

        # [v502-GAP1a] Las tres cuotas del mercado 1X2
        st.markdown(
            "<div class='am-muted' style='margin:12px 0 8px;'>CUOTAS DEL MERCADO 1X2</div>",
            unsafe_allow_html=True
        )
        cq1, cq2, cq3, cq4 = st.columns(4)
        with cq1:
            cuota_1 = st.number_input("CUOTA 1 (LOCAL)",    min_value=1.01, value=2.10, step=0.05)
        with cq2:
            cuota_x = st.number_input("CUOTA X (EMPATE)",   min_value=1.01, value=3.30, step=0.05)
        with cq3:
            cuota_2 = st.number_input("CUOTA 2 (VISITA)",   min_value=1.01, value=3.50, step=0.05)
        with cq4:
            bankroll = st.number_input("BANKROLL $", min_value=100.0, value=1000.0, step=100.0)

        # [v51-GAP7] Mercado Over/Under 2.5 -- OPCIONAL, mismo patrón que el
        # CLI de CONTINUITY (main()): la probabilidad se calcula siempre desde
        # la misma matriz de Poisson Dixon-Coles (sin costo extra), pero la
        # evaluación financiera (Kelly) solo se ejecuta si el operador activa
        # el checkbox y provee ambas cuotas reales del mercado.
        st.markdown(
            "<div class='am-muted' style='margin:12px 0 8px;'>MERCADO OVER/UNDER 2.5 GOLES (OPCIONAL)</div>",
            unsafe_allow_html=True
        )
        cou1, cou2, cou3 = st.columns(3)
        with cou1:
            evaluar_ou = st.checkbox(
                "EVALUAR O/U 2.5",
                help="Activa el cálculo de EV y stake Kelly para Over/Under 2.5 con las cuotas de la derecha.",
            )
        with cou2:
            cuota_over = st.number_input("CUOTA OVER 2.5", min_value=1.01, value=1.90, step=0.05)
        with cou3:
            cuota_under = st.number_input("CUOTA UNDER 2.5", min_value=1.01, value=1.90, step=0.05)

        st.markdown(
            "<div class='am-muted' style='margin:12px 0 8px;'>MODIFICADORES</div>",
            unsafe_allow_html=True
        )
        c4, c5, c6 = st.columns(3)
        with c4:
            jornada = st.number_input(
                "JORNADA" + (" [DESHABILITADO]" if deshabilitar_jornada else ""),
                min_value=1, max_value=46, value=1 if deshabilitar_jornada else 10,
                disabled=deshabilitar_jornada,
            )
        with c5:
            temp_c = st.number_input("TEMP °C", value=22.0, step=1.0)
        with c6:
            asist  = st.slider("LLENADO ESTADIO", 0.0, 1.0, 0.8, 0.05)

        c7, c8, c9, c10 = st.columns(4)
        with c7:  es_clasi  = st.checkbox("CLASICO")
        with c8:  es_elim   = st.checkbox("ELIMINATORIA")
        with c9:  es_vuelta = st.checkbox("VUELTA")
        with c10: es_final  = st.checkbox(
            "FINAL TORNEO",
            help="Deshabilita el campo Jornada.",
        )

        c11, c12, _ = st.columns(3)
        with c11: desc_l = st.number_input("DESC LOCAL días",  min_value=0, max_value=30, value=3)
        with c12: desc_v = st.number_input("DESC VISITA días", min_value=0, max_value=30, value=3)

        ejecutar = st.form_submit_button("EJECUTAR MOTOR", type="primary", use_container_width=True)

    # [v501-INF-2] Persistir liga y flag final
    if st.session_state.get("inf_liga_sel") != liga_id or st.session_state.get("inf_es_final") != es_final:
        st.session_state["inf_liga_sel"] = liga_id
        st.session_state["inf_es_final"] = es_final
        if not ejecutar:
            st.rerun()

    if not ejecutar:
        hist_i = st.session_state.get("historial_partidos_sesion", [])
        if hist_i:
            st.divider()
            st.markdown("<div class='am-muted' style='margin-bottom:8px;'>INFERENCIAS DE SESION (pendientes de cierre)</div>", unsafe_allow_html=True)
            rows = []
            for h in hist_i:
                rows.append({
                    "HORA":    h.get("ts", "—"),
                    "LIGA":    h.get("liga", "—"),
                    "LOCAL":   h.get("local", "—"),
                    "VISITA":  h.get("visita", "—"),
                    "P1":      f"{h.get('p1', 0)*100:.1f}%",
                    "PX":      f"{h.get('px', 0)*100:.1f}%",
                    "P2":      f"{h.get('p2', 0)*100:.1f}%",
                    "EV":      f"{h.get('ev', 0)*100:+.2f}%",
                    "MERCADO": h.get("mercado_label", "—"),
                    "SEÑAL":   "OPERAR" if h.get("operar") else "—",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        return

    # Validar IDs
    local_id,  err_l = _normalizar_equipo_id(local,  liga_id, conn)
    visita_id, err_v = _normalizar_equipo_id(visita, liga_id, conn)
    if err_l:
        st.error(f"LOCAL: {err_l}")
        return
    if err_v:
        st.error(f"VISITANTE: {err_v}")
        return

    progress = st.progress(0, text="Iniciando pipeline v5.0.2...")
    pasos = [
        (14,  "FeatureStore — verificando cache Parquet..."),
        (28,  "DataPipeline — lambdas sliding window..."),
        (42,  "EstadoAdaptativo + MotorOnline — inercias EWMA..."),
        (56,  "AjustadorCuantico — blend xG + termal..."),
        (70,  "MotorInferenciaIA — RF/GBM prediccion..."),
        (85,  "TransmutadorEstocastico — Dixon-Coles τ..."),
        (100, "EscudoFinanciero — Kelly 25% · 3 mercados..."),
    ]

    try:
        pipeline      = DataPipeline(DB_NAME)
        feature_store = FeatureStore(DB_NAME)
        estado_obj    = EstadoAdaptativo()
        motor_online  = MotorOnline()
        blend_obj     = BlendAdaptativo()
        ajustador     = AjustadorCuantico()
        transmutor    = TransmutadorEstocastico()

        progress.progress(pasos[0][0], text=pasos[0][1])
        meta = pipeline.obtener_meta_liga(liga_id)

        progress.progress(pasos[1][0], text=pasos[1][1])
        lam_l_sw, lam_v_sw = pipeline.obtener_lambdas_sliding_window(
            local_id, visita_id, liga_id, meta, feature_store
        )

        progress.progress(pasos[2][0], text=pasos[2][1])
        estado_obj.inicializar_equipo(local_id,  liga_id, meta["lambda_a"])
        estado_obj.inicializar_equipo(visita_id, liga_id, meta["lambda_d"])
        motor_online.inicializar(local_id,  meta["lambda_a"])
        motor_online.inicializar(visita_id, meta["lambda_d"])
        lam_inercia_l = estado_obj.memoria.get(local_id,  {}).get("ataque", meta["lambda_a"])
        lam_inercia_v = estado_obj.memoria.get(visita_id, {}).get("ataque", meta["lambda_d"])
        lam_l_online, lam_v_online = motor_online.predecir(local_id, visita_id, liga_id, meta)

        progress.progress(pasos[3][0], text=pasos[3][1])
        payload_l = pipeline.obtener_payload_micro(local_id)
        payload_v = pipeline.obtener_payload_micro(visita_id)
        payload_l["temperatura_celsius"] = temp_c
        payload_v["temperatura_celsius"] = temp_c
        lam_l_adj, _ = ajustador.aplicar(lam_l_sw, lam_v_sw, payload_l)
        lam_v_adj, _ = ajustador.aplicar(lam_v_sw, lam_l_sw, payload_v)

        progress.progress(pasos[4][0], text=pasos[4][1])
        jornada_efectiva = 1 if deshabilitar_jornada else jornada
        # [v51-GAP3] Vector construido con señales REALES del historial (forma,
        # h2h, tsr, diferenciales de prestigio/altitud/ELO) vía
        # DataPipeline.construir_vector_inferencia() -- el MISMO cálculo que
        # construir_features() usa en entrenamiento. Reemplaza el vector
        # hardcodeado (forma_pts_local_5=7.5, h2h_win_rate_local=0.45,
        # tsr_local=0.5, dif_prestigio/altitud/elo=0) que producía un
        # desajuste de distribución entre lo que el RF aprendió y lo que
        # recibía al predecir (el docstring del módulo ya afirmaba esto
        # resuelto -- esta es la implementación real que faltaba).
        vector = pipeline.construir_vector_inferencia(
            equipo_local=local_id, equipo_visitante=visita_id, liga_id=liga_id, meta=meta,
            numero_jornada=jornada_efectiva, es_clasico=int(es_clasi),
            es_eliminatoria=int(es_elim or es_final), es_partido_vuelta=int(es_vuelta),
            dias_descanso_local=desc_l, dias_descanso_vis=desc_v,
            factor_presion=asist, temperatura=temp_c,
        )
        motor_base = MotorInferenciaIA()
        motor_base.cargar_modelos()
        lam_l_rf, lam_v_rf, src_rf = motor_base.predecir(vector, liga_id)

        progress.progress(pasos[5][0], text=pasos[5][1])
        wb, wo = blend_obj.pesos(liga_id)
        lam_l_sw_puro = 0.40 * lam_l_adj + 0.20 * lam_inercia_l
        lam_v_sw_puro = 0.40 * lam_v_adj + 0.20 * lam_inercia_v
        lam_l_final   = float(np.clip(lam_l_sw_puro + 0.40 * (wb * lam_l_rf + wo * lam_l_online), 0.2, 5.0))
        lam_v_final   = float(np.clip(lam_v_sw_puro + 0.40 * (wb * lam_v_rf + wo * lam_v_online), 0.2, 5.0))

        # [v51-GAP6] Ajuste post-blend por posición real en tabla (puntos por
        # partido del equipo vs. promedio de la liga en la temporada en
        # curso). Neutro (factor=1.0) si la liga no tiene historial de
        # temporada o el equipo aún no jugó AjustadorCuantico.MIN_PJ_TABLA
        # partidos -- nunca degrada el comportamiento anterior.
        temporada_actual = pipeline.obtener_temporada_actual(liga_id)
        tabla_posiciones = pipeline.calcular_tabla_posiciones(liga_id, temporada_actual) \
                           if temporada_actual else {}
        factor_tabla_l = AjustadorCuantico.factor_tabla_posiciones(tabla_posiciones, local_id)
        factor_tabla_v = AjustadorCuantico.factor_tabla_posiciones(tabla_posiciones, visita_id)
        lam_l_final = float(np.clip(lam_l_final * factor_tabla_l, 0.2, 5.0))
        lam_v_final = float(np.clip(lam_v_final * factor_tabla_v, 0.2, 5.0))

        es_inicio     = 1 if jornada_efectiva <= 3 else 0
        rho_base_liga = float(meta.get("rho", -0.04))
        # [v51-RHO] Rho dinámico endógeno (RhoDinamicoCalculator): responde al
        # MAE reciente del MotorOnline para esta liga y a la apertura
        # ofensiva del partido (suma de lambdas), en lugar del rho estático
        # fijo por liga que se usaba antes. Mismo componente que ya usa
        # CONTINUITY.main() (CLI) -- aquí solo faltaba invocarlo desde la UI.
        mae_liga     = motor_online.mae_reciente(liga_id, n=10)
        rho_dinamico = RhoDinamicoCalculator.calcular(
            lam_l_final, lam_v_final, rho_base_liga, mae_liga
        )
        p1, px, p2 = transmutor.calcular_1x2(
            lam_l_final, lam_v_final, es_inicio, int(es_clasi),
            rho_liga=rho_base_liga, rho_dinamico=rho_dinamico,
        )

        # [v51-GAP9] Racha de derrotas consecutivas de esta liga (Capa 9b,
        # AuditorRentabilidad.racha_actual) -- penaliza el stake sugerido sin
        # dejar de operar mientras el EV siga siendo positivo tras el filtro
        # de edge (Gap4). Sustituye la llamada directa a evaluar_triple().
        progress.progress(pasos[6][0], text=pasos[6][1])
        auditor = AuditorRentabilidad()
        racha   = auditor.racha_actual(liga_id)
        operar, mercado, ev, stake, detalle_fin = EscudoFinanciero.evaluar_con_racha(
            p1, px, p2, cuota_1, cuota_x, cuota_2, bankroll, racha=racha
        )

        # Mapa de cuotas para recuperar la cuota del mercado elegido en UI
        cuotas_mapa     = {"1": cuota_1, "X": cuota_x, "2": cuota_2}
        cuota_elegida   = cuotas_mapa.get(mercado, 0.0) if mercado else 0.0
        mercado_label   = MERCADOS_LABELS.get(mercado, "—") if mercado else "—"
        # [v51-GAP8] Predictor líder del blend en el momento de esta
        # predicción -- se persiste en la cola para que CIERRE pueda
        # etiquetar la apuesta al registrarla en AuditorRentabilidad, base de
        # atribución de BlendAdaptativo.ajustar_por_roi().
        predictor_lider = "RF" if wb >= wo else "ONLINE"

        # [v51-GAP7] Over/Under 2.5 -- misma matriz de Poisson Dixon-Coles del
        # 1X2 (transmutor.calcular_over_under reutiliza _matriz_poisson), sin
        # recálculo de lambdas ni costo extra de inferencia. La evaluación
        # financiera (EscudoFinanciero.evaluar_binario) es un mercado
        # independiente del 1X2 -- no compite por el mismo stake ni altera
        # `operar`/`mercado`/`stake` del bloque 1X2 de arriba.
        p_over, p_under = transmutor.calcular_over_under(
            lam_l_final, lam_v_final, rho_liga=rho_base_liga,
            rho_dinamico=rho_dinamico, linea=2.5
        )
        operar_ou, mercado_ou, ev_ou, stake_ou, detalle_ou = False, None, 0.0, 0.0, {}
        if evaluar_ou:
            operar_ou, mercado_ou, ev_ou, stake_ou, detalle_ou = EscudoFinanciero.evaluar_binario(
                p_over, p_under, cuota_over, cuota_under, bankroll, etiquetas=("OVER", "UNDER")
            )

        time.sleep(0.2)
        progress.empty()

        partido_id = _gen_partido_id(liga_id)
        timestamp  = datetime.now().strftime("%H:%M:%S")

        # [v52-GAP13] Cola de partidos ahora persiste en SQLite
        # (cola_partidos_pendientes) — nuevos campos siguen siendo aditivos,
        # no rompen CIERRE, y ahora además sobreviven a un reinicio del
        # proceso Streamlit.
        _cola_append(conn, {
            "partido_id":    partido_id,
            "timestamp":     timestamp,
            "local":         local_id,
            "visita":        visita_id,
            "liga_id":       liga_id,
            "lam_l":         lam_l_final,
            "lam_v":         lam_v_final,
            "lam_l_online":  lam_l_online,
            "lam_v_online":  lam_v_online,
            "p1": p1, "px": px, "p2": p2,
            "ev": ev, "stake": stake, "operar": operar,
            "wb": wb, "wo": wo, "src": src_rf,
            "bankroll":      bankroll,
            # [v502-GAP1a] nuevos campos
            "mercado":       mercado,
            "mercado_label": mercado_label,
            "cuota_elegida": cuota_elegida,
            "cuota_1":       cuota_1,
            "cuota_x":       cuota_x,
            "cuota_2":       cuota_2,
            "detalle_fin":   detalle_fin,
            # [v51-GAP8/9] Contexto de blend y riesgo en el momento de la
            # predicción -- CIERRE lo usa para registrar el resultado
            # monetario real en AuditorRentabilidad con atribución correcta.
            "predictor_lider": predictor_lider,
            "racha":           racha,
            "rho_base_liga":   rho_base_liga,
            "rho_dinamico":    rho_dinamico,
            "factor_tabla_l":  factor_tabla_l,
            "factor_tabla_v":  factor_tabla_v,
            # [v51-GAP7] Mercado Over/Under 2.5 -- segundo mercado, registrado
            # en CIERRE de forma independiente del 1X2 (ver modulo_feedback).
            "p_over":        p_over,
            "p_under":       p_under,
            "evaluar_ou":    evaluar_ou,
            "cuota_over":    cuota_over,
            "cuota_under":   cuota_under,
            "operar_ou":     operar_ou,
            "mercado_ou":    mercado_ou,
            "ev_ou":         ev_ou,
            "stake_ou":      stake_ou,
            "detalle_ou":    detalle_ou,
        })

        if "historial_partidos_sesion" not in st.session_state:
            st.session_state["historial_partidos_sesion"] = []
        st.session_state["historial_partidos_sesion"].append({
            "ts":           timestamp,
            "liga":         liga_id,
            "local":        local_id.split(":")[-1],
            "visita":       visita_id.split(":")[-1],
            "p1": p1, "px": px, "p2": p2,
            "ev": ev, "stake": stake, "operar": operar,
            "lam_l":        lam_l_final,
            "lam_v":        lam_v_final,
            "mercado":      mercado,
            "mercado_label": mercado_label,
        })

    except Exception as e:
        progress.empty()
        st.error(f"ERROR EN MOTOR: {e}")
        st.code(traceback.format_exc(), language="text")
        return

    # ── Resultados ──
    st.divider()
    loc_abrev = local_id.split(":")[-1]
    vis_abrev = visita_id.split(":")[-1]
    overround_pct = detalle_fin.get("overround", 0.0) * 100

    st.markdown(
        f"<div style='background:#0D1526;border:1px solid #1E2D5E;border-radius:8px;"
        f"padding:14px 20px;margin-bottom:16px;'>"
        f"<div class='am-muted'>INFERENCIA — {partido_id}</div>"
        f"<div class='am-title' style='font-size:20px;margin:6px 0;'>"
        f"{loc_abrev.upper()} <span style='color:#4A5568;'>vs</span> {vis_abrev.upper()}"
        f"</div>"
        f"<div style='font-size:11px;color:#4A5568;line-height:1.6;'>"
        f"{liga_id.upper()} · BLEND {wb:.2f}·RF + {wo:.2f}·ONLINE · {src_rf} · "
        f"Overround mercado: {overround_pct:.1f}%"
        f"</div>"
        f"<div style='margin-top:4px;font-size:10px;color:#00D4AA;'>"
        f"COLA: {len(_cola_get(conn))} pendiente(s)</div>"
        f"</div>",
        unsafe_allow_html=True
    )

    # Lambdas
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>LAMBDAS FINALES</div>", unsafe_allow_html=True)
    ml1, ml2, ml3, ml4 = st.columns(4)
    with ml1:
        st.markdown(
            f"<div style='font-size:10px;color:#4A5568;'>λ LOCAL</div>"
            f"<div style='font-size:20px;font-family:Geist Mono;color:#FFFFFF;'>{lam_l_final:.3f}</div>"
            f"{_render_estado_bar(lam_l_final, 4.0, '#C8FF00')}",
            unsafe_allow_html=True
        )
    with ml2:
        st.markdown(
            f"<div style='font-size:10px;color:#4A5568;'>λ VISITANTE</div>"
            f"<div style='font-size:20px;font-family:Geist Mono;color:#FFFFFF;'>{lam_v_final:.3f}</div>"
            f"{_render_estado_bar(lam_v_final, 4.0, '#00D4AA')}",
            unsafe_allow_html=True
        )
    with ml3:
        st.markdown(
            f"<div style='font-size:10px;color:#4A5568;'>λ ONLINE LOCAL</div>"
            f"<div style='font-size:20px;font-family:Geist Mono;color:#6B7A99;'>{lam_l_online:.3f}</div>"
            f"{_render_estado_bar(lam_l_online, 4.0, '#3B82F6')}",
            unsafe_allow_html=True
        )
    with ml4:
        st.markdown(
            f"<div style='font-size:10px;color:#4A5568;'>ρ DINÁMICO</div>"
            f"<div style='font-size:20px;font-family:Geist Mono;color:#6B7A99;'>{rho_dinamico:.4f}</div>"
            f"<div style='font-size:9px;color:#4A5568;margin-top:4px;'>"
            f"base {rho_base_liga:.4f} · MAE liga {mae_liga:.3f}</div>",
            unsafe_allow_html=True
        )

    # Distribución de probabilidad
    st.divider()
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>DISTRIBUCIÓN DE PROBABILIDAD</div>", unsafe_allow_html=True)
    pp1, ppx, pp2, pps = st.columns(4)

    def _prob_block(col, label, val, color):
        col.markdown(
            f"<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
            f"padding:12px;text-align:center;'>"
            f"<div style='font-size:10px;color:#4A5568;letter-spacing:0.1em;line-height:1.6;'>{label}</div>"
            f"<div style='font-family:Outfit,sans-serif;font-size:26px;font-weight:800;"
            f"color:{color};margin:6px 0;line-height:1.2;'>{val*100:.1f}%</div>"
            f"<div style='background:#1A2642;border-radius:2px;height:3px;'>"
            f"<div style='background:{color};width:{int(val*100)}%;height:3px;border-radius:2px;'></div>"
            f"</div></div>",
            unsafe_allow_html=True
        )

    _prob_block(pp1, "LOCAL  (1)", p1, "#C8FF00")
    _prob_block(ppx, "EMPATE (X)", px, "#F59E0B")
    _prob_block(pp2, "VISITA (2)", p2, "#FF3B3B")
    pps.markdown(
        f"<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
        f"padding:12px;text-align:center;'>"
        f"<div style='font-size:10px;color:#4A5568;letter-spacing:0.1em;line-height:1.6;'>SUMA</div>"
        f"<div style='font-family:Outfit,sans-serif;font-size:26px;font-weight:800;"
        f"color:#00D4AA;margin:6px 0;line-height:1.2;'>{(p1+px+p2)*100:.2f}%</div>"
        f"<div style='font-size:9px;color:#4A5568;'>~100%</div>"
        f"</div>",
        unsafe_allow_html=True
    )

    # EV de los 3 mercados + señal financiera
    st.divider()
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>EV POR MERCADO · SEÑAL FINANCIERA KELLY 25%</div>", unsafe_allow_html=True)

    # Fila de EVs de los 3 mercados
    ev_cols = st.columns(3)
    ev_data = [
        ("EV LOCAL (1)",   detalle_fin.get("ev_1", 0.0),  detalle_fin.get("edge_1", 0.0),  cuota_1, "#C8FF00"),
        ("EV EMPATE (X)",  detalle_fin.get("ev_x", 0.0),  detalle_fin.get("edge_x", 0.0),  cuota_x, "#F59E0B"),
        ("EV VISITA (2)",  detalle_fin.get("ev_2", 0.0),  detalle_fin.get("edge_2", 0.0),  cuota_2, "#FF3B3B"),
    ]
    for col, (label, ev_val, edge_val, cuota_val, color) in zip(ev_cols, ev_data):
        es_elegido = (
            mercado == label[10:11] or
            (label.endswith("(1)") and mercado == "1") or
            (label.endswith("(X)") and mercado == "X") or
            (label.endswith("(2)") and mercado == "2")
        )
        borde = f"border: 1px solid {color};" if es_elegido else "border: 1px solid #1A2642;"
        ev_color = "#00D4AA" if ev_val > EV_THRESHOLD else ("#F59E0B" if ev_val > 0 else "#FF3B3B")
        col.markdown(
            f"<div style='background:#0D1526;{borde}border-radius:6px;"
            f"padding:10px;text-align:center;line-height:1.6;'>"
            f"<div style='font-size:10px;color:#4A5568;'>{label}</div>"
            f"<div style='font-size:18px;font-family:Geist Mono;color:{ev_color};'>"
            f"{ev_val*100:+.2f}%</div>"
            f"<div style='font-size:9px;color:#4A5568;'>cuota {cuota_val:.2f} · edge {edge_val:+.3f}</div>"
            f"</div>",
            unsafe_allow_html=True
        )

    st.markdown("<div style='margin-top:12px;'></div>", unsafe_allow_html=True)

    # [v51-GAP9] Aviso de racha adversa -- solo si hay penalización activa
    if racha > 0:
        factor_racha    = detalle_fin.get("factor_racha", 1.0)
        stake_sin_penal = detalle_fin.get("stake_sin_penalizar", stake)
        st.markdown(
            f"<div style='background:rgba(245,158,11,0.08);border:1px solid #F59E0B;"
            f"border-radius:6px;padding:8px 14px;font-size:11px;color:#F59E0B;margin-bottom:10px;line-height:1.6;'>"
            f"RACHA {liga_id.upper()}: {racha} derrota(s) consecutiva(s) — stake penalizado "
            f"×{factor_racha:.2f} (${stake_sin_penal:.2f} → ${stake:.2f})"
            f"</div>", unsafe_allow_html=True
        )

    # Señal financiera
    sf1, sf2, sf3 = st.columns(3)
    sf1.metric("EV MERCADO ELEGIDO", f"{ev*100:+.2f}%",  delta=f"umbral {EV_THRESHOLD*100:.0f}%")
    sf2.metric("STAKE SUGERIDO", f"${stake:.2f}",
               delta=f"{(stake/bankroll)*100:.2f}% bankroll" if operar else "—")
    if operar:
        sf3.markdown(
            f"<div style='background:rgba(200,255,0,0.08);border:1px solid #C8FF00;"
            f"border-radius:6px;padding:14px;text-align:center;line-height:1.5;'>"
            f"<div class='am-title' style='color:#C8FF00;font-size:15px;'>OPERAR</div>"
            f"<div style='font-size:12px;color:#9DB4A0;margin-top:4px;'>"
            f"${stake:.2f} → {mercado_label.upper()}</div>"
            f"<div style='font-size:10px;color:#6B7A99;margin-top:2px;'>"
            f"cuota {cuota_elegida:.2f} · overround {overround_pct:.1f}%</div>"
            f"</div>", unsafe_allow_html=True
        )
    else:
        sf3.markdown(
            "<div style='background:rgba(255,59,59,0.08);border:1px solid #FF3B3B;"
            "border-radius:6px;padding:14px;text-align:center;line-height:1.5;'>"
            "<div class='am-title' style='color:#FF3B3B;font-size:15px;'>NO OPERAR</div>"
            "<div style='font-size:11px;color:#9B7070;margin-top:4px;'>EV insuficiente en los 3 mercados</div>"
            "</div>", unsafe_allow_html=True
        )

    # [v51-GAP7] Mercado Over/Under 2.5 — segundo mercado, independiente del
    # 1X2. La probabilidad se muestra siempre (viene de la misma matriz
    # Dixon-Coles ya calculada arriba); el EV/stake/señal Kelly solo se
    # calculan y muestran si el operador activó "EVALUAR O/U 2.5" en el
    # formulario.
    st.divider()
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>MERCADO OVER/UNDER 2.5 GOLES</div>", unsafe_allow_html=True)

    pou1, pou2 = st.columns(2)
    _prob_block(pou1, "OVER 2.5",  p_over,  "#3B82F6")
    _prob_block(pou2, "UNDER 2.5", p_under, "#7C3AED")

    if evaluar_ou:
        st.markdown("<div style='margin-top:12px;'></div>", unsafe_allow_html=True)
        souf1, souf2, souf3 = st.columns(3)
        ev_over_pct  = detalle_ou.get("ev_over", 0.0)  * 100
        ev_under_pct = detalle_ou.get("ev_under", 0.0) * 100
        souf1.metric("EV OVER",  f"{ev_over_pct:+.2f}%",  delta=f"cuota {cuota_over:.2f}")
        souf2.metric("EV UNDER", f"{ev_under_pct:+.2f}%", delta=f"cuota {cuota_under:.2f}")
        if operar_ou:
            souf3.markdown(
                f"<div style='background:rgba(59,130,246,0.08);border:1px solid #3B82F6;"
                f"border-radius:6px;padding:14px;text-align:center;line-height:1.5;'>"
                f"<div class='am-title' style='color:#3B82F6;font-size:15px;'>OPERAR</div>"
                f"<div style='font-size:12px;color:#9DB4A0;margin-top:4px;'>"
                f"${stake_ou:.2f} → {mercado_ou}</div>"
                f"<div style='font-size:10px;color:#6B7A99;margin-top:2px;'>"
                f"EV {ev_ou*100:+.2f}%</div>"
                f"</div>", unsafe_allow_html=True
            )
        else:
            souf3.markdown(
                "<div style='background:rgba(255,59,59,0.08);border:1px solid #FF3B3B;"
                "border-radius:6px;padding:14px;text-align:center;line-height:1.5;'>"
                "<div class='am-title' style='color:#FF3B3B;font-size:15px;'>NO OPERAR</div>"
                "<div style='font-size:11px;color:#9B7070;margin-top:4px;'>EV/edge insuficiente</div>"
                "</div>", unsafe_allow_html=True
            )
    else:
        st.markdown(
            "<div style='font-size:10px;color:#4A5568;'>"
            "Activa 'EVALUAR O/U 2.5' en el formulario para calcular EV y stake Kelly de este mercado."
            "</div>", unsafe_allow_html=True
        )


# ==============================================================================
# MÓDULO 3 — CIERRE DE JORNADA
# [v52-GAP13] Cola persistente — lee/escribe cola_partidos_pendientes
# ==============================================================================
def modulo_feedback(conn):
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("CIERRE DE JORNADA", "LAZO CERRADO — ACTUALIZACIÓN DE ESTADO ADAPTATIVO")

    cola = _cola_get(conn)
    if not cola:
        st.markdown(
            "<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
            "padding:32px;text-align:center;color:#4A5568;font-size:12px;line-height:1.6;'>"
            "COLA VACÍA — Ejecuta una inferencia en MOTOR DE INFERENCIA"
            "</div>", unsafe_allow_html=True
        )
        return

    ligas_dup = _cola_ligas_duplicadas(conn)
    if ligas_dup:
        st.markdown(
            f"<div style='background:rgba(245,158,11,0.08);border:1px solid #F59E0B;"
            f"border-radius:6px;padding:10px 16px;font-size:11px;color:#F59E0B;margin-bottom:12px;line-height:1.6;'>"
            f"ORDEN DE CIERRE — Ligas con múltiples partidos: {'  ·  '.join(ligas_dup).upper()}. "
            f"Cierra en orden cronológico real."
            f"</div>", unsafe_allow_html=True
        )

    st.markdown(f"<div class='am-muted' style='margin-bottom:10px;'>COLA: {len(cola)} PARTIDO(S)</div>", unsafe_allow_html=True)

    filas = []
    for p in cola:
        filas.append({
            "ID":        p["partido_id"],
            "HORA":      p.get("timestamp", "—"),
            "LIGA":      p["liga_id"].upper(),
            "LOCAL":     p["local"].split(":")[-1].upper()  if ":" in p["local"]  else p["local"].upper(),
            "VISITANTE": p["visita"].split(":")[-1].upper() if ":" in p["visita"] else p["visita"].upper(),
            "λL":        f"{p['lam_l']:.3f}",
            "λV":        f"{p['lam_v']:.3f}",
            "P1":        f"{p['p1']*100:.1f}%",
            "EV":        f"{p['ev']*100:+.2f}%",
            "SEÑAL":     "OPERAR" if p.get("operar") else "—",
        })
    st.dataframe(pd.DataFrame(filas), use_container_width=True, hide_index=True)
    st.divider()

    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>SELECCIONAR PARTIDO</div>", unsafe_allow_html=True)
    opciones = {}
    for p in cola:
        la = p["local"].split(":")[-1]  if ":" in p["local"]  else p["local"]
        va = p["visita"].split(":")[-1] if ":" in p["visita"] else p["visita"]
        label = f"{p['partido_id']}  —  {la.upper()} vs {va.upper()}  ({p['liga_id'].upper()})"
        opciones[label] = p["partido_id"]

    sel_label = st.radio("Partido", list(opciones.keys()), label_visibility="collapsed")
    pid_sel   = opciones[sel_label]
    p         = next((x for x in cola if x["partido_id"] == pid_sel), None)
    if p is None:
        st.error("Partido no encontrado.")
        return

    _render_partido_card(p)

    with st.form(f"form_cierre_{pid_sel}"):
        loc_a = p["local"].split(":")[-1]  if ":" in p["local"]  else p["local"]
        vis_a = p["visita"].split(":")[-1] if ":" in p["visita"] else p["visita"]
        cg1, cg2, cg3 = st.columns(3)
        with cg1: goles_l_r = st.number_input(f"GOLES {loc_a.upper()}", min_value=0, max_value=20, value=0, key=f"gl_{pid_sel}")
        with cg2: goles_v_r = st.number_input(f"GOLES {vis_a.upper()}", min_value=0, max_value=20, value=0, key=f"gv_{pid_sel}")
        with cg3: anomalia  = st.checkbox("ANOMALIA CRITICA", key=f"an_{pid_sel}")
        cerrar = st.form_submit_button("CERRAR LAZO", type="primary", use_container_width=True)

    if not cerrar:
        return
    if not MOTOR_DISPONIBLE:
        st.error("Motor no disponible para cerrar lazo.")
        return

    with st.spinner("Actualizando estado adaptativo..."):
        try:
            estado_obj   = EstadoAdaptativo()
            motor_online = MotorOnline()
            blend_obj    = BlendAdaptativo()
            feedback     = RegistradorFeedback(estado_obj, motor_online, blend_obj)
            res = feedback.registrar(
                p["local"], p["visita"],
                goles_l_r, goles_v_r,
                p["lam_l"], p["lam_v"],
                p.get("lam_l_online", p["lam_l"]),
                p.get("lam_v_online", p["lam_v"]),
                p["liga_id"], bool(anomalia),
                rho_usado=p.get("rho_dinamico"),
            )

            # [v51-GAP5+8] Registrar el resultado MONETARIO real (Capa 9b),
            # separado del error de lambda (Capa 9) que RegistradorFeedback
            # ya cerró arriba. "gano" compara el resultado 1X2 real contra el
            # mercado EFECTIVAMENTE apostado (no solo victoria local).
            # predictor_lider viaja desde INFERENCIA para habilitar
            # BlendAdaptativo.ajustar_por_roi() con atribución real.
            resultado_roi = None
            if p.get("operar") and p.get("mercado"):
                auditor_cierre = AuditorRentabilidad()
                resultado_real = "1" if goles_l_r > goles_v_r else ("X" if goles_l_r == goles_v_r else "2")
                gano_1x2 = (resultado_real == p["mercado"])
                resultado_roi = auditor_cierre.registrar_apuesta(
                    p["liga_id"], p["mercado"], p.get("stake", 0.0),
                    p.get("cuota_elegida", 0.0), gano_1x2,
                    predictor_lider=p.get("predictor_lider"),
                )

            # [v51-GAP7+5] Registrar también el resultado del mercado O/U si
            # se evaluó y se operó -- mismo AuditorRentabilidad (liga_id
            # compartido), mercado etiquetado "OU2.5_OVER"/"OU2.5_UNDER" para
            # no mezclar su ROI con la atribución RF/Online del 1X2.
            resultado_roi_ou = None
            if p.get("operar_ou") and p.get("mercado_ou"):
                auditor_cierre_ou = AuditorRentabilidad()
                total_goles_real  = goles_l_r + goles_v_r
                resultado_ou_real = "OVER" if total_goles_real > 2.5 else "UNDER"
                gano_ou = (resultado_ou_real == p["mercado_ou"])
                cuota_ou_elegida = (
                    p.get("cuota_over") if p["mercado_ou"] == "OVER" else p.get("cuota_under")
                )
                resultado_roi_ou = auditor_cierre_ou.registrar_apuesta(
                    p["liga_id"], f"OU2.5_{p['mercado_ou']}", p.get("stake_ou", 0.0),
                    cuota_ou_elegida or 0.0, gano_ou,
                )

            hist = st.session_state.get("historial_resultados_sesion", [])
            hist.append({
                "partido_id": pid_sel,
                "ts_cierre":  datetime.now().strftime("%H:%M:%S"),
                "local":      loc_a,
                "visita":     vis_a,
                "goles_l":    goles_l_r,
                "goles_v":    goles_v_r,
                "lam_l":      p["lam_l"],
                "lam_v":      p["lam_v"],
                "err_l":      res.get("err_base_local", 0.0),
                "err_v":      res.get("err_base_visitante", 0.0),
                "anomalia":   bool(anomalia),
                "opero":      p.get("operar", False),
                "stake":      p.get("stake", 0.0),
                "ev":         p.get("ev", 0.0),
                "resultado":  "L" if goles_l_r > goles_v_r else ("E" if goles_l_r == goles_v_r else "V"),
                # [v51-GAP5+8] Resultado monetario real, si se operó
                "pnl_1x2":    resultado_roi["pnl_apuesta"] if resultado_roi else 0.0,
                "roi_liga":   resultado_roi["roi_liga"]    if resultado_roi else None,
                "pnl_ou":     resultado_roi_ou["pnl_apuesta"] if resultado_roi_ou else 0.0,
            })
            st.session_state["historial_resultados_sesion"] = hist

            # [v52-GAP13] Soft-close: marca cerrado=1 en cola_partidos_pendientes
            # en vez de solo remover de una lista en memoria. El registro
            # permanece en la tabla (auditable) pero ya no cuenta como
            # pendiente en ningún módulo (_cola_get filtra cerrado=0).
            _cola_cerrar(conn, pid_sel)

            if anomalia:
                st.markdown(
                    "<div style='background:rgba(245,158,11,0.08);border:1px solid #F59E0B;"
                    "border-radius:6px;padding:12px;font-size:11px;color:#F59E0B;line-height:1.6;'>"
                    "ANOMALIA REGISTRADA — Estado base CONGELADO | Online actualizado"
                    "</div>", unsafe_allow_html=True
                )
            else:
                wb_n = res.get("peso_base",  0.55)
                wo_n = res.get("peso_online", 0.45)
                st.markdown(
                    f"<div style='background:rgba(0,212,170,0.08);border:1px solid #00D4AA;"
                    f"border-radius:6px;padding:12px;font-size:11px;color:#00D4AA;line-height:1.6;'>"
                    f"LAZO CERRADO — {goles_l_r}-{goles_v_r} | "
                    f"Blend nuevo: {wb_n:.2f}·RF + {wo_n:.2f}·Online"
                    f"</div>", unsafe_allow_html=True
                )

            # [v51-GAP5+8] Feedback de ROI real -- solo aparece si esta
            # inferencia había operado alguno de los dos mercados.
            if resultado_roi is not None:
                gano_txt = "ACIERTO" if resultado_roi["pnl_apuesta"] > 0 else "FALLO"
                color_roi = "#00D4AA" if resultado_roi["pnl_apuesta"] > 0 else "#FF3B3B"
                st.markdown(
                    f"<div style='background:rgba(0,0,0,0.2);border:1px solid {color_roi}40;"
                    f"border-radius:6px;padding:10px 14px;font-size:11px;color:{color_roi};"
                    f"margin-top:8px;line-height:1.7;'>"
                    f"[ROI 1X2] {p.get('mercado_label','—')} ({gano_txt}) · predictor: {p.get('predictor_lider','—')} · "
                    f"pnl={resultado_roi['pnl_apuesta']:+.2f} · "
                    f"ROI liga {p['liga_id']}: {resultado_roi['roi_liga']:+.2%} · "
                    f"ROI global: {resultado_roi['roi_global']:+.2%}"
                    f"</div>", unsafe_allow_html=True
                )
                blend_obj_roi = BlendAdaptativo()
                wb_roi, wo_roi, diag_roi = blend_obj_roi.ajustar_por_roi(p["liga_id"], AuditorRentabilidad())
                if diag_roi["ajuste_aplicado"]:
                    st.markdown(
                        f"<div style='font-size:10px;color:#6B7A99;margin-top:4px;line-height:1.6;'>"
                        f"[GAP8] Sugerencia por ROI: {wb_roi:.2f}·RF + {wo_roi:.2f}·Online — {diag_roi['razon']}"
                        f"</div>", unsafe_allow_html=True
                    )
            if resultado_roi_ou is not None:
                gano_ou_txt = "ACIERTO" if resultado_roi_ou["pnl_apuesta"] > 0 else "FALLO"
                color_ou = "#00D4AA" if resultado_roi_ou["pnl_apuesta"] > 0 else "#FF3B3B"
                st.markdown(
                    f"<div style='background:rgba(0,0,0,0.2);border:1px solid {color_ou}40;"
                    f"border-radius:6px;padding:10px 14px;font-size:11px;color:{color_ou};"
                    f"margin-top:6px;line-height:1.7;'>"
                    f"[ROI O/U] {p.get('mercado_ou','—')} ({gano_ou_txt}) · "
                    f"pnl={resultado_roi_ou['pnl_apuesta']:+.2f}"
                    f"</div>", unsafe_allow_html=True
                )
        except Exception as e:
            st.error(f"ERROR EN FEEDBACK: {e}")
            st.code(traceback.format_exc(), language="text")


# ==============================================================================
# MÓDULO 4 — DASHBOARD FINANCIERO  [v501-EMPTY-2]
# ==============================================================================
def modulo_dashboard(conn):
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("DASHBOARD FINANCIERO", "RENDIMIENTO DE SESIÓN · MÉTRICAS ACUMULADAS")

    # [v501-EMPTY-2] Leer con .get() defensivo
    hist_r = st.session_state.get("historial_resultados_sesion") or []
    hist_i = st.session_state.get("historial_partidos_sesion")   or []

    col_reset, _ = st.columns([1, 4])
    with col_reset:
        if st.button("RESET SESION", type="secondary"):
            st.session_state["confirm_reset"] = True

    if st.session_state.get("confirm_reset"):
        st.markdown(
            "<div style='background:rgba(255,59,59,0.08);border:1px solid #FF3B3B;"
            "border-radius:6px;padding:10px 14px;font-size:11px;color:#FF3B3B;margin:8px 0;line-height:1.6;'>"
            "¿CONFIRMAR? Borra historial de sesión. Los JSONs de estado no se modifican."
            "</div>", unsafe_allow_html=True
        )
        ca, cb = st.columns(2)
        if ca.button("CONFIRMAR RESET", type="primary"):
            st.session_state["historial_resultados_sesion"] = []
            st.session_state["historial_partidos_sesion"]   = []
            st.session_state["confirm_reset"] = False
            st.rerun()
        if cb.button("CANCELAR"):
            st.session_state["confirm_reset"] = False
            st.rerun()

    # [v501-EMPTY-2] Placeholder informativo cuando no hay datos
    if not hist_r:
        n_inf = len(hist_i)
        st.markdown(
            f"<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
            f"padding:24px;text-align:center;color:#4A5568;font-size:12px;line-height:1.8;'>"
            f"SIN PARTIDOS CERRADOS EN ESTA SESIÓN<br>"
            f"<span style='font-size:10px;'>"
            f"{'Hay '+str(n_inf)+' inferencia(s) pendiente(s) de cerrar en CIERRE.' if n_inf else 'Ejecuta inferencias en MOTOR DE INFERENCIA y ciérralas en CIERRE.'}"
            f"</span>"
            f"</div>", unsafe_allow_html=True
        )
        # Mostrar historial de inferencias aunque no haya cierres
        if hist_i:
            st.divider()
            st.markdown("<div class='am-muted' style='margin-bottom:8px;'>INFERENCIAS PENDIENTES</div>", unsafe_allow_html=True)
            rows = [{"HORA": h.get("ts","—"), "LIGA": h.get("liga","—"),
                     "LOCAL": h.get("local","—"), "VISITA": h.get("visita","—"),
                     "EV": f"{h.get('ev',0)*100:+.2f}%", "SEÑAL": "OPERAR" if h.get("operar") else "—"}
                    for h in hist_i]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        return

    df_r = pd.DataFrame(hist_r)

    # [v501-EMPTY-2] Fallback a 0 si columnas faltan
    for col in ["opero","stake","ev","err_l","err_v","resultado","lam_l","lam_v","pnl_1x2"]:
        if col not in df_r.columns:
            df_r[col] = 0

    total    = len(df_r)
    operados = df_r[df_r["opero"] == True] if "opero" in df_r.columns else pd.DataFrame()
    n_op     = len(operados)
    stake_tot= float(operados["stake"].sum()) if n_op else 0.0
    ev_medio = float(df_r["ev"].mean()) * 100 if total else 0.0
    err_medio= float((df_r["err_l"].abs().mean() + df_r["err_v"].abs().mean()) / 2) if total else 0.0

    # [BUGFIX] "Acierto" se determina por el resultado MONETARIO real del
    # mercado efectivamente apostado (pnl_1x2, calculado en CIERRE por
    # AuditorRentabilidad.registrar_apuesta contra p["mercado"] -- puede ser
    # "1", "X" o "2"), no por si ganó el local. Antes: row["resultado"]=="L"
    # marcaba como "fallo" cualquier acierto en X o 2, y como "acierto"
    # cualquier victoria local aunque se hubiera apostado X o 2 y perdido.
    aciertos = int((operados["pnl_1x2"] > 0).sum()) if n_op else 0
    tasa_ac  = (aciertos / n_op * 100) if n_op else 0.0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("PARTIDOS",     total)
    k2.metric("OPERADOS",     n_op,       delta=f"{(n_op/total*100):.0f}% del total" if total else "—")
    k3.metric("EV MEDIO",     f"{ev_medio:+.2f}%")
    k4.metric("TASA ACIERTO", f"{tasa_ac:.1f}%",  delta=f"{aciertos}/{n_op}")
    k5.metric("MAE SESION",   f"{err_medio:.3f}")

    st.divider()

    # [BUGFIX] pl_partido = pnl_1x2 real (AuditorRentabilidad.registrar_apuesta
    # ya lo calculó contra la cuota_elegida real del mercado apostado). Se
    # reemplaza la reconstrucción ficticia stake*(1/(1-ev))-stake -- esa
    # fórmula no corresponde a ninguna cuota real y además asumía
    # incorrectamente que "ganar" == "resultado local".
    df_r["pl_partido"] = df_r["pnl_1x2"]
    df_r["pl_acum"] = df_r["pl_partido"].cumsum()

    try:
        import plotly.graph_objects as go

        fig_pl = go.Figure()
        fig_pl.add_scatter(
            x=list(range(1, len(df_r)+1)), y=df_r["pl_acum"].tolist(),
            mode="lines+markers",
            line=dict(color="#C8FF00", width=2), marker=dict(color="#C8FF00", size=5),
            fill="tozeroy", fillcolor="rgba(200,255,0,0.05)",
        )
        fig_pl.add_hline(y=0, line_dash="dot", line_color="#FF3B3B", line_width=1)
        fig_pl.update_layout(
            title=dict(text="CURVA P&L SESION", font=dict(family="Outfit", size=12, color="#6B7A99")),
            paper_bgcolor="#0A0F1E", plot_bgcolor="#0D1526",
            font=dict(family="Geist Mono", color="#6B7A99", size=10),
            height=250, margin=dict(l=40, r=20, t=40, b=30),
            xaxis=dict(gridcolor="#1A2642", title="N partido"),
            yaxis=dict(gridcolor="#1A2642", title="$ acumulado"),
            showlegend=False,
        )
        st.plotly_chart(fig_pl, use_container_width=True)

        colors  = ["#C8FF00" if op else "#FF3B3B" for op in df_r["opero"]]
        fig_ev  = go.Figure()
        fig_ev.add_bar(
            x=[f"P{i+1}" for i in range(len(df_r))],
            y=(df_r["ev"] * 100).tolist(), marker_color=colors,
        )
        fig_ev.add_hline(y=EV_THRESHOLD*100, line_dash="dot", line_color="#F59E0B", line_width=1)
        fig_ev.update_layout(
            title=dict(text=f"EV POR PARTIDO (umbral {EV_THRESHOLD*100:.0f}%)", font=dict(family="Outfit", size=12, color="#6B7A99")),
            paper_bgcolor="#0A0F1E", plot_bgcolor="#0D1526",
            font=dict(family="Geist Mono", color="#6B7A99", size=10),
            height=210, margin=dict(l=40, r=20, t=40, b=30),
            xaxis=dict(gridcolor="#1A2642"), yaxis=dict(gridcolor="#1A2642", title="EV %"),
            showlegend=False,
        )
        st.plotly_chart(fig_ev, use_container_width=True)

    except ImportError:
        st.warning("Instala plotly: pip install plotly")
        st.dataframe(
            df_r[["ts_cierre","local","visita","resultado","stake","ev","pnl_1x2","err_l","err_v"]],
            use_container_width=True, hide_index=True
        )

    st.divider()
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>HISTORIAL DE SESION</div>", unsafe_allow_html=True)
    cols_disp = [c for c in ["ts_cierre","local","visita","goles_l","goles_v",
                              "resultado","lam_l","lam_v","err_l","err_v","opero","stake","ev","pnl_1x2"] if c in df_r.columns]
    df_display = df_r[cols_disp].copy()
    if "ev" in df_display.columns:
        df_display["ev"] = (df_display["ev"] * 100).round(2)
    st.dataframe(df_display, use_container_width=True, hide_index=True)


# ==============================================================================
# MÓDULO 5 — ETL INCREMENTAL  [v501-ETL-1]
# ==============================================================================
def modulo_etl(conn):
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("ETL INCREMENTAL", "CEREBELLIUM v4.1 — IMPORTACION DE DATOS")

    if not ETL_DISPONIBLE:
        st.markdown(
            f"<div style='background:rgba(255,59,59,0.08);border:1px solid #FF3B3B;"
            f"border-radius:6px;padding:14px;font-size:11px;color:#FF3B3B;line-height:1.6;'>"
            f"CEREBELLIUM NO DISPONIBLE — {ETL_ERROR_MSG[:200]}"
            f"</div>", unsafe_allow_html=True
        )
        return

    # [v501-ETL-1] DATA_DIR info y selector de archivo
    st.markdown(
        f"<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
        f"padding:12px 16px;font-size:11px;color:#6B7A99;margin-bottom:16px;line-height:1.7;'>"
        f"Carpeta activa: <span style='color:#C8FF00;'>{os.path.abspath(DATA_DIR)}</span><br>"
        f"Formato europeo (HomeTeam): EPL · LaLiga · Serie A · Bundesliga · Ligue 1<br>"
        f"Formato americano (Home): Liga MX · MLS · CONCACAF"
        f"</div>", unsafe_allow_html=True
    )

    # [v501-ETL-1] Detectar CSVs disponibles en DATA_DIR
    csvs_disponibles = []
    if os.path.isdir(DATA_DIR):
        csvs_disponibles = sorted(
            [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".csv")]
        )

    with st.form("form_etl_v501"):
        if csvs_disponibles:
            # Selectbox de archivos disponibles + opción manual
            opciones_csv = csvs_disponibles + ["[ ruta manual ]"]
            sel_csv      = st.selectbox("ARCHIVO CSV", opciones_csv, index=0)
            if sel_csv == "[ ruta manual ]":
                csv_nombre = st.text_input("RUTA MANUAL (relativa o absoluta)", placeholder="otro_directorio/archivo.csv")
                csv_path   = csv_nombre.strip()
            else:
                csv_path = os.path.join(DATA_DIR, sel_csv)
                st.markdown(
                    f"<div style='font-size:10px;color:#4A5568;margin-top:2px;'>"
                    f"Ruta: {csv_path}</div>",
                    unsafe_allow_html=True
                )
        else:
            # Directorio vacío o no existe → fallback a text_input con aviso
            if not os.path.isdir(DATA_DIR):
                st.markdown(
                    f"<div style='font-size:11px;color:#F59E0B;'>"
                    f"Directorio '{DATA_DIR}' no encontrado. "
                    f"Crea la carpeta o ajusta AEGIST_DATA_DIR.</div>",
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f"<div style='font-size:11px;color:#4A5568;'>"
                    f"No hay archivos .csv en '{DATA_DIR}'.</div>",
                    unsafe_allow_html=True
                )
            csv_input = st.text_input("RUTA CSV (manual)", placeholder="csv_data/E0_2425.csv")
            csv_path  = csv_input.strip()

        ce1, ce2, ce3 = st.columns(3)
        with ce1: liga_override = st.text_input("LIGA ID (auto si vacío)", placeholder="premier_league")
        with ce2: temp_override = st.text_input("TEMPORADA (auto si vacío)", placeholder="2024-25")
        with ce3: limite        = st.number_input("LIMITE REGISTROS (0=todos)", min_value=0, value=0, step=50)
        importar = st.form_submit_button("IMPORTAR", type="primary", use_container_width=True)

    if importar:
        if not csv_path:
            st.error("Selecciona o ingresa un archivo CSV.")
            return
        if not os.path.exists(csv_path):
            st.error(f"Archivo no encontrado: {csv_path}")
            return
        with st.spinner("Importando..."):
            try:
                res = importar_csv(
                    csv_path,
                    liga_id_override=liga_override.strip() or None,
                    temporada=temp_override.strip()         or None,
                    limite_registros=int(limite) if limite > 0 else None,
                    db_path=DB_NAME,
                )
                st.markdown(
                    f"<div style='background:rgba(0,212,170,0.08);border:1px solid #00D4AA;"
                    f"border-radius:6px;padding:12px;font-size:11px;color:#00D4AA;line-height:1.7;'>"
                    f"IMPORTADO: {res.get('archivo')} | "
                    f"+{res.get('insertados',0)} filas | "
                    f"{res.get('duplicados',0)} dup | "
                    f"{res.get('errores',0)} err | "
                    f"Liga: {res.get('liga_id')} | "
                    f"hash: {res.get('hash_run','—')}"
                    f"</div>", unsafe_allow_html=True
                )
                if MOTOR_DISPONIBLE and res.get("liga_id"):
                    try:
                        FeatureStore(DB_NAME).invalidar(res["liga_id"])
                    except Exception:
                        pass
                st.cache_resource.clear()
                _diagnostico_sistema.clear()
            except Exception as e:
                st.error(f"ERROR ETL: {e}")
                st.code(traceback.format_exc(), language="text")

    st.divider()
    # [v501-EMPTY-3] Leer con query_df fresco en cada render
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>HISTORIAL DE IMPORTACIONES</div>", unsafe_allow_html=True)
    if tabla_existe(conn, "etl_log"):
        df_log = query_df(conn, """
            SELECT id, timestamp, archivo, liga_id, formato,
                   insertados, duplicados, errores, hash_run
            FROM etl_log ORDER BY id DESC LIMIT 20
        """)
        if df_log.empty:
            st.markdown(
                "<div style='color:#4A5568;font-size:11px;padding:12px;'>Sin importaciones registradas.</div>",
                unsafe_allow_html=True
            )
        else:
            st.dataframe(df_log, use_container_width=True, hide_index=True)
    else:
        st.markdown(
            "<div style='color:#4A5568;font-size:11px;padding:12px;'>"
            "Tabla etl_log no existe — ejecuta inicializar_schema() primero.</div>",
            unsafe_allow_html=True
        )


# ==============================================================================
# MÓDULO 6 — AUDITORÍA DB
# [v52-GAP13] cola_partidos_pendientes añadida al panel de objetos de DB —
# ahora es una tabla operativa real, no solo memoria de sesión.
# ==============================================================================
def modulo_auditoria(conn):
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("AUDITORIA DEL SISTEMA", "CEREBELLIUM v4.1 · ESTADO DE INTEGRIDAD")

    ca, cb, cc = st.columns(3)
    for col, activo, label in [(ca, MOTOR_DISPONIBLE, "CONTINUITY"), (cb, ETL_DISPONIBLE, "ETL")]:
        with col:
            color = "#00D4AA" if activo else "#FF3B3B"
            st.markdown(
                f"<div style='background:rgba(0,0,0,0.3);border:1px solid {color};"
                f"border-radius:6px;padding:10px;text-align:center;line-height:1.5;'>"
                f"<div style='font-size:10px;color:{color};letter-spacing:0.1em;'>{label}</div>"
                f"<div style='font-size:13px;color:{color};margin-top:4px;font-weight:600;'>"
                f"{'ACTIVO' if activo else 'OFFLINE'}</div></div>", unsafe_allow_html=True
            )
    with cc:
        n_eq_state, state_ok = 0, False
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    n_eq_state = len(json.load(f))
                state_ok = True
            except Exception:
                pass
        color = "#00D4AA" if state_ok else "#F59E0B"
        st.markdown(
            f"<div style='background:rgba(0,0,0,0.3);border:1px solid {color};"
            f"border-radius:6px;padding:10px;text-align:center;line-height:1.5;'>"
            f"<div style='font-size:10px;color:{color};letter-spacing:0.1em;'>STATE.JSON</div>"
            f"<div style='font-size:13px;color:{color};margin-top:4px;font-weight:600;'>"
            f"{n_eq_state} equipos</div></div>", unsafe_allow_html=True
        )

    st.divider()
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>OBJETOS DE BASE DE DATOS</div>", unsafe_allow_html=True)
    objs     = list(_TABLAS_REQUERIDAS) + list(_VISTAS_REQUERIDAS)
    cols_db  = st.columns(len(objs))
    for i, obj in enumerate(objs):
        with cols_db[i]:
            existe = tabla_existe(conn, obj)
            n      = contar_tabla(conn, obj) if existe else 0
            tipo   = "VIEW" if obj in _VISTAS_REQUERIDAS else "TABLE"
            color  = "#00D4AA" if existe else "#FF3B3B"
            st.markdown(
                f"<div style='background:#0D1526;border-top:2px solid {color};"
                f"border:1px solid {color}20;border-radius:6px;padding:10px 8px;text-align:center;line-height:1.5;'>"
                f"<div style='font-size:8px;color:#4A5568;'>{tipo}</div>"
                f"<div style='font-size:9px;color:{color};font-weight:600;margin:2px 0;'>{obj.replace('_',' ').upper()}</div>"
                f"<div style='font-size:14px;color:#FFFFFF;font-family:Geist Mono;'>{n:,}</div>"
                f"</div>", unsafe_allow_html=True
            )

    st.divider()
    if tabla_existe(conn, "etl_log"):
        st.markdown("<div class='am-muted' style='margin-bottom:8px;'>ÚLTIMAS IMPORTACIONES ETL</div>", unsafe_allow_html=True)
        df_etl = query_df(conn, "SELECT timestamp,archivo,liga_id,formato,insertados,duplicados,errores FROM etl_log ORDER BY id DESC LIMIT 10")
        if not df_etl.empty:
            st.dataframe(df_etl, use_container_width=True, hide_index=True)

    st.divider()
    vista_f = "historial_hot" if tabla_existe(conn, "historial_hot") else "historial_partidos"
    st.markdown(f"<div class='am-muted' style='margin-bottom:8px;'>HISTORIAL RECIENTE — {vista_f.upper()}</div>", unsafe_allow_html=True)
    df_h = query_df(conn, f"""
        SELECT fecha, liga_id, equipo_local, equipo_visitante,
               goles_local, goles_visitante, numero_jornada
        FROM {vista_f} ORDER BY fecha DESC LIMIT 50
    """)
    if df_h.empty:
        st.markdown("<div style='color:#4A5568;font-size:11px;padding:12px;'>Sin registros. Importa CSV con ETL.</div>", unsafe_allow_html=True)
    else:
        st.dataframe(df_h, use_container_width=True, hide_index=True)


# ==============================================================================
# MÓDULO 7 — FLUJO DEL PIPELINE
# ==============================================================================
@st.cache_data(ttl=300)
def _calcular_estado_pipeline(n_tablas_ok, n_vistas_ok, n_partidos, n_equipos,
                               state_ok, n_eq_state, online_ok, n_eq_on,
                               blend_ok, pkl_ok, n_parquets, n_parquets_exp) -> dict:
    return {
        "DB":    {"ok": n_tablas_ok == len(_TABLAS_REQUERIDAS) and n_vistas_ok == len(_VISTAS_REQUERIDAS),
                  "parcial": 0 < n_tablas_ok < len(_TABLAS_REQUERIDAS),
                  "detalle": f"{n_tablas_ok}/{len(_TABLAS_REQUERIDAS)} tablas · {n_partidos:,} partidos · {n_equipos} equipos"},
        "C1":   {"ok": state_ok, "parcial": False,
                  "detalle": f"state.json: {'OK' if state_ok else 'FALTA'} · {n_eq_state} equipos"},
        "C2":   {"ok": MOTOR_DISPONIBLE, "parcial": not MOTOR_DISPONIBLE,
                  "detalle": f"historial_hot · {n_parquets} parquets ({n_parquets_exp} exp) · {len(FEATURE_COLUMNS)} features"},
        "C3A":  {"ok": MOTOR_DISPONIBLE and pkl_ok, "parcial": MOTOR_DISPONIBLE and not pkl_ok,
                  "detalle": "RF 300 + GBM 150 · TimeSeriesSplit · joblib"},
        "C3B":  {"ok": online_ok, "parcial": MOTOR_DISPONIBLE and not online_ok,
                  "detalle": f"EWMA adaptativo · {n_eq_on} equipos · α∈[0.03,0.25]"},
        "C3C":  {"ok": blend_ok, "parcial": MOTOR_DISPONIBLE and not blend_ok,
                  "detalle": "softmax(1/MAE) · pesos∈[0.30,0.70] · N=10"},
        "C4":   {"ok": MOTOR_DISPONIBLE, "parcial": False, "detalle": "xG blend 60/40 · termal >32°C · rojas · fatiga"},
        "C5":   {"ok": MOTOR_DISPONIBLE, "parcial": False, "detalle": "Dixon-Coles 1997 · τ(x,y) · MAX_G=10"},
        "C6":   {"ok": MOTOR_DISPONIBLE, "parcial": False, "detalle": f"Kelly·25% · EV>{EV_THRESHOLD*100:.0f}%"},
        "C7":   {"ok": MOTOR_DISPONIBLE and state_ok, "parcial": MOTOR_DISPONIBLE and not state_ok,
                  "detalle": "residuos base+online · anomalía→congelar state"},
        "RADAR":{"ok": True, "parcial": False, "detalle": f"{len(ALL_LIGAS)} ligas · NAFTA + UEFA"},
        "WC26": {"ok": True, "parcial": False, "detalle": "Módulo Mundial 2026 · localía limitada MEX/USA/CAN"},
    }


def modulo_flujo_pipeline(conn):
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("FLUJO DEL PIPELINE", "ARQUITECTURA CONTINUUM v4.1 — ESTADO EN VIVO")

    tablas_ok  = [t for t in _TABLAS_REQUERIDAS if tabla_existe(conn, t)]
    vistas_ok  = [v for v in _VISTAS_REQUERIDAS if tabla_existe(conn, v)]
    n_p        = contar_tabla(conn, "historial_partidos") if tabla_existe(conn, "historial_partidos") else 0
    n_e        = contar_tabla(conn, "dim_equipos")        if tabla_existe(conn, "dim_equipos")        else 0
    state_ok, n_eq_s = False, 0
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f: n_eq_s = len(json.load(f)); state_ok = True
        except Exception: pass
    online_ok, n_eq_on = False, 0
    if os.path.exists(STATE_ONLINE):
        try:
            with open(STATE_ONLINE) as f: n_eq_on = len(json.load(f).get("estado",{})); online_ok = True
        except Exception: pass
    blend_ok = os.path.exists(STATE_BLEND)
    pkl_ok   = os.path.exists(os.path.join(MODELOS_DIR, "motor_ia.pkl"))
    n_pq = n_pq_exp = 0
    if os.path.exists(FEATURE_STORE_DIR):
        pqs    = [f for f in os.listdir(FEATURE_STORE_DIR) if f.endswith(".parquet")]
        n_pq   = len(pqs)
        ahora  = datetime.now()
        n_pq_exp = sum(1 for pq in pqs if (ahora - datetime.fromtimestamp(
            os.path.getmtime(os.path.join(FEATURE_STORE_DIR, pq)))).total_seconds() > _LIMITE_PARQUET_HRS * 3600)

    estado_n = _calcular_estado_pipeline(
        len(tablas_ok), len(vistas_ok), n_p, n_e, state_ok, n_eq_s,
        online_ok, n_eq_on, blend_ok, pkl_ok, n_pq, n_pq_exp
    )

    NODOS = [
        ("DB",    "BASE",  "Cerebellium DB v4.1",  "db"),
        ("C1",    "C1",    "EstadoAdaptativo",       ""),
        ("C2",    "C2",    "DataPipeline + FS",      ""),
        ("C3A",   "C3A",   "MotorBase RF+GBM",       ""),
        ("C3B",   "C3B",   "MotorOnline EWMA",       ""),
        ("C3C",   "C3C",   "BlendAdaptativo",        ""),
        ("C4",    "C4",    "AjustadorCuantico",      ""),
        ("C5",    "C5",    "Transmutador Dixon-C.",  ""),
        ("C6",    "C6",    "EscudoFinanciero",       ""),
        ("C7",    "C7",    "RegistradorFeedback",    ""),
        ("RADAR", "RADAR", "Radar de Mercados",      "radar"),
        ("WC26",  "WC26",  "Mundial 2026",           "radar"),
    ]

    col_fl, col_det = st.columns([1, 1.6])
    with col_fl:
        st.markdown("<div class='am-muted' style='margin-bottom:10px;'>ESTADO DE CAPAS</div>", unsafe_allow_html=True)
        for i, (nid, capa, nombre, css_extra) in enumerate(NODOS):
            est = estado_n.get(nid, {"ok": False, "parcial": False, "detalle": ""})
            if est["ok"]:        badge, estado_str = "ACTIVO", "ok"
            elif est["parcial"]: badge, estado_str = "PARCIAL","warn"
            else:                badge, estado_str = "OFFLINE","err"
            node_css = css_extra or estado_str
            st.markdown(
                f"<div class='am-node' data-estado='{node_css}'>"
                f"<span class='am-node-title'>{capa} — {nombre}</span>"
                f"<span class='am-node-badge badge-{estado_str}'>{badge}</span>"
                f"<div class='am-node-detail'>{est['detalle']}</div>"
                f"</div>",
                unsafe_allow_html=True
            )
            if i < len(NODOS) - 1:
                st.markdown(
                    "<div style='text-align:center;color:#1A2642;font-size:11px;margin:-2px 0;'>│</div>",
                    unsafe_allow_html=True
                )
            if st.button(f"DETALLE {capa}", key=f"btn_nodo_{nid}", type="secondary"):
                st.session_state["nodo_sel"] = nid

    with col_det:
        nid      = st.session_state.get("nodo_sel", "DB")
        nodo_obj = next((n for n in NODOS if n[0] == nid), NODOS[0])
        est      = estado_n.get(nid, {"ok": False, "parcial": False, "detalle": ""})
        estado_c = "#00D4AA" if est["ok"] else ("#F59E0B" if est["parcial"] else "#FF3B3B")
        estado_t = "ACTIVO"  if est["ok"] else ("PARCIAL" if est["parcial"] else "OFFLINE")
        st.markdown(
            f"<div style='border:1px solid {estado_c}20;background:#0D1526;"
            f"border-top:2px solid {estado_c};border-radius:8px;padding:18px;'>"
            f"<div class='am-muted'>{nodo_obj[1]}</div>"
            f"<div class='am-title' style='font-size:16px;margin:6px 0;'>{nodo_obj[2]}</div>"
            f"<div style='font-size:10px;color:{estado_c};letter-spacing:0.12em;'>{estado_t}</div>"
            f"<div style='margin-top:12px;border-top:1px solid #1A2642;padding-top:10px;"
            f"font-size:11px;color:#4A5568;line-height:1.8;'>{est['detalle']}</div>"
            f"</div>", unsafe_allow_html=True
        )

# ==============================================================================
# MÓDULO 8 — RENTABILIDAD  [v51-GAP10]
# ------------------------------------------------------------------------------
# Expone capacidades que ya existían en CONTINUITY v4.5 (Capa 9b/10) pero no
# tenían ningún punto de acceso desde PACIFICO KyJ: ROI real por liga/global
# (AuditorRentabilidad), sugerencia de ajuste de blend por ROI atribuido
# (BlendAdaptativo.ajustar_por_roi) y backtesting walk-forward contra cuotas
# reales (Backtester). Los datos de ROI que aquí se muestran son los mismos
# que CIERRE alimenta al registrar cada apuesta -- este módulo es de solo
# lectura + un botón de simulación (Backtester no escribe en state_roi.json).
# ==============================================================================
def modulo_rentabilidad(conn):
    _render_estado_sistema_global(conn)
    _render_seccion_titulo("RENTABILIDAD", "AUDITORÍA DE ROI REAL · BACKTESTING WALK-FORWARD")

    if not MOTOR_DISPONIBLE:
        st.markdown(
            f"<div style='background:rgba(255,59,59,0.08);border:1px solid #FF3B3B;"
            f"border-radius:6px;padding:16px;font-size:12px;color:#FF3B3B;line-height:1.6;'>"
            f"MOTOR OFFLINE — {MOTOR_ERROR_MSG[:200]}"
            "</div>", unsafe_allow_html=True
        )
        return

    auditor = AuditorRentabilidad()

    # ── Resumen global ──
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>RESUMEN GLOBAL (state_roi.json)</div>", unsafe_allow_html=True)
    rg = auditor.resumen_global()
    if rg["n_apuestas"] == 0:
        st.markdown(
            "<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
            "padding:24px;text-align:center;color:#4A5568;font-size:12px;line-height:1.8;'>"
            "SIN APUESTAS REGISTRADAS TODAVÍA<br>"
            "<span style='font-size:10px;'>Se registran automáticamente al cerrar partidos operados en CIERRE.</span>"
            "</div>", unsafe_allow_html=True
        )
    else:
        rg1, rg2, rg3, rg4, rg5 = st.columns(5)
        rg1.metric("LIGAS CON DATOS", rg["n_ligas"])
        rg2.metric("APUESTAS",        rg["n_apuestas"])
        rg3.metric("TASA ACIERTO",    f"{rg['tasa_acierto']*100:.1f}%")
        rg4.metric("PNL TOTAL",       f"${rg['pnl_total']:+.2f}")
        rg5.metric("ROI GLOBAL",      f"{rg['roi']*100:+.2f}%")

    st.divider()

    # ── Resumen por liga + racha + sugerencia de blend por ROI ──
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>DETALLE POR LIGA</div>", unsafe_allow_html=True)
    liga_sel = st.selectbox("LIGA", list(ALL_LIGAS.keys()), key="rent_liga_sel")
    rl = auditor.resumen_liga(liga_sel)
    rl1, rl2, rl3, rl4 = st.columns(4)
    rl1.metric("APUESTAS",     rl["n_apuestas"])
    rl2.metric("TASA ACIERTO", f"{rl['tasa_acierto']*100:.1f}%")
    rl3.metric("PNL",          f"${rl['pnl_total']:+.2f}")
    rl4.metric("ROI",          f"{rl['roi']*100:+.2f}%")

    racha_liga = auditor.racha_actual(liga_sel)
    if racha_liga > 0:
        st.markdown(
            f"<div style='background:rgba(245,158,11,0.08);border:1px solid #F59E0B;"
            f"border-radius:6px;padding:8px 14px;font-size:11px;color:#F59E0B;margin-top:8px;line-height:1.6;'>"
            f"Racha activa: {racha_liga} derrota(s) consecutiva(s) — "
            f"INFERENCIA ya aplica la penalización de stake automáticamente."
            f"</div>", unsafe_allow_html=True
        )

    if MOTOR_DISPONIBLE:
        blend_obj = BlendAdaptativo()
        wb_roi, wo_roi, diag_roi = blend_obj.ajustar_por_roi(liga_sel, auditor)
        st.markdown(
            f"<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
            f"padding:10px 14px;font-size:11px;color:#6B7A99;margin-top:8px;line-height:1.7;'>"
            f"[GAP8] Blend actual: {diag_roi['wb_original']:.2f}·RF + {diag_roi['wo_original']:.2f}·Online "
            f"(RF n={diag_roi['n_rf']} roi={diag_roi['roi_rf']*100:+.1f}% · "
            f"Online n={diag_roi['n_online']} roi={diag_roi['roi_online']*100:+.1f}%)<br>"
            f"{'Sugerencia: ' + f'{wb_roi:.2f}·RF + {wo_roi:.2f}·Online' if diag_roi['ajuste_aplicado'] else diag_roi['razon']}"
            f"</div>", unsafe_allow_html=True
        )

    st.divider()

    # ── Backtester walk-forward ──
    st.markdown("<div class='am-muted' style='margin-bottom:8px;'>BACKTESTER WALK-FORWARD (Capa 10)</div>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-size:10px;color:#4A5568;margin-bottom:10px;line-height:1.7;'>"
        "Simula evaluar_triple() + Kelly + overround contra cuotas REALES históricas "
        "(cuotas_1x2), con lambda por sliding window walk-forward simple. No incluye "
        "RF/GBM, rho dinámico ni tabla de posiciones -- ver docstring de Backtester en "
        "CONTINUITY.py para el alcance metodológico completo."
        "</div>", unsafe_allow_html=True
    )
    with st.form("form_backtest"):
        bt1, bt2, bt3 = st.columns(3)
        with bt1:
            liga_bt = st.selectbox("LIGA A BACKTESTEAR", list(ALL_LIGAS.keys()), key="bt_liga_sel")
        with bt2:
            bankroll_bt = st.number_input("BANKROLL INICIAL $", min_value=100.0, value=1000.0, step=100.0)
        with bt3:
            usar_racha_bt = st.checkbox("APLICAR PENALIZACIÓN POR RACHA (Gap9)")
        correr_bt = st.form_submit_button("EJECUTAR BACKTEST", type="primary", use_container_width=True)

    if correr_bt:
        with st.spinner(f"Backtesteando {liga_bt}..."):
            try:
                bt = Backtester(DB_NAME)
                resultado_bt = bt.ejecutar(
                    liga_bt, bankroll_inicial=bankroll_bt,
                    usar_penalizacion_racha=usar_racha_bt,
                )
            except Exception as e:
                st.error(f"ERROR EN BACKTEST: {e}")
                st.code(traceback.format_exc(), language="text")
                resultado_bt = None

        if resultado_bt is not None:
            if resultado_bt.get("error"):
                st.warning(resultado_bt["error"])
            else:
                bt1r, bt2r, bt3r, bt4r, bt5r = st.columns(5)
                bt1r.metric("EVALUADOS", resultado_bt["n_partidos_evaluados"])
                bt2r.metric("OPERADOS",  resultado_bt["n_partidos_operados"])
                bt3r.metric("ACIERTO",   f"{resultado_bt['tasa_acierto']*100:.1f}%")
                bt4r.metric("PNL",       f"${resultado_bt['pnl_total']:+.2f}")
                bt5r.metric("ROI",       f"{resultado_bt['roi_sobre_stake']*100:+.2f}%")
                st.markdown(
                    f"<div style='font-size:10px;color:#4A5568;margin-top:8px;'>"
                    f"Bankroll: ${resultado_bt['bankroll_inicial']:.2f} → "
                    f"${resultado_bt['bankroll_final']:.2f} · "
                    f"Penalización racha: {'SÍ' if resultado_bt['usar_penalizacion_racha'] else 'NO'}"
                    f"</div>", unsafe_allow_html=True
                )
                detalle_bt = resultado_bt.get("detalle_apuestas", [])
                if detalle_bt:
                    st.dataframe(pd.DataFrame(detalle_bt), use_container_width=True, hide_index=True)


# ==============================================================================
# MÓDULO 0 — CENTRAL DE OPERACIONES  [OPERACION-1]
# ------------------------------------------------------------------------------
# Centro de control unificado para el ciclo completo de operación del proyecto.
# Reemplaza el flujo manual de 4 terminales:
#   PURGA.py → diana.py → CEREBELLIUM.py → CONTINUITY.py (entrenar) → pacificokyj
# Todo se ejecuta ahora desde aquí con un botón por paso, con log visual en vivo.
# ==============================================================================
def modulo_operacion(conn):
    _render_seccion_titulo(
        "CENTRAL DE OPERACIONES",
        "PACIFICO KyJ v5.2 — CICLO COMPLETO EN UN SOLO LUGAR"
    )

    st.markdown('''
    <link href="https://fonts.googleapis.com/css2?family=Ubuntu:wght@300;400;500&display=swap" rel="stylesheet">
    <style>
    div.stButton > button[data-testid="baseButton-primary"] {
        background: rgba(255, 255, 255, 0.03) !important;
        color: #E2E8F0 !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 4px !important;
        font-weight: 300 !important;
        letter-spacing: 0.15em !important;
        box-shadow: none !important;
        transition: all 0.3s ease !important;
        padding: 0.5rem 1rem !important;
    }
    div.stButton > button[data-testid="baseButton-primary"]:hover {
        background: rgba(255, 255, 255, 0.08) !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
        color: #FFFFFF !important;
        box-shadow: 0 0 15px rgba(255, 255, 255, 0.02) !important;
    }
    div.stButton > button[data-testid="baseButton-primary"]:active {
        background: rgba(255, 255, 255, 0.12) !important;
    }
    </style>
    ''', unsafe_allow_html=True)

    def _header_paso(titulo_grande, paso_numero, descripcion, color_grande):
        st.markdown(f'''
        <div style="display:flex; justify-content:space-between; align-items:baseline; margin-bottom:12px; border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:8px; margin-top:24px;">
            <div style="font-family:'Ubuntu', sans-serif; font-size:20px; font-weight:300; color:{color_grande}; letter-spacing:0.05em; line-height:1;">
                {titulo_grande}
            </div>
            <div style="color:rgba(255,255,255,0.4); letter-spacing:0.2em; font-size:9px; font-weight:300; text-transform:uppercase;">
                {paso_numero} <span style="color:rgba(255,255,255,0.15); margin:0 4px;">|</span> {descripcion}
            </div>
        </div>
        ''', unsafe_allow_html=True)







    # ── Descripción del flujo ──────────────────────────────────────────────────
    st.markdown(
        "<div style='background:#0D1526;border:1px solid #1A2642;border-radius:6px;"
        "padding:14px 18px;font-size:11px;color:#6B7A99;line-height:1.9;margin-bottom:18px;'>"
        "Ejecuta cada PASO en orden para completar un ciclo de actualización completo.<br>"
        "<span style='color:#C8FF00;'>PASO 1 PURGAR</span> → limpiar caches, parquets y modelo antiguo<br>"
        "<span style='color:#C8FF00;'>PASO 2 DESCARGAR</span> → DIANA descarga CSVs frescos desde football-data.co.uk<br>"
        "<span style='color:#C8FF00;'>PASO 3 ETL</span> → CEREBELLIUM ingesta todos los CSVs en cerebrillum.db<br>"
        "<span style='color:#C8FF00;'>PASO 4 ENTRENAR</span> → construir features y re-entrenar motor_ia.pkl"
        "</div>", unsafe_allow_html=True
    )

    # ── Estado de módulos de soporte ──────────────────────────────────────────
    sc1, sc2, sc3, sc4 = st.columns(4)
    for col, disponible, label in [
        (sc1, PURGA_DISPONIBLE,  "PURGA"),
        (sc2, DIANA_DISPONIBLE,  "DIANA"),
        (sc3, ETL_DISPONIBLE,    "CEREBELLIUM"),
        (sc4, MOTOR_DISPONIBLE,  "MOTOR IA"),
    ]:
        with col:
            color = "#00D4AA" if disponible else "#FF3B3B"
            st.markdown(
                f"<div style='background:rgba(0,0,0,0.3);border:1px solid {color};"
                f"border-radius:6px;padding:10px;text-align:center;line-height:1.5;'>"
                f"<div style='font-size:9px;color:{color};letter-spacing:0.1em;'>{label}</div>"
                f"<div style='font-size:12px;color:{color};margin-top:3px;font-weight:600;'>"
                f"{'OK' if disponible else 'OFFLINE'}</div></div>",
                unsafe_allow_html=True
            )

    st.divider()

    # ── PASO 1: PURGAR ────────────────────────────────────────────────────────
    _header_paso('PURGA', 'PASO 1', 'ENTORNO', '#CBD5E1')
    st.markdown(
        "<div style='font-size:10px;color:#4A5568;margin-bottom:10px;line-height:1.7;'>"
        "Elimina parquets de feature_store/, __pycache__ y el modelo motor_ia.pkl. "
        "Ejecutar antes de un ciclo de re-entrenamiento limpio."
        "</div>", unsafe_allow_html=True
    )
    col_p1, col_p2 = st.columns([1, 3])
    with col_p1:
        purgar_btn = st.button(
            "▶ EJECUTAR PURGA", key="btn_purgar",
            type="primary", use_container_width=True,
            disabled=not PURGA_DISPONIBLE
        )
    with col_p2:
        if not PURGA_DISPONIBLE:
            st.markdown(
                f"<div style='color:#FF3B3B;font-size:10px;padding-top:8px;'>"
                f"PURGA OFFLINE — {PURGA_ERROR_MSG[:120]}</div>",
                unsafe_allow_html=True
            )

    if purgar_btn and PURGA_DISPONIBLE:
        with st.spinner("Ejecutando PURGA..."):
            log_purga = []
            import io, contextlib
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                try:
                    _purga_mod.limpiar_caches()
                    _purga_mod.limpiar_modelo()
                except Exception as e_purga:
                    print(f"[ERROR] {e_purga}")
            log_purga = buf.getvalue()
        st.markdown(
            "<div style='background:rgba(0,212,170,0.06);border:1px solid #00D4AA22;"
            "border-left:3px solid #00D4AA;border-radius:4px;padding:10px 14px;"
            "font-size:10px;color:#6B7A99;line-height:1.7;white-space:pre-wrap;'>"
            f"{log_purga or '[OK] Purga completada.'}"
            "</div>", unsafe_allow_html=True
        )
        st.cache_resource.clear()
        _diagnostico_sistema.clear()

    st.divider()

    # ── PASO 2: DESCARGAR CON DIANA ───────────────────────────────────────────
    _header_paso('DIANA', 'PASO 2', 'EXTRACCIÓN', '#93C5FD')
    d2a, d2b, d2c = st.columns(3)
    with d2a:
        diana_hist = st.number_input(
            "Temporadas históricas extra", min_value=0, max_value=5, value=1,
            key="op_diana_hist",
            help="0 = solo temporada vigente. 1 = vigente + la anterior, etc."
        )
    with d2b:
        diana_forzar = st.checkbox(
            "Forzar re-descarga (ignora TTL)", key="op_diana_forzar",
            help="Re-descarga aunque el archivo ya exista y sea reciente"
        )
    with d2c:
        st.markdown(
            f"<div style='font-size:10px;color:#4A5568;padding-top:28px;'>"
            f"Carpeta destino: <span style='color:#C8FF00;'>{os.path.abspath(DATA_DIR)}</span></div>",
            unsafe_allow_html=True
        )

    col_d1, col_d2 = st.columns([1, 3])
    with col_d1:
        diana_btn = st.button(
            "▶ EJECUTAR DIANA", key="btn_diana",
            type="primary", use_container_width=True,
            disabled=not DIANA_DISPONIBLE
        )
    with col_d2:
        if not DIANA_DISPONIBLE:
            st.markdown(
                f"<div style='color:#FF3B3B;font-size:10px;padding-top:8px;'>"
                f"DIANA OFFLINE — {DIANA_ERROR_MSG[:120]}</div>",
                unsafe_allow_html=True
            )

    if diana_btn and DIANA_DISPONIBLE:
        with st.spinner("DIANA descargando CSVs..."):
            try:
                res_diana = _diana_ejecutar(
                    carpeta=DATA_DIR,
                    historico_temporadas=int(diana_hist),
                    forzar=diana_forzar,
                    auto_importar=False,
                )
                todos_d  = res_diana.get("europeas", []) + res_diana.get("extra", [])
                n_ok_d   = sum(1 for r in todos_d if r.get("estado") == "ok")
                n_omit_d = sum(1 for r in todos_d if r.get("estado") == "omitido")
                n_err_d  = sum(1 for r in todos_d if r.get("estado") == "error")
                resumen_html = "".join([
                    f"<div style='color:"
                    f"{'#00D4AA' if r.get('estado')=='ok' else ('#4A5568' if r.get('estado')=='omitido' else '#FF3B3B')}'>"
                    f"{r.get('liga_id','?')}&nbsp;&nbsp;"
                    f"<span style='color:#2D3748;'>{r.get('temporada','')}</span>&nbsp;&nbsp;"
                    f"{r.get('estado','?').upper()}"
                    f"{'&nbsp;&nbsp;'+str(r.get('bytes',0)//1024)+'KB' if r.get('estado')=='ok' else ''}"
                    f"{'&nbsp;&nbsp;'+r.get('motivo','')[:60] if r.get('estado') in ('error','omitido') else ''}"
                    f"</div>"
                    for r in todos_d
                ])
                st.markdown(
                    f"<div style='background:rgba(0,212,170,0.06);border:1px solid #00D4AA22;"
                    f"border-left:3px solid #00D4AA;border-radius:4px;padding:12px 16px;"
                    f"font-size:10px;line-height:1.8;'>"
                    f"<div style='color:#00D4AA;font-weight:600;margin-bottom:8px;'>"
                    f"DIANA OK — {n_ok_d} descargado(s) · {n_omit_d} en caché · {n_err_d} error(es)"
                    f"</div>{resumen_html}</div>",
                    unsafe_allow_html=True
                )
            except Exception as e_diana:
                st.error(f"ERROR DIANA: {e_diana}")
                st.code(traceback.format_exc(), language="text")

    st.divider()

    # ── PASO 3: ETL — CEREBELLIUM ─────────────────────────────────────────────
    _header_paso('CEREBELLIUM', 'PASO 3', 'TRANSFORMACIÓN', '#A78BFA')
    st.markdown(
        f"<div style='font-size:10px;color:#4A5568;margin-bottom:10px;line-height:1.7;'>"
        f"Importa todos los CSV de <span style='color:#C8FF00;'>{os.path.abspath(DATA_DIR)}</span> "
        f"a cerebrillum.db con deduplicación SHA-1.</div>",
        unsafe_allow_html=True
    )
    col_e1, col_e2 = st.columns([1, 3])
    with col_e1:
        etl_btn = st.button(
            "▶ EJECUTAR ETL", key="btn_etl_op",
            type="primary", use_container_width=True,
            disabled=not ETL_DISPONIBLE
        )
    with col_e2:
        if not ETL_DISPONIBLE:
            st.markdown(
                f"<div style='color:#FF3B3B;font-size:10px;padding-top:8px;'>"
                f"CEREBELLIUM OFFLINE — {ETL_ERROR_MSG[:120]}</div>",
                unsafe_allow_html=True
            )

    if etl_btn and ETL_DISPONIBLE:
        with st.spinner("CEREBELLIUM importando carpeta completa..."):
            try:
                inicializar_schema(DB_NAME)
                resultados_etl = importar_carpeta(DATA_DIR, db_path=DB_NAME)
                total_ins = sum(r.get("insertados", 0) for r in resultados_etl)
                total_dup = sum(r.get("duplicados", 0) for r in resultados_etl)
                total_err = sum(r.get("errores", 0)    for r in resultados_etl)
                if MOTOR_DISPONIBLE:
                    for r in resultados_etl:
                        if r.get("liga_id"):
                            try:
                                FeatureStore(DB_NAME).invalidar(r["liga_id"])
                            except Exception:
                                pass
                st.markdown(
                    f"<div style='background:rgba(0,212,170,0.06);border:1px solid #00D4AA22;"
                    f"border-left:3px solid #00D4AA;border-radius:4px;padding:12px 16px;"
                    f"font-size:11px;color:#00D4AA;line-height:1.7;'>"
                    f"ETL OK — {len(resultados_etl)} archivo(s) procesado(s) · "
                    f"+{total_ins} nuevos · {total_dup} dup · {total_err} err"
                    f"</div>",
                    unsafe_allow_html=True
                )
                st.cache_resource.clear()
                _diagnostico_sistema.clear()
            except Exception as e_etl:
                st.error(f"ERROR ETL: {e_etl}")
                st.code(traceback.format_exc(), language="text")

    st.divider()

    # ── PASO 4: ENTRENAR MODELO ───────────────────────────────────────────────
    _header_paso('CONTINUITY', 'PASO 4', 'INFERENCIA', '#E2E8F0')
    pkl_path = os.path.join(MODELOS_DIR, "motor_ia.pkl")
    if os.path.exists(pkl_path):
        age_d = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(pkl_path))).days
        sz_mb = os.path.getsize(pkl_path) / (1024 * 1024)
        st.markdown(
            f"<div style='font-size:10px;color:#4A5568;margin-bottom:10px;'>"
            f"Modelo actual: {age_d}d antiguo · {sz_mb:.1f}MB</div>",
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            "<div style='font-size:10px;color:#F59E0B;margin-bottom:10px;'>"
            "Sin modelo entrenado — entrena ahora para activar predicciones RF/GBM</div>",
            unsafe_allow_html=True
        )

    col_t1, col_t2 = st.columns([1, 3])
    with col_t1:
        entrenar_btn = st.button(
            "▶ ENTRENAR MODELO", key="btn_entrenar",
            type="primary", use_container_width=True,
            disabled=not MOTOR_DISPONIBLE
        )
    with col_t2:
        if not MOTOR_DISPONIBLE:
            st.markdown(
                f"<div style='color:#FF3B3B;font-size:10px;padding-top:8px;'>"
                f"MOTOR OFFLINE — {MOTOR_ERROR_MSG[:120]}</div>",
                unsafe_allow_html=True
            )

    if entrenar_btn and MOTOR_DISPONIBLE:
        with st.spinner("Construyendo features y entrenando modelo... (puede tardar 1-3 min)"):
            try:
                from continuitis import (
                    DataPipeline as _DP_tr, FeatureStore as _FS_tr, MotorInferenciaIA as _Motor_tr,
                    MIN_REGISTROS_GLOBAL as _MRG,
                )
                pipeline_tr  = _DP_tr(DB_NAME)
                fs_tr        = _FS_tr(DB_NAME)
                motor_tr     = _Motor_tr()
                df_base_tr   = pipeline_tr.extraer_dataset_base()
                if len(df_base_tr) >= _MRG:
                    meta_g = {"avg_goles": 2.6, "rho": -0.04, "lambda_a": 1.3, "lambda_d": 1.3}
                    ligas_en_db = df_base_tr["liga_id"].unique()
                    dfs_tr = []
                    for lig in ligas_en_db:
                        df_lig = fs_tr.cargar_o_calcular(lig, pipeline_tr)
                        if not df_lig.empty:
                            dfs_tr.append(df_lig)
                    df_feat_tr = pd.concat(dfs_tr, ignore_index=True) if dfs_tr \
                                 else pipeline_tr.construir_features(df_base_tr, meta_g)
                    motor_tr.entrenar(df_feat_tr)
                    n_vect = len(df_feat_tr)
                    st.markdown(
                        f"<div style='background:rgba(0,212,170,0.06);border:1px solid #00D4AA22;"
                        f"border-left:3px solid #00D4AA;border-radius:4px;padding:12px 16px;"
                        f"font-size:11px;color:#00D4AA;line-height:1.7;'>"
                        f"MODELO ENTRENADO OK · {n_vect:,} vectores · {len(ligas_en_db)} liga(s) · "
                        f"guardado en {pkl_path}"
                        f"</div>",
                        unsafe_allow_html=True
                    )
                    st.cache_resource.clear()
                    _diagnostico_sistema.clear()
                else:
                    st.warning(
                        f"Solo {len(df_base_tr)} registros en DB "
                        f"(mínimo {_MRG}). "
                        "Importa más datos en el PASO 3 primero."
                    )
            except Exception as e_train:
                st.error(f"ERROR ENTRENAMIENTO: {e_train}")
                st.code(traceback.format_exc(), language="text")

    st.divider()

    # ── Estado final del sistema después de los pasos ─────────────────────────
    _render_estado_sistema_global(conn)


# ==============================================================================
# SIDEBAR  [v51-BUG-1] Etiqueta y clave del menú alineadas: no hay módulo
# Mundial 2026 implementado -- la posición 8 corresponde a RENTABILIDAD
# (_MENU_KEYS[7]), y ahora el label lo refleja (antes decía "MUNDIAL 2026"
# pero internamente activaba "RENTABILIDAD", que además no tenía módulo ni
# entrada en dispatch -- caía en RADAR silenciosamente).
# [v52-GAP13] n_pend ahora viene de la cola persistente en SQLite.
# ==============================================================================
def _render_sidebar(conn) -> str:
    n_pend = len(_cola_get(conn))
    checks = _diagnostico_sistema()
    n_crit = sum(1 for c in checks if c["nivel"] == "CRITICAL")
    n_warn = sum(1 for c in checks if c["nivel"] == "WARN")
    dg_color = "#FF3B3B" if n_crit else ("#F59E0B" if n_warn else "#00D4AA")
    dg_label = f"{n_crit} CRITICO" if n_crit else (f"{n_warn} AVISO" if n_warn else "SANO")

    with st.sidebar:
        st.markdown(
            "<div style='padding:16px 0 8px; position:relative;'>"
            "<img src='data:image/png;base64,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' style='position:absolute; top:-4px; right:-10px; width:75px; opacity:0.95; filter: drop-shadow(0px 2px 4px rgba(0,0,0,0.5));'/>"
            "<div style='font-family:Outfit,sans-serif;font-weight:800;font-size:20px;"
            "color:#FFFFFF;letter-spacing:-0.02em;line-height:1.2;'>PACIFICO</div>"
            "<div style='font-family:Outfit,sans-serif;font-weight:600;font-size:14px;"
            "color:#C4A1FF;letter-spacing:0.3em;line-height:1.4;'>KyJ</div>"
            f"<div style='font-size:9px;color:#1E2D5E;letter-spacing:0.18em;margin-top:2px;'>"
            f"CONTINUITY {VERSION}</div>"
            "</div>", unsafe_allow_html=True
        )
        st.divider()

        motor_c = "#00D4AA" if MOTOR_DISPONIBLE else "#FF3B3B"
        etl_c   = "#00D4AA" if ETL_DISPONIBLE   else "#F59E0B"
        st.markdown(
            f"<div style='font-family:Geist Mono;font-size:10px;line-height:2;'>"
            f"<span style='color:#4A5568;'>MOTOR </span><span style='color:{motor_c};'>{'ON' if MOTOR_DISPONIBLE else 'OFF'}</span>"
            f"&nbsp;&nbsp;"
            f"<span style='color:#4A5568;'>ETL </span><span style='color:{etl_c};'>{'ON' if ETL_DISPONIBLE else 'OFF'}</span>"
            f"<br><span style='color:#4A5568;'>SALUD </span><span style='color:{dg_color};'>{dg_label}</span>"
            f"</div>", unsafe_allow_html=True
        )

        if n_pend > 0:
            st.markdown(
                f"<div class='am-queue-badge'>COLA — {n_pend} pendiente(s)</div>",
                unsafe_allow_html=True
            )
        st.divider()

        # [v51-BUG-1] Construido desde _MENU_LABELS_BASE (fuente única,
        # mismo orden/longitud que _MENU_KEYS) en vez de una lista duplicada
        # a mano -- así una etiqueta nunca puede volver a apuntar a la clave
        # equivocada. Solo CIERRE recibe el badge de pendientes dinámico.
        menu_labels = [
            f"{label} {'[ '+str(n_pend)+' ]' if (label == 'CIERRE' and n_pend) else ''}".strip()
            for label in _MENU_LABELS_BASE
        ]

        modulo_actual = _get_modulo()
        idx_actual    = _MENU_KEYS.index(modulo_actual) if modulo_actual in _MENU_KEYS else 0
        sel_label     = st.radio("NAV", menu_labels, index=idx_actual, label_visibility="collapsed")
        sel_idx       = menu_labels.index(sel_label)
        sel_key       = _MENU_KEYS[sel_idx]
        if sel_key != modulo_actual:
            _set_modulo(sel_key)

        st.divider()

        if n_crit or n_warn:
            with st.expander(f"DIAGNÓSTICO ({dg_label})", expanded=False):
                for c in checks:
                    nc    = c["nivel"]
                    color = "#FF3B3B" if nc=="CRITICAL" else ("#F59E0B" if nc=="WARN" else "#4A5568")
                    det   = f"<br><span style='color:#2D3748;font-size:9px;'>{c['detalle']}</span>" if c.get("detalle") else ""
                    st.markdown(
                        f"<div style='font-size:10px;color:{color};margin:3px 0;line-height:1.6;'>"
                        f"[{nc}] {c['msg']}{det}</div>",
                        unsafe_allow_html=True
                    )

        st.markdown(
            f"<div class='am-muted' style='margin-top:8px;'>"
            f"{datetime.now().strftime('%H:%M:%S')} · {DB_NAME}"
            f"</div>", unsafe_allow_html=True
        )
        if st.button("REFRESH", type="secondary"):
            _diagnostico_sistema.clear()
            st.cache_resource.clear()
            st.rerun()

    return _get_modulo()


# ==============================================================================
# FOOTER
# ==============================================================================
def _render_footer():
    st.markdown(
        "<div class='am-footer'>"
        "POWERED BY &nbsp;<span>PACIFICO KyJ</span>&nbsp; ·&nbsp; "
        "CONTINUITY v4.5 (10 gaps) + CEREBELLIUM v4.1 &nbsp;·&nbsp; "
        f"BUILD {VERSION} &nbsp;·&nbsp; "
        "ADAPTIVE PREDICTION ECOSYSTEM"
        "</div>", unsafe_allow_html=True
    )


# ==============================================================================
# MAIN — ENSAMBLAJE v5.2
# [v52-GAP13] inicializar_schema(DB_NAME) se ejecuta al arrancar, idempotente
# (solo CREATE TABLE IF NOT EXISTS), para garantizar que
# cola_partidos_pendientes exista incluso si nunca se corrió CEREBELLIUM.py
# ni diana.py --importar antes -- sin esto, _cola_get()/_cola_append()
# fallarían con "no such table" en una instalación nueva.
# ==============================================================================
def main():
    st.markdown(_CSS, unsafe_allow_html=True)
    conn = conectar_cerebrillum()

    if ETL_DISPONIBLE:
        try:
            inicializar_schema(DB_NAME)
        except Exception as e:
            st.session_state["_schema_init_error"] = str(e)

    modulo = _render_sidebar(conn)

    dispatch = {
        "OPERACION":     lambda: modulo_operacion(conn),
        "RADAR":         lambda: modulo_radar(conn),
        "INFERENCIA":    lambda: modulo_inferencia(conn),
        "CIERRE":        lambda: modulo_feedback(conn),
        "DASHBOARD":     lambda: modulo_dashboard(conn),
        "ETL":           lambda: modulo_etl(conn),
        "AUDITORIA":     lambda: modulo_auditoria(conn),
        "PIPELINE":      lambda: modulo_flujo_pipeline(conn),
        "RENTABILIDAD":  lambda: modulo_rentabilidad(conn),
    }

    fn = dispatch.get(modulo)
    if fn:
        fn()
    else:
        modulo_operacion(conn)

    _render_footer()


if __name__ == "__main__":
    main()