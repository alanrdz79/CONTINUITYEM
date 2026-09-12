# -*- coding: utf-8 -*-
"""
==============================================================================
  CONTINUITY v4.6  |  SHIM DE COMPATIBILIDAD
==============================================================================

  Código modularizado en el paquete continuitis/:
    constantes.py  — constantes globales y helpers
    estado.py      — EstadoAdaptativo, MotorOnline, BlendAdaptativo
    features.py    — FeatureStore, DataPipeline
    inferencia.py  — MotorInferenciaIA
    ajuste.py      — AjustadorCuantico, RhoDinamicoCalculator, TransmutadorEstocastico
    financiero.py  — EscudoFinanciero
    auditoria.py   — RegistradorFeedback, AuditorRentabilidad, Backtester

  Este archivo preserva la compatibilidad regresiva:
    from CONTINUITY import DataPipeline  →  sigue funcionando
  y mantiene las funciones CLI (ejecutar_radar_mercados, main) que
  usan input() interactivo y no pertenecen al paquete reutilizable.

==============================================================================
"""

# Re-exportar TODO desde continuitis para compatibilidad regresiva
from continuitis import *                       # noqa: F401,F403

# Imports explícitos que main() y ejecutar_radar_mercados() necesitan
from continuitis import (
    EstadoAdaptativo, MotorOnline, BlendAdaptativo,
    DataPipeline, FeatureStore, MotorInferenciaIA,
    AjustadorCuantico, RhoDinamicoCalculator, TransmutadorEstocastico,
    EscudoFinanciero, RegistradorFeedback, AuditorRentabilidad,
    DATABASE_URL, ALL_LIGAS, FASE_1_NAFTA, FASE_2_EUROPA,
    PARON_FIFA_MESES, MERCADOS_LABELS, MIN_REGISTROS_GLOBAL,
    FEATURE_COLUMNS,
    _safe_float, _safe_int,
)

import numpy as np
import pandas as pd
from datetime import datetime
import sqlite3
import difflib

def _normalizar_equipo_id_cli(raw: str, liga_id: str, db_url: str) -> tuple:
    raw = raw.strip().lower()
    if not raw:
        return "", "El ID de equipo no puede estar vacío."
    
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
            return "", f"El equipo '{raw}' pertenece a '{otra_liga}'. Usa '{liga_id}:nombre_equipo'."
        id_buscado = raw

    try:
        conn = sqlite3.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT equipo_id FROM dim_equipos WHERE liga_id=?", (liga_id,))
        equipos_validos = [row[0] for row in cur.fetchall()]
        conn.close()
        
        if equipos_validos:
            if id_buscado in equipos_validos:
                return id_buscado, ""
            
            matches = difflib.get_close_matches(id_buscado, equipos_validos, n=1, cutoff=0.6)
            if matches:
                print(f"  [INFO] Equipo autocorregido: '{raw}' -> '{matches[0]}'")
                return matches[0], ""
            
            slug_buscado = id_buscado.split(":")[1] if ":" in id_buscado else id_buscado
            slugs_validos = {eq.split(":")[1]: eq for eq in equipos_validos if ":" in eq}
            matches_slug = difflib.get_close_matches(slug_buscado, list(slugs_validos.keys()), n=1, cutoff=0.5)
            if matches_slug:
                sugerido = slugs_validos[matches_slug[0]]
                print(f"  [INFO] Equipo autocorregido: '{raw}' -> '{sugerido}'")
                return sugerido, ""
            
            return "", f"Equipo '{raw}' no encontrado. Sugerencias: {', '.join([e.split(':')[1] for e in equipos_validos[:5]])}..."
    except Exception:
        pass

    return id_buscado, ""# ==============================================================================
# RADAR DE MERCADOS
# ==============================================================================
def ejecutar_radar_mercados() -> None:
    ahora    = datetime.now()
    mes, ano = ahora.month, ahora.year

    def _activa(rango):
        ini, fin = rango
        if ini > fin: return mes >= ini or mes <= fin
        return ini <= mes <= fin

    def _fase(lid, activa):
        if not activa: return "[OFF] RECESO"
        if lid in ("liga_mx_clausura","liga_mx_apertura"):
            if mes in [11,12]: return "[HOT] LIGUILLA"
            if mes == 10:      return "REPECHAJE"
            if mes in [1,7]:   return "[>>] INICIO"
        if lid == "champions_league":
            for meses, label in [
                ({9,10,11,12}, "GRUPOS"),
                ({2,3},        "OCTAVOS"),
                ({4},          "CUARTOS+SEMIS 🔥"),
                ({5,6},        "FINAL"),
            ]:
                if mes in meses: return label
        if lid == "mls" and mes in [10,11,12]: return "[HOT] MLS PLAYOFFS"
        if mes in [4,5]:  return "[HOT] RECTA FINAL"
        if mes in [8,9]:  return "[>>] INICIO"
        if mes == 12:     return "[FRIO] RECESO"
        return "[GOL] REGULAR"

    W = 78
    print("\n" + "=" * W)
    print(f"[RADAR] CONTINUITY v4.1 | {ahora.strftime('%Y-%m-%d %H:%M')}")
    print("=" * W)
    if mes in PARON_FIFA_MESES:
        print(f"  [WARN] PARÓN FIFA MES {mes} | reducir stake ~20%")
    if ano == 2026 and mes in [6,7]:
        print("  [COPA] WORLD CUP 2026 ACTIVO")
    for titulo, ligas in [("NAFTA", FASE_1_NAFTA), ("UEFA", FASE_2_EUROPA)]:
        print(f"\n  {titulo}")
        for lid, meta in ligas.items():
            activa = _activa(meta["rango"])
            print(f"  >> {meta['nombre']:<34} {'ACTIVA' if activa else 'RECESO'} | {_fase(lid,activa)}")
    print("=" * W + "\n")


# ==============================================================================
# MAIN — Flujo completo v4.1
# ==============================================================================
def main():
    ejecutar_radar_mercados()

    estado        = EstadoAdaptativo()
    motor_online  = MotorOnline()
    blend         = BlendAdaptativo()
    pipeline      = DataPipeline(DATABASE_URL)
    feature_store = FeatureStore(DATABASE_URL)
    motor_base    = MotorInferenciaIA()
    ajustador     = AjustadorCuantico()
    transmutor    = TransmutadorEstocastico()
    feedback      = RegistradorFeedback(estado, motor_online, blend)
    auditor       = AuditorRentabilidad()  # [v4.3-GAP5] ROI real por liga

    print("[Procesando] Extrayendo features...")
    modelo_disponible = False
    try:
        df_base = pipeline.extraer_dataset_base()
        if len(df_base) >= MIN_REGISTROS_GLOBAL:
            meta_global = {"avg_goles": 2.6, "rho": -0.04, "lambda_a": 1.3, "lambda_d": 1.3}
            try:
                ligas_en_db = df_base["liga_id"].unique()
                dfs = []
                for lig in ligas_en_db:
                    df_lig = feature_store.cargar_o_calcular(lig, pipeline)
                    if not df_lig.empty:
                        dfs.append(df_lig)
                df_feat = pd.concat(dfs, ignore_index=True) if dfs \
                          else pipeline.construir_features(df_base, meta_global)
            except Exception:
                df_feat = pipeline.construir_features(df_base, meta_global)

            motor_base.entrenar(df_feat)
            modelo_disponible = True
            print(f"[OK] Motor listo con {len(df_base)} vectores.\n")
        else:
            print(f"[INFO] {len(df_base)} registros — Sliding Window puro.\n")
    except Exception as e:
        print(f"[WARN] Pipeline: {e}\n")

    print("-" * 55)
    liga_id   = input("  > Liga (ej. premier_league): ").strip().lower()
    local_raw = input("  > ID Equipo Local          : ").strip().lower()
    local, err_l = _normalizar_equipo_id_cli(local_raw, liga_id, DATABASE_URL)
    if err_l:
        print(f"  [ERROR] LOCAL: {err_l}")
        return
    visita_raw = input("  > ID Equipo Visitante      : ").strip().lower()
    visita, err_v = _normalizar_equipo_id_cli(visita_raw, liga_id, DATABASE_URL)
    if err_v:
        print(f"  [ERROR] VISITANTE: {err_v}")
        return
    jornada   = _safe_int(input("  > Jornada                  : ").strip()) or 1
    cuota_1   = _safe_float(input("  > Cuota Victoria Local (1) : ").strip()) or 1.01
    cuota_x   = _safe_float(input("  > Cuota Empate         (X) : ").strip()) or 1.01
    cuota_2   = _safe_float(input("  > Cuota Victoria Visita(2) : ").strip()) or 1.01
    es_clasi  = 1 if input("  > ¿Clásico? (s/n)[n]: ").strip().lower() == "s" else 0
    es_elim   = 1 if input("  > ¿Eliminatoria? (s/n)[n]: ").strip().lower() == "s" else 0
    es_vuelta = 1 if input("  > ¿Vuelta? (s/n)[n]: ").strip().lower() == "s" else 0
    desc_l    = _safe_int(input("  > Días descanso local [3]: ").strip())   or 3
    desc_v    = _safe_int(input("  > Días descanso visita [3]: ").strip())  or 3
    asist_est = _safe_float(input("  > Llenado estadio [0.8]: ").strip())   or 0.8
    temp_c    = _safe_float(input("  > Temperatura °C [22.0]: ").strip())   or 22.0
    bankroll  = _safe_float(input("  > Bankroll $ [1000.0]: ").strip())     or 1000.0
    es_inicio = 1 if jornada <= 3 else 0

    cuota_over_raw  = input("  > Cuota Over 2.5  (Enter=omitir): ").strip()
    cuota_under_raw = input("  > Cuota Under 2.5 (Enter=omitir): ").strip()
    cuota_over  = _safe_float(cuota_over_raw)  if cuota_over_raw  else None
    cuota_under = _safe_float(cuota_under_raw) if cuota_under_raw else None

    meta = pipeline.obtener_meta_liga(liga_id)
    estado.inicializar_equipo(local,  liga_id, meta["lambda_a"])
    estado.inicializar_equipo(visita, liga_id, meta["lambda_d"])
    motor_online.inicializar(local,  meta["lambda_a"])
    motor_online.inicializar(visita, meta["lambda_d"])

    lam_l_sw, lam_v_sw = pipeline.obtener_lambdas_sliding_window(
        local, visita, liga_id, meta, feature_store
    )
    payload_l = pipeline.obtener_payload_micro(local)
    payload_v = pipeline.obtener_payload_micro(visita)
    payload_l["temperatura_celsius"] = temp_c
    payload_v["temperatura_celsius"] = temp_c

    lam_l_adj, _ = ajustador.aplicar(lam_l_sw, lam_v_sw, payload_l)
    lam_v_adj, _ = ajustador.aplicar(lam_v_sw, lam_l_sw, payload_v)

    lam_l_online, lam_v_online = motor_online.predecir(local, visita, liga_id, meta)

    lam_inercia_l = estado.memoria.get(local,  {}).get("ataque", meta["lambda_a"])
    lam_inercia_v = estado.memoria.get(visita, {}).get("ataque", meta["lambda_d"])

    wb, wo = blend.pesos(liga_id)
    print(f"\n  {blend.descripcion(liga_id)}")

    vector = pipeline.construir_vector_inferencia(
        equipo_local=local, equipo_visitante=visita, liga_id=liga_id, meta=meta,
        numero_jornada=jornada, es_clasico=es_clasi,
        es_eliminatoria=es_elim, es_partido_vuelta=es_vuelta,
        dias_descanso_local=desc_l, dias_descanso_vis=desc_v,
        factor_presion=asist_est, temperatura=temp_c,
    )

    if modelo_disponible:
        lam_l_rf, lam_v_rf, src = motor_base.predecir(vector, liga_id)
        lam_l_sw_puro = 0.40 * lam_l_adj + 0.20 * lam_inercia_l
        lam_v_sw_puro = 0.40 * lam_v_adj + 0.20 * lam_inercia_v
        lam_l_final   = lam_l_sw_puro + 0.40 * (wb * lam_l_rf + wo * lam_l_online)
        lam_v_final   = lam_v_sw_puro + 0.40 * (wb * lam_v_rf + wo * lam_v_online)
    else:
        src         = "Sliding Window + Online"
        lam_l_final = 0.55 * lam_l_adj + 0.25 * lam_l_online + 0.20 * lam_inercia_l
        lam_v_final = 0.55 * lam_v_adj + 0.25 * lam_v_online + 0.20 * lam_inercia_v

    lam_l_final = float(np.clip(lam_l_final, 0.2, 5.0))
    lam_v_final = float(np.clip(lam_v_final, 0.2, 5.0))

    temporada_actual = pipeline.obtener_temporada_actual(liga_id)
    tabla_posiciones = pipeline.calcular_tabla_posiciones(liga_id, temporada_actual) \
                       if temporada_actual else {}
    factor_tabla_l = AjustadorCuantico.factor_tabla_posiciones(tabla_posiciones, local)
    factor_tabla_v = AjustadorCuantico.factor_tabla_posiciones(tabla_posiciones, visita)
    if tabla_posiciones:
        print(f"\n  [GAP6] Tabla {temporada_actual}: factor_local={factor_tabla_l:.4f} "
              f"factor_visita={factor_tabla_v:.4f} "
              f"({len(tabla_posiciones)} equipos con historial)")
    lam_l_final = float(np.clip(lam_l_final * factor_tabla_l, 0.2, 5.0))
    lam_v_final = float(np.clip(lam_v_final * factor_tabla_v, 0.2, 5.0))

    _meta_liga_tacde  = ALL_LIGAS.get(liga_id, {})
    _rango_liga       = _meta_liga_tacde.get("rango", [8, 5])
    _JORNADAS_LIGA = {
        "premier_league": 38, "la_liga": 38, "serie_a": 38,
        "ligue_1": 34, "bundesliga": 34, "eredivisie": 34,
        "league_one": 46, "champions_league": 8,
        "liga_mx_clausura": 17, "liga_mx_apertura": 17,
        "concacaf_cl": 8, "mls": 34,
    }
    _total_jornadas = _JORNADAS_LIGA.get(liga_id, 38)

    tacde_local  = pipeline.calcular_presion_tacde(
        local, liga_id, tabla_posiciones, jornada, _total_jornadas
    )
    tacde_visita = pipeline.calcular_presion_tacde(
        visita, liga_id, tabla_posiciones, jornada, _total_jornadas
    )
    factor_tacde_l = AjustadorCuantico.factor_intensidad_adaptativa_tacde(tacde_local)
    factor_tacde_v = AjustadorCuantico.factor_intensidad_adaptativa_tacde(tacde_visita)
    lam_l_final    = float(np.clip(lam_l_final * factor_tacde_l, 0.2, 5.0))
    lam_v_final    = float(np.clip(lam_v_final * factor_tacde_v, 0.2, 5.0))

    _tacde_activo = (factor_tacde_l != 1.0 or factor_tacde_v != 1.0)
    print(
        f"\n  [TACDE v4.6] Local  PA={tacde_local['PA']:.3f} RC={tacde_local['RC']:.3f} "
        f"IA={tacde_local['IA']:.3f} EC={tacde_local['EC']:.3f} → factor={factor_tacde_l:.4f}"
    )
    print(
        f"  [TACDE v4.6] Visita PA={tacde_visita['PA']:.3f} RC={tacde_visita['RC']:.3f} "
        f"IA={tacde_visita['IA']:.3f} EC={tacde_visita['EC']:.3f} → factor={factor_tacde_v:.4f}"
    )
    if not _tacde_activo:
        print("  [TACDE v4.6] Sin asimetría adaptativa detectada — lambdas sin ajuste TACDE")

    rho_base_liga = _safe_float(meta.get("rho", -0.04)) or -0.04
    mae_liga      = motor_online.mae_reciente(liga_id, n=10)
    rho_dinamico  = RhoDinamicoCalculator.calcular(
        lam_l_final, lam_v_final, rho_base_liga, mae_liga
    )
    print(f"\n  {RhoDinamicoCalculator.describir(lam_l_final, lam_v_final, rho_base_liga, mae_liga)}")

    p1, px, p2 = transmutor.calcular_1x2(
        lam_l_final, lam_v_final,
        es_inicio, es_clasi,
        rho_liga=rho_base_liga,
        rho_dinamico=rho_dinamico,
    )

    print("\n" + "-" * 55)
    print(f"[PROBABILIDADES v4.6] {src}")
    print(f"  λ Local  [{local}]:   {lam_l_final:.3f}  (online: {lam_l_online:.3f})")
    print(f"  λ Visita [{visita}]:  {lam_v_final:.3f}  (online: {lam_v_online:.3f})")
    print(f"  ρ dinámico: {rho_dinamico:.4f} (base: {rho_base_liga:.4f})")
    print(f"  P(1)={p1*100:.1f}%  P(X)={px*100:.1f}%  P(2)={p2*100:.1f}%")
    print(f"  Σ = {(p1+px+p2)*100:.2f}%  (debe ser ~100%)")

    operar, mercado, ev, stake, detalle_fin = EscudoFinanciero.evaluar_triple(
        p1, px, p2, cuota_1, cuota_x, cuota_2, bankroll
    )
    cuotas_1x2_map = {"1": cuota_1, "X": cuota_x, "2": cuota_2}
    cuota_elegida  = cuotas_1x2_map.get(mercado, 0.0) if mercado else 0.0
    mercado_label  = MERCADOS_LABELS.get(mercado, "—") if mercado else "—"

    print(f"\n  [1X2] EV1={detalle_fin.get('ev_1',0)*100:+.2f}% "
          f"EVX={detalle_fin.get('ev_x',0)*100:+.2f}% "
          f"EV2={detalle_fin.get('ev_2',0)*100:+.2f}% "
          f"| overround={detalle_fin.get('overround',0)*100:.2f}%")
    if operar:
        print(f"  OPERAR: {mercado_label} (mercado {mercado}) | "
              f"cuota {cuota_elegida:.2f} | EV {ev*100:+.2f}% | Stake ${stake}")
    else:
        print(f"  NO OPERAR | mejor EV visto: {ev*100:+.2f}% "
              f"(no supera EV_THRESHOLD y/o MIN_EDGE_REQUERIDO en ningún mercado)")

    p_over, p_under = transmutor.calcular_over_under(
        lam_l_final, lam_v_final, rho_liga=rho_base_liga,
        rho_dinamico=rho_dinamico, linea=2.5
    )
    print(f"\n  [O/U 2.5] P(Over)={p_over*100:.1f}%  P(Under)={p_under*100:.1f}%")

    operar_ou, mercado_ou, ev_ou, stake_ou, detalle_ou = False, None, 0.0, 0.0, {}
    if cuota_over is not None and cuota_under is not None:
        operar_ou, mercado_ou, ev_ou, stake_ou, detalle_ou = EscudoFinanciero.evaluar_binario(
            p_over, p_under, cuota_over, cuota_under, bankroll, etiquetas=("OVER", "UNDER")
        )
        if operar_ou:
            print(f"  OPERAR O/U: {mercado_ou} | EV {ev_ou*100:+.2f}% | Stake ${stake_ou}")
        else:
            print(f"  NO OPERAR O/U | mejor EV visto: {ev_ou*100:+.2f}%")
    else:
        print("  (cuotas O/U no ingresadas — sin evaluación financiera de este mercado)")

    print("\n" + "=" * 55)
    goles_l_r = _safe_int(input("Goles reales LOCAL:     ") or "0") or 0
    goles_v_r = _safe_int(input("Goles reales VISITANTE: ") or "0") or 0
    anomalia  = input("¿Anomalía? (s/n): ").lower() == "s"

    feedback.registrar(
        local, visita, goles_l_r, goles_v_r,
        lam_l_final, lam_v_final,
        lam_l_online, lam_v_online,
        liga_id, anomalia,
        rho_usado=rho_dinamico,
    )

    if operar:
        resultado_real = "1" if goles_l_r > goles_v_r else ("X" if goles_l_r == goles_v_r else "2")
        gano = (resultado_real == mercado)
        predictor_lider = "RF" if wb >= wo else "ONLINE"
        resultado_roi = auditor.registrar_apuesta(
            liga_id, mercado, stake, cuota_elegida, gano,
            predictor_lider=predictor_lider,
            tacde_activo=_tacde_activo,
        )
        print(f"\n  [ROI] {mercado_label} ({'ACIERTO' if gano else 'FALLO'}) | "
              f"predictor líder: {predictor_lider} | "
              f"pnl={resultado_roi['pnl_apuesta']:+.2f} | "
              f"ROI liga {liga_id}: {resultado_roi['roi_liga']:+.2%} | "
              f"ROI global: {resultado_roi['roi_global']:+.2%}")

        wb_roi, wo_roi, diag_roi = blend.ajustar_por_roi(liga_id, auditor)
        if diag_roi["ajuste_aplicado"]:
            print(f"  [GAP8] Sugerencia por ROI: {wb_roi:.2f}·RF + {wo_roi:.2f}·Online "
                  f"({diag_roi['razon']})")
        else:
            print(f"  [GAP8] Sin ajuste por ROI aún: {diag_roi['razon']}")

    if operar_ou:
        total_goles_real = goles_l_r + goles_v_r
        resultado_ou_real = "OVER" if total_goles_real > 2.5 else "UNDER"
        gano_ou = (resultado_ou_real == mercado_ou)
        cuota_ou_elegida = cuota_over if mercado_ou == "OVER" else cuota_under
        resultado_roi_ou = auditor.registrar_apuesta(
            liga_id, f"OU2.5_{mercado_ou}", stake_ou, cuota_ou_elegida, gano_ou,
            tacde_activo=_tacde_activo,
        )
        print(f"  [ROI O/U] {mercado_ou} ({'ACIERTO' if gano_ou else 'FALLO'}) | "
              f"pnl={resultado_roi_ou['pnl_apuesta']:+.2f}")

    print("\n[OK] Ciclo v4.6 completo.")


if __name__ == "__main__":
    main()