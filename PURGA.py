# -*- coding: utf-8 -*-
"""
=====================================================================================================================
PURGA v1.0  |  Sistema de Limpieza y Mantenimiento del Proyecto CONTINUITY
                  Capa Afectada: Transversal (Infraestructura)
======================================================================================================================
Objetivo: Automatizar la purga de cachés, archivos temporales y modelos obsoletos
para garantizar un entorno limpio y consistente para el re-entrenamiento de modelos predictivos y la ejecución de la aplicación Streamlit.
Descripción:
Automatiza el vaciado de cachés temporales, memoria de Streamlit y archivos pre-compilados 
antes de eliminar el modelo predictivo. Garantiza que el próximo ciclo de entrenamiento 
se construya sobre datos completamente frescos, evitando la contaminación cruzada por cachés residuales.
"""

import os
import glob
import shutil
import subprocess
from datetime import datetime

# ==============================================================================
# CONFIGURACIÓN (Basada en las constantes de CONTINUITY v4.6 y AEGIST v5.1)
# ==============================================================================
FEATURE_STORE_DIR = "feature_store"
MODELOS_DIR       = "modelos_rf"
MODELO_PKL        = "motor_ia.pkl"
CSV_DATA_DIR      = "csv_data"

class Log:
    @staticmethod
    def paso(msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] >>> {msg}")

#------------------------FASE 1: PURGA DE CACHES DE DATOS Y ENTORNO------------------------
def limpiar_caches():
    Log.paso("Fase 1: Iniciando purga de cachés de datos y entorno...")

    # 1. Borrar archivos Parquet en Feature Store (Capa 3)
    if os.path.exists(FEATURE_STORE_DIR):
        parquets = glob.glob(os.path.join(FEATURE_STORE_DIR, "*.parquet"))
        for p in parquets:
            try:
                os.remove(p)
            except Exception as e:
                Log.paso(f"[WARN] No se pudo borrar {p}: {e}")
        Log.paso(f"[OK] Eliminados {len(parquets)} archivos de caché (.parquet) en '{FEATURE_STORE_DIR}/'")
    else:
        Log.paso(f"[INFO] Directorio '{FEATURE_STORE_DIR}' no encontrado. Omitiendo.")

    # 1.5 Borrar archivos en csv_data
    if os.path.exists(CSV_DATA_DIR):
        archivos_csv = glob.glob(os.path.join(CSV_DATA_DIR, "*.*"))
        count_csv = 0
        for arc in archivos_csv:
            try:
                os.remove(arc)
                count_csv += 1
            except Exception as e:
                Log.paso(f"[WARN] No se pudo borrar {arc}: {e}")
        Log.paso(f"[OK] Eliminados {count_csv} archivos en '{CSV_DATA_DIR}/'")
    else:
        Log.paso(f"[INFO] Directorio '{CSV_DATA_DIR}' no encontrado. Omitiendo.")

    # 2. Borrar __pycache__ (Caché de compilación de Python) excluyendo '.venv' y 'pruebas'
    count_pyc = 0
    for root, dirs, files in os.walk("."):
        # Excluir directorios que no deben tocarse para evitar daños colaterales
        if ".venv" in dirs:
            dirs.remove(".venv")
        if "pruebas" in dirs:
            dirs.remove("pruebas")
            
        if "__pycache__" in dirs:
            pc_path = os.path.join(root, "__pycache__")
            try:
                shutil.rmtree(pc_path, ignore_errors=True)
                count_pyc += 1
            except Exception as e:
                Log.paso(f"[WARN] No se pudo borrar {pc_path}: {e}")
            dirs.remove("__pycache__") # No iterar dentro de __pycache__
    
    Log.paso(f"[OK] Eliminados {count_pyc} directorios '__pycache__'.")

#---------------------- FASE 2: PURGA DE MODELO PREDICTIVO Y CACHÉ DE STREAMLIT ------------------------
    
    # 3. Limpiar caché de Streamlit (UI - AEGIST)
    Log.paso("Ejecutando 'streamlit cache clear'...")
    try:
        # Se invoca el comando CLI de Streamlit de forma programática
        subprocess.run(["streamlit", "cache", "clear"], check=True, capture_output=True)
        Log.paso("[OK] Caché interno de Streamlit purgado con éxito.")
    except Exception as e:
        Log.paso(f"[WARN] No se pudo limpiar la caché de Streamlit automáticamente: {e}")

def limpiar_modelo():
    Log.paso("Fase 2: Iniciando eliminación del modelo de Machine Learning...")
    modelo_path = os.path.join(MODELOS_DIR, MODELO_PKL)
    
    if os.path.exists(modelo_path):
        try:
            os.remove(modelo_path)
            Log.paso(f"[OK] Modelo eliminado exitosamente: {modelo_path}")
        except Exception as e:
            Log.paso(f"[ERROR] No se pudo eliminar el modelo: {e}")
    else:
        Log.paso(f"[INFO] El modelo '{modelo_path}' ya no existe o no fue encontrado.")

def main():
    print("\n" + "=" * 65)
    print("  PURGA v1.0  |  Mantenimiento de Entorno CONTINUITY")
    print("=" * 65)

    # REGLA ESTRUCTURAL: Caches primero, modelo al final.
    limpiar_caches()
    limpiar_modelo()

    print("=" * 65)
    Log.paso("Purga completada. El entorno exige un re-entrenamiento limpio.\n")

if __name__ == "__main__":
    main()