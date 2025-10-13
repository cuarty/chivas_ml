# chivas-ml/main.py
import sqlite3
import time
import pandas as pd
from pathlib import Path
from src.chivas_ml.utils import (
    configurar_rutas,
    inicializar_etl,
    cargar_jugadores,
    mostrar_encabezados,
    procesar_entrenamientos_dir,
    procesar_partidos_dir,
)

def main():
    # 🔹 Limpiar conexiones previas
    db_path = Path("data/external/chivas_dw.sqlite")
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA optimize;")
            conn.close()
            time.sleep(1)
        except Exception:
            pass

    rutas = configurar_rutas()
    etl = inicializar_etl(rutas)
    mostrar_encabezados(rutas['RAW_ENTRENAMIENTOS'], rutas['RAW_PARTIDOS'])

    # 1️⃣ Jugadores y calendario
    etl.cargar_calendario_partidos(rutas['CAL_PARTIDOS'])
    if not cargar_jugadores(etl, rutas['JUGADORES_XLSX']):
        return
    etl.validar_aliases()

    # 2️⃣ Lesiones
    ruta_lesiones = Path("data/raw/lesiones/lesiones_musculares.xlsx")
    if ruta_lesiones.exists():
        n = etl.cargar_lesiones_desde_excel(ruta_lesiones)
        print(f"[OK] Lesiones cargadas/actualizadas: {n}")

    # 3️⃣ Microciclos
    print("\n[INFO] Actualizando microciclos (tabla maestra)…")
    etl.actualizar_tabla_microciclos_excel()

    # 4️⃣ ENTRENAMIENTOS
    resultado_entrenos = procesar_entrenamientos_dir(etl, rutas['RAW_ENTRENAMIENTOS'])
    print(f"[RESUMEN] Entrenamientos procesados: {resultado_entrenos.get('entrenamientos', 0)}")

    print("\n[INFO] Normalizando entrenos (relleno huecos / descanso)…")
    etl.actualizar_db_entrenamientos()

    # 5️⃣ MICROciclos y sobrecargas
    etl.etiquetar_microciclo()
    etl.generar_tabla_db_microciclo()
    etl.actualizar_sobrecarga_en_db_microciclo()

    # 6️⃣ PARTIDOS
    n_partidos = procesar_partidos_dir(etl, rutas['RAW_PARTIDOS'])
    if rutas['PARTIDOS_MASTER'].exists():
        n_partidos += etl.cargar_partidos_desde_master(rutas['PARTIDOS_MASTER'])

    etl.etiquetar_microciclo()

    # 7️⃣ Tablas BI
    etl.publicar_bi_microciclo_total()
    etl.publicar_bi_cargas_diarias()

    # 🔟 Consolidar rivales
    try:
        import gc; gc.collect(); time.sleep(2)
        etl.consolidar_rivales()
        etl.estandarizar_rival_display_mayusculas()
    except Exception as e:
        print(f"[WARN] Error consolidando rivales: {e}")

    # 🔹 Resumen final
    with etl._conectar() as conn:
        resumen = {
            'Jugadores': pd.read_sql("SELECT COUNT(*) FROM DB_Jugadores", conn).iloc[0, 0],
            'Entrenamientos': pd.read_sql("SELECT COUNT(*) FROM DB_Entrenamientos", conn).iloc[0, 0],
            'Partidos': pd.read_sql("SELECT COUNT(*) FROM DB_Partidos", conn).iloc[0, 0],
            'Microciclos': pd.read_sql("SELECT COUNT(*) FROM DB_Microciclo", conn).iloc[0, 0],
        }
    print("\n[RESUMEN FINAL]")
    for k, v in resumen.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    main()
