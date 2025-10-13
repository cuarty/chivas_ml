# src/chivas_ml/utils.py
from pathlib import Path
import pandas as pd
import sqlite3
import time

def configurar_rutas():
    """Configura todas las rutas necesarias"""
    HERE = Path(__file__).resolve().parents[2]  # sube hasta la raíz del proyecto
    return {
        'DB_PATH': HERE / "data" / "external" / "chivas_dw.sqlite",
        'RAW_DIR': HERE / "data" / "raw",
        'RAW_ENTRENAMIENTOS': HERE / "data" / "raw" / "entrenamientos",
        'RAW_PARTIDOS': HERE / "data" / "raw" / "partidos",
        'REF_DIR': HERE / "data" / "ref",
        'CAL_PARTIDOS': HERE / "data" / "ref" / "calendario_partidos.xlsx",
        'JUGADORES_XLSX': HERE / "data" / "ref" / "DB_Jugadores.xlsx",
        'PARTIDOS_MASTER': HERE / "data" / "ref" / "partidos_jugados.xlsx"
    }


def inicializar_etl(rutas):
    """Inicializa la instancia ETL y asegura estructura de directorios"""
    from src.chivas_ml.etl.pipeline import ETLChivas
    rutas['DB_PATH'].parent.mkdir(parents=True, exist_ok=True)
    rutas['RAW_DIR'].mkdir(parents=True, exist_ok=True)
    rutas['RAW_ENTRENAMIENTOS'].mkdir(parents=True, exist_ok=True)
    rutas['RAW_PARTIDOS'].mkdir(parents=True, exist_ok=True)

    calendario = rutas['CAL_PARTIDOS'] if rutas['CAL_PARTIDOS'].exists() else None
    return ETLChivas(ruta_sqlite=rutas['DB_PATH'], calendario_partidos_xlsx=calendario)


def cargar_jugadores(etl, jugadores_xlsx):
    """Carga los jugadores en la base de datos con verificación"""
    if not jugadores_xlsx.exists():
        print(f"[ERROR] Archivo de jugadores no encontrado: {jugadores_xlsx}")
        return False

    print(f"\n[INFO] Cargando jugadores desde {jugadores_xlsx.name}")
    n_jug = etl.cargar_db_jugadores(jugadores_xlsx)
    print(f"[OK] Jugadores cargados: {n_jug}")

    with etl._conectar() as conn:
        jugadores = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)
        print(f"\n[DEBUG] Total jugadores en DB: {len(jugadores)}")
        print("Ejemplos:", jugadores.head(5).to_dict('records'))

        print("\n[DEBUG] Todos los jugadores en DB:")
        print(jugadores.to_string(index=False))

    return True


def mostrar_encabezados(dir_entrenos, dir_partidos):
    def _dump(d, titulo):
        print(f"\n[DEBUG] Encabezados en {titulo}:")
        for p in sorted(d.glob("*.xlsx")):
            if p.name.startswith('~$'):
                continue
            try:
                df_tmp = pd.read_excel(p, nrows=2)
                print(f"  - {p.name}: {list(df_tmp.columns)}")
            except Exception as e:
                print(f"  - {p.name}: ERROR -> {e}")
    _dump(dir_entrenos, "data/raw/entrenamientos")
    _dump(dir_partidos, "data/raw/partidos")


def procesar_entrenamientos_dir(etl, dir_entrenos: Path):
    if not dir_entrenos.exists():
        print(f"[WARN] No se encontró {dir_entrenos}")
        return {'entrenamientos': 0}
    print("\n[INFO] Procesando ENTRENAMIENTOS (carpeta completa)…")
    return etl.procesar_carpeta(dir_entrenos)


def procesar_partidos_dir(etl, dir_partidos: Path):
    if not dir_partidos.exists():
        print(f"[WARN] No se encontró {dir_partidos}")
        return 0
    print("\n[INFO] Procesando PARTIDOS (carpeta completa)…")
    n_total = 0
    for p in sorted(dir_partidos.glob("*.xlsx")):
        if p.name.startswith('~$'):
            continue
        n_total += etl.cargar_partidos_desde_master(p)

    try:
        with etl._conectar() as conn:
            df_range = pd.read_sql(
                "SELECT MIN(Fecha) AS fmin, MAX(Fecha) AS fmax FROM DB_Partidos",
                conn
            )
        fmin_glob = df_range['fmin'][0]
        fmax_glob = df_range['fmax'][0]
        etl._actualizar_sobrecargas(
            jugadores=None,
            fecha_desde=fmin_glob,
            fecha_hasta=fmax_glob
        )
        print("[OK] Sobrecargas/ACWR recalculadas tras cargar partidos")
    except Exception as e:
        print(f"[WARN] No se pudieron recalcular sobrecargas: {e}")

    etl.actualizar_performance_partidos(fecha_desde=fmin_glob, fecha_hasta=fmax_glob, usar_acc3=True)

    try:
        etl.refrescar_tabla_grupal()
        print("[OK] Tabla grupal lista para Power BI")
    except Exception as e:
        print(f"[WARN] No se pudo refrescar DB_Analisis_Grupal: {e}")

    return n_total
