# chivas-ml/main.py
from pathlib import Path
from src.chivas_ml.etl.pipeline import ETLChivas
import pandas as pd
import sqlite3
import warnings
import time

# Configuración de warnings
warnings.filterwarnings('ignore', category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)


def configurar_rutas():
    """Configura todas las rutas necesarias"""
    HERE = Path(__file__).resolve().parent
    return {
        'DB_PATH': HERE / "data" / "external" / "chivas_dw.sqlite",
        'RAW_DIR': HERE / "data" / "raw",
        'RAW_ENTRENAMIENTOS': HERE / "data" / "raw" / "entrenamientos",
        'RAW_PARTIDOS': HERE / "data" / "raw" / "partidos",
        'REF_DIR': HERE / "data" / "ref",
        'CAL_PARTIDOS': HERE / "data" / "ref" / "calendario_partidos.xlsx",
        'JUGADORES_XLSX': HERE / "data" / "ref" / "DB_Jugadores.xlsx",
        # soporte legacy: si tenés un “master” único seguí usándolo (opcional)
        'PARTIDOS_MASTER': HERE / "data" / "ref" / "partidos_jugados.xlsx"
    }


def inicializar_etl(rutas):
    """Inicializa la instancia ETL y asegura estructura de directorios"""
    # Crear directorios si no existen
    rutas['DB_PATH'].parent.mkdir(parents=True, exist_ok=True)
    rutas['RAW_DIR'].mkdir(parents=True, exist_ok=True)
    rutas['RAW_ENTRENAMIENTOS'].mkdir(parents=True, exist_ok=True)
    rutas['RAW_PARTIDOS'].mkdir(parents=True, exist_ok=True)

    
    # Instanciar ETL
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
    
    # Verificar carga
    with etl._conectar() as conn:
        jugadores = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)
        print(f"\n[DEBUG] Total jugadores en DB: {len(jugadores)}")
        print("Ejemplos:", jugadores.head(5).to_dict('records'))

    # Debug: Ver todos los jugadores en DB
    with etl._conectar() as conn:
        todos_jugadores = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores ORDER BY id_jugador", conn)
        print("\n[DEBUG] Todos los jugadores en DB:")
        print(todos_jugadores.to_string(index=False))
    
    return True



def procesar_entrenamientos(etl, raw_dir):
    """Procesa todos los archivos de entrenamiento"""
    if not raw_dir.exists():
        print(f"[WARN] No se encontró directorio: {raw_dir}")
        return {'entrenamientos': 0, 'filas_rendimiento_semanal': 0}
    
    print("\n[INFO] Procesando entrenamientos...")
    return etl.procesar_carpeta(raw_dir)


def procesar_partidos(etl, partidos_master):
    """Procesa el archivo maestro de partidos"""
    if not partidos_master.exists():
        print(f"[WARN] No se encontró archivo maestro: {partidos_master}")
        return 0
    
    print("\n[INFO] Procesando partidos desde archivo maestro...")
    return etl.cargar_partidos_desde_master(partidos_master)


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
        return {'entrenamientos': 0, 'filas_rendimiento_semanal': 0}
    print("\n[INFO] Procesando ENTRENAMIENTOS (carpeta completa)…")
    return etl.procesar_carpeta(dir_entrenos)  # ya separa y carga entrenos


def procesar_partidos_dir(etl, dir_partidos: Path):
    if not dir_partidos.exists():
        print(f"[WARN] No se encontró {dir_partidos}")
        return 0
    print("\n[INFO] Procesando PARTIDOS (carpeta completa)…")
    n_total = 0
    for p in sorted(dir_partidos.glob("*.xlsx")):
        if p.name.startswith('~$'):
            continue
        n_total += etl.cargar_partidos_desde_master(p)  # reutilizamos tu loader robusto
    
    # 👉 Recalcular sobrecargas para TODO el rango de partidos cargados
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


    return n_total


def main():
    # Limpiar conexiones previas
    db_path = Path("data/external/chivas_dw.sqlite")
    if db_path.exists():
        try:
            # Cerrar todas las conexiones
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA optimize")
            conn.close()
            time.sleep(1)
        except:
            pass

    # 1. Configuración inicial
    rutas = configurar_rutas()
    etl = inicializar_etl(rutas)
    
    # 2. Mostrar estructura de archivos (debug)
    mostrar_encabezados(rutas['RAW_ENTRENAMIENTOS'], rutas['RAW_PARTIDOS'])
    
    # 3. Cargar calendario de partidos PRIMERO
    etl.cargar_calendario_partidos(rutas['CAL_PARTIDOS'])
    
    # 4. Cargar jugadores (verificación exhaustiva)
    if not cargar_jugadores(etl, rutas['JUGADORES_XLSX']):
        return  # Terminar si no hay jugadores
    
    # 5. Validar aliases
    etl.validar_aliases()
    
    # 6. Cargar lesiones de jugadores (si existen)
    ruta_lesiones = Path("data/raw/lesiones/lesiones_musculares.xlsx")
    if ruta_lesiones.exists():
        n = etl.cargar_lesiones_desde_excel(ruta_lesiones)
        print(f"[OK] Lesiones cargadas/actualizadas: {n}")
    
    # 7. PROCESAR ENTRENAMIENTOS
    resultado_entrenos = procesar_entrenamientos_dir(etl, rutas['RAW_ENTRENAMIENTOS'])
    print(f"\n[RESUMEN] Entrenamientos procesados: {resultado_entrenos['entrenamientos']}")
    print(f"[RESUMEN] Semanal actualizado: {resultado_entrenos['filas_rendimiento_semanal']}")
    
    # 8. PROCESAR PARTIDOS
    n_partidos = procesar_partidos_dir(etl, rutas['RAW_PARTIDOS'])
    print(f"\n[RESUMEN] Partidos actualizados (carpeta): {n_partidos}")
    
    # 9. Procesar archivo maestro legacy (si existe)
    if rutas['PARTIDOS_MASTER'].exists():
        print("\n[INFO] Procesando PARTIDOS desde master legacy…")
        n_partidos += procesar_partidos(etl, rutas['PARTIDOS_MASTER'])
        print(f"[RESUMEN] Partidos total (carpeta + master): {n_partidos}")
    
    # 10. CONSOLIDAR DATOS (con manejo de errores mejorado)
    try:
        print("Intentando consolidar rivales...")
        
        # Forzar cierre de todas las conexiones previas
        import gc
        gc.collect()
        time.sleep(2)
        
        etl.consolidar_rivales()
        etl.estandarizar_rival_display_mayusculas()
        print("Consolidación completada exitosamente")
        
    except sqlite3.OperationalError as e:
        if "locked" in str(e).lower():
            print("[WARN] Base de datos bloqueada, omitiendo consolidación...")
            # Continuar sin consolidación
        else:
            print(f"[ERROR] Error de base de datos: {e}")
    except Exception as e:
        print(f"[ERROR] Error inesperado durante consolidación: {e}")

    # 11. Validar aliases nuevamente después de procesar todo
    etl.validar_aliases()
    
    # 12. Resumen final
    with etl._conectar() as conn:
        resumen = {
            'Jugadores': pd.read_sql("SELECT COUNT(*) FROM DB_Jugadores", conn).iloc[0,0],
            'Entrenamientos': pd.read_sql("SELECT COUNT(*) FROM DB_Entrenamientos", conn).iloc[0,0],
            'Partidos': pd.read_sql("SELECT COUNT(*) FROM DB_Partidos", conn).iloc[0,0]
        }
        print("\n[RESUMEN FINAL]")
        for k, v in resumen.items():
            print(f"{k}: {v}")
    
    # 13. Debug info
    with etl._conectar() as conn:
        print("\n[DEBUG] Resumen de entrenamientos cargados:")
        resumen_entrenos = pd.read_sql("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT id_jugador) as jugadores_unicos,
            MIN(Fecha) as fecha_min,
            MAX(Fecha) as fecha_max
        FROM DB_Entrenamientos
        """, conn)
        print(resumen_entrenos)
        
        print("\n[DEBUG] Jugadores sin entrenamientos:")
        jugadores_sin_entrenos = pd.read_sql("""
        SELECT j.id_jugador, j.Nombre 
        FROM DB_Jugadores j
        LEFT JOIN DB_Entrenamientos e ON j.id_jugador = e.id_jugador
        WHERE e.id_entrenamiento IS NULL
        """, conn)
        print(jugadores_sin_entrenos)
    
    # Debug adicional para partidos
    with etl._conectar() as conn:
        partidos_debug = pd.read_sql("SELECT * FROM DB_Partidos LIMIT 5", conn)
        print("\n[DEBUG] Primeros 5 partidos en DB:")
        print(partidos_debug)
        
        count_partidos = pd.read_sql("SELECT COUNT(*) as total_partidos FROM DB_Partidos", conn)
        print(f"\n[DEBUG] Total de partidos en DB: {count_partidos.iloc[0,0]}")

if __name__ == "__main__":
    main()
