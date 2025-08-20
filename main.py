# chivas-ml/main.py
from pathlib import Path
from src.chivas_ml.etl.pipeline import ETLChivas
import pandas as pd
import sqlite3
import warnings

# Configuración de warnings
warnings.filterwarnings('ignore', category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)

def configurar_rutas():
    """Configura todas las rutas necesarias"""
    HERE = Path(__file__).resolve().parent
    return {
        'DB_PATH': HERE / "data" / "external" / "chivas_dw.sqlite",
        'RAW_DIR': HERE / "data" / "raw",
        'REF_DIR': HERE / "data" / "ref",
        'CAL_PARTIDOS': HERE / "data" / "ref" / "calendario_partidos.xlsx",
        'JUGADORES_XLSX': HERE / "data" / "ref" / "DB_Jugadores.xlsx",
        'PARTIDOS_MASTER': HERE / "data" / "ref" / "partidos_jugados.xlsx"
    }

def inicializar_etl(rutas):
    """Inicializa la instancia ETL y asegura estructura de directorios"""
    # Crear directorios si no existen
    rutas['DB_PATH'].parent.mkdir(parents=True, exist_ok=True)
    rutas['RAW_DIR'].mkdir(parents=True, exist_ok=True)
    
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

def mostrar_encabezados(raw_dir):
    """Muestra los encabezados de los archivos raw para debug"""
    print("\n[DEBUG] Encabezados en data/raw:")
    for p in sorted(raw_dir.glob("*.xlsx")):
        if p.name.startswith('~$'):
            continue
        try:
            df_tmp = pd.read_excel(p, nrows=2)
            print(f"  - {p.name}: {list(df_tmp.columns)}")
        except Exception as e:
            print(f"  - {p.name}: ERROR -> {e}")



def main():
    # 1. Configuración inicial
    rutas = configurar_rutas()
    etl = inicializar_etl(rutas)
    
    # 2. Mostrar estructura de archivos (debug)
    mostrar_encabezados(rutas['RAW_DIR'])

    # En main.py, antes de procesar partidos:
    etl.cargar_calendario_partidos(rutas['CAL_PARTIDOS'])
    
    # 3. Cargar jugadores (verificación exhaustiva)
    if not cargar_jugadores(etl, rutas['JUGADORES_XLSX']):
        return  # Terminar si no hay jugadores

    # En main.py, antes de procesar
    etl.validar_aliases()

    
    # 4. Procesar entrenamientos
    resultado_entrenos = procesar_entrenamientos(etl, rutas['RAW_DIR'])
    print(f"\n[RESUMEN] Entrenamientos procesados: {resultado_entrenos['entrenamientos']}")
    print(f"[RESUMEN] Semanal actualizado: {resultado_entrenos['filas_rendimiento_semanal']}")
    
    # 5. Procesar partidos
    n_partidos = procesar_partidos(etl, rutas['PARTIDOS_MASTER'])
    print(f"\n[RESUMEN] Partidos actualizados: {n_partidos}")

    

    # En main.py, antes de procesar
    etl.validar_aliases()

    etl.consolidar_rivales()                  # fusiona duplicados
    etl.estandarizar_rival_display_mayusculas()  # pone MAYÚSCULAS en DB_Partidos

    
    # 6. Resumen final
    with etl._conectar() as conn:
        resumen = {
            'Jugadores': pd.read_sql("SELECT COUNT(*) FROM DB_Jugadores", conn).iloc[0,0],
            'Entrenamientos': pd.read_sql("SELECT COUNT(*) FROM DB_Entrenamientos", conn).iloc[0,0],
            'Partidos': pd.read_sql("SELECT COUNT(*) FROM DB_Partidos", conn).iloc[0,0]
        }
        print("\n[RESUMEN FINAL]")
        for k, v in resumen.items():
            print(f"{k}: {v}")

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

if __name__ == "__main__":
    main()
