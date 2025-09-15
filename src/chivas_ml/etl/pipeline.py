"""
 ============================================================
 ÍNDICE (pipeline.py)
 ============================================================
 00) CONSTANTES Y TIPOS
     - MAPEO_COLUMNAS_POR_DEFECTO
     - COLUMNAS_NUMERICAS
     - ETLChivas._PESOS_POR_POSICION
     - ETLChivas.STOP_RIVAL

 01) Clase principal del ETL
     - @dataclass ETLChivas (attrs: ruta_sqlite, calendario_partidos_xlsx, mapeo_columnas)

 02) INICIALIZACIÓN DE CLASE
     - __post_init__()

 03) Conexión y administración de la DB (infra)
     - _conectar()
     - _asegurar_indices()   (incluye creación de índices/tabla DB_Lesiones)

 04) Rutas y archivado post-proceso
     - _dir_processed()
     - _archivar_archivo()

 05) Catálogo de rivales (aliases, normalización y consolidación)
     - _get_aliases_rivales_map()
     - consolidar_rivales()
     - estandarizar_rival_display_mayusculas()
     - _obtener_o_crear_id_rival()
     - _adjuntar_id_rival()

 06) Normalización de texto y parsers utilitarios
     - _norm_txt_simple()
     - _norm_texto()
     - _normalizar_txt()
     - _derivar_rival_y_local()
     - _inferir_fecha()
     - _es_chivas()
     - _obtener_fecha_por_rival()
     - _parsear_fecha_wimu()

 07) Calendario de partidos
     - cargar_calendario_partidos()

 08) Normalización de columnas y casting numérico
     - _renombrar_columnas()
     - _a_numerico()

 09) Identidad de jugadores (IDs, aliases y validaciones)
     - _anexar_id_jugador_por_nombre()
     - _aplicar_alias_jugadores()
     - _fabricar_alias_desde_db()
     - _resolver_id_por_alias_heuristico()
     - _buscar_jugadores_similares()
     - validar_aliases()
     - _asegurar_id_jugador()

 10) Fechas
     - normalizar_fechas()      (staticmethod)
     - transformar_archivo()

 11) Cálculo de métricas (CE/CS/CR y Rendimiento)
     - _serie_segura()
     - _calcular_ce_cs_cr()
     - _posicion_por_jugador()
     - _percentiles_por_jugador()
     - _escala_0a100()          (staticmethod)
     - _calcular_rendimiento_total()

 12) Clasificación “Entrenamiento vs Partido”
     - dividir_por_calendario()
     - corregir_local_visitante()

 14) UPSERTs a la base
     - upsert_entrenamientos()
     - upsert_partidos()

 15) Agregaciones / Reporting en DB
     - _inicio_semana()         (staticmethod)
     - recalcular_rendimiento_semanal()

 16) Procesamiento de entrenamientos (archivo y carpeta)
     - procesar_excel()
     - procesar_carpeta()

 17) Lesiones — Ingesta y upsert (NUEVA SECCIÓN)
     - _mapeo_columnas_lesiones()
     - _leer_excel_lesiones()
     - _preparar_df_lesiones()
     - upsert_lesiones()
     - cargar_lesiones_desde_excel()

 18) Procesamiento de partidos (archivo y carpeta)
     - cargar_partidos_desde_master()
     - procesar_carpeta_partidos()

 19) Helpers específicos para partidos (validación/normalización “extra”)
     - _leer_y_validar_excel()
     - _normalizar_columnas_partidos()
     - _filtrar_fechas_validas()
     - _asignar_ids_jugadores()
     - _normalizar_rivales()
     - _completar_metricas_base()
     - _cargar_datos_validos()

 20) Helper para leer excel 97-2003
     - _detectar_formato_excel()
     - _leer_excel_robusto()

 21) Carga de referencia de jugadores
     - cargar_db_jugadores()
 ============================================================

 Notas:
    - Los Excel originales de entrenamiento se dejan en /data/raw/entrenamientos
    - Los Excel originales de entrenamiento se dejan en /data/raw/partidos
    - La DB SQLite se guarda en /data/chivas_dw.sqlite
    - El proceso es incremental: cada semana se pueden cargar
      nuevos Excel sin duplicar datos
===========================================================
"""


# src/chivas_ml/etl/pipeline.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional
import sqlite3
import pandas as pd
import numpy as np
import unicodedata
import re
import traceback
from fuzzywuzzy import fuzz, process
from datetime import datetime
import shutil
import time



# ============================================================
# 0- CONSTANTES Y TIPOS
# ============================================================

MAPEO_COLUMNAS_POR_DEFECTO: Dict[str, str] = {
    # --- Claves
    "ID": "id_jugador","id_jugador": "id_jugador", "ID_Jugador": "id_jugador", "jugador_id": "id_jugador",

    # --- Identidad jugador (alias para nombre y/o id)
    "Players": "Nombre",
    "Player": "Nombre",
    "Jugador": "Nombre",
    "Nombre jugador": "Nombre",
    "Nombre_jugador": "Nombre",

    # --- Fecha
    # Mapeo para fechas (agregar todas las variantes posibles)
    "Days": "Fecha",
    "Fecha": "Fecha", 
    "Date": "Fecha",
    "Día": "Fecha",
    "Dia": "Fecha",
    "Match Date": "Fecha",
    "Game Date": "Fecha",

    # --- Entrenamiento / métricas generales
    "Dia_Semana": "Dia_Semana", "Tipo_Dia": "Tipo_Dia",

    # Distancia total (alias comunes)
    "Distance (m)": "Distancia_total",
    "Distance": "Distancia_total",
    "Distancia_total": "Distancia_total",

    # Intensidad media
    "Distance/time (m/min)": "Velocidad_prom_m_min",

    # HSR absoluto y relativo (mantener separados)
    "Distance - Abs HSR (m)": "HSR_abs_m",
    "HSR_m": "HSR_abs_m",                           # compatibilidad hacia atrás
    "Distance - HSR Rel  (m)": "HSR_rel_m",         # (con doble espacio)
    "Distance - HSR Rel (m)": "HSR_rel_m",          # (sin doble espacio)

    # HMLD (nombres largos comunes)
    "HMLD_m": "HMLD_m",
    "HMLD (m)": "HMLD_m",
    "High Metabolic Load Distance (m)": "HMLD_m",
    "High Metabolic Load Distance": "HMLD_m",

    "Distance/time (m/min)": "Velocidad_prom_m_min",
    "Sprints - Max Speed (km/h)": "Sprints_vel_max_kmh",
    # por si vienen variantes:
    "Max Speed (km/h)": "Sprints_vel_max_kmh",
    "Distance/time m/min": "Velocidad_prom_m_min",
    "Avg Speed (m/min)": "Velocidad_prom_m_min",

    # Sprints (tres métricas separadas)
    "Sprints - Distance Abs(m)": "Sprints_distancia_m",
    "Sprints - Sprints Abs (count)": "Sprints_cantidad",
    "Sprints - Max Speed (km/h)": "Sprints_vel_max_kmh",

    # Aceleraciones / Desaceleraciones (variantes)
    "Acc +3": "Acc_3","Acc+3": "Acc_3", "Acc_3": "Acc_3", "Acc >3": "Acc_3", "Acc>3": "Acc_3",
    "Dec +3": "Dec_3","Dec+3": "Dec_3", "Dec_3": "Dec_3", "Dec >3": "Dec_3", "Dec>3": "Dec_3",

    # PlayerLoad / RPE (variantes)
    "Player Load (a.u.)": "Player_Load","PlayerLoad": "Player_Load", "Player_Load": "Player_Load",
    "RPE - RPE General": "RPE", "RPE General": "RPE", "RPE": "RPE",

    # Partidos
    "Minutos_Jugados": "Minutos_jugados", "Minutos_jugados": "Minutos_jugados",
    "Rival": "Rival", "id_rival": "id_rival",
    "Local_Visitante": "Local_Visitante", "Local / Visitante": "Local_Visitante", "Local-Visitante": "Local_Visitante",
}

COLUMNAS_NUMERICAS = [
    "id_jugador", "Distancia_total", "Velocidad_prom_m_min",
    "HSR_abs_m", "HSR_rel_m", "HMLD_m",
    "Sprints_distancia_m", "Sprints_cantidad", "Sprints_vel_max_kmh",
    "Acc_3", "Dec_3", "Player_Load", "RPE",
    "Minutos_jugados", "id_rival",
]


# ============================================================
# 1- Clase principal del ETL
# ============================================================

@dataclass
class ETLChivas:
    ruta_sqlite: Path
    calendario_partidos_xlsx: Optional[Path] = None
    mapeo_columnas: Optional[Dict[str, str]] = None

# ============================================================
# 2- INICIALIZACIÓN DE CLASE
# ============================================================    

    def __post_init__(self):
        self.ruta_sqlite = Path(self.ruta_sqlite)
        self.mapeo_columnas = {**MAPEO_COLUMNAS_POR_DEFECTO, **(self.mapeo_columnas or {})}
        self._fechas_partidos = set()
        if self.calendario_partidos_xlsx:
            self.cargar_calendario_partidos(self.calendario_partidos_xlsx)
        self._asegurar_indices()

# ============================================================
# 3- Conexión y administración de la DB (infra)
# ============================================================  

    def _conectar(self) -> sqlite3.Connection:
        # Asegurar que el directorio existe
        self.ruta_sqlite.parent.mkdir(parents=True, exist_ok=True)
        
        # Configuración robusta de conexión
        conn = sqlite3.connect(
            str(self.ruta_sqlite),  # Asegurar que es string
            timeout=60,
            check_same_thread=False
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=10000;")  # 10 segundos
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA cache_size=10000;")
        return conn


    def _asegurar_indices(self):
        with self._conectar() as conn:
            # ---- índices/tablas base ----
            conn.executescript("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_ent_jugador_fecha
                ON DB_Entrenamientos(id_jugador, Fecha);

            CREATE UNIQUE INDEX IF NOT EXISTS uq_part_jugador_fecha_rival
                ON DB_Partidos(id_jugador, Fecha, ifnull(id_rival,-1));

            CREATE UNIQUE INDEX IF NOT EXISTS uq_rend_semanal_jugador_fecha
                ON Rendimiento_Semanal(id_jugador, Fecha);

            CREATE INDEX IF NOT EXISTS idx_rend_semanal_jugador_fecha
                ON Rendimiento_Semanal(id_jugador, Fecha);

            CREATE TABLE IF NOT EXISTS DB_Lesiones (
                id_lesion     INTEGER PRIMARY KEY AUTOINCREMENT,
                id_jugador    INTEGER NOT NULL,
                Fecha_inicio  DATE    NOT NULL,
                Tipo_lesion   TEXT    NOT NULL,
                Musculo       TEXT,
                Lado          TEXT,
                Tejido        TEXT,
                Fuente        TEXT,
                FOREIGN KEY (id_jugador) REFERENCES DB_Jugadores(id_jugador)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS uq_lesion_unica
                ON DB_Lesiones(id_jugador, Fecha_inicio, Tipo_lesion, Musculo, Lado, Tejido);

            CREATE INDEX IF NOT EXISTS idx_lesiones_jugador_fecha
                ON DB_Lesiones(id_jugador, Fecha_inicio);

            CREATE INDEX IF NOT EXISTS idx_entrenos_jugador_fecha  ON DB_Entrenamientos(id_jugador, Fecha);
            CREATE INDEX IF NOT EXISTS idx_partidos_jugador_fecha ON DB_Partidos(id_jugador, Fecha);
            """)

            # ---- columnas nuevas en DB_Partidos ----
            cols = {row[1] for row in conn.execute("PRAGMA table_info(DB_Partidos)")}
            def add(col, sqltype):
                if col not in cols:
                    conn.execute(f"ALTER TABLE DB_Partidos ADD COLUMN {col} {sqltype};")

            add("Rendimiento_Partido",   "REAL")
            add("Rendimiento_Intensidad","REAL")
            add("RvE_Intensidad",        "REAL")
            add("Rendimiento_vs_Entreno","REAL")
            add("CE_prev7d",             "REAL")
            add("CS_prev7d",             "REAL")
            add("CR_prev7d",             "REAL")
            # ACWR / sobrecarga
            add("CT_7d",                 "REAL")
            add("CT_28d_avg",            "REAL")
            add("ACWR_raw",              "REAL")
            add("ACWR_pct",              "REAL")
            add("ACWR_flag",             "TEXT")


            
            conn.commit()


            

# --- Constantes de clase  ---

    _PESOS_POR_POSICION = {
            #        CE,   CS,   CR
            "Arquero":   (0.45, 0.35, 0.20),
            "Defensa":   (0.40, 0.45, 0.15),
            "Medio":     (0.45, 0.40, 0.15),
            "Delantera": (0.55, 0.35, 0.10),
            # fallback si no hay posición
            "_default":  (0.45, 0.40, 0.15),
        }

    STOP_RIVAL = {"fc", "cf", "club", "deportivo", "cd", "c"}  # c. juarez -> juarez


# ============================================================
# 4- Rutas y archivado post-proceso
# ============================================================  

    def _dir_processed(self) -> Path:
        # data/external/chivas_dw.sqlite -> BASE=data ; processed=BASE/processed
        return self.ruta_sqlite.parent.parent / "processed"

    def _archivar_archivo(self, src: Path, tipo: str, fecha_ref=None):
        base = self._dir_processed()
        destino_dir = base / tipo
        destino_dir.mkdir(parents=True, exist_ok=True)

        # fecha
        if fecha_ref is None:
            ts = datetime.fromtimestamp(src.stat().st_mtime)
            fecha_str = ts.strftime("%Y-%m-%d")
        else:
            fecha_str = str(fecha_ref)[:10]

        # nombre limpio y corto (máx ~80 chars sin extensión)
        stem = re.sub(r"\s+", "_", src.stem)
        stem = re.sub(r"[^A-Za-z0-9_\-]", "", stem)
        stem = re.sub(r"(?:\d{4}-\d{2}-\d{2}_)+", "", stem)  # quita repeticiones de fechas
        stem = stem[:80]  # recorte duro

        nombre_final = f"{fecha_str}_{stem}{src.suffix}"
        destino = destino_dir / nombre_final

        # evitar colisiones
        suf = 1
        while destino.exists():
            destino = destino_dir / f"{fecha_str}_{stem}_{suf}{src.suffix}"
            suf += 1

        # si el origen ya no existe, salgo elegante
        if not src.exists():
            print(f"[WARN] No pude archivar: origen no existe: {src}")
            return

        try:
            shutil.move(str(src), str(destino))
            print(f"[ARCHIVO] Movido a {destino.relative_to(self.ruta_sqlite.parent.parent)}")
        except FileNotFoundError:
            # típicamente por path largo -> fallback a copy+remove con nombre más corto
            try:
                shutil.copy2(str(src), str(destino))
                src.unlink(missing_ok=True)
                print(f"[ARCHIVO] Copiado (fallback) a {destino.relative_to(self.ruta_sqlite.parent.parent)}")
            except Exception as e:
                print(f"[ERROR] No pude archivar {src.name}: {e}")


# ============================================================
# 5- Catálogo de rivales (aliases, normalización y consolidación)
# ============================================================  

    def _get_aliases_rivales_map(self):
        import pandas as pd
        from pathlib import Path

        # cache
        if hasattr(self, "_aliases_rivales_cache"):
            return self._aliases_rivales_cache

        path = Path(self.ruta_sqlite).parent.parent / "ref" / "aliases_rivales.csv"
        aliases_map = {}  # ⚠️ NO usar nombre 'map' para no chocar con el builtin

        if path.exists():
            df = pd.read_csv(path)
            if {"Rival_maestro", "Rival_calendario"}.issubset(df.columns):
                src = df["Rival_maestro"].apply(lambda x: self._norm_texto(x, drop_tokens=self.STOP_RIVAL))
                dst = df["Rival_calendario"].apply(lambda x: self._norm_texto(x, drop_tokens=self.STOP_RIVAL))
                aliases_map = dict(zip(src, dst))
            else:
                print("[WARN] aliases_rivales.csv sin encabezados Rival_maestro,Rival_calendario")
        else:
            print(f"[INFO] No hay aliases_rivales.csv en {path.resolve()}")

        self._aliases_rivales_cache = aliases_map
        return aliases_map

    def consolidar_rivales(self):
        max_intentos = 3
        intento = 0
        
        while intento < max_intentos:
            try:
                # Usar with statement para asegurar que la conexión se cierra
                with self._conectar() as con:
                    con.execute("PRAGMA foreign_keys=ON")
                    
                    # 0) asegurar columna
                    cols = {row[1] for row in con.execute("PRAGMA table_info(DB_Rivales)")}
                    if "Nombre_norm" not in cols:
                        con.execute("ALTER TABLE DB_Rivales ADD COLUMN Nombre_norm TEXT")
                        con.commit()

                    # 0.b) quitar índice UNIQUE si existe
                    con.execute("DROP INDEX IF EXISTS uq_rivales_nombre_norm")
                    con.commit()

                    # 1) cargar rivales y RE-CALCULAR SIEMPRE clave canónica
                    df = pd.read_sql("SELECT id_rival, Nombre FROM DB_Rivales", con)
                    df["Nombre_norm"] = df["Nombre"].apply(
                        lambda s: self._norm_texto(s, drop_tokens=self.STOP_RIVAL)
                    )

                    # 2) persistir Nombre_norm recalculado
                    for _id, _norm in df[["id_rival","Nombre_norm"]].itertuples(index=False):
                        con.execute("UPDATE DB_Rivales SET Nombre_norm=? WHERE id_rival=?", (_norm, _id))
                    con.commit()

                    # 3) agrupar duplicados por Nombre_norm
                    grupos = (df.dropna(subset=["Nombre_norm"])
                                .groupby("Nombre_norm")["id_rival"].apply(list))

                    # columnas para “calidad” de fila de partidos (más no-nulos = mejor)
                    metric_cols = [
                        "Distancia_total","HSR_abs_m","HMLD_m","Sprints_distancia_m","Sprints_cantidad",
                        "Sprints_vel_max_kmh","Acc_3","Dec_3","Player_Load","Carga_Explosiva",
                        "Carga_Sostenida","Carga_Regenerativa","Rendimiento_Partido","Duracion_min",
                        "HSR_rel_m","Velocidad_prom_m_min"
                    ]

                    for _, ids in grupos.items():
                        if len(ids) <= 1:
                            continue
                        keep = min(ids)                  # conservamos el menor id
                        to_merge = [i for i in ids if i != keep]
                        ids_all = [keep] + to_merge

                        # 4) traer partidos afectados y resolver choques del UNIQUE (jugador, fecha, rival)
                        q = ",".join("?"*len(ids_all))
                        part = pd.read_sql(f"""
                            SELECT id_partido, id_jugador, Fecha, id_rival, {",".join(metric_cols)}
                            FROM DB_Partidos
                            WHERE id_rival IN ({q})
                        """, con, params=ids_all)

                        if not part.empty:
                            part["key"] = (part["id_jugador"].astype(str) + "|" +
                                        part["Fecha"].astype(str) + "|" + str(keep))
                            part["nn"] = part[metric_cols].notna().sum(axis=1)
                            # conservamos por key la fila más “rica”
                            keep_ids = (part.sort_values(["key","nn","Duracion_min"], ascending=[True, False, False])
                                            .groupby("key")["id_partido"].first().tolist())
                            del_ids = part[~part["id_partido"].isin(keep_ids)]["id_partido"].tolist()
                            if del_ids:
                                con.executemany("DELETE FROM DB_Partidos WHERE id_partido=?", [(i,) for i in del_ids])

                        # 5) actualizar rivales en partidos y borrar duplicados en catálogo
                        if to_merge:
                            marks = ",".join("?"*len(to_merge))
                            con.execute(f"UPDATE DB_Partidos SET id_rival=? WHERE id_rival IN ({marks})", (keep, *to_merge))
                            con.execute(f"DELETE FROM DB_Rivales WHERE id_rival IN ({marks})", (*to_merge,))
                        con.commit()

                    # 6) recrear índice UNIQUE por la clave canónica
                    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_rivales_nombre_norm ON DB_Rivales(Nombre_norm)")
                    con.commit()
                
                break

            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and intento < max_intentos - 1:
                    intento += 1
                    print(f"[WARN] Base bloqueada, reintento {intento}/{max_intentos}")
                    time.sleep(3 * intento)  # Espera progresiva
                    continue
                else:
                    print(f"[ERROR] No se pudo consolidar rivales después de {max_intentos} intentos: {e}")
                    raise


    def estandarizar_rival_display_mayusculas(self):
        with self._conectar() as con:
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("""
                UPDATE DB_Partidos AS p
                SET Rival = UPPER((SELECT r.Nombre FROM DB_Rivales r WHERE r.id_rival = p.id_rival))
                WHERE p.id_rival IS NOT NULL
            """)
            con.execute("UPDATE DB_Partidos SET Rival = NULL WHERE id_rival IS NULL")
            # no hace falta close()


    def _obtener_o_crear_id_rival(self, nombre_rival):
        import sqlite3
        norm = self._norm_texto(nombre_rival, drop_tokens=self.STOP_RIVAL)
        if not norm:
            return None

        con = sqlite3.connect(self.ruta_sqlite)
        con.execute("PRAGMA foreign_keys=ON")

        # asegurar columna antes de consultar
        cols = {row[1] for row in con.execute("PRAGMA table_info(DB_Rivales)")}
        if "Nombre_norm" not in cols:
            con.execute("ALTER TABLE DB_Rivales ADD COLUMN Nombre_norm TEXT")

        cur = con.cursor()
        # 1) buscar por clave canónica
        cur.execute("SELECT id_rival FROM DB_Rivales WHERE Nombre_norm=?", (norm,))
        row = cur.fetchone()
        if row:
            con.close()
            return row[0]

        # 2) insertar una sola vez con Nombre “lindo” y Nombre_norm
        pretty = (str(nombre_rival).strip() or norm.title())
        cur.execute("INSERT INTO DB_Rivales (Nombre, Nombre_norm) VALUES (?,?)", (pretty, norm))
        rid = cur.lastrowid
        con.commit()
        con.close()
        return rid

    def _adjuntar_id_rival(self, df_partidos):
        if df_partidos.empty:
            return df_partidos
        if "Rival" not in df_partidos.columns:
            df_partidos["Rival"] = None
        if "id_rival" not in df_partidos.columns:
            df_partidos["id_rival"] = None

        nombres = (
            df_partidos["Rival"].dropna().astype(str).str.strip().replace({"": np.nan}).dropna().unique().tolist()
        )
        cache = {nom: self._obtener_o_crear_id_rival(nom) for nom in nombres}

        def completar_id(row):
            if pd.notna(row.get("id_rival")):
                return row["id_rival"]
            nom = row.get("Rival")
            if pd.isna(nom):
                return None
            return cache.get(str(nom).strip())

        df_partidos["id_rival"] = df_partidos.apply(completar_id, axis=1)
        return df_partidos

# ============================================================
# 6- Normalización de texto y parsers utilitarios
# ============================================================ 

    def _norm_txt_simple(self, s):
        if s is None: return None
        s = str(s).lower().strip()
        s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s or None

    def _norm_texto(self, s, drop_tokens=None):
        import re, unicodedata
        if s is None:
            return None
        s = str(s).casefold().strip()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = re.sub(r"[^\w\s]", " ", s)      # quita puntos/guiones/etc
        s = re.sub(r"\s+", " ", s).strip()
        if not s:
            return None

        tokens = s.split()
        if drop_tokens:
            tokens = [t for t in tokens if t not in drop_tokens]

        # compactar secuencias de letras sueltas: "u d g" -> "udg"
        out = []
        i = 0
        while i < len(tokens):
            if len(tokens[i]) == 1:
                j = i
                pack = []
                while j < len(tokens) and len(tokens[j]) == 1:
                    pack.append(tokens[j]); j += 1
                out.append("".join(pack))
                i = j
            else:
                out.append(tokens[i])
                i += 1

        s = " ".join(t for t in out if t)
        return s or None

    def _normalizar_txt(self, s: str) -> str:
        if s is None:
            return ""
        s = str(s).strip().lower()
        # remover acentos
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        # limpiar espacios múltiples
        s = re.sub(r"\s+", " ", s)
        return s

    def _derivar_rival_y_local(self, serie_sessions: pd.Series) -> pd.DataFrame:
        rivales, local_visit = [], []
        def norm(s): return str(s).strip()

        pats = [
            (r"^\s*(.+?)\s+v(?:s\.?)?\s+guadalajara\s*$", "Visitante", 1),  # Rival vs Guadalajara -> Visitante
            (r"^\s*guadalajara\s+v(?:s\.?)?\s+(.+?)\s*$", "Local", 1),      # Guadalajara vs Rival -> Local
        ]

        for txt in serie_sessions.fillna(""):
            s = norm(txt)
            rival, lv = None, None
            for pat, lv_val, g in pats:
                m = re.match(pat, s, flags=re.I)
                if m:
                    rival = norm(m.group(g))
                    lv = lv_val
                    break
            if rival is None:
                # fallback: quitar “guadalajara” y quedarme con lo otro
                s_clean = re.sub(r"guadalajara", "", s, flags=re.I).strip()
                # si aún queda un “vs...” separar por vs/contra
                parts = re.split(r"\b(?:v(?:s\.?)?|contra)\b", s_clean, flags=re.I)
                rival = parts[-1].strip() if parts else None
                lv = None
            rivales.append(rival if rival else None)
            local_visit.append(lv)
        return pd.DataFrame({"Rival_from_sess": rivales, "Local_Visitante_from_sess": local_visit})

    def _inferir_fecha(self, rival, lv):
        if not hasattr(self, "_calendario_partidos_df"): 
            return None
        cal = self._calendario_partidos_df.copy()
        cal["Rival_norm"] = cal["Rival"].apply(self._norm_txt_simple)
        cal["LV_norm"]    = cal.get("Local_Visitante", pd.Series([None]*len(cal))).apply(self._norm_txt_simple)

        r = self._norm_txt_simple(rival)
        l = self._norm_txt_simple(lv)
        if r is None: 
            return None

        # 1) Rival + LV
        m = cal[(cal["Rival_norm"] == r) & (cal["LV_norm"] == l)]
        if len(m) == 1: 
            return m.iloc[0]["Fecha"]

        # 2) Sólo Rival
        m = cal[cal["Rival_norm"] == r]
        if len(m) == 1:
            return m.iloc[0]["Fecha"]

        return None

    def _es_chivas(self, nombre_equipo: str) -> bool:
        """Detecta si 'nombre_equipo' hace referencia a Chivas (varios alias)."""
        alias_chivas = {
            "guadalajara",
            "chivas",
            "chivas de guadalajara",
            "club deportivo guadalajara",
            "cd guadalajara",
            "c.d. guadalajara",
            "guadalajara chivas",
            "chivas guadalajara",
        }
        return self._normalizar_txt(nombre_equipo) in alias_chivas

    def _obtener_fecha_por_rival(self, rival: str) -> Optional[date]:
        """Obtiene la fecha del partido basado en el rival desde el calendario"""
        if not hasattr(self, '_calendario_partidos_df') or rival is None:
            return None
        
        try:
            # Buscar en el calendario
            match = self._calendario_partidos_df[
                self._calendario_partidos_df['Rival'].str.contains(rival, case=False, na=False)
            ]
            
            if not match.empty:
                return match.iloc[0]['Fecha']
            
            # Intentar con nombres normalizados
            rival_norm = self._normalizar_nombre_rival(rival)
            match = self._calendario_partidos_df[
                self._calendario_partidos_df['Rival'].str.contains(rival_norm, case=False, na=False)
            ]
            
            return match.iloc[0]['Fecha'] if not match.empty else None
            
        except Exception:
            return None

    def _parsear_fecha_wimu(self, serie: pd.Series) -> pd.Series:
        """
        Convierte fechas provenientes de WIMU:
        - "Sat Jun 28 17:32:38 UTC 2025"  -> match específico
        - datetimes ya parseados           -> .dt.date
        - inferencia general               -> to_datetime(..., infer_datetime_format=True)
        - seriales de Excel (44927, etc.)  -> origin='1899-12-30'
        Devuelve objeto date (no datetime).
        """
        if serie is None or len(serie) == 0:
            return pd.to_datetime(serie, errors="coerce").dt.date

        s = serie.copy()

        # 0) si ya viene como datetime
        if pd.api.types.is_datetime64_any_dtype(s):
            return pd.to_datetime(s, errors="coerce", utc=True).dt.tz_localize(None).dt.date

        # 1) intento con el formato WIMU explícito
        dt = pd.to_datetime(
            s.astype(str),
            format="%a %b %d %H:%M:%S UTC %Y",
            errors="coerce",
            utc=True,
        ).dt.tz_localize(None)

        # 2) inferencia para los que quedaron NaT
        faltan = dt.isna()
        if faltan.any():
            dt.loc[faltan] = pd.to_datetime(
                s[faltan].astype(str),
                errors="coerce",
                utc=True,
                infer_datetime_format=True,
            ).dt.tz_localize(None)

        # 3) seriales de Excel (numéricos)
        faltan = dt.isna()
        if faltan.any():
            # intentar convertir a float por si vienen como texto "44927"
            ser_num = pd.to_numeric(s[faltan], errors="coerce")
            dt.loc[faltan] = pd.to_datetime(
                ser_num,
                unit="D",
                origin="1899-12-30",
                errors="coerce",
            )

        return dt.dt.date


# ============================================================
# 7- Calendario de partidos
# ============================================================ 

    def cargar_calendario_partidos(self, ruta_xlsx: Path, sheet_name: str | int | None = None):
        """
        Lee un Excel con columnas: Fecha (obligatoria), Rival (opcional), Local_Visitante (opcional), id_rival (opcional).
        Acepta sheet_name como nombre/índice; si es None y hay varias hojas, toma la primera no vacía.
        """
        raw = pd.read_excel(ruta_xlsx, sheet_name=sheet_name)

        # Si viene un dict (varias hojas), elegimos la primera con columnas
        if isinstance(raw, dict):
            # priorizar la primera con alguna columna que parezca fecha
            candidatos = []
            for name, df0 in raw.items():
                if isinstance(df0, pd.DataFrame) and len(df0.columns) > 0 and len(df0) > 0:
                    candidatos.append((name, df0))
            if not candidatos:
                raise ValueError("El archivo del calendario no tiene hojas con datos.")
            df_cal = candidatos[0][1].copy()
        else:
            df_cal = raw.copy()

        # Identificar la columna de fecha
        cols_lower = {c: str(c).strip().lower() for c in df_cal.columns}
        candidatas = [c for c, s in cols_lower.items() if "fecha" in s or s in ("date",)]
        if not candidatas:
            raise ValueError("El calendario de partidos debe incluir una columna de fecha.")

        # Normalizar fecha -> 'Fecha'
        df_cal = df_cal.rename(columns={candidatas[0]: "Fecha"})
        df_cal["Fecha"] = self.normalizar_fechas(df_cal["Fecha"])

        # Normalizar nombres posibles
        ren = {"Local / Visitante": "Local_Visitante", "Local-Visitante": "Local_Visitante"}
        df_cal = df_cal.rename(columns=ren)

        # Guardar para merge + set de fechas
        keep = ["Fecha"] + [c for c in ["Rival", "Local_Visitante", "id_rival"] if c in df_cal.columns]
        self._calendario_partidos_df = df_cal[keep].copy()
        self._fechas_partidos = set(self._calendario_partidos_df["Fecha"].dropna().tolist())

# ============================================================
# 8- Normalización de columnas y casting numérico
# ============================================================ 

    def _renombrar_columnas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Versión mejorada que maneja espacios y mayúsculas"""
        nuevos_nombres = {}
        for col_original in df.columns:
            # Normalizar el nombre para comparación
            col_normalizada = (
                str(col_original).strip().lower().replace(" ", "_").replace("-", "_"))
            
            # Buscar en el mapeo ignorando mayúsculas/espacios
            for patron, destino in self.mapeo_columnas.items():
                patron_normalizado = (
                    str(patron).strip().lower().replace(" ", "_").replace("-", "_"))
                if patron_normalizado == col_normalizada:
                    nuevos_nombres[col_original] = destino
                    break
            else:
                # Mantener el original si no hay match
                nuevos_nombres[col_original] = col_original
        
        return df.rename(columns=nuevos_nombres)

    def _a_numerico(self, df, columnas=None):
        import pandas as pd, re

        def _to_num(x):
            if x.dtype == "object":
                x = (x.astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.replace(r"[^\d\.\-]", "", regex=True))
            return pd.to_numeric(x, errors="coerce")

        # ⛔ Nunca tocar columnas de texto/fecha
        PROTEGER = {"Fecha", "Nombre", "Rival", "Local_Visitante", "Sessions"}

        if columnas is None:
            columnas = [c for c in df.columns if c not in PROTEGER]

        for c in columnas:
            if c in df.columns and c not in PROTEGER:
                try:
                    df[c] = _to_num(df[c])
                except Exception:
                    pass
        return df

# ============================================================
# 9- Agregación Rendimiento vs Partido DB_Partidos
# ============================================================ 
    def _baseline_entreno_rango(self, conn, id_jugador: int, fecha: str, dias: int):
        sql = """
        WITH vals AS (
            SELECT Rendimiento_Diario AS x
            FROM DB_Entrenamientos
            WHERE id_jugador = ?
            AND Fecha >= date(?, ?)
            AND Fecha <  date(?, '+0 day')
            AND Rendimiento_Diario IS NOT NULL
            ORDER BY x
        ), cnt AS (SELECT COUNT(*) c FROM vals)
        SELECT
        CASE
            WHEN (SELECT c FROM cnt) = 0 THEN NULL
            WHEN (SELECT c FROM cnt) % 2 = 1
            THEN (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))
            ELSE (
            (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2 - 1)) +
            (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))
            ) / 2.0
        END AS mediana,
        (SELECT c FROM cnt) AS n
        """
        delta = f"-{int(dias)} day"
        row = conn.execute(sql, (id_jugador, fecha, delta, fecha)).fetchone()
        mediana = float(row[0]) if row and row[0] is not None else None
        n = int(row[1]) if row else 0
        return mediana, n

    def _baseline_entreno_historico(self, conn, id_jugador: int):
        sql = """
        WITH vals AS (
            SELECT Rendimiento_Diario AS x
            FROM DB_Entrenamientos
            WHERE id_jugador = ? AND Rendimiento_Diario IS NOT NULL
            ORDER BY x
        ), cnt AS (SELECT COUNT(*) c FROM vals)
        SELECT
        CASE
            WHEN (SELECT c FROM cnt) = 0 THEN NULL
            WHEN (SELECT c FROM cnt) % 2 = 1
            THEN (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))
            ELSE (
            (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2 - 1)) +
            (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))
            ) / 2.0
        END AS mediana,
        (SELECT c FROM cnt) AS n
        """
        row = conn.execute(sql, (id_jugador,)).fetchone()
        mediana = float(row[0]) if row and row[0] is not None else None
        n = int(row[1]) if row else 0
        return mediana, n

    def _baseline_entreno_rd_rango(self, conn, jid, fch_str, dias):
            sql = """
            WITH vals AS (
                SELECT Rendimiento_Diario AS x
                FROM DB_Entrenamientos
                WHERE id_jugador = ?
                AND Fecha >= date(?, ?)
                AND Fecha <  date(?, '+0 day')
                AND Rendimiento_Diario IS NOT NULL
                ORDER BY x
            ), cnt AS (SELECT COUNT(*) c FROM vals)
            SELECT
            CASE WHEN (SELECT c FROM cnt)=0 THEN NULL
                WHEN (SELECT c FROM cnt)%2=1
                    THEN (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))
                ELSE ((SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2 - 1)) +
                    (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))) / 2.0
            END AS mediana,
            (SELECT c FROM cnt) AS n
            """
            delta = f"-{int(dias)} day"
            row = conn.execute(sql, (jid, fch_str, delta, fch_str)).fetchone()
            mediana = float(row[0]) if row and row[0] is not None else None
            n = int(row[1]) if row else 0
            return mediana, n

    def _baseline_entreno_rd_hist(self, conn, jid):
        sql = """
        WITH vals AS (
            SELECT Rendimiento_Diario AS x
            FROM DB_Entrenamientos
            WHERE id_jugador = ? AND Rendimiento_Diario IS NOT NULL
            ORDER BY x
        ), cnt AS (SELECT COUNT(*) c FROM vals)
        SELECT
        CASE WHEN (SELECT c FROM cnt)=0 THEN NULL
            WHEN (SELECT c FROM cnt)%2=1
                THEN (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))
            ELSE ((SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2 - 1)) +
                (SELECT x FROM vals LIMIT 1 OFFSET ((SELECT c FROM cnt)/2))) / 2.0
        END AS mediana,
        (SELECT c FROM cnt) AS n
        """
        row = conn.execute(sql, (jid,)).fetchone()
        mediana = float(row[0]) if row and row[0] is not None else None
        n = int(row[1]) if row else 0
        return mediana, n


    def _agregar_rve(self, df_partidos: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega Rendimiento_vs_Entreno (%) y RvE_flag.
        Usa mediana de entrenos 21d -> 60d -> histórico.
        """
        if df_partidos.empty:
            df_partidos["Rendimiento_vs_Entreno"] = pd.NA
            df_partidos["RvE_flag"] = "Sin datos"
            return df_partidos

        # Resolver columnas por alias (por si vienen en snake_case)
        cols = {c.lower(): c for c in df_partidos.columns}
        col_fecha = cols.get("fecha", "Fecha" if "Fecha" in df_partidos.columns else None)
        col_rp    = cols.get("rendimiento_partido", "Rendimiento_Partido" if "Rendimiento_Partido" in df_partidos.columns else None)
        if "id_jugador" not in df_partidos.columns or col_fecha is None or col_rp is None:
            df = df_partidos.copy()
            df["Rendimiento_vs_Entreno"] = pd.NA
            df["RvE_flag"] = "Sin datos"
            return df

        df = df_partidos.copy()
        df["Rendimiento_vs_Entreno"] = pd.NA
        df["RvE_flag"] = "Sin datos"

        with self._conectar() as conn:
            for idx, row in df.iterrows():
                jid = row.get("id_jugador")
                fch = row.get(col_fecha)
                rp  = row.get(col_rp)

                if pd.isna(jid) or pd.isna(fch) or pd.isna(rp):
                    continue

                fch_dt = pd.to_datetime(fch, errors="coerce")
                if pd.isna(fch_dt):
                    continue
                fch_str = fch_dt.date().isoformat()

                # ---- Fallbacks: 21d -> 60d -> histórico ----
                mediana, n = self._baseline_entreno_rango(conn, int(jid), fch_str, dias=21)
                if mediana is None or n == 0:
                    mediana, n = self._baseline_entreno_rango(conn, int(jid), fch_str, dias=60)
                if mediana is None or n == 0:
                    mediana, n = self._baseline_entreno_historico(conn, int(jid))

                if mediana is not None and mediana > 0:
                    rve = float(rp) / float(mediana) * 100.0
                    # Si querés **guardar** el valor real pero **clipear** solo visualmente, NO sobrescribas a 250:
                    # df.at[idx, "Rendimiento_vs_Entreno"] = rve
                    # Acá mantengo tu lógica original (clip duro a 250):
                    if rve > 250:
                        df.at[idx, "Rendimiento_vs_Entreno"] = 250.0
                        df.at[idx, "RvE_flag"] = ">250% (posible baseline bajo o pico excepcional)"
                    elif rve < 50:
                        df.at[idx, "Rendimiento_vs_Entreno"] = rve
                        df.at[idx, "RvE_flag"] = "<50% (rindió mucho menos que entrenamientos)"
                    else:
                        df.at[idx, "Rendimiento_vs_Entreno"] = rve
                        df.at[idx, "RvE_flag"] = "Normal"
                else:
                    df.at[idx, "Rendimiento_vs_Entreno"] = pd.NA
                    df.at[idx, "RvE_flag"] = "Sin baseline suficiente"

        return df

    def _agregar_rve_intensidad(self, df_partidos: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
        """
        Calcula RvE_Intensidad (%) = 100 * Rendimiento_Intensidad / baseline_entreno
        Baseline = mediana de Rendimiento_Diario en 21d -> 60d -> histórico.
        Requiere columnas: id_jugador, Fecha, Rendimiento_Intensidad
        """
        if df_partidos.empty:
            df_partidos["RvE_Intensidad"] = pd.NA
            return df_partidos

        req = {"id_jugador", "Fecha", "Rendimiento_Intensidad"}
        if not req.issubset(df_partidos.columns):
            df = df_partidos.copy()
            df["RvE_Intensidad"] = pd.NA
            return df

        df = df_partidos.copy()
        df["RvE_Intensidad"] = pd.NA

        for idx, row in df.iterrows():
            jid = row.get("id_jugador")
            fch = row.get("Fecha")
            rint = row.get("Rendimiento_Intensidad")
            if pd.isna(jid) or pd.isna(fch) or pd.isna(rint):
                continue

            fch_dt = pd.to_datetime(fch, errors="coerce")
            if pd.isna(fch_dt):
                continue
            fch_str = fch_dt.date().isoformat()

            # fallbacks 21d -> 60d -> hist (usando tus helpers ya definidos arriba)
            mediana, n = self._baseline_entreno_rd_rango(conn, int(jid), fch_str, dias=21)
            if mediana is None or n == 0:
                mediana, n = self._baseline_entreno_rd_rango(conn, int(jid), fch_str, dias=60)
            if mediana is None or n == 0:
                mediana, n = self._baseline_entreno_rd_hist(conn, int(jid))

            if mediana and mediana > 0:
                df.at[idx, "RvE_Intensidad"] = float(rint) / float(mediana) * 100.0
            else:
                df.at[idx, "RvE_Intensidad"] = pd.NA

        return df



# ============================================================
# 9- Identidad de jugadores (IDs, aliases y validaciones)
# ============================================================ 
         
    def _anexar_id_jugador_por_nombre(self, df: pd.DataFrame) -> pd.DataFrame:
        """Completa id_jugador buscando por Nombre en DB_Jugadores (normaliza acentos/case)."""
        if df.empty:
            df["id_jugador"] = pd.Series([], dtype="Int64")
            return df

        if "Nombre" not in df.columns:
            df["id_jugador"] = pd.NA
            return df

        with self._conectar() as conn:
            ref = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)

        ref["Nombre_norm"] = ref["Nombre"].astype(str).map(self._norm_texto)
        df = df.copy()
        df["Nombre_norm"] = df["Nombre"].astype(str).map(self._norm_texto)

        m = df.merge(ref[["id_jugador","Nombre_norm"]], on="Nombre_norm", how="left")
        m = m.drop(columns=["Nombre_norm"])

        # log
        if "id_jugador" in m.columns:
            no_map = m[m["id_jugador"].isna()]
            if not no_map.empty:
                ejemplos = no_map["Nombre"].dropna().astype(str).unique().tolist()[:8]
                print(f"[WARN] No pude mapear {len(no_map)} jugador(es) por nombre. Ejemplos: {ejemplos}")

        return m

    def _aplicar_alias_jugadores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica aliases de manera robusta"""
        if 'Nombre' not in df.columns and 'Players' in df.columns:
            df = df.rename(columns={'Players': 'Nombre'})
        
        if 'Nombre' not in df.columns:
            return df
            
        # Cargar aliases
        alias_path = Path("data/ref/aliases_jugadores.csv")
        if not alias_path.exists():
            print(f"[WARN] No existe archivo de aliases: {alias_path}")
            return df
            
        try:
            aliases = pd.read_csv(alias_path)
            alias_dict = {}
            
            # Crear mapeo case-insensitive
            for _, row in aliases.iterrows():
                fuente = str(row['Nombre_Fuente']).strip().lower()
                alias_dict[fuente] = row['id_jugador']
                
            # Aplicar aliases
            df['nombre_normalizado'] = df['Nombre'].astype(str).str.strip().str.lower()
            
            # Crear columna id_jugador si no existe
            if 'id_jugador' not in df.columns:
                df['id_jugador'] = pd.NA
                
            # Mapear aliases
            df['id_jugador'] = df['id_jugador'].fillna(
                df['nombre_normalizado'].map(alias_dict)
            )
            
            return df.drop(columns=['nombre_normalizado'], errors='ignore')
            
        except Exception as e:
            print(f"[ERROR] Error aplicando aliases: {e}")
            return df

    def _fabricar_alias_desde_db(self) -> dict[str, int]:
        """Versión mejorada que maneja mejor los apellidos solos"""
        with self._conectar() as conn:
            ref = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)

        def norm(s): 
            return self._norm_texto(s) if s else ""
        
        ref["Nombre_norm"] = ref["Nombre"].astype(str).map(norm)
        alias2id, colisiones = {}, set()

        def add_alias(k, vid):
            if not k: return
            if k in alias2id and alias2id[k] != vid:
                colisiones.add(k)
            else:
                alias2id[k] = vid

        for _, row in ref.iterrows():
            jid = int(row["id_jugador"])
            nombre_completo = row["Nombre_norm"]
            if not nombre_completo: continue
            
            toks = nombre_completo.split()
            nombre_pila = toks[0] if toks else ""
            apellido = toks[-1] if toks else ""
            
            # APELLIDOS EN MAYÚSCULAS (para casos como 'ALVARADO')
            apellido_mayus = apellido.upper() if apellido else ""
            
            # Variantes principales
            cand = {
                nombre_completo,           # "isaac brizuela"
                apellido,                  # "brizuela" 
                apellido_mayus,            # "BRIZUELA" (MAYÚSCULAS)
                nombre_pila,               # "isaac"
                (nombre_pila[:1] + " " + apellido) if nombre_pila else "",  # "i brizuela"
                (nombre_pila[:1] + apellido) if nombre_pila else "",        # "ibrizuela"
            }

            for a in cand:
                a = a.strip()
                if a:
                    add_alias(a, jid)

        # eliminar los alias ambiguos
        for k in colisiones:
            alias2id.pop(k, None)

        return alias2id

    def _resolver_id_por_alias_heuristico(self, serie_nombres: pd.Series) -> pd.Series:
        """Resuelve IDs con matching fuzzy mejorado para partidos"""
        if serie_nombres is None or serie_nombres.empty:
            return pd.Series(pd.NA, index=serie_nombres.index, dtype="Int64")

        # Cargar todos los jugadores de la DB
        with self._conectar() as conn:
            jugadores_db = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)
        
        # Crear lista de nombres para fuzzy matching
        nombres_db = jugadores_db['Nombre'].tolist()
        id_map = dict(zip(jugadores_db['Nombre'], jugadores_db['id_jugador']))
        
        def encontrar_jugador(nombre_input):
            nombre_input = str(nombre_input).strip()
            if not nombre_input:
                return pd.NA
            
            # 1. Buscar coincidencia exacta primero
            for nombre_db, jid in id_map.items():
                if nombre_input.lower() == nombre_db.lower():
                    return jid
            
            # 2. Buscar por apellido solamente
            for nombre_db, jid in id_map.items():
                apellido_db = nombre_db.split()[-1].lower() if ' ' in nombre_db else nombre_db.lower()
                if nombre_input.lower() == apellido_db:
                    return jid
            
            # 3. Fuzzy matching como último recurso
            try:
                mejor_coincidencia, score = process.extractOne(
                    nombre_input, 
                    nombres_db, 
                    scorer=fuzz.token_sort_ratio
                )
                if score >= 70:  # Umbral de similitud
                    return id_map[mejor_coincidencia]
            except:
                pass
            
            return pd.NA
        
        return serie_nombres.apply(encontrar_jugador)
       
    def _buscar_jugadores_similares(self, nombre: str, umbral=0.7) -> list[dict]:
        """
        Busca jugadores en la DB con nombres similares al proporcionado usando fuzzy matching.
        """
        with self._conectar() as conn:
            jugadores = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)
        
        nombre_norm = self._norm_texto(nombre)
        similares = []
        
        for _, row in jugadores.iterrows():
            nombre_db_norm = self._norm_texto(row['Nombre'])
            # Calcula similaridad entre ambos nombres normalizados
            similitud = fuzz.ratio(nombre_norm, nombre_db_norm) / 100
            
            if similitud >= umbral:
                similares.append({
                    'id_jugador': row['id_jugador'],
                    'Nombre': row['Nombre'],
                    'similitud': similitud
                })
        
        # Ordenar por similitud descendente
        similares.sort(key=lambda x: x['similitud'], reverse=True)
        return similares
        
    def validar_aliases(self):
        """Verifica que todos los IDs en aliases existan en la DB"""
        with self._conectar() as conn:
            ids_db = set(pd.read_sql("SELECT id_jugador FROM DB_Jugadores", conn)['id_jugador'])
        
        alias_path = Path("data/ref/aliases_jugadores.csv")
        aliases = pd.read_csv(alias_path)
        
        ids_invalidos = set(aliases['id_jugador']) - ids_db
        
        if ids_invalidos:
            print(f"[ERROR] Los siguientes IDs en aliases no existen en DB_Jugadores: {ids_invalidos}")
            # Mostrar los alias problemáticos
            problematicos = aliases[aliases['id_jugador'].isin(ids_invalidos)]
            print("Aliases con problemas:")
            print(problematicos.to_string(index=False))
            
            # Sugerir correcciones
            print("\nSugerencias:")
            for _, row in problematicos.iterrows():
                similares = self._buscar_jugadores_similares(row['Nombre_Fuente'])
                print(f"Alias: {row['Nombre_Fuente']} -> ID {row['id_jugador']} (no existe)")
                if similares:
                    print("  Jugadores similares en DB:")
                    for j in similares:
                        print(f"  - ID {j['id_jugador']}: {j['Nombre']}")

    def _asegurar_id_jugador(self, df: pd.DataFrame) -> pd.DataFrame:
        """Asigna IDs de jugador con robustez y logging"""
        df = df.copy()
        
        if 'id_jugador' not in df.columns:
            df['id_jugador'] = pd.NA
        
        # 1. Verificar nombres únicos a mapear
        nombres_unicos = df['Nombre'].dropna().unique()
        print(f"[DEBUG] Nombres únicos a mapear ({len(nombres_unicos)}): {nombres_unicos[:10]}...")
        
        # 2. Aplicar alias desde CSV
        df = self._aplicar_alias_jugadores(df)
        
        # 3. Buscar coincidencias directas en DB
        with self._conectar() as conn:
            jugadores_db = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)
            print("[DEBUG] Jugadores en DB:", jugadores_db['Nombre'].tolist()[:10])
        
        # Normalizar nombres para matching
        def normalizar_nombre(nombre):
            nombre = str(nombre).strip().lower()
            nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) 
                            if unicodedata.category(c) != 'Mn')
            return re.sub(r'\s+', ' ', nombre).strip()
        
        # Crear mapeo normalizado
        jugadores_db['nombre_norm'] = jugadores_db['Nombre'].apply(normalizar_nombre)
        mapeo_norm = dict(zip(jugadores_db['nombre_norm'], jugadores_db['id_jugador']))
        
        # Aplicar matching
        df['nombre_norm'] = df['Nombre'].apply(normalizar_nombre)
        df['id_jugador'] = df['id_jugador'].fillna(df['nombre_norm'].map(mapeo_norm))
        
        falt = df['id_jugador'].isna()
        if falt.any():
            df.loc[falt, 'id_jugador'] = self._resolver_id_por_alias_heuristico(df.loc[falt, 'Nombre'])

        # Reportar no mapeados
        no_map = df[df['id_jugador'].isna()]['Nombre'].unique()
        if len(no_map) > 0:
            print(f"[WARN] No mapeados ({len(no_map)}): {no_map[:10]}...")
        
        return df.drop(columns=['nombre_norm'], errors='ignore')

# ============================================================
# 10- Fechas
# ============================================================ 
    @staticmethod
    def normalizar_fechas(serie: pd.Series) -> pd.Series:
        """
        Versión mejorada con más formatos de fecha y mejor manejo de errores
        """

        """Versión mejorada que maneja timestamps UTC"""
        # 1. Intentar parsear como timestamp UTC
        try:
            fechas = pd.to_datetime(
                serie, 
                format='%a %b %d %H:%M:%S UTC %Y', 
                errors='coerce'
            )
            if fechas.notna().any():
                return fechas.dt.date
        except:
            pass

        # 1. Intentar conversión directa si ya son fechas
        if pd.api.types.is_datetime64_any_dtype(serie):
            return serie.dt.date
        
        # 2. Lista de formatos a probar (ordenados por probabilidad)
        formatos = [
            '%d/%m/%Y',   # 31/12/2023
            '%Y-%m-%d',   # 2023-12-31
            '%m/%d/%Y',   # 12/31/2023 (formato americano)
            '%d-%m-%Y',   # 31-12-2023
            '%Y/%m/%d',   # 2023/12/31
            '%d.%m.%Y',   # 31.12.2023
            '%Y%m%d',     # 20231231
            '%d-%b-%y',   # 31-Dic-23
            '%d-%b-%Y',   # 31-Dic-2023
            '%d %b %Y',   # 31 Dic 2023
        ]
        
        # 3. Probar cada formato secuencialmente
        for fmt in formatos:
            try:
                fechas = pd.to_datetime(serie, format=fmt, errors='coerce')
                if fechas.notna().any():
                    return fechas.dt.date
            except:
                continue
        
        # 4. Manejar seriales de Excel (números como 44927)
        if pd.api.types.is_numeric_dtype(serie):
            try:
                fechas = pd.to_datetime(
                    serie.astype(float),
                    unit='D',
                    origin='1899-12-30',  # Para Excel Windows
                    errors='coerce'
                )
                return fechas.dt.date
            except:
                pass
        
        # 5. Si todo falla, devolver serie con NaT/None
        return pd.to_datetime(serie, errors='coerce').dt.date
    
    def transformar_archivo(self, ruta: Path) -> pd.DataFrame:  # Añade 'self' como primer parámetro
        """Convierte formatos problemáticos antes del ETL"""
        df = self._leer_excel_robusto(ruta)
        
        # Convertir timestamps UTC a fecha simple
        if 'Days' in df.columns:
            try:
                # Primero intentar parsear como timestamp UTC
                df['Days'] = pd.to_datetime(
                    df['Days'], 
                    format='%a %b %d %H:%M:%S UTC %Y',
                    errors='coerce'
                )
                
                # Si falla, intentar otros formatos
                if df['Days'].isna().any():
                    df['Days'] = pd.to_datetime(
                        df['Days'], 
                        infer_datetime_format=True,
                        errors='coerce'
                    )
                
                # Convertir a date
                df['Days'] = df['Days'].dt.date
            except Exception as e:
                print(f"[WARN] Error transformando fechas: {str(e)}")
        
        return df
    
# ============================================================
# 11- Cálculo de métricas (CE/CS/CR y Rendimiento)
# ============================================================ 

    def _serie_segura(self, df: pd.DataFrame, col: str, dtype=float) -> pd.Series:
        """Devuelve la columna como Serie numérica con NaN→0.
        Si no existe, devuelve una Serie de ceros con el mismo index."""
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0).astype(dtype)
        else:
            return pd.Series(0, index=df.index, dtype=dtype)

    def _calcular_ce_cs_cr(self, df: pd.DataFrame) -> pd.DataFrame:
        # Componentes seguros (si no existen, series de 0)
        dist_total = self._serie_segura(df, "Distancia_total")
        hmld      = self._serie_segura(df, "HMLD_m")
        hsr_abs   = self._serie_segura(df, "HSR_abs_m")
        # fallback legacy para HSR_m si Abs no viene
        if (hsr_abs == 0).all() and "HSR_m" in df.columns:
            hsr_abs = self._serie_segura(df, "HSR_m")

        sprints_ct   = self._serie_segura(df, "Sprints_cantidad")
        acc3         = self._serie_segura(df, "Acc_3")
        dec3         = self._serie_segura(df, "Dec_3")
        player_load  = self._serie_segura(df, "Player_Load")

        # Distancia baja (>=0)
        dist_baja = (dist_total - hsr_abs - hmld).clip(lower=0)
        df["Dist_baja"] = dist_baja

        # CE, CS, CR solo si no existen ya
        if "Carga_Explosiva" not in df.columns:
            df["Carga_Explosiva"] = hsr_abs * 1.2 + sprints_ct * 5 + acc3 * 3 + dec3 * 2.5

        if "Carga_Sostenida" not in df.columns:
            df["Carga_Sostenida"] = hmld * 1.0 + player_load * 0.2

        if "Carga_Regenerativa" not in df.columns:
            df["Carga_Regenerativa"] = dist_baja * 0.5

        return df
    
    def _posicion_por_jugador(self, ids: list[int]) -> dict[int, str]:
        """Devuelve {id_jugador: Posicion} leyendo DB_Jugadores."""
        if not ids:
            return {}
        with self._conectar() as conn:
            q = f"SELECT id_jugador, Posicion FROM DB_Jugadores WHERE id_jugador IN ({','.join(map(str, ids))})"
            df = pd.read_sql(q, conn)
        return dict(zip(df["id_jugador"], df["Posicion"].fillna("").astype(str)))

    def _percentiles_por_jugador(self, id_jugador: int) -> dict:
        """
        Calcula p10 y p90 de CE/CS/CR para un jugador (histórico de entrenos+partidos).
        Si no hay suficientes datos, cae a percentiles globales.
        """
        with self._conectar() as conn:
            df_e = pd.read_sql(
                "SELECT Carga_Explosiva, Carga_Sostenida, Carga_Regenerativa FROM DB_Entrenamientos WHERE id_jugador=?",
                conn, params=(id_jugador,)
            )
            df_p = pd.read_sql(
                "SELECT Carga_Explosiva, Carga_Sostenida, Carga_Regenerativa FROM DB_Partidos WHERE id_jugador=?",
                conn, params=(id_jugador,)
            )
            df = pd.concat([df_e, df_p], ignore_index=True)
            # Fallback global si no hay
            if len(df.dropna(how='all')) < 10:
                df_e = pd.read_sql(
                    "SELECT Carga_Explosiva, Carga_Sostenida, Carga_Regenerativa FROM DB_Entrenamientos", conn
                )
                df_p = pd.read_sql(
                    "SELECT Carga_Explosiva, Carga_Sostenida, Carga_Regenerativa FROM DB_Partidos", conn
                )
                df = pd.concat([df_e, df_p], ignore_index=True)

        def p10_90(s: pd.Series) -> tuple[float, float]:
            s = pd.to_numeric(s, errors="coerce").dropna()
            if len(s) < 10:
                # fallback duro si hay muy pocos datos
                if len(s) == 0:
                    return (0.0, 1.0)
                return (float(s.min()), float(max(s.quantile(0.9), s.min()+1e-6)))
            return (float(s.quantile(0.10)), float(s.quantile(0.90)))

        p10_ce, p90_ce = p10_90(df["Carga_Explosiva"])
        p10_cs, p90_cs = p10_90(df["Carga_Sostenida"])
        p10_cr, p90_cr = p10_90(df["Carga_Regenerativa"])

        return {
            "CE": (p10_ce, p90_ce),
            "CS": (p10_cs, p90_cs),
            "CR": (p10_cr, p90_cr),
        }

    @staticmethod
    def _escala_0a100(x: pd.Series, p10: float, p90: float) -> pd.Series:
        """
        Normaliza por percentiles:
        - p10 = referencia mínima
        - p90 = referencia máxima
        Puede devolver valores >100 si el jugador rinde por encima de su histórico.
        """
        denom = max(p90 - p10, 1e-6)
        z = (x.fillna(0) - p10) / denom
        return (z * 100)  # <-- SIN clip

         
    def _calcular_rendimiento_total(self, df: pd.DataFrame, destino_col: str) -> pd.DataFrame:
        
        """
        Agrega columna 'destino_col' (Rendimiento_Diario o Rendimiento_Partido) con score 0–100.
        - Normaliza CE/CS/CR por percentiles p10–p90 del jugador (fallback global).
        - Pondera por posición.
        """

        if df.empty:
            df[destino_col] = pd.Series([], dtype=float)
            return df

        for c in ["Carga_Explosiva", "Carga_Sostenida", "Carga_Regenerativa"]:
            if c not in df.columns:
                df[destino_col] = np.nan
                return df

        ids = df.get("id_jugador", pd.Series(dtype=int)).dropna().astype(int).unique().tolist()
        pos_por_j = self._posicion_por_jugador(ids)

        # ✅ Cache de percentiles por jugador (una sola query por jugador)
        percentiles_cache = {jid: self._percentiles_por_jugador(jid) for jid in ids}

        out = []
        for _, row in df.iterrows():
            jid = int(row["id_jugador"]) if pd.notna(row.get("id_jugador")) else -1
            ce, cs, cr = row.get("Carga_Explosiva", 0), row.get("Carga_Sostenida", 0), row.get("Carga_Regenerativa", 0)

            P = percentiles_cache.get(jid, {"CE": (0,1), "CS": (0,1), "CR": (0,1)})
            ce_n = float(self._escala_0a100(pd.Series([ce]), *P["CE"]).iloc[0])
            cs_n = float(self._escala_0a100(pd.Series([cs]), *P["CS"]).iloc[0])
            cr_n = float(self._escala_0a100(pd.Series([cr]), *P["CR"]).iloc[0])

            pos = pos_por_j.get(jid, "")
            w = self._PESOS_POR_POSICION.get(pos, self._PESOS_POR_POSICION["_default"])

            score = ce_n * w[0] + cs_n * w[1] + cr_n * w[2]

            out.append(score)

        df[destino_col] = pd.Series(out, index=df.index).clip(0, 100)
        return df

    def _calcular_rendimiento_partido_hibrido(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula:
        - Rendimiento_Intensidad (0–100): usando CE/CS/CR por minuto + percentiles por jugador.
        - Rendimiento_Partido (0–100 ajustado por minutos): score total * (min/90).
        Requiere: Carga_Explosiva, Carga_Sostenida, Carga_Regenerativa, Minutos_jugados.
        """
        if df.empty:
            df["Rendimiento_Intensidad"] = np.nan
            df["Rendimiento_Partido"] = np.nan
            return df

        # Asegurar columnas mínimas
        for c in ["Carga_Explosiva","Carga_Sostenida","Carga_Regenerativa"]:
            if c not in df.columns:
                df[c] = np.nan

        # 👉 minutos: preferir Duracion_min, si no, Minutos_jugados
        mins_col = "Duracion_min" if "Duracion_min" in df.columns else "Minutos_jugados"
        mins = pd.to_numeric(df.get(mins_col, 0), errors="coerce").fillna(0)
        mins_safe = mins.clip(lower=1)

        # --- INTENSIDAD: CE/CS/CR por minuto (evitar /0)
        mins = pd.to_numeric(df["Duracion_min"], errors="coerce").fillna(0)
        mins_safe = mins.clip(lower=1)
        ce_pm = df["Carga_Explosiva"]    / mins_safe
        cs_pm = df["Carga_Sostenida"]    / mins_safe
        cr_pm = df["Carga_Regenerativa"] / mins_safe

        # ---- Normalización 0–100 usando percentiles POR MINUTO
        ids = df.get("id_jugador", pd.Series(dtype=int)).dropna().astype(int).unique().tolist()
        pos_por_j = self._posicion_por_jugador(ids)
        perc_cache = {jid: self._percentiles_por_jugador(jid) for jid in ids}

        rint = []
        for i, row in df.iterrows():
            jid = int(row["id_jugador"]) if pd.notna(row.get("id_jugador")) else -1
            P = perc_cache.get(jid, {"CE": (0,1), "CS": (0,1), "CR": (0,1)})
            # Derivar percentiles por minuto ≈ percentiles de totales / 90
            p10_ce, p90_ce = P["CE"][0]/90.0, P["CE"][1]/90.0
            p10_cs, p90_cs = P["CS"][0]/90.0, P["CS"][1]/90.0
            p10_cr, p90_cr = P["CR"][0]/90.0, P["CR"][1]/90.0

            ce_n = float(self._escala_0a100(pd.Series([ce_pm.loc[i]]), p10_ce, p90_ce).iloc[0])
            cs_n = float(self._escala_0a100(pd.Series([cs_pm.loc[i]]), p10_cs, p90_cs).iloc[0])
            cr_n = float(self._escala_0a100(pd.Series([cr_pm.loc[i]]), p10_cr, p90_cr).iloc[0])

            pos = pos_por_j.get(jid, "")
            w = self._PESOS_POR_POSICION.get(pos, self._PESOS_POR_POSICION["_default"])
            rint.append(ce_n*w[0] + cs_n*w[1] + cr_n*w[2])

        df["Rendimiento_Intensidad"] = pd.Series(rint, index=df.index)

        # --- TOTAL (igual que ya tenías)
        tmp2 = df.copy()
        tmp2 = self._calcular_rendimiento_total(tmp2, destino_col="__R_TOT__")
        factor_min = (mins.clip(lower=1) / 90.0).astype(float)
        df["Rendimiento_Partido"] = (tmp2["__R_TOT__"].astype(float) * factor_min)
        
        return df


# ============================================================
# 12- Clasificación “Entrenamiento vs Partido”
# ============================================================ 

    def dividir_por_calendario(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Devuelve (entrenamientos, partidos) usando el calendario de partidos.
        """
        df = self._renombrar_columnas(df) 
        df = self._asegurar_id_jugador(df)

        if "Fecha" not in df.columns:
            raise ValueError("No se encuentra la columna 'Fecha' en el Excel.")
        df["Fecha"] = self.normalizar_fechas(df["Fecha"])
        df = self._a_numerico(df, columnas=COLUMNAS_NUMERICAS)
        df = self._calcular_ce_cs_cr(df)

        if self._fechas_partidos:
            es_partido = df["Fecha"].isin(self._fechas_partidos)
        else:
            flag_rival = df["Rival"].notna() if "Rival" in df.columns else pd.Series(False, index=df.index)
            flag_min   = df["Minutos_jugados"].notna() if "Minutos_jugados" in df.columns else pd.Series(False, index=df.index)
            flag_dur   = df["Duracion_min"].notna() if "Duracion_min" in df.columns else pd.Series(False, index=df.index)
            es_partido = df["Fecha"].notna() & (flag_rival | flag_min | flag_dur)

        partidos = df[es_partido].copy()
        entrenos = df[~es_partido].copy()

        #  ✅ SOLO corregir Local/Visitante si los partidos tienen columna Rival
        if (hasattr(self, "_calendario_partidos_df") and 
            not partidos.empty and 
            "Rival" in partidos.columns):
            
            partidos = self.corregir_local_visitante(partidos, self._calendario_partidos_df)
            print(f"[DEBUG] Local/Visitante corregido para {len(partidos)} partidos")
        else:
            print(f"[DEBUG] No se corrigió Local/Visitante: partidos vacíos o sin columna Rival")

        # Enriquecer PARTIDOS con otros datos del calendario
        if hasattr(self, "_calendario_partidos_df") and not partidos.empty: 
            partidos = partidos.merge(
                self._calendario_partidos_df,
                on="Fecha",
                how="left",
                suffixes=("", "_cal")
            )

            # Completar Rival e id_rival desde calendario si faltan
            if "Rival" not in partidos.columns and "Rival_cal" in partidos.columns:
                partidos["Rival"] = partidos["Rival_cal"]
            elif "Rival" in partidos.columns and "Rival_cal" in partidos.columns:
                partidos["Rival"] = partidos["Rival"].fillna(partidos["Rival_cal"])

            if "id_rival" not in partidos.columns and "id_rival_cal" in partidos.columns:
                partidos["id_rival"] = partidos["id_rival_cal"]
            elif "id_rival" in partidos.columns and "id_rival_cal" in partidos.columns:
                partidos["id_rival"] = partidos["id_rival"].fillna(partidos["id_rival_cal"])

            # limpiar columnas *_cal auxiliares
            drop_cols = [c for c in partidos.columns if c.endswith("_cal")]
            if drop_cols:
                partidos = partidos.drop(columns=drop_cols)

        # CALCULAR RENDIMIENTO
        if not entrenos.empty:
            entrenos = self._calcular_rendimiento_total(entrenos, "Rendimiento_Diario")
        
        if not partidos.empty:
            partidos = self._calcular_rendimiento_total(partidos, "Rendimiento_Partido")

        return entrenos, partidos

    def corregir_local_visitante(self, df_partidos, df_cal):
        if df_partidos.empty or df_cal.empty or "Rival" not in df_partidos.columns:
            return df_partidos
        a = df_partidos.copy()
        b = df_cal.copy()
        a["R_norm"] = a["Rival"].apply(self._norm_txt_simple)
        b["R_norm"] = b["Rival"].apply(self._norm_txt_simple)
        a["LV_norm"] = a.get("Local_Visitante", pd.Series([None]*len(a))).apply(self._norm_txt_simple)
        b["LV_norm"] = b.get("Local_Visitante", pd.Series([None]*len(b))).apply(self._norm_txt_simple)
        a = a.merge(b[["Fecha","R_norm","LV_norm","Local_Visitante"]]
                    .rename(columns={"Local_Visitante":"Local_Visitante_correcto"}),
                    on=["Fecha","R_norm","LV_norm"], how="left")
        a["Local_Visitante"] = a["Local_Visitante_correcto"].fillna(a["Local_Visitante"])
        return a.drop(columns=["R_norm","LV_norm","Local_Visitante_correcto"], errors="ignore")

# ============================================================
# 14- UPSERTs a la base
# ============================================================ 

    def upsert_entrenamientos(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        # Verificación de claves primarias
        print(f"[DEBUG] Filas recibidas para upsert: {len(df)}")
        print(f"[DEBUG] Valores nulos en id_jugador: {df['id_jugador'].isnull().sum()}")
        print(f"[DEBUG] Valores nulos en Fecha: {df['Fecha'].isnull().sum()}")
        
        # Filtrar filas válidas
        df = df.dropna(subset=['id_jugador', 'Fecha']).copy()
        print(f"[DEBUG] Filas válidas después de filtrar nulos: {len(df)}")
        
        # Verificar IDs contra DB
        with self._conectar() as conn:
            ids_validos = set(pd.read_sql("SELECT id_jugador FROM DB_Jugadores", conn)['id_jugador'])
        
        df_valido = df[df['id_jugador'].isin(ids_validos)].copy()
        print(f"[DEBUG] Filas con IDs válidos: {len(df_valido)}")
        
        if len(df) != len(df_valido):
            print("[WARN] Algunos IDs no existen en DB_Jugadores")
            print("Ejemplos:", df[~df['id_jugador'].isin(ids_validos)][['Nombre', 'id_jugador']].head(5))

        # VERIFICAR IDs EXISTENTES
        with self._conectar() as conn:
            ids_existentes = set(pd.read_sql("SELECT id_jugador FROM DB_Jugadores", conn)['id_jugador'])
        
        # FILTRAR SOLO IDs VÁLIDOS
        df_valido = df[df['id_jugador'].isin(ids_existentes)].copy()
        if len(df) != len(df_valido):
            invalidos = df[~df['id_jugador'].isin(ids_existentes)]
            print(f"[WARN] {len(invalidos)} filas con IDs inválidos. Ejemplos:")
            print(invalidos[['Nombre', 'id_jugador']].head(5))

        # si falta id_jugador, intentar por nombre
        if "id_jugador" not in df.columns or df["id_jugador"].isna().all():
            # aseguro que el nombre esté (viene como Players en tu archivo)
            if "Nombre" not in df.columns and "Players" in df.columns:
                df = df.rename(columns={"Players": "Nombre"})
            if "Nombre" in df.columns:
                df = self._anexar_id_jugador_por_nombre(df)
                df = self._aplicar_alias_jugadores(df)


        if "Fecha" not in df.columns:
            raise ValueError("Falta columna 'Fecha' para DB_Entrenamientos.")

        # Filtrar filas sin claves mínimas
        antes = len(df)
        df = df.dropna(subset=["id_jugador", "Fecha"]).copy()
        if df.empty:
            print(f"[WARN] Entrenamientos: {antes} filas leídas, 0 válidas (sin id_jugador/Fecha). No se insertó nada.")
            return 0

        # Tipos seguros
        df["id_jugador"] = pd.to_numeric(df["id_jugador"], errors="coerce").astype("Int64")
        df["Fecha"] = self.normalizar_fechas(df["Fecha"])

        # Columnas objetivo
        columnas = [
            "id_jugador", "Fecha", "Distancia_total",
            "HSR_abs_m", "HSR_rel_m", "HMLD_m",
            "Sprints_distancia_m", "Sprints_cantidad", "Sprints_vel_max_kmh",
            "Velocidad_prom_m_min",
            "Acc_3", "Dec_3", "Player_Load", "RPE",
            "Carga_Explosiva", "Carga_Sostenida", "Carga_Regenerativa", "Rendimiento_Diario",
        ]
        for c in columnas:
            if c not in df.columns:
                df[c] = None

        # si no vino id_jugador, intentar por Nombre + alias
        if "id_jugador" not in df.columns or df["id_jugador"].notna().sum() == 0:
            if "Nombre" in df.columns:
                df = self._anexar_id_jugador_por_nombre(df)
                df = self._aplicar_alias_jugadores(df)  # <<< NUEVO
            if "id_jugador" not in df.columns:
                df["id_jugador"] = pd.NA

        # Quitar cualquier fila que, tras normalizar, haya quedado sin clave
        df = df.dropna(subset=["id_jugador", "Fecha"]).copy()
        if df.empty:
            print("[WARN] Entrenamientos: tras normalizar claves quedaron 0 filas válidas.")
            return 0

        with self._conectar() as conn:
            placeholders = ",".join(["?"] * len(columnas))
            set_clause = ",".join([f"{c}=excluded.{c}" for c in columnas if c != "id_entrenamiento"])
            sql = f"""
            INSERT INTO DB_Entrenamientos ({",".join(columnas)})
            VALUES ({placeholders})
            ON CONFLICT(id_jugador, Fecha) DO UPDATE SET {set_clause};
            """
            data = df[columnas].where(pd.notnull(df[columnas]), None).values.tolist()
            conn.executemany(sql, data)

        # Recalcular sobrecargas solo para lo afectado
        try:
            ids_jug = df["id_jugador"].dropna().astype(int).unique().tolist()
            fmin = pd.to_datetime(df["Fecha"], errors="coerce").min()
            fmax = pd.to_datetime(df["Fecha"], errors="coerce").max()
            fmin_str = fmin.date().isoformat() if pd.notnull(fmin) else None
            fmax_str = fmax.date().isoformat() if pd.notnull(fmax) else None

            # Y SIEMPRE: sobrecargas (CT_7d, CT_28d_avg, ACWR)
            self._actualizar_sobrecargas(
                jugadores=ids_jug, fecha_desde=fmin_str, fecha_hasta=fmax_str
            )
        except Exception as e:
            print(f"[WARN] No se pudieron recalcular sobrecargas tras entrenos: {e}")

        return len(df)

    def upsert_partidos(self, df: pd.DataFrame, conn: sqlite3.Connection | None = None) -> int:
        if df.empty:
            return 0
        
        for c in ["id_jugador", "Fecha"]:
            if c not in df.columns:
                raise ValueError(f"Falta columna obligatoria '{c}' para DB_Partidos.")

        columnas = [
            "id_jugador","Fecha","id_rival","Rival","Local_Visitante","Duracion_min",
            "Distancia_total","HSR_abs_m","HMLD_m",
            "Sprints_distancia_m","Sprints_cantidad","Sprints_vel_max_kmh",
            "Acc_3","Dec_3","Player_Load",
            "Carga_Explosiva","Carga_Sostenida","Carga_Regenerativa",
            "Rendimiento_Partido","Rendimiento_Intensidad","RvE_Intensidad",
            "HSR_rel_m","Velocidad_prom_m_min"
        ]
        
        for c in columnas:
            if c not in df.columns:
                df = df.copy()
                df.loc[:, c] = None

        placeholders = ",".join(["?"] * len(columnas))
        set_clause = ",".join([f"{c}=excluded.{c}" for c in columnas if c != "id_partido"])
        sql = f"""
        INSERT INTO DB_Partidos ({",".join(columnas)})
        VALUES ({placeholders})
        ON CONFLICT(id_jugador, Fecha, ifnull(id_rival,-1))
        DO UPDATE SET {set_clause};
        """
        
        data = df[columnas].where(pd.notnull(df[columnas]), None).values.tolist()

        # 🔥 CAMBIO CRÍTICO: Manejar la conexión correctamente
        if conn is None:
            with self._conectar() as _c:
                _c.executemany(sql, data)
                n_inserted = len(data)
        else:
            # Usar la conexión existente
            conn.executemany(sql, data)
            n_inserted = len(data)

        # Recalcular sobrecargas solo si hay inserciones
        if n_inserted > 0:
            try:
                ids_jug = df["id_jugador"].dropna().astype(int).unique().tolist()
                fmin = pd.to_datetime(df["Fecha"], errors="coerce").min()
                fmax = pd.to_datetime(df["Fecha"], errors="coerce").max()
                fmin_str = fmin.date().isoformat() if pd.notnull(fmin) else None
                fmax_str = fmax.date().isoformat() if pd.notnull(fmax) else None

                self._actualizar_sobrecargas(
                    jugadores=ids_jug, fecha_desde=fmin_str, fecha_hasta=fmax_str
                )
            except Exception as e:
                print(f"[WARN] No se pudieron recalcular sobrecargas tras partidos: {e}")

        return n_inserted

# ============================================================
# 15- Agregaciones / Reporting en DB
# ============================================================

    @staticmethod
    def _inicio_semana(serie_fechas: pd.Series) -> pd.Series:
        d = pd.to_datetime(serie_fechas, errors="coerce")
        # Lunes como inicio de semana
        return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.date.astype(str)

    def recalcular_rendimiento_semanal(self, jugadores: Optional[Iterable[int]] = None) -> int:
        """
        Calcula promedios semanales por jugador a partir de DB_Entrenamientos
        y los upsertea en Rendimiento_Semanal (Fecha = lunes de esa semana).
        """
        with self._conectar() as conn:
            consulta = "SELECT * FROM DB_Entrenamientos"
            if jugadores:
                lista = ",".join(map(str, jugadores))
                consulta += f" WHERE id_jugador IN ({lista})"
            df = pd.read_sql(consulta, conn)

        if df.empty:
            return 0

        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
        df = df.dropna(subset=["Fecha", "id_jugador"])
        df["Semana"] = self._inicio_semana(df["Fecha"])

        sem = df.groupby(["id_jugador", "Semana"], as_index=False).agg(
            Promedio_Carga_Explosiva=("Carga_Explosiva", "mean"),
            Promedio_Carga_Sostenida=("Carga_Sostenida", "mean"),
            Promedio_Carga_Regenerativa=("Carga_Regenerativa", "mean"),
            Promedio_Rendimiento=("Rendimiento_Diario", "mean"),
        ).rename(columns={"Semana": "Fecha"})

        columnas = [
            "id_jugador", "Fecha",
            "Promedio_Carga_Explosiva", "Promedio_Carga_Sostenida",
            "Promedio_Carga_Regenerativa", "Promedio_Rendimiento",
        ]

        with self._conectar() as conn:
            placeholders = ",".join(["?"] * len(columnas))
            set_clause = ",".join([f"{c}=excluded.{c}" for c in columnas if c != "id"])
            sql = f"""
            INSERT INTO Rendimiento_Semanal ({",".join(columnas)})
            VALUES ({placeholders})
            ON CONFLICT(id_jugador, Fecha) DO UPDATE SET {set_clause};
            """
            data = sem[columnas].where(pd.notnull(sem[columnas]), None).values.tolist()
            conn.executemany(sql, data)
        return len(sem)



    def _actualizar_sobrecargas(self, jugadores=None, fecha_desde=None, fecha_hasta=None) -> None:
        import time, sqlite3

        where, params = [], []

        if jugadores:
            marca = ",".join("?" for _ in jugadores)
            where.append(f"p.id_jugador IN ({marca})")
            params.extend(list(map(int, jugadores)))

        if fecha_desde:
            where.append("date(p.Fecha) >= ?"); params.append(fecha_desde)
        if fecha_hasta:
            where.append("date(p.Fecha) <= ?"); params.append(fecha_hasta)

        filtro = ("WHERE " + " AND ".join(where)) if where else ""

        sqls = [
            # --- CE_prev7d
            f"""UPDATE DB_Partidos AS p
                SET CE_prev7d = (
                    SELECT COALESCE(SUM(e.Carga_Explosiva),0)
                    FROM DB_Entrenamientos e
                    WHERE e.id_jugador = p.id_jugador
                    AND e.Fecha >= date(p.Fecha,'-7 day')
                    AND e.Fecha  <  date(p.Fecha)
                )
                {filtro};""",

            # --- CS_prev7d
            f"""UPDATE DB_Partidos AS p
                SET CS_prev7d = (
                    SELECT COALESCE(SUM(e.Carga_Sostenida),0)
                    FROM DB_Entrenamientos e
                    WHERE e.id_jugador = p.id_jugador
                    AND e.Fecha >= date(p.Fecha,'-7 day')
                    AND e.Fecha  <  date(p.Fecha)
                )
                {filtro};""",

            # --- CR_prev7d
            f"""UPDATE DB_Partidos AS p
                SET CR_prev7d = (
                    SELECT COALESCE(SUM(e.Carga_Regenerativa),0)
                    FROM DB_Entrenamientos e
                    WHERE e.id_jugador = p.id_jugador
                    AND e.Fecha >= date(p.Fecha,'-7 day')
                    AND e.Fecha  <  date(p.Fecha)
                )
                {filtro};""",

            # --- CT_7d, CT_28d_avg, ACWR y ACWR_pct (0–100)
            f"""UPDATE DB_Partidos AS p
                SET
                CT_7d = COALESCE(CE_prev7d,0)+COALESCE(CS_prev7d,0)+COALESCE(CR_prev7d,0),
                CT_28d_avg = (
                    SELECT CASE WHEN COUNT(*)=0 THEN NULL ELSE AVG(x.CT) END
                    FROM (
                    SELECT (COALESCE(e.Carga_Explosiva,0)+COALESCE(e.Carga_Sostenida,0)+COALESCE(e.Carga_Regenerativa,0)) AS CT
                    FROM DB_Entrenamientos e
                    WHERE e.id_jugador = p.id_jugador
                        AND e.Fecha >= date(p.Fecha,'-28 day')
                        AND e.Fecha  <  date(p.Fecha)
                    ) x
                ),
                ACWR = CASE
                        WHEN (
                            SELECT COUNT(*)
                            FROM DB_Entrenamientos e
                            WHERE e.id_jugador = p.id_jugador
                            AND e.Fecha >= date(p.Fecha,'-28 day')
                            AND e.Fecha  <  date(p.Fecha)
                        ) = 0
                        THEN NULL
                        ELSE (
                            COALESCE(CE_prev7d,0)+COALESCE(CS_prev7d,0)+COALESCE(CR_prev7d,0)
                        ) / (
                            SELECT AVG(x.CT)
                            FROM (
                            SELECT (COALESCE(e.Carga_Explosiva,0)+COALESCE(e.Carga_Sostenida,0)+COALESCE(e.Carga_Regenerativa,0)) AS CT
                            FROM DB_Entrenamientos e
                            WHERE e.id_jugador = p.id_jugador
                                AND e.Fecha >= date(p.Fecha,'-28 day')
                                AND e.Fecha  <  date(p.Fecha)
                            ) x
                        )
                        END,
                ACWR_pct = CASE
                            WHEN (
                                SELECT COUNT(*)
                                FROM DB_Entrenamientos e
                                WHERE e.id_jugador = p.id_jugador
                                AND e.Fecha >= date(p.Fecha,'-28 day')
                                AND e.Fecha  <  date(p.Fecha)
                            ) = 0
                            THEN NULL
                            ELSE 100.0 * (
                                (
                                COALESCE(CE_prev7d,0)+COALESCE(CS_prev7d,0)+COALESCE(CR_prev7d,0)
                                ) / (
                                SELECT AVG(x.CT)
                                FROM (
                                    SELECT (COALESCE(e.Carga_Explosiva,0)+COALESCE(e.Carga_Sostenida,0)+COALESCE(e.Carga_Regenerativa,0)) AS CT
                                    FROM DB_Entrenamientos e
                                    WHERE e.id_jugador = p.id_jugador
                                    AND e.Fecha >= date(p.Fecha,'-28 day')
                                    AND e.Fecha  <  date(p.Fecha)
                                ) x
                                )
                            )
                            END
                {filtro};""",

            # --- ACWR_flag (con alias también)
            f"""UPDATE DB_Partidos AS p
                SET ACWR_flag = CASE
                    WHEN ACWR_pct IS NULL THEN 'Sin baseline'
                    WHEN ACWR_pct < 80   THEN 'Baja'
                    WHEN ACWR_pct <= 130 THEN 'Óptima'
                    WHEN ACWR_pct <= 150 THEN 'Alta'
                    ELSE 'Muy alta'
                END
                {filtro};"""
        ]


        for intento in range(3):  # Reintentar hasta 3 veces
            try:
                with self._conectar() as conn:
                    cur = conn.cursor()
                    for q in sqls:
                        try:
                            cur.execute(q, params)
                        except sqlite3.OperationalError as e:
                            if "locked" in str(e).lower() and intento < 4:
                                time.sleep(0.5 * (intento + 1))  # Esperar progresivamente
                                continue
                            raise
                    conn.commit()
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and intento < 4:
                    time.sleep(1 * (intento + 1))
                    continue
                raise




# ============================================================
# 16- Procesamiento de entrenamientos (archivo y carpeta)
# ============================================================

    def procesar_excel(self, ruta_xlsx: Path) -> dict:
        resultado = {'entrenamientos': 0, 'partidos': 0, 'filas_rendimiento_semanal': 0}
        try:
            print(f"\n[INFO] Procesando archivo: {ruta_xlsx.name}")

            # ✅ 1) USAR la versión transformada (NO volver a leer el Excel)
            df = self.transformar_archivo(ruta_xlsx)
            print(f"[DEBUG] Filas leídas: {len(df)}")
            print("[DEBUG] Columnas originales (post-transformar):", df.columns.tolist())

            # Guardar una copia cruda de la columna de fecha ANTES de renombrar
            col_fecha_original = next(
                (c for c in df.columns if str(c).lower() in ['days', 'date', 'fecha', 'día', 'dia']),
                None
            )
            if col_fecha_original:
                df['_fecha_original_'] = df[col_fecha_original]

            # 2) Renombrar columnas
            df = self._renombrar_columnas(df)
            print("[DEBUG] Columnas después de renombrar:", df.columns.tolist())

            # 3) Validar estructura básica
            columnas_requeridas = {'Fecha', 'Nombre', 'Distancia_total'}
            faltantes = columnas_requeridas - set(df.columns)
            if faltantes:
                raise ValueError(f"Faltan columnas requeridas: {faltantes}")

            # 4. Normalización de fechas (robusta para WIMU/UTC/Excel)
            if "Fecha" not in df.columns:
                raise ValueError("Falta columna 'Fecha' (mapeada desde 'Days' / 'Date' / 'Fecha').")

            # guardo original para diagnóstico
            df["_fecha_original_"] = df["Fecha"]

            # convierto a date con parser robusto
            df["Fecha"] = self._parsear_fecha_wimu(df["Fecha"])

            # reporte de nulos
            if df["Fecha"].isnull().any():
                n_fechas_nulas = int(df["Fecha"].isnull().sum())
                print(f"[WARN] {n_fechas_nulas} registros con fechas no reconocidas")
                ejemplos = (
                    df.loc[df["Fecha"].isnull(), "_fecha_original_"]
                    .dropna().astype(str).unique().tolist()[:5]
                )
                if ejemplos:
                    print("Ejemplos de valores no parseados:", ejemplos)


            # 5) Asignación de IDs
            print("\n[DEBUG] Proceso de asignación de IDs:")
            df = self._asegurar_id_jugador(df)

            # 6) Separar entrenos/partidos
            entrenos, partidos = self.dividir_por_calendario(df)
            print(f"[DEBUG] Entrenamientos detectados: {len(entrenos)}")
            print(f"[DEBUG] Partidos detectados: {len(partidos)}")

            # 7) Procesar entrenamientos
            if not entrenos.empty:
                entrenos = self._calcular_ce_cs_cr(entrenos)
                entrenos = self._calcular_rendimiento_total(entrenos, "Rendimiento_Diario")
                print("[DEBUG] Muestra de entrenamientos pre-upsert:")
                print(entrenos[['Nombre', 'id_jugador', 'Fecha', 'Carga_Explosiva', 'Carga_Sostenida']].head(3))

                n_entrenos = self.upsert_entrenamientos(entrenos)

                n_entrenos = self.upsert_entrenamientos(entrenos)

            # Atualizar sobrecargas (prev7d + CT_7d + CT_28d_avg + ACWR)
            if n_entrenos > 0:
                ids = entrenos['id_jugador'].dropna().astype(int).unique().tolist()
                fmax = pd.to_datetime(entrenos['Fecha'], errors='coerce').max()
                fmax_str = fmax.date().isoformat() if pd.notnull(fmax) else None
                fplus7 = (fmax + pd.Timedelta(days=7)).date().isoformat() if pd.notnull(fmax) else None
                self._actualizar_sobrecargas(jugadores=ids, fecha_desde=fmax_str, fecha_hasta=fplus7)

            resultado['entrenamientos'] = n_entrenos


            # 8) Procesar partidos (si estás en Opción A: solo log)
            if not partidos.empty:
                print(f"[INFO] Se detectaron {len(partidos)} filas como partidos (serán ignoradas en esta opción)")
            else:
                print("[DEBUG] No hay partidos en este archivo.")

            # 9) Recalcular semanal solo si hubo entrenos nuevos
            if resultado['entrenamientos'] > 0:
                ids_jugadores = entrenos['id_jugador'].dropna().unique().tolist()
                n_semanal = self.recalcular_rendimiento_semanal(ids_jugadores)
                resultado['filas_rendimiento_semanal'] = n_semanal
                print(f"[DEBUG] Recalculado rendimiento semanal para {len(ids_jugadores)} jugadores")
            else:
                print("[DEBUG] Salto recálculo semanal (no hubo nuevos entrenos).")

            # 10) Archivar si hubo algo de trabajo útil
            try:
                fecha_ref = None
                if "Fecha" in df.columns and not df["Fecha"].isna().all():
                    fmax = pd.to_datetime(df["Fecha"], errors="coerce").dropna()
                    if not fmax.empty:
                        fecha_ref = fmax.max().date().isoformat()
            except Exception:
                fecha_ref = None

            if resultado.get('entrenamientos', 0) > 0 or resultado.get('filas_rendimiento_semanal', 0) > 0:
                self._archivar_archivo(ruta_xlsx, "entrenamientos", fecha_ref)

            print(f"[SUCCESS] Archivo {ruta_xlsx.name} procesado: {resultado}")
            return resultado

        except PermissionError as e:
            print(f"[ERROR] No se pudo leer el archivo (¿está abierto?): {e}")
        except ValueError as e:
            print(f"[ERROR] Error de validación: {e}")
        except Exception as e:
            print(f"[ERROR] Error inesperado: {str(e)}")
            traceback.print_exc()
        return resultado

    def procesar_carpeta(self, carpeta_raw: Path) -> dict:
        carpeta_raw = Path(carpeta_raw)
        totales = {"entrenamientos": 0, "partidos": 0, "filas_rendimiento_semanal": 0}

        # ⬇️ Barrer .xlsx y .xls
        archivos = []
        for patron in ("*.xlsx", "*.xls"):
            archivos.extend(sorted(carpeta_raw.glob(patron)))

        for archivo in archivos:
            if archivo.name.startswith("~$"):
                print(f"[INFO] Ignorado lock file: {archivo.name}")
                continue
            try:
                res = self.procesar_excel(archivo)
                for k, v in res.items(): totales[k] += v
            except PermissionError as e:
                print(f"[WARN] No se pudo abrir {archivo.name} (bloqueado). Detalle: {e}")
            except Exception as e:
                print(f"[ERROR] Falló {archivo.name}: {e}")
        return totales

# ============================================================
# 17- Lesiones — Ingesta y upsert (NUEVA SECCIÓN)
# ============================================================

    def _mapeo_columnas_lesiones(self, cols):
        base = {
            "jugador":"Nombre","player":"Nombre","nombre":"Nombre",
            
            "id_jugador":"id_jugador","jugador_id":"id_jugador",

            "fecha":"Fecha_inicio","date":"Fecha_inicio","inicio":"Fecha_inicio",

            "lesion":"Tipo_lesion","lesión":"Tipo_lesion","Lesion":"Tipo_lesion","injury":"Tipo_lesion","tipo_lesion":"Tipo_lesion",

            "musculo":"Musculo","músculo":"Musculo",

            "lado":"Lado",
            
            "tejido":"Tejido","tejido_afectado":"Tejido","tejido_afectado":"Tejido",

            "fuente":"Fuente",
        }
        ren={}
        for c in cols:
            k=str(c).strip().lower().replace("-", "_").replace(" ", "_")
            ren[c]=base.get(k, c)
        return ren

    def _leer_excel_lesiones(self, ruta: Path) -> pd.DataFrame:
        ruta = Path(ruta)
        if ruta.suffix.lower() == ".xls":
            return pd.read_excel(ruta, engine="xlrd")      # Excel 97–2003
        return pd.read_excel(ruta, engine="openpyxl")      # .xlsx moderno

    def _preparar_df_lesiones(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns=self._mapeo_columnas_lesiones(df.columns))

        # Fecha obligatoria
        if "Fecha_inicio" in df.columns:
            df["Fecha_inicio"] = self.normalizar_fechas(df["Fecha_inicio"])
        else:
            df["Fecha_inicio"] = pd.NaT

        # id_jugador (por nombre si hace falta)
        if "id_jugador" not in df.columns or df["id_jugador"].isna().all():
            if "Nombre" in df.columns:
                df = self._asegurar_id_jugador(df)
            else:
                df["id_jugador"] = pd.NA

        # Tipo_lesion obligatoria
        if "Tipo_lesion" not in df.columns:
            df["Tipo_lesion"] = None
        df["Tipo_lesion"] = df["Tipo_lesion"].astype(str).str.strip().replace({"": None})

        # Para el UNIQUE, no dejar NULL en Musculo/Lado/Tejido → usar ''
        for c in ["Musculo","Lado","Tejido"]:
            if c not in df.columns:
                df[c] = ""
            df[c] = df[c].fillna("").astype(str).str.strip()

        # Clave mínima
        df["id_jugador"] = pd.to_numeric(df["id_jugador"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["id_jugador","Fecha_inicio","Tipo_lesion"]).copy()

        cols = ["id_jugador","Fecha_inicio","Tipo_lesion","Musculo","Lado","Tejido","Fuente"]
        return df[cols]

    def upsert_lesiones(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        # Normalizar tipos mínimos
        if "Fecha_inicio" in df.columns:
            df["Fecha_inicio"] = self.normalizar_fechas(df["Fecha_inicio"])

        # Asegurar id_jugador
        if "id_jugador" not in df.columns or df["id_jugador"].isna().any():
            # si viene "Jugador" o "Nombre", los usamos
            if "Jugador" in df.columns and "Nombre" not in df.columns:
                df = df.rename(columns={"Jugador": "Nombre"})
            if "Nombre" in df.columns:
                df = self._anexar_id_jugador_por_nombre(df)
                df = self._aplicar_alias_jugadores(df)

        # Filtrar filas válidas
        df = df.dropna(subset=["id_jugador", "Fecha_inicio"]).copy()

        if df.empty:
            return 0

        # --- Detectar columnas reales en la tabla ---
        with self._conectar() as conn:
            cols_db = {row[1] for row in conn.execute("PRAGMA table_info(DB_Lesiones)")}
        # columnas candidatas que podemos recibir del Excel
        candidatas = [
            "id_jugador", "Fecha_inicio",
            "Tipo_lesion", "Musculo", "Lado", "Tejido", "Severidad",  # core
            "Fuente"  # opcional, por si existe en tu tabla
        ]
        # nos quedamos solo con las que existen en la tabla Y en el df
        cols_final = [c for c in candidatas if (c in cols_db and c in df.columns)]
        if "id_jugador" not in cols_final or "Fecha_inicio" not in cols_final:
            raise ValueError("DB_Lesiones debe tener al menos (id_jugador, Fecha_inicio) y venir en el dataframe.")

        # preparar datos
        df_ins = df[cols_final].where(pd.notnull(df[cols_final]), None)

        # claves únicas del índice uq_lesion_unica que definiste
        conflict_cols = ["id_jugador", "Fecha_inicio", "Tipo_lesion", "Musculo", "Lado", "Tejido"]
        conflict_cols = [c for c in conflict_cols if c in cols_final]  # por si faltara alguna en la tabla

        # columnas a actualizar en conflicto (todas menos las de conflicto)
        set_cols = [c for c in cols_final if c not in conflict_cols]

        placeholders = ",".join(["?"] * len(cols_final))
        set_clause = ",".join([f"{c}=excluded.{c}" for c in set_cols]) if set_cols else ""

        sql = f"""
        INSERT INTO DB_Lesiones ({",".join(cols_final)})
        VALUES ({placeholders})
        """
        if conflict_cols and set_clause:
            sql += f"""
            ON CONFLICT({",".join(conflict_cols)})
            DO UPDATE SET {set_clause}
            """

        with self._conectar() as conn:
            conn.executemany(sql, df_ins.values.tolist())

        return len(df_ins)

    def cargar_lesiones_desde_excel(self, ruta_excel: Path) -> int:
        ruta_excel = Path(ruta_excel)
        print(f"[INFO] Cargando lesiones desde: {ruta_excel.name}")
        try:
            df_raw = self._leer_excel_lesiones(ruta_excel)
            df = self._preparar_df_lesiones(df_raw)
            if df.empty:
                print("[WARN] No hay filas válidas (Jugador/Fecha/Lesión).")
                return 0
            n = self.upsert_lesiones(df)
            print(f"[SUCCESS] Lesiones insertadas/actualizadas: {n}")

            # Archivar (opcional)
            try:
                fecha_ref = None
                if "Fecha_inicio" in df.columns and not df["Fecha_inicio"].isna().all():
                    fmin = pd.to_datetime(df["Fecha_inicio"], errors="coerce").dropna()
                    if not fmin.empty:
                        fecha_ref = fmin.min().date().isoformat()
                self._archivar_archivo(ruta_excel, "lesiones", fecha_ref)
            except Exception:
                pass
            return n
        except Exception as e:
            print(f"[ERROR] Fallo cargando {ruta_excel.name}: {e}")
            import traceback; traceback.print_exc()
            return 0


# ============================================================
# 18- Procesamiento de partidos (archivo y carpeta)
# ============================================================
    
    def cargar_partidos_desde_master(self, ruta_excel: Path) -> int:
        """
        Versión que funciona sin fechas, cruzando con calendario
        """
        try:
            # 1. Cargar archivo
            engine = "openpyxl" if ruta_excel.suffix.lower() == ".xlsx" else None
            df = self._leer_excel_robusto(ruta_excel)
            
            print(f"[DEBUG] Archivo maestro cargado: {len(df)} filas")
            
            # 2. Eliminar duplicados
            df = df.drop_duplicates()
            
            # DEBUG: Mostrar nombres problemáticos
            print(f"[DEBUG] Nombres únicos en archivo: {df['Players'].unique() if 'Players' in df.columns else 'No hay columna Players'}")
            
            # 3. Extraer información de Sessions
            if "Sessions" in df.columns:
                session_info = self._derivar_rival_y_local(df["Sessions"])
                df["Rival"] = session_info["Rival_from_sess"]
                df["Local_Visitante"] = session_info["Local_Visitante_from_sess"]

            # 4. Aplicar aliases y verificar - CORREGIDO
            df = self._aplicar_alias_jugadores(df)

            print(f"[DEBUG] Columnas después de aplicar aliases: {list(df.columns)}")
            if 'id_jugador' in df.columns:
                print(f"[DEBUG] IDs mapeados: {df['id_jugador'].notna().sum()} de {len(df)}")
                print(f"[DEBUG] Ejemplos de IDs: {df['id_jugador'].head(5).tolist()}")
            else:
                print("[DEBUG] Columna id_jugador no existe después de aliases")
            
            # DEBUG CORREGIDO: Verificar si la columna id_jugador existe primero
            if 'id_jugador' in df.columns:
                no_mapeados = df[df['id_jugador'].isna()]['Players' if 'Players' in df.columns else 'Nombre'].unique()
                if len(no_mapeados) > 0:
                    print(f"[DEBUG] Jugadores sin ID después de aliases: {no_mapeados}")
            else:
                print("[DEBUG] Columna id_jugador no existe aún después de aplicar aliases")
                
            # 5. aplicar alias y normalizar rival (canónico)
            aliases_map = self._get_aliases_rivales_map()
            df["Rival_original"] = df["Rival"]

            # normalizo rival del master ignorando tokens decorativos
            df["_R_norm"] = df["Rival"].apply(lambda x: self._norm_texto(x, drop_tokens=self.STOP_RIVAL))
            # aplico alias (si hay) y dejo rival canónico
            df["_R_norm"] = df["_R_norm"].apply(lambda r: aliases_map.get(r, r))
            df["Rival"] = df["_R_norm"]

            # 6. asignar id_rival usando lookup/alta por nombre normalizado
            df["id_rival"] = df["Rival"].apply(self._obtener_o_crear_id_rival)
                    
            # 7. Inferir Fecha por join (Rival + L/V) con fallback por Rival único
            if hasattr(self, "_calendario_partidos_df") and not self._calendario_partidos_df.empty:
                cal = self._calendario_partidos_df.copy()

                # normalizo master (si no viene de 3.1)
                if "_R_norm" not in df.columns:
                    df["_R_norm"] = df["Rival"].apply(lambda x: self._norm_texto(x, drop_tokens=self.STOP_RIVAL))
                df["_LV_norm"] = df["Local_Visitante"].apply(self._norm_texto) if "Local_Visitante" in df.columns else None

                # normalizo calendario
                cal["_R_norm"]  = cal["Rival"].apply(lambda x: self._norm_texto(x, drop_tokens=self.STOP_RIVAL)) if "Rival" in cal.columns else None
                cal["_LV_norm"] = cal["Local_Visitante"].apply(self._norm_texto) if "Local_Visitante" in cal.columns else None

                # join estricto sólo si ambos lados tienen L/V
                puede_stricto = (
                    "_LV_norm" in df.columns and "_LV_norm" in cal.columns
                    and df["_LV_norm"].notna().any() and cal["_LV_norm"].notna().any()
                )
                if puede_stricto:
                    j = df.merge(
                        cal[["Fecha","_R_norm","_LV_norm"]].dropna(subset=["_R_norm","Fecha"]),
                        on=["_R_norm","_LV_norm"], how="left", suffixes=("","_cal")
                    )
                    df["Fecha"] = j["Fecha"]
                else:
                    df["Fecha"] = pd.NaT

                # fallback: Rival con una sola fecha
                faltan = df["Fecha"].isna()
                if faltan.any():
                    cal_unico = (cal.dropna(subset=["_R_norm","Fecha"])
                                    .groupby("_R_norm")["Fecha"].nunique()
                                    .reset_index(name="n"))
                    cal_unico = cal.merge(cal_unico[cal_unico["n"] == 1], on="_R_norm")[["_R_norm","Fecha"]].drop_duplicates()
                    if not cal_unico.empty:
                        j2 = df.loc[faltan, ["_R_norm"]].merge(cal_unico, on="_R_norm", how="left")
                        df.loc[faltan, "Fecha"] = j2["Fecha"].values

                # log
                sin_fecha = df[df["Fecha"].isna()]
                if not sin_fecha.empty:
                    ej = (sin_fecha.get("Rival_original", sin_fecha["Rival"])
                        .dropna().astype(str).head(10).tolist())
                    print(f"[WARN] {len(sin_fecha)} partidos sin fecha (revisar calendario/aliases). Ejemplos: {ej}")

            # 8. Validar que tenemos fechas
            if "Fecha" not in df.columns or df["Fecha"].isna().all():
                raise ValueError("No se pudieron obtener fechas para los partidos")
            
            # 9. Filtrar partidos con fecha válida
            df = df.dropna(subset=["Fecha"])
            print(f"[DEBUG] Partidos con fecha válida: {len(df)}")
            
            # 10. Mapeo de columnas (robusto)
            column_map = {
                # nombre del jugador
                "Players": "Nombre", "Player": "Nombre", "Jugador": "Nombre",

                # duración / distancia
                "Duration (min)": "Duracion_min",
                "Distance (m)": "Distancia_total",

                # velocidades
                "Distance/time (m/min)": "Velocidad_prom_m_min",
                "Distance/time m/min": "Velocidad_prom_m_min",
                "Avg Speed (m/min)": "Velocidad_prom_m_min",

                # HMLD / HSR
                "HMLD (m)": "HMLD_m",
                "Abs HSR(m)": "HSR_abs_m",
                "HSR Rel (m)": "HSR_rel_m",
                "HSR Rel (m)": "HSR_rel_m",
                "HSR Rel  (m)": "HSR_rel_m",      # 👈 doble espacio
                "HSR Rel(m)": "HSR_rel_m",        # 👈 sin espacio
                "HSR Rel  (m/min)": "HSR_rel_m_min",
                "HSR Rel (m/min)":  "HSR_rel_m_min",
                "HSR Rel(m/min)":   "HSR_rel_m_min",

                # sprints
                "Sprints - Distance Abs(m)": "Sprints_distancia_m",
                "Sprint Abs(m)": "Sprints_distancia_m",
                "Sprints - Sprints Abs (count)": "Sprints_cantidad",
                "Sprints Abs (count)": "Sprints_cantidad",
                "Sprints - Max Speed (km/h)": "Sprints_vel_max_kmh",
                "Max Speed (km/h)": "Sprints_vel_max_kmh",

                # aceleraciones / desaceleraciones
                "Acc +3": "Acc_3",
                "Dec +3": "Dec_3",

                # cargas externas
                "Player Load (a.u.)": "Player_Load",

                # esfuerzo percibido
                "RPE General": "RPE",
                "RPE - RPE General": "RPE",
            }
            df = df.rename(columns=column_map)  # errors='ignore' implícito

            # 11. Asignar IDs de jugadores y aplicar aliases
            df = self._anexar_id_jugador_por_nombre(df)
            df = self._aplicar_alias_jugadores(df)

            # 12. (ya tenés id_rival de 3.2). Si tu _adjuntar_id_rival pisa valores, omitirlo:
            # df = self._adjuntar_id_rival(df)   # <- evítalo si ya seteaste df["id_rival"]

            # 13. Tipar a numérico (maneja coma decimal, strings, etc.)
            cols_num = [
                "Distancia_total","HSR_abs_m","HMLD_m","Sprints_distancia_m","Sprints_cantidad",
                "Sprints_vel_max_kmh","Acc_3","Dec_3","Player_Load","HSR_rel_m",
                "Velocidad_prom_m_min","Duracion_min","Rendimiento_Partido"
            ]
            df = self._a_numerico(df, columnas=cols_num)

            # 14. Corregir Local/Visitante con calendario si corresponde
            df_valido = df.dropna(subset=["id_jugador","Fecha"]).copy()
            if hasattr(self, "_calendario_partidos_df") and not df_valido.empty:
                df_valido = self.corregir_local_visitante(df_valido, self._calendario_partidos_df)

            # 15. Calcular CE/CS/CR e híbridos
            df_valido = self._calcular_ce_cs_cr(df_valido)
            df_valido = self._calcular_rendimiento_partido_hibrido(df_valido)

            # 16. NUEVO: abrir UNA conexión y usarla para cálculos/UPSERT
            with self._conectar() as conn:
                # RvE clásico
                df_valido = self._agregar_rve(df_valido)

                # RvE basado en intensidad usando la MISMA conexión
                df_valido = self._agregar_rve_intensidad(df_valido, conn)

                # 17. Asegurar columnas para UPSERT
                cols_partidos = [
                    "id_jugador","Fecha","id_rival","Rival","Local_Visitante","Duracion_min",
                    "Distancia_total","HSR_abs_m","HMLD_m",
                    "Sprints_distancia_m","Sprints_cantidad","Sprints_vel_max_kmh",
                    "Acc_3","Dec_3","Player_Load",
                    "Carga_Explosiva","Carga_Sostenida","Carga_Regenerativa",
                    "Rendimiento_Partido","Rendimiento_Intensidad","RvE_Intensidad",
                    "HSR_rel_m","Velocidad_prom_m_min"
                ]
                df_valido = df_valido.reindex(columns=cols_partidos, fill_value=None)

                # 18. UPSERT dentro del contexto de la conexión
                n = self.upsert_partidos(df_valido, conn=conn)  # 🔥 Pasar la conexión activa

            # Mover el archivado FUERA del bloque with
            if n and n > 0:
                fecha_ref = pd.to_datetime(df_valido['Fecha'].iloc[0]).date().isoformat() if not df_valido.empty else None
                self._archivar_archivo(ruta_excel, "partidos", fecha_ref)

            return n

                
        except Exception as e:
            print(f"[ERROR] Fallo al cargar partidos: {str(e)}")
            import traceback
            traceback.print_exc()
            return 0

    def procesar_carpeta_partidos(self, dir_partidos: Path) -> int:
        dir_partidos = Path(dir_partidos)
        n_total = 0
        fmin_glob, fmax_glob = None, None
        ids_glob = set()

        archivos = []
        for patron in ("*.xlsx", "*.xls"):
            archivos.extend(sorted(dir_partidos.glob(patron)))

        # Procesar todos los archivos primero
        for archivo in archivos:
            if archivo.name.startswith("~$"):
                continue
            try:
                n = self.cargar_partidos_desde_master(archivo)
                n_total += n
            except Exception as e:
                print(f"[ERROR] Falló {archivo.name}: {e}")
        
        # Luego actualizar sobrecargas UNA SOLA VEZ
        try:
            with self._conectar() as conn:
                df_range = pd.read_sql(
                    "SELECT MIN(Fecha) AS fmin, MAX(Fecha) AS fmax FROM DB_Partidos",
                    conn
                )
            fmin_glob = df_range['fmin'][0]
            fmax_glob = df_range['fmax'][0]
            
            self._actualizar_sobrecargas(
                jugadores=None,
                fecha_desde=fmin_glob, 
                fecha_hasta=fmax_glob
            )
        except Exception as e:
            print(f"[WARN] No se pudieron recalcular sobrecargas: {e}")
        
        return n_total


# ============================================================
# 19- Helpers específicos para partidos (validación/normalización “extra”)
# ============================================================

    def _leer_y_validar_excel(self, ruta: Path) -> pd.DataFrame:
        """Lee el Excel y valida estructura básica - VERSIÓN CORREGIDA"""
        engine = "openpyxl" if ruta.suffix.lower() == ".xlsx" else None
        df = pd.read_excel(ruta, engine=engine)
        
        # ELIMINAR esta línea problemática:
        # df['_raw_data'] = df.to_dict('records')  # ← ESTO CAUSA EL ERROR
        
        # Detectar y eliminar duplicados exactos (solo en columnas normales)
        duplicates = df.duplicated()
        if duplicates.any():
            print(f"[WARN] Eliminando {duplicates.sum()} filas duplicadas exactas")
            df = df[~duplicates].copy()
        
        return df

    def _normalizar_columnas_partidos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza nombres de columnas con mapeo mejorado"""
        mapeo_columnas = {
            'fecha': ['fecha', 'date', 'match date', 'día'],
            'nombre': ['jugador', 'player', 'nombre jugador'],
            'rival': ['rival', 'opponent', 'equipo contrario'],
            'duracion_min': ['duracion_min', 'minutos', 'duration (min)'],
            'local_visitante': ['local_visitante', 'condicion', 'local/visitante']
        }
        
        # Normalizar nombres de columnas
        df.columns = [str(col).strip().lower() for col in df.columns]
        
        # Aplicar mapeo
        for std_col, variants in mapeo_columnas.items():
            for variant in variants:
                if variant in df.columns:
                    df[std_col] = df[variant]
                    break
                    
        return df

    def _filtrar_fechas_validas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra fechas en rango válido (2000-2025)"""
        if 'fecha' not in df.columns:
            raise ValueError("No se encontró columna de fecha")
        
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        # Filtrar fechas imposibles (antes de 2000 o después de 2025)
        mask = (df['fecha'].dt.year >= 2000) & (df['fecha'].dt.year <= 2025)
        if not mask.all():
            invalid_count = len(df) - mask.sum()
            print(f"[WARN] Eliminando {invalid_count} registros con fechas fuera de rango (2000-2025)")
            df = df[mask].copy()
        
        return df

    def _asignar_ids_jugadores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Asigna IDs de jugadores con manejo mejorado de nombres"""
        if 'nombre' not in df.columns:
            raise ValueError("No se encontró columna con nombres de jugadores")
        
        # Convertir a formato estándar
        df['nombre'] = df['nombre'].astype(str).str.strip()
        
        # Primero intentar con aliases
        df = self._aplicar_alias_jugadores(df.rename(columns={'nombre': 'Nombre'}))
        
        # Luego con matching directo
        if 'id_jugador' not in df.columns or df['id_jugador'].isna().any():
            df = self._anexar_id_jugador_por_nombre(df)
        
        return df

    def _normalizar_rivales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza nombres de rivales y asigna IDs"""
        if 'rival' not in df.columns:
            df['rival'] = None
        
        # Aplicar correcciones de nombres
        correcciones = {
            'cincrimati': 'cincinnati',
            'gmrcotte': 'guadalajara',
            'zacatecas': 'zacatecas fc',
            # Añadir más correcciones según necesidad
        }
        df['rival'] = df['rival'].str.lower().replace(correcciones)
        
        # Asignar IDs de rivales
        df = self._adjuntar_id_rival(df)
        
        return df

    def _completar_metricas_base(self, df: pd.DataFrame) -> pd.DataFrame:
        """Completa métricas faltantes con valores por defecto"""
        metricas = [
            'distancia_total', 'hsr_abs_m', 'hmlD_m', 'duracion_min',
            'sprints_distancia_m', 'sprints_cantidad', 'player_load'
        ]
        
        for metrica in metricas:
            if metrica not in df.columns:
                df[metrica] = None
            else:
                df[metrica] = pd.to_numeric(df[metrica], errors='coerce')
        
        return df

    def _cargar_datos_validos(self, df: pd.DataFrame) -> int:
        """Filtra y carga solo datos válidos"""
        # Filtrar filas completas
        df = df.dropna(subset=['id_jugador', 'fecha']).copy()
        
        # Columnas requeridas para el upsert
        columnas_requeridas = [
            'id_jugador', 'fecha', 'id_rival', 'rival', 'local_visitante',
            'distancia_total', 'hsr_abs_m', 'hmlD_m', 'duracion_min'
        ]
        
        # Verificar que existan todas las columnas
        for col in columnas_requeridas:
            if col not in df.columns:
                df[col] = None
        
        # Asegurar tipos correctos
        df['id_rival'] = pd.to_numeric(df['id_rival'], errors='coerce').astype('Int64')
        df['local_visitante'] = df['local_visitante'].fillna('Desconocido')
        
        return self.upsert_partidos(df[columnas_requeridas])

# ============================================================
# 20- Helper para leer excel 97-2003
# ============================================================

    def _detectar_formato_excel(self, ruta: Path) -> str:
        """
        Devuelve 'xls' si es CFBF (Excel 97-2003), 'xlsx' si es ZIP (Office OpenXML),
        o '' si no se puede detectar.
        """
        ruta = Path(ruta)
        with open(ruta, "rb") as f:
            magic = f.read(8)
        # CFBF (xls): D0 CF 11 E0 A1 B1 1A E1
        if magic.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
            return "xls"
        # ZIP (xlsx/xlsm): PK..
        if magic.startswith(b"PK"):
            return "xlsx"
        return ""

    def _leer_excel_robusto(self, ruta: Path) -> pd.DataFrame:
        """
        Lee .xlsx/.xlsm con openpyxl y .xls con xlrd==1.2.0.
        Si la extensión no coincide con la firma, intenta el engine correcto igual.
        """
        ruta = Path(ruta)
        firma = self._detectar_formato_excel(ruta)
        suf = ruta.suffix.lower()

        # Prioridad por firma (más confiable que la extensión)
        if firma == "xlsx":
            return pd.read_excel(ruta, engine="openpyxl")
        if firma == "xls":
            # Requiere xlrd==1.2.0
            return pd.read_excel(ruta, engine="xlrd")

        # Fallback por extensión si no pudimos detectar
        if suf in (".xlsx", ".xlsm"):
            return pd.read_excel(ruta, engine="openpyxl")
        if suf == ".xls":
            return pd.read_excel(ruta, engine="xlrd")

        # Último intento sin engine (por si pandas lo resuelve solo)
        return pd.read_excel(ruta)

# ============================================================
# 21- Carga de referencia de jugadores
# ============================================================ 

    def cargar_db_jugadores(self, ruta_excel: Path) -> int:
        try:
            # Leer el archivo Excel
            df = pd.read_excel(ruta_excel)
            print("[DEBUG] Columnas originales:", df.columns.tolist())

            # Mapeo directo de columnas (sin normalización agresiva)
            mapeo_columnas = {
                'ID_Jugador': 'id_jugador',
                'Nombre': 'Nombre',
                'Edad': 'Edad',
                'Posicion': 'Posicion',
                'Línea': 'Linea',
                'Peso_kg': 'Peso_kg',
                'Estatura_cm': 'Estatura_cm',
                'Foto_URL': 'Foto_URL',
                'Carnet_URL': 'Carnet_URL'
            }

            # Renombrar columnas
            df = df.rename(columns={c: mapeo_columnas[c] for c in df.columns if c in mapeo_columnas})
            print("[DEBUG] Columnas después de renombrar:", df.columns.tolist())

            # Columnas requeridas
            columnas_requeridas = ["id_jugador", "Nombre", "Posicion", "Linea"]
            for c in columnas_requeridas:
                if c not in df.columns:
                    raise ValueError(f"Falta columna requerida: {c}")

            # Asegurar tipos de datos
            df["id_jugador"] = pd.to_numeric(df["id_jugador"], errors="coerce")
            df = df.dropna(subset=["id_jugador"])
            df = df.drop_duplicates(subset=["id_jugador"], keep="last")

            # Preparar datos para SQL (manejar valores nulos)
            columnas_sql = ["id_jugador", "Nombre", "Edad", "Posicion", "Linea", 
                        "Peso_kg", "Estatura_cm", "Foto_URL", "Carnet_URL"]
            
            # Asegurar que todas las columnas existan
            for c in columnas_sql:
                if c not in df.columns:
                    df[c] = None

            registros = df[columnas_sql].where(pd.notnull(df[columnas_sql]), None).values.tolist()
            print(f"[DEBUG] Preparados {len(registros)} registros para insertar")

            # Ejecutar UPSERT
            sql = """
            INSERT INTO DB_Jugadores
                (id_jugador, Nombre, Edad, Posicion, Linea, Peso_kg, Estatura_cm, Foto_URL, Carnet_URL)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id_jugador) DO UPDATE SET
                Nombre=excluded.Nombre,
                Edad=excluded.Edad,
                Posicion=excluded.Posicion,
                Linea=excluded.Linea,
                Peso_kg=excluded.Peso_kg,
                Estatura_cm=excluded.Estatura_cm,
                Foto_URL=excluded.Foto_URL,
                Carnet_URL=excluded.Carnet_URL;
            """

            with self._conectar() as conn:
                conn.executemany(sql, registros)
                print(f"[SUCCESS] Insertados/actualizados {len(registros)} jugadores")
                
            return len(registros)

        except Exception as e:
            print(f"[ERROR] Fallo al cargar jugadores: {str(e)}")
            traceback.print_exc()
            return 0

# en pipeline.py (o helpers.py)



