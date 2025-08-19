"""
===========================================================
 Proyecto Chivas - pipeline.py
 Autor: Nicolás Di Bartolo
 Descripción:
     Script principal para procesar los Excel del PF, 
     limpiar datos, separarlos entre entrenamientos y partidos,
     calcular métricas (CE, CS, CR, Rendimiento) y cargarlos
     en el Data Warehouse (SQLite).

 Índice de secciones
 ----------------------------------------------------------
 1. Utilidades base
    - _conectar: abre conexión a la DB
    - _asegurar_indices: crea índices únicos y foráneos

 2. Normalización de datos
    - _normalizar_fecha: limpia fechas con formato mixto
    - MAPEO_COLUMNAS_POR_DEFECTO: renombra columnas
    - COLUMNAS_NUMERICAS: asegura que sean numéricas

 3. Calendario de partidos
    - _cargar_calendario_partidos: lee Excel con fechas + rivales
    - dividir_por_calendario: separa entrenamientos y partidos
      según calendario cargado

 4. Cálculos de métricas
    - _calcular_ce_cs_cr: calcula CE, CS, CR
    - _calcular_rendimiento_total: calcula rendimiento ponderado
      por posición del jugador

 5. Procesamiento de datos
    - upsert_entrenamientos: inserta/actualiza entrenos en DB
    - upsert_partidos: inserta/actualiza partidos en DB
    - recalcular_rendimiento_semanal: resume cargas semanales

 6. Flujo principal
    - procesar_excel: procesa un Excel completo y actualiza DB
    - cargar_jugadores: carga info de jugadores en DB_Jugadores

 Notas:
    - Los Excel originales se dejan en /data/raw
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
import fuzzywuzzy



# ============================================================
# Configuración y mapeos de columnas
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
# Clase principal del ETL
# ============================================================

@dataclass
class ETLChivas:
    ruta_sqlite: Path
    calendario_partidos_xlsx: Optional[Path] = None
    mapeo_columnas: Optional[Dict[str, str]] = None

    def __post_init__(self):
        self.ruta_sqlite = Path(self.ruta_sqlite)
        self.mapeo_columnas = {**MAPEO_COLUMNAS_POR_DEFECTO, **(self.mapeo_columnas or {})}
        self._fechas_partidos = set()
        if self.calendario_partidos_xlsx:
            self.cargar_calendario_partidos(self.calendario_partidos_xlsx)
        self._asegurar_indices()


    # ------------------ Rendimiento Total (0–100) ------------------

    # Ponderaciones por posición (ajustables):
    _PESOS_POR_POSICION = {
        #        CE,   CS,   CR
        "Arquero":   (0.45, 0.35, 0.20),
        "Defensa":   (0.40, 0.45, 0.15),
        "Medio":     (0.45, 0.40, 0.15),
        "Delantera": (0.55, 0.35, 0.10),
        # fallback si no hay posición
        "_default":  (0.45, 0.40, 0.15),
    }

    
    # --------------------------------------------------------
    # Utilidades base
    # --------------------------------------------------------

    def _conectar(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.ruta_sqlite)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _asegurar_indices(self):
        with self._conectar() as conn:
            conn.executescript("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_ent_jugador_fecha
                ON DB_Entrenamientos(id_jugador, Fecha);

            CREATE UNIQUE INDEX IF NOT EXISTS uq_part_jugador_fecha_rival
                ON DB_Partidos(id_jugador, Fecha, ifnull(id_rival,-1));

            CREATE UNIQUE INDEX IF NOT EXISTS uq_rend_semanal_jugador_fecha
                ON Rendimiento_Semanal(id_jugador, Fecha);

            CREATE INDEX IF NOT EXISTS idx_rend_semanal_jugador_fecha
                ON Rendimiento_Semanal(id_jugador, Fecha);
            """)

    
    def transformar_archivo(self, ruta: Path) -> pd.DataFrame:  # Añade 'self' como primer parámetro
        """Convierte formatos problemáticos antes del ETL"""
        df = pd.read_excel(ruta)
        
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

    def _fabricar_alias_desde_db(self) -> dict[str, int]:
        """
        Construye {alias_norm -> id_jugador} a partir de DB_Jugadores.
        Alias cubiertos:
        - apellido ("brizuela")
        - nombre apellido ("isaac brizuela")
        - inicial + apellido ("i brizuela", "ibrizuela")
        - nombre de pila único ("bryan", si no hay dos Bryan)
        - variantes sin acentos, minúsculas, colapsando espacios
        Alias ambiguos (colisionan entre jugadores) se descartan.
        """
        with self._conectar() as conn:
            ref = pd.read_sql("SELECT id_jugador, Nombre FROM DB_Jugadores", conn)

        # conteo de nombres de pila para saber si son únicos
        def norm(s): return self._norm_texto(s)
        ref["Nombre_norm"] = ref["Nombre"].astype(str).map(norm)
        ref["Nombre_pila"] = ref["Nombre_norm"].str.split().str[0].fillna("")
        counts_pila = ref["Nombre_pila"].value_counts()

        alias2id, colisiones = {}, set()

        def add_alias(k, vid):
            if not k:
                return
            if k in alias2id and alias2id[k] != vid:
                colisiones.add(k)
            else:
                alias2id[k] = vid

        for _, row in ref.iterrows():
            jid = int(row["id_jugador"])
            base = row["Nombre_norm"]              # "isaac brizuela"
            if not base: 
                continue
            toks = base.split()
            nombre_pila = toks[0] if toks else ""
            apellido = toks[-1] if toks else ""

            # variantes principales
            cand = {
                base,                               # "isaac brizuela"
                apellido,                           # "brizuela"
                (nombre_pila[:1] + " " + apellido) if nombre_pila else "",  # "i brizuela"
                (nombre_pila[:1] + apellido) if nombre_pila else "",        # "ibrizuela"
                apellido.replace(" ", ""),          # por si apellidos compuestos
            }

            # nombre de pila único (p.ej. "bryan") -> solo si no es ambiguo
            if nombre_pila and counts_pila.get(nombre_pila, 0) == 1:
                cand.add(nombre_pila)

            for a in cand:
                a = a.strip()
                if a:
                    add_alias(a, jid)

        # eliminar los alias ambiguos
        for k in colisiones:
            alias2id.pop(k, None)

        return alias2id


    def _resolver_id_por_alias_heuristico(self, serie_nombres: pd.Series) -> pd.Series:
        """
        Resuelve id_jugador usando los alias fabricados.
        Cubre casos como 'ALVARADO', 'Brizuela', 'D Aguirre', 'GSepulveda', 'Efrain', 'Bryan', 'Govea'.
        """
        if serie_nombres is None or serie_nombres.empty:
            return pd.Series(pd.NA, index=(serie_nombres.index if serie_nombres is not None else []), dtype="Int64")

        alias2id = self._fabricar_alias_desde_db()
        if not alias2id:
            return pd.Series(pd.NA, index=serie_nombres.index, dtype="Int64")

        def resolver(raw):
            n = self._norm_texto(raw)                # lower, sin acentos, colapsa espacios
            if not n:
                return pd.NA

            # 1) match directo
            if n in alias2id:
                return alias2id[n]

            # 2) "gsepulveda" -> "g sepulveda"
            if len(n) >= 2 and n[0].isalpha():
                n2 = n[0] + " " + n[1:]
                if n2 in alias2id:
                    return alias2id[n2]

            # 3) si viene "d. aguirre" u otras con puntos
            n3 = n.replace(".", " ")
            n3 = re.sub(r"\s+", " ", n3).strip()
            if n3 in alias2id:
                return alias2id[n3]

            return pd.NA

        out = serie_nombres.astype(str).map(resolver).astype("Int64")
        return out



    @staticmethod
    def _norm_texto(s: str) -> str:
        if s is None:
            return ""
        s = str(s).strip().lower()
        # remover acentos
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        # colapsar espacios múltiples y espacios raros (\xa0)
        s = s.replace("\xa0", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

        
    def _obtener_o_crear_id_rival(self, nombre_rival: str) -> Optional[int]:
        if not nombre_rival or str(nombre_rival).strip() == "":
            return None
        nombre_rival = str(nombre_rival).strip()
        with self._conectar() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id_rival FROM DB_Rivales WHERE Nombre = ?", (nombre_rival,))
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute("INSERT INTO DB_Rivales (Nombre) VALUES (?)", (nombre_rival,))
            conn.commit()
            return cur.lastrowid


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
        # Cargar aliases con manejo de mayúsculas/espacios
        alias_path = Path("data/ref/aliases_jugadores.csv")
        aliases = pd.read_csv(alias_path)
        
        # Crear mapeo normalizado
        alias_dict = {}
        for _, row in aliases.iterrows():
            # Todas las variantes posibles
            nombres = [
                str(row['Nombre_Fuente']).strip(),
                str(row['Nombre_Fuente']).strip().upper(),
                str(row['Nombre_Fuente']).strip().title()
            ]
            for nombre in nombres:
                alias_dict[self._norm_texto(nombre)] = row['id_jugador']
        
        # Aplicar aliases
        df['nombre_norm'] = df['Nombre'].apply(self._norm_texto)
        df['id_jugador'] = df['id_jugador'].fillna(df['nombre_norm'].map(alias_dict))
        
        return df.drop(columns=['nombre_norm'], errors='ignore')
        
    def _buscar_jugadores_similares(self, nombre: str, umbral=0.7) -> list[dict]:
        """
        Busca jugadores en la DB con nombres similares al proporcionado usando fuzzy matching.
        
        Args:
            nombre: Nombre a buscar
            umbral: Similaridad mínima (0-1)
            
        Returns:
            Lista de diccionarios con id_jugador y Nombre de jugadores similares
        """
        from fuzzywuzzy import fuzz  # Necesitarás instalar: pip install fuzzywuzzy python-Levenshtein
        
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
        
        # Reportar no mapeados
        no_map = df[df['id_jugador'].isna()]['Nombre'].unique()
        if len(no_map) > 0:
            print(f"[WARN] No mapeados ({len(no_map)}): {no_map[:10]}...")
        
        return df.drop(columns=['nombre_norm'], errors='ignore')


    def _normalizar_txt(self, s: str) -> str:
        if s is None:
            return ""
        s = str(s).strip().lower()
        # remover acentos
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        # limpiar espacios múltiples
        s = re.sub(r"\s+", " ", s)
        return s

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

    def _derivar_rival_y_local(self, serie_sessions: pd.Series) -> pd.DataFrame:
        """
        A partir de 'Sessions' (p.ej. 'Guadalajara vs America' o 'Tigres vs Guadalajara'),
        devuelve un DataFrame con columnas: Rival, Local_Visitante.
        """
        rivales = []
        lv = []
        # patrón tolerant a: vs, VS, vs., con espacios
        patron = re.compile(r"^\s*(.+?)\s+vs\.?\s+(.+?)\s*$", flags=re.IGNORECASE)

        for txt in serie_sessions.fillna(""):
            txt_clean = str(txt).strip()
            rival, local_visit = None, None

            m = patron.match(txt_clean)
            if m:
                eq_izq = m.group(1).strip()
                eq_der = m.group(2).strip()
                if self._es_chivas(eq_izq):
                    # Chivas a la izquierda → Local
                    rival = eq_der
                    local_visit = "Local"
                elif self._es_chivas(eq_der):
                    # Chivas a la derecha → Visitante
                    rival = eq_izq
                    local_visit = "Visitante"
                else:
                    # Ninguno parece Chivas → desconocido pero guardamos algo útil
                    rival = eq_der  # por convención, tomamos el de la derecha
                    local_visit = "Desconocido"
            else:
                # No matchea el patrón → dejar desconocido
                rival = None
                local_visit = "Desconocido"

            rivales.append(rival)
            lv.append(local_visit)

        return pd.DataFrame({"Rival_from_sess": rivales, "Local_Visitante_from_sess": lv})


    # --------------------------------------------------------
    # 1) Normalización de fechas
    # --------------------------------------------------------

    
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



    # --------------------------------------------------------
    # 2) Calendario de partidos
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # 3) Normalización / Cálculos
    # --------------------------------------------------------

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

    def _a_numerico(self, df: pd.DataFrame) -> pd.DataFrame:
        # Reemplazo coma decimal por punto cuando vengan strings + strip
        for c in COLUMNAS_NUMERICAS:
            if c in df.columns and df[c].dtype == object:
                try:
                    df[c] = df[c].astype(str).str.strip().str.replace(",", ".", regex=False)
                except Exception:
                    pass
        for c in COLUMNAS_NUMERICAS:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df



    # --------------------------------------------------------
    # 3.a) Cálculos Jugadores
    # --------------------------------------------------------

    
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
        # Normaliza por percentiles y recorta a [0, 100]
        denom = max(p90 - p10, 1e-6)
        z = (x.fillna(0) - p10) / denom
        return (z.clip(0, 1) * 100)

    
        
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


    # --------------------------------------------------------
    # 4) Separación entrenamientos vs. partidos
    # --------------------------------------------------------

    def dividir_por_calendario(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Devuelve (entrenamientos, partidos) usando el calendario de partidos.
        Si no hay calendario, usa heurística (si hay Rival/Minutos_jugados = partido).
        """
        df = self._renombrar_columnas(df) 
        df = self._asegurar_id_jugador(df)

        if "Fecha" not in df.columns:
            raise ValueError("No se encuentra la columna 'Fecha' en el Excel.")
        df["Fecha"] = self.normalizar_fechas(df["Fecha"])
        df = self._a_numerico(df)
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

        # Enriquecer PARTIDOS con Rival / Local_Visitante / id_rival del calendario (si existe)
        if hasattr(self, "_calendario_partidos_df") and not partidos.empty: 
            partidos = partidos.merge(
                self._calendario_partidos_df,
                on="Fecha",
                how="left",
                suffixes=("", "_cal")
            )

            # Si el Excel del PF NO trae Rival/Local_Visitante, completar con el calendario
            if "Rival" not in partidos.columns and "Rival_cal" in partidos.columns:
                partidos["Rival"] = partidos["Rival_cal"]
            else:
                # si viene en ambos, dar prioridad al dato del PF y completar vacíos desde el cal
                if "Rival" in partidos.columns and "Rival_cal" in partidos.columns:
                    partidos["Rival"] = partidos["Rival"].fillna(partidos["Rival_cal"])

            if "Local_Visitante" not in partidos.columns and "Local_Visitante_cal" in partidos.columns:
                partidos["Local_Visitante"] = partidos["Local_Visitante_cal"]
            else:
                if "Local_Visitante" in partidos.columns and "Local_Visitante_cal" in partidos.columns:
                    partidos["Local_Visitante"] = partidos["Local_Visitante"].fillna(partidos["Local_Visitante_cal"])

            # id_rival: si ya vino, respetar; si no, tomar el del calendario (si existe)
            if "id_rival" not in partidos.columns and "id_rival_cal" in partidos.columns:
                partidos["id_rival"] = partidos["id_rival_cal"]
            else:
                if "id_rival" in partidos.columns and "id_rival_cal" in partidos.columns:
                    partidos["id_rival"] = partidos["id_rival"].fillna(partidos["id_rival_cal"])

            # limpiar columnas *_cal auxiliares
            drop_cols = [c for c in partidos.columns if c.endswith("_cal")]
            if drop_cols:
                partidos = partidos.drop(columns=drop_cols)

        return entrenos, partidos


    # --------------------------------------------------------
    # 5) UPSERTs (cargas en DB)
    # --------------------------------------------------------

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
            "id_jugador", "Fecha", "Dia_Semana", "Tipo_Dia", "Distancia_total",
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

        return len(df)


    def upsert_partidos(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        for c in ["id_jugador", "Fecha"]:
            if c not in df.columns:
                raise ValueError(f"Falta columna obligatoria '{c}' para DB_Partidos.")

        columnas = [
            "id_jugador", "Fecha", "id_rival", "Rival", "Local_Visitante", "Distancia_total",
            "HSR_abs_m", "HSR_rel_m", "HMLD_m",
            "Sprints_distancia_m", "Sprints_cantidad", "Sprints_vel_max_kmh",
            "Velocidad_prom_m_min",
            "Acc_3", "Dec_3", "Player_Load", "Duracion_min",
            "Carga_Explosiva", "Carga_Sostenida", "Carga_Regenerativa", "Rendimiento_Partido",
        ]
        for c in columnas:
            if c not in df.columns:
                df[c] = None

        # dentro de upsert_partidos, justo después del for que completa columnas faltantes:
        df["Local_Visitante"] = df["Local_Visitante"].fillna("Desconocido")
                
        # tipos consistentes
        df["id_rival"] = pd.to_numeric(df["id_rival"], errors="coerce").astype("Int64")


        with self._conectar() as conn:
            placeholders = ",".join(["?"] * len(columnas))
            set_clause = ",".join([f"{c}=excluded.{c}" for c in columnas if c != "id_partido"])
            sql = f"""
            INSERT INTO DB_Partidos ({",".join(columnas)})
            VALUES ({placeholders})
            ON CONFLICT(id_jugador, Fecha, ifnull(id_rival,-1))
            DO UPDATE SET {set_clause};
            """
            data = df[columnas].where(pd.notnull(df[columnas]), None).values.tolist()
            conn.executemany(sql, data)
        return len(df)

    # --------------------------------------------------------
    # 6) Rendimiento semanal (agregado)
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 7) Procesamiento de archivos
    # --------------------------------------------------------

    def procesar_excel(self, ruta_xlsx: Path) -> dict:
        """
        Procesa un archivo Excel del preparador físico, carga los entrenamientos en la base de datos
        y recalcula las métricas semanales. Versión mejorada con:
        - Mejor manejo de fechas
        - Registro detallado de errores
        - Conservación de datos originales para diagnóstico
        
        Args:
            ruta_xlsx: Ruta al archivo Excel a procesar
            
        Returns:
            dict: Conteo de registros procesados {
                'entrenamientos': int, 
                'partidos': int, 
                'filas_rendimiento_semanal': int
            }
        """
        resultado = {'entrenamientos': 0, 'partidos': 0, 'filas_rendimiento_semanal': 0}
        
        try:
            print(f"\n[INFO] Procesando archivo: {ruta_xlsx.name}")

            # Usar transformación previa
            df = self.transformar_archivo(ruta_xlsx)
            
            # 1. Cargar archivo conservando datos originales
            df = pd.read_excel(ruta_xlsx)
            print(f"[DEBUG] Filas leídas: {len(df)}")
            print("[DEBUG] Columnas originales:", df.columns.tolist())
            
            # Guardar copia de columna de fecha original para diagnóstico
            col_fecha_original = next((c for c in df.columns 
                                    if str(c).lower() in ['days', 'date', 'fecha', 'día']), None)
            if col_fecha_original:
                df['_fecha_original_'] = df[col_fecha_original]
            
            # 2. Renombrar columnas
            df = self._renombrar_columnas(df)
            print("[DEBUG] Columnas después de renombrar:", df.columns.tolist())
            
            # 3. Validar estructura básica
            columnas_requeridas = {'Fecha', 'Nombre', 'Distancia_total'}
            faltantes = columnas_requeridas - set(df.columns)
            if faltantes:
                raise ValueError(f"Faltan columnas requeridas: {faltantes}")
            
            # 4. Normalización de fechas con registro detallado
            df["Fecha"] = self.normalizar_fechas(df["Fecha"])
            
            # Reporte de fechas problemáticas
            if df["Fecha"].isnull().any():
                n_fechas_nulas = df["Fecha"].isnull().sum()
                print(f"[WARN] {n_fechas_nulas} registros con fechas no reconocidas")
                
                if '_fecha_original_' in df.columns:
                    ejemplos = df[df["Fecha"].isnull()]['_fecha_original_'].dropna().unique()[:5]
                    print("Ejemplos de valores no parseados:", ejemplos)
            
            # 5. Asignación de IDs de jugadores
            print("\n[DEBUG] Proceso de asignación de IDs:")
            df = self._asegurar_id_jugador(df)
            
            # 6. Separar entrenamientos y partidos
            entrenos, partidos = self.dividir_por_calendario(df)
            print(f"[DEBUG] Entrenamientos detectados: {len(entrenos)}")
            print(f"[DEBUG] Partidos detectados: {len(partidos)}")
            
            # 7. Procesar entrenamientos
            if not entrenos.empty:
                # Calcular métricas
                entrenos = self._calcular_ce_cs_cr(entrenos)
                entrenos = self._calcular_rendimiento_total(entrenos, "Rendimiento_Diario")
                
                # Log de muestra
                print("[DEBUG] Muestra de entrenamientos pre-upsert:")
                print(entrenos[['Nombre', 'id_jugador', 'Fecha', 'Carga_Explosiva', 'Carga_Sostenida']].head(3))
                
                # UPSERT a la base de datos
                n_entrenos = self.upsert_entrenamientos(entrenos)
                resultado['entrenamientos'] = n_entrenos
            
            # 8. Procesar partidos (Opción A: solo log)
            if not partidos.empty:
                print(f"[INFO] Se detectaron {len(partidos)} filas como partidos (serán ignoradas en Opción A)")
            
            # 9. Recalcular rendimiento semanal
            if not entrenos.empty:
                ids_jugadores = entrenos['id_jugador'].dropna().unique().tolist()
                n_semanal = self.recalcular_rendimiento_semanal(ids_jugadores)
                resultado['filas_rendimiento_semanal'] = n_semanal
                print(f"[DEBUG] Recalculado rendimiento semanal para {len(ids_jugadores)} jugadores")
            
            # 10. Guardar registros problemáticos para análisis
            if '_fecha_original_' in df.columns and df["Fecha"].isnull().any():
                problematicos = df[df["Fecha"].isnull()].copy()
                ruta_problem = ruta_xlsx.parent / f"problemas_{ruta_xlsx.stem}.csv"
                problematicos.to_csv(ruta_problem, index=False)
                print(f"[INFO] Registros problemáticos guardados en {ruta_problem}")
            
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
        """Procesa todos los .xlsx válidos de una carpeta en una sola pasada."""
        carpeta_raw = Path(carpeta_raw)
        totales = {"entrenamientos": 0, "partidos": 0, "filas_rendimiento_semanal": 0}

        for archivo in sorted(carpeta_raw.glob("*.xlsx")):
            # Ignorar lock files de Excel (empiezan con ~$)
            if archivo.name.startswith("~$"):
                print(f"[INFO] Ignorado lock file: {archivo.name}")
                continue
            try:
                res = self.procesar_excel(archivo)
                for k, v in res.items():
                    totales[k] += v
            except PermissionError as e:
                print(f"[WARN] No se pudo abrir {archivo.name} (bloqueado). Detalle: {e}")
            except Exception as e:
                print(f"[ERROR] Falló {archivo.name}: {e}")
        return totales

    
    def cargar_partidos_desde_master(self, ruta_excel: Path) -> int:
        """
        Versión mejorada que:
        1. Detecta y elimina duplicados
        2. Valida fechas correctas
        3. Normaliza nombres de rivales
        4. Completa métricas faltantes
        """
        try:
            # 1. Cargar archivo con validación de duplicados
            df = self._leer_y_validar_excel(ruta_excel)
            
            # 2. Normalizar columnas
            df = self._normalizar_columnas_partidos(df)
            
            # 3. Validar fechas (eliminar años imposibles)
            df = self._filtrar_fechas_validas(df)
            
            # 4. Asignar IDs de jugadores
            df = self._asignar_ids_jugadores(df)
            
            # 5. Normalizar rivales y asignar IDs
            df = self._normalizar_rivales(df)
            
            # 6. Completar métricas base
            df = self._completar_metricas_base(df)
            
            # 7. Filtrar y cargar datos válidos
            return self._cargar_datos_validos(df)
            
        except Exception as e:
            print(f"[ERROR] Fallo al cargar partidos: {str(e)}")
            traceback.print_exc()
            return 0

    # Métodos auxiliares (añadir a la clase ETLChivas)

    def _leer_y_validar_excel(self, ruta: Path) -> pd.DataFrame:
        """Lee el Excel y valida estructura básica"""
        engine = "openpyxl" if ruta.suffix.lower() == ".xlsx" else None
        df = pd.read_excel(ruta, engine=engine)
        
        # Registrar datos crudos para diagnóstico
        df['_raw_data'] = df.to_dict('records')
        
        # Detectar y eliminar duplicados exactos
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

    # --------------------------------------------------------
    # 8) Cargar DB_Jugadores
    # --------------------------------------------------------

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

