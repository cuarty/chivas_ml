from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

def load_jugadores_from_excel(excel_path: str | Path) -> pd.DataFrame:
    df = pd.read_excel(excel_path)
    rename_map = {
        "ID_Jugador": "id_jugador",
        "Nombre": "Nombre",
        "Edad": "Edad",
        "Posición": "Posicion",
        "Línea": "Linea",
        "Peso (kg)": "Peso_kg",
        "Estatura (cm)": "Estatura_cm",
        "Link da la Imagen": "Foto_URL",
    }
    df = df.rename(columns=rename_map)
    if "Carnet_URL" not in df.columns:
        df["Carnet_URL"] = None
    for col in ["Edad", "Peso_kg", "Estatura_cm"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def write_jugadores_to_sqlite(df: pd.DataFrame, sqlite_path: str | Path = "data/chivas_dw.sqlite"):
    engine = create_engine(f"sqlite:///{sqlite_path}")
    df.to_sql("DB_Jugadores", engine, if_exists="append", index=False)
