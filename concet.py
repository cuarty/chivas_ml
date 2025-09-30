import pandas as pd
import sqlite3

# Cargar Excel
df = pd.read_excel("C:/Users/Nico/Desktop/DATA SCIENCE/PP- VOLUNTAREADO/chivas-ml/data/ref/Microciclos.xlsx")

# Conexión a la base
conn = sqlite3.connect("C:/Users/Nico/Desktop/DATA SCIENCE/PP- VOLUNTAREADO/chivas-ml/data/external/chivas_dw.sqlite")

# Guardar como nueva tabla
df.to_sql("DB_MicrociclosExcel", conn, if_exists="replace", index=False)

conn.close()
