import pandas as pd
import sqlite3
import joblib
import numpy as np
from pathlib import Path

def predecir_riesgo(df_actual=None):
    """
    Ejecuta el modelo de ML de sobrecarga sobre el dataset recibido o,
    si no se provee ninguno, lee directamente desde la base SQLite.
    Devuelve un DataFrame con:
        ['id_jugador', 'Fecha', 'riesgo_pred', 'prob_riesgo', 'nivel_riesgo']
    """

    # =============================================
    # 🔹 Rutas de modelo y base
    # =============================================
    
    model_path = Path('C:/Users/Nico/Desktop/DATA SCIENCE/PP- VOLUNTAREADO/chivas-ml/src/chivas_ml/ml/registry/modelo_predecir_riesgo/model_overload_rf.pkl')
    scaler_path = Path('C:/Users/Nico/Desktop/DATA SCIENCE/PP- VOLUNTAREADO/chivas-ml/src/chivas_ml/ml/registry/modelo_predecir_riesgo/scaler_overload.pkl')
    db_path = Path('C:/Users/Nico/Desktop/DATA SCIENCE/PP- VOLUNTAREADO/chivas-ml/data/external/chivas_dw.sqlite')


    if not model_path.exists() or not scaler_path.exists():
        raise FileNotFoundError("No se encontraron los archivos del modelo o el escalador.")

    # =============================================
    # 🔹 Cargar modelo y escalador
    # =============================================
    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)

    # =============================================
    # 🔹 Leer datos si no se pasa un DataFrame
    # =============================================
    if df_actual is None:
        if not db_path.exists():
            raise FileNotFoundError("No se encontró la base de datos.")
        conn = sqlite3.connect(db_path)
        query = "SELECT * FROM vw_cargas_rolling_7d_full"
        df_actual = pd.read_sql_query(query, conn)
        conn.close()

    # =============================================
    # 🔹 Preprocesamiento igual al del entrenamiento
    # =============================================
    features = [
        'CE_7d',
        'CT_7d',
        'CT_28d_avg',
        'ACWR_7d_real',
        'retorno_actividad',
        'dias_sin_entrenar',
        'partidos_7d',
        'minutos_7d',
        'jugo_partido_7d'
    ]

    df_pred = df_actual[features].dropna().copy()

    # Escalar
    X_scaled = scaler.transform(df_pred)

    # Predicciones
    y_pred = model.predict(X_scaled)
    y_prob = model.predict_proba(X_scaled)[:, 1]

    # Inicializar columnas vacías
    df_actual['riesgo_pred'] = np.nan
    df_actual['prob_riesgo'] = np.nan
    df_actual['nivel_riesgo'] = np.nan

    mask_validas = df_actual[features].notna().all(axis=1)

    df_actual.loc[mask_validas, 'riesgo_pred'] = y_pred
    df_actual.loc[mask_validas, 'prob_riesgo'] = y_prob

    # 💧 Suavizado temporal de probabilidades
    df_actual = df_actual.sort_values(['id_jugador', 'Fecha'])

    # 🔹 Suavizado operativo (3 días)
    df_actual['prob_riesgo_suavizado_3d'] = (
        df_actual.groupby('id_jugador')['prob_riesgo']
        .transform(lambda x: x.shift(1).rolling(window=2, min_periods=1).mean())
    )

    # 🔹 Suavizado estratégico (5 días)
    df_actual['prob_riesgo_suavizado_5d'] = (
        df_actual.groupby('id_jugador')['prob_riesgo']
        .transform(lambda x: x.shift(1).rolling(window=4, min_periods=1).mean())
    )



    # Definir niveles con la probabilidad suavizada
    # Definir condiciones SOLO sobre las filas válidas
    df_validas = df_actual.loc[mask_validas].copy()

    condiciones_validas = [
        (df_validas['prob_riesgo_suavizado_3d'] < 0.45),
        (df_validas['prob_riesgo_suavizado_3d'] >= 0.45) & (df_validas['prob_riesgo_suavizado_3d'] < 0.75),
        (df_validas['prob_riesgo_suavizado_3d'] >= 0.75)
    ]
    categorias = ['Bajo', 'Medio', 'Alto']

    niveles = np.select(condiciones_validas, categorias, default="")

    df_actual.loc[mask_validas, 'nivel_riesgo'] = niveles

    # =============================================
    # 🔹 Tabla de salida
    # =============================================
    df_salida = df_actual[['id_jugador', 'Fecha',
                           'prob_riesgo', 'prob_riesgo_suavizado_3d', 'prob_riesgo_suavizado_5d',
                           'riesgo_pred', 'nivel_riesgo']]

    print("✅ Predicciones de riesgo suavizadas y generadas correctamente.")
    return df_salida


# ==========================================================
# Si se ejecuta directamente, guarda la tabla
# ==========================================================
if __name__ == "__main__":
    print("[INFO] Ejecutando modelo de riesgo de sobrecarga (suavizado)...")
    df_pred = predecir_riesgo()
    conn = sqlite3.connect('data/external/chivas_dw.sqlite')
    df_pred.to_sql('ML_Sugeridos_Sobrecarga', conn, if_exists='replace', index=False)
    conn.close()
    print("✅ Tabla 'ML_Sugeridos_Sobrecarga' actualizada correctamente.")






    