
# ================================================
# 🔮 PREDICCIÓN DEL PRÓXIMO MICROCICLO (FINAL)
# ================================================
import os
import sqlite3
import pandas as pd
import joblib

# ================================================
# 📁 Configuración de rutas
# ================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = r"C:/Users/Nico/Desktop/DATA SCIENCE/PP- VOLUNTAREADO/chivas-ml/data/external/chivas_dw.sqlite"
REGISTRY_DIR = os.path.join(BASE_DIR, "ml", "registry")

# ================================================
# 🧩 Carga de modelos
# ================================================
def load_model(path_folder, model_name, scaler_name):
    model = joblib.load(os.path.join(path_folder, model_name))
    scaler = joblib.load(os.path.join(path_folder, scaler_name))
    return model, scaler


# ================================================
# 🔍 Último microciclo completo
# ================================================
def obtener_microciclo_completo(conn):
    query = """
        SELECT Microciclo_Num, COUNT(DISTINCT Fecha) AS dias
        FROM BI_Cargas_Diarias
        GROUP BY Microciclo_Num
        ORDER BY Microciclo_Num DESC
    """
    df = pd.read_sql(query, conn)
    completos = df[df["dias"] >= 6]
    return completos["Microciclo_Num"].max()


# ================================================
# 🧱 Preparar datos de entrada
# ================================================
def preparar_datos(conn):
    microciclo_actual = obtener_microciclo_completo(conn)
    print(f"📆 Último microciclo completo detectado: {microciclo_actual}")

    query = f"""
        SELECT *
        FROM vw_predicciones_diarias_extendida
        WHERE Microciclo_actual = {microciclo_actual}
    """
    df = pd.read_sql(query, conn)
    print(f"[OK] Datos cargados desde vw_predicciones_diarias_extendida ({len(df)} filas).")
    print("Columnas disponibles:", list(df.columns))

    # ============================================
    # 🧠 CONTEXTO FISIOLÓGICO: jugador + posición
    # ============================================
    df['Posicion'] = df['Posicion'].fillna('Desconocido')
    df['Linea'] = df['Linea'].fillna('Desconocido')

    player_mean = df.groupby('id_jugador').agg({
        'Distancia_total': 'mean',
        'Player_Load': 'mean',
        'Acc_3': 'mean',
        'Dec_3': 'mean'
    }).rename(columns={
        'Distancia_total': 'jugador_mean_dist',
        'Player_Load': 'jugador_mean_load',
        'Acc_3': 'jugador_mean_acc',
        'Dec_3': 'jugador_mean_dec'
    }).reset_index()

    df = df.merge(player_mean, on='id_jugador', how='left')
    df = pd.get_dummies(df, columns=['Posicion', 'Linea'], prefix=['Pos', 'Lin'])

    print("✅ Nuevas columnas añadidas:",
          [c for c in df.columns if c.startswith('Pos_') or c.startswith('Lin_')])

    return df, microciclo_actual


# ================================================
# 🧮 Pipeline de predicción jerárquica
# ================================================
def predict_pipeline(df):
    print("🚀 Iniciando flujo de predicción jerárquica...\n")

    # 1️⃣ Predicción tipo de semana
    folder = "modelo_clas_carga_semanal"
    model, scaler = load_model(
        os.path.join(REGISTRY_DIR, folder),
        "model_weektype_rf.pkl",
        "scaler_weektype.pkl"
    )
    FEATURES_SEMANA = [
        'CT_total_actual', 'CE_total_actual', 'CS_total_actual', 'CR_total_actual',
        'entrenos_total_next', 'descansos_total_next', 'partidos_total_next',
        'descansos_pre_partido_next', 'entrenos_pre_partido_next', 'entrenos_post_partido_next'
    ]
    X_semana = df[FEATURES_SEMANA]
    df["tipo_semana_pred"] = model.predict(scaler.transform(X_semana))
    print("✅ Tipo de semana predicha")

    # 🔄 Compatibilidad con entrenamiento
    df["tipo_semana_next"] = df["tipo_semana_pred"]

    # 2️⃣ Predicción Distancia total
    folder = "modelo_clas_distancia_total"
    model, scaler = load_model(
        os.path.join(REGISTRY_DIR, folder),
        "model_carga_diaria_rf_tendencias.pkl",
        "scaler_carga_diaria.pkl"
    )
    FEATURES_DIST = [
        'tipo_semana_next', 'tipo_dia_next',
        'CT_total_actual', 'CE_total_actual', 'CS_total_actual', 'CR_total_actual',
        'riesgo_suavizado_3d_actual',
        'entrenos_total_next', 'descansos_total_next', 'partidos_total_next',
        'entrenos_pre_partido_next', 'entrenos_post_partido_next',
        'jugador_mean_dist', 'jugador_mean_load', 'jugador_mean_acc', 'jugador_mean_dec'
    ] + [c for c in df.columns if c.startswith('Pos_') or c.startswith('Lin_')]

    # 🔧 Solo tomamos las columnas que realmente existen en df
    X_dist = df[[c for c in FEATURES_DIST if c in df.columns]].copy()

    # 🧹 Eliminamos posibles duplicados o columnas que no estaban en el fit
    if 'tipo_semana_pred' in X_dist.columns:
        X_dist = X_dist.drop(columns=['tipo_semana_pred'])

    df["Distancia_total_pred"] = model.predict(scaler.transform(X_dist))
    print("✅ Distancia total predicha")


    # 3️⃣ Predicción Carga Explosiva y Sostenida
    for name, folder, m, s in [
        ("CE", "modelo_clas_CE", "model_rf_CE_tendencias.pkl", "scaler_CE.pkl"),
        ("CS", "modelo_clas_CS", "model_rf_CS_tendencias.pkl", "scaler_CS.pkl")
    ]:
        model, scaler = load_model(os.path.join(REGISTRY_DIR, folder), m, s)

        # 🧩 Compatibilidad con nombres de entrenamiento
        df["tipo_semana_next"] = df["tipo_semana_pred"]
        df["Distancia_total"] = df["Distancia_total_pred"]

        FEATURES_CARGAS = [
            'tipo_semana_next', 'tipo_dia_next', 'Distancia_total',
            'CT_total_actual', 'CE_total_actual', 'CS_total_actual', 'CR_total_actual',
            'riesgo_suavizado_3d_actual',
            'entrenos_total_next', 'descansos_total_next', 'partidos_total_next',
            'entrenos_pre_partido_next', 'entrenos_post_partido_next',
            'jugador_mean_dist', 'jugador_mean_load', 'jugador_mean_acc', 'jugador_mean_dec'
        ] + [c for c in df.columns if c.startswith('Pos_') or c.startswith('Lin_')]

        X_cargas = df[[c for c in FEATURES_CARGAS if c in df.columns]].copy()

        # 🧹 Eliminar duplicados si existen
        for col in ['tipo_semana_pred', 'Distancia_total_pred']:
            if col in X_cargas.columns:
                X_cargas = X_cargas.drop(columns=[col])

        df[f"{name}_pred"] = model.predict(scaler.transform(X_cargas))
    print("✅ Cargas CE y CS predichas")

    # 4️⃣ Predicción de métricas micro (Acc, Dec, HMLD, HSR, Sprint)
    print("🔄 Iniciando predicción de métricas micro...")

    # 🔄 Compatibilidad con nombres esperados por los modelos
    df["tipo_semana_next"] = df["tipo_semana_pred"]
    df["Distancia_total"] = df["Distancia_total_pred"]
    df["Carga_Explosiva"] = df["CE_pred"]
    df["Carga_Sostenida"] = df["CS_pred"]

    micro_models = {
        "Acc_3": ("modelo_clas_Acc_3", "model_rf_acc_3_tendencias.pkl", "scaler_acc_3.pkl"),
        "Dec_3": ("modelo_clas_Dec_3", "model_rf_dec_3_tendencias.pkl", "scaler_dec_3.pkl"),
        "HMLD_m": ("modelo_clas_hmld", "model_rf_hmld_m_tendencias.pkl", "scaler_hmld_m.pkl"),
        "HSR_abs_m": ("modelo_clas_hsr", "model_rf_hsr_abs_m_tendencias.pkl", "scaler_hsr_abs_m.pkl"),
        "Sprint_vel_max_kmh": ("modelo_clas_Sprints_vel_max_kmh", "model_rf_sprints_vel_tendencias.pkl", "scaler_sprints_vel.pkl")
    }

    for col, (folder, model_name, scaler_name) in micro_models.items():
        model, scaler = load_model(os.path.join(REGISTRY_DIR, folder), model_name, scaler_name)

        FEATURES_MICRO = [
            'tipo_semana_next', 'tipo_dia_next', 'Carga_Explosiva', 'Carga_Sostenida', 'Distancia_total',
            'CT_total_actual', 'CE_total_actual', 'CS_total_actual', 'CR_total_actual',
            'riesgo_suavizado_3d_actual',
            'entrenos_total_next', 'descansos_total_next', 'partidos_total_next',
            'entrenos_pre_partido_next', 'entrenos_post_partido_next',
            'jugador_mean_dist', 'jugador_mean_load', 'jugador_mean_acc', 'jugador_mean_dec'
        ] + [c for c in df.columns if c.startswith('Pos_') or c.startswith('Lin_')]

        X_micro = df[[c for c in FEATURES_MICRO if c in df.columns]].copy()

        # 🧹 Eliminar columnas que no estaban en el fit
        for col_drop in ['tipo_semana_pred', 'Distancia_total_pred', 'CE_pred', 'CS_pred']:
            if col_drop in X_micro.columns:
                X_micro = X_micro.drop(columns=[col_drop])

        df[f"{col}_pred"] = model.predict(scaler.transform(X_micro))

    print("✅ Métricas micro predichas correctamente")


    return df


# ================================================
# 💤 Agregar días de descanso (relleno con 0)
# ================================================
def agregar_dias_descanso(df_result, conn, microciclo_predicho):
    print("🧩 Agregando días de descanso al resultado...")

    # 1️⃣ Detectar columnas reales en la tabla
    columnas_query = "PRAGMA table_info(DB_MicrociclosExcel)"
    cols = pd.read_sql(columnas_query, conn)["name"].tolist()

    # 2️⃣ Determinar qué columna de tipo de día usar
    if "Tipo_Dia" in cols:
        col_tipo = "Tipo_Dia"
    elif "tipo_dia_next" in cols:
        col_tipo = "tipo_dia_next"
    else:
        col_tipo = None

    # 3️⃣ Construir el query dinámico
    if col_tipo:
        query_descansos = f"""
        SELECT Fecha, {col_tipo} AS Tipo_Dia, Intensidad
        FROM DB_MicrociclosExcel
        WHERE Microciclo_Num = {microciclo_predicho}
        """
    else:
        query_descansos = f"""
        SELECT Fecha, 'DESCANSO' AS Tipo_Dia, NULL AS Intensidad
        FROM DB_MicrociclosExcel
        WHERE Microciclo_Num = {microciclo_predicho}
        """

    df_plan = pd.read_sql(query_descansos, conn)

    # 4️⃣ Crear combinaciones jugador-fecha
    jugadores = df_result["id_jugador"].unique()
    dias = df_plan["Fecha"].unique()
    combinaciones = pd.MultiIndex.from_product([jugadores, dias], names=["id_jugador", "Fecha"]).to_frame(index=False)

    # 5️⃣ Combinar con plan y predicciones
    df_completo = combinaciones.merge(df_plan, on="Fecha", how="left")
    df_final = df_completo.merge(df_result, on="id_jugador", how="left")

        # 🔹 Si existe Tipo_Dia_x, lo usamos como el definitivo
    if "Tipo_Dia_x" in df_final.columns:
        df_final["Tipo_Dia"] = df_final["Tipo_Dia_x"]
        df_final.drop(columns=["Tipo_Dia_x"], inplace=True, errors="ignore")
    elif "Tipo_Dia" not in df_final.columns:
        df_final["Tipo_Dia"] = "DESCANSO"

    # 🔹 Rellenar valores faltantes en Tipo_Dia
    df_final["Tipo_Dia"] = df_final["Tipo_Dia"].fillna("DESCANSO")

    # 🔹 Rellenar métricas con 0 para días sin carga o partidos
    metricas = [
        "Distancia_total_pred", "CE_pred", "CS_pred", "Acc_3_pred",
        "Dec_3_pred", "HMLD_m_pred", "HSR_abs_m_pred", "Sprint_vel_max_kmh_pred"
    ]
    for m in metricas:
        if m in df_final.columns:
            df_final.loc[df_final["Tipo_Dia"].isin(["DESCANSO", "PARTIDO"]), m] = 0
            df_final[m] = df_final[m].fillna(0)

    # 🔹 Agregar contexto final
    df_final["microciclo_next"] = microciclo_predicho
    if "tipo_semana_pred" in df_result.columns:
        df_final["tipo_semana_pred"] = df_result["tipo_semana_pred"].iloc[0]

    print("✅ Días de descanso agregados correctamente.")
    return df_final





# ================================================
# 🧩 Ejecución principal (llamada desde pipeline)
# ================================================
def ejecutar_prediccion_microciclo():
    conn = sqlite3.connect(DB_PATH)
    df_input, microciclo_actual = preparar_datos(conn)
    print(f"🎯 Prediciendo el microciclo {microciclo_actual + 1}...\n")

    df_result = predict_pipeline(df_input)
    df_final = agregar_dias_descanso(df_result, conn, microciclo_actual + 1)
    
    # ======================================================
    # 🧹 DEPURACIÓN FINAL DE COLUMNAS (para vista Power BI)
    # ======================================================
    columnas_finales = [
        # Identificación y contexto
        "id_jugador", "Fecha_x", "microciclo_actual", "microciclo_next",

        # Planificación semanal
        "Tipo_Dia", "Intensidad", "entrenos_total_next", "descansos_total_next",
        "partidos_total_next", "descansos_pre_partido_next",
        "entrenos_pre_partido_next", "entrenos_post_partido_next",
        "tipo_semana_pred",

        # Predicciones principales
        "Distancia_total_pred", "CE_pred", "CS_pred", "Acc_3_pred",
        "Dec_3_pred", "HMLD_m_pred", "HSR_abs_m_pred", "Sprint_vel_max_kmh_pred"
    ]

    # Verificar qué columnas existen realmente en df_final
    columnas_presentes = [c for c in columnas_finales if c in df_final.columns]
    df_final = df_final[columnas_presentes].copy()

    # Eliminar duplicadas o antiguas
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]

    # Renombrar columnas para consistencia
    df_final.rename(columns={
        "Fecha_x": "Fecha",
        "tipo_semana_pred": "Tipo_Semana_Pred"
    }, inplace=True)

    print(f"🧾 Tabla final depurada con {len(df_final.columns)} columnas listas para guardar.")

    df_final.to_sql("ML_Predicciones_Semanales", conn, if_exists="replace", index=False)
    conn.close()
    print("📊 Resultados guardados correctamente (incluyendo descansos)")

    return df_final
