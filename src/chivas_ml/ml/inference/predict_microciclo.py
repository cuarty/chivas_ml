# ================================================
# 🔮 PREDICCIÓN DEL PRÓXIMO MICROCICLO (FINAL)
# ================================================
import os
import sqlite3
import pandas as pd
import joblib
import numpy as np

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
    # 1️⃣ Detectar último microciclo completo
    microciclo_actual = obtener_microciclo_completo(conn)
    microciclo_next = microciclo_actual + 1
    print(f"📆 Último microciclo completo detectado: {microciclo_actual}")

    # 2️⃣ Cargar vista base
    query = f"""
        SELECT *
        FROM vw_predicciones_diarias_extendida
        WHERE Microciclo_actual = {microciclo_actual}
    """
    df = pd.read_sql(query, conn)
    print(f"[OK] Datos cargados desde vw_predicciones_diarias_extendida ({len(df)} filas).")

    # 3️⃣ Cargar planificación del microciclo siguiente
    query_plan = f"""
        SELECT Fecha, Tipo_Dia, Intensidad, Microciclo_Num
        FROM DB_MicrociclosExcel
        WHERE Microciclo_Num = {microciclo_next}
        ORDER BY Fecha
    """
    df_plan = pd.read_sql(query_plan, conn)
    df_plan["Fecha"] = pd.to_datetime(df_plan["Fecha"]).dt.normalize()
    if df_plan.empty:
        raise ValueError(f"⚠️ No hay planificación para el microciclo {microciclo_next}")

    # 4️⃣ Mapear tipo_dia_next con la misma codificación usada en entrenamiento
    def map_tipo_dia_next(row):
        if row["Tipo_Dia"].upper() == "DESCANSO":
            return 0
        elif row["Tipo_Dia"].upper() == "ENTRENO":
            if row["Intensidad"] <= -2:
                return 1   # muy suave
            elif row["Intensidad"] == -1:
                return 2   # medio-bajo
            elif row["Intensidad"] == 1:
                return 3   # medio-alto
            elif row["Intensidad"] >= 2:
                return 4   # alta carga
            else:
                return 2
        elif row["Tipo_Dia"].upper() == "PARTIDO":
            return 0
        else:
            return 0

    df_plan["tipo_dia_next"] = df_plan.apply(map_tipo_dia_next, axis=1).astype(float)

    # 5️⃣ Contexto fisiológico del jugador
    df["Posicion"] = df["Posicion"].fillna("Desconocido")
    df["Linea"] = df["Linea"].fillna("Desconocido")

    player_mean = df.groupby("id_jugador").agg({
        "Distancia_total": "mean",
        "Player_Load": "mean",
        "Acc_3": "mean",
        "Dec_3": "mean"
    }).rename(columns={
        "Distancia_total": "jugador_mean_dist",
        "Player_Load": "jugador_mean_load",
        "Acc_3": "jugador_mean_acc",
        "Dec_3": "jugador_mean_dec"
    }).reset_index()

    df = df.merge(player_mean, on="id_jugador", how="left")
    df = pd.get_dummies(df, columns=["Posicion", "Linea"], prefix=["Pos", "Lin"])

        # 💡 Asegurar todas las columnas dummy que existían en el entrenamiento
    columnas_esperadas = [
        "Pos_Defensor", "Pos_Delantero", "Pos_Mediocampista",
        "Lin_Defensa Central", "Lin_Defensa Lateral", "Lin_Delantera",
        "Lin_Extremo", "Lin_Medio Defensivo", "Lin_Medio Ofensivo"
    ]

    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = 0.0  # agregamos columna dummy faltante

    print("✅ Columnas dummy normalizadas (todas las posiciones y líneas presentes).")


    # 🧹 Asegurar formato de fecha y eliminar duplicados
    if "Fecha" not in df.columns:
        for col in ["Fecha_x", "Fecha_y"]:
            if col in df.columns:
                df.rename(columns={col: "Fecha"}, inplace=True)
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.normalize()

    # 🧩 Eliminar duplicados de la vista base (una fila por jugador y fecha)
    numeric_cols = df.select_dtypes(include=["number"]).columns
    df = df.groupby(["id_jugador", "Fecha"], as_index=False)[numeric_cols].mean(numeric_only=True)
    print(f"🧩 Agrupado df base: {len(df)} filas únicas (1 por jugador y día).")

    # 6️⃣ Crear estructura jugador × fecha del microciclo siguiente
    jugadores = df["id_jugador"].unique()
    fechas_plan = df_plan["Fecha"].unique()
    grid = pd.MultiIndex.from_product([jugadores, fechas_plan], names=["id_jugador", "Fecha"]).to_frame(index=False)

    # 7️⃣ Combinar planificación e info de jugadores
    df_next = grid.merge(df_plan, on="Fecha", how="left")
    df_next = df_next.merge(df.drop(columns=["Fecha"]), on="id_jugador", how="left")

    # 🧹 Unificar columnas duplicadas (_x / _y)
    for col in ["Tipo_Dia", "tipo_dia_next"]:
        cols_posibles = [c for c in df_next.columns if c.startswith(col)]
        if len(cols_posibles) > 1:
            df_next[col] = df_next[cols_posibles].bfill(axis=1).iloc[:, 0]
            df_next.drop(columns=[c for c in cols_posibles if c != col], inplace=True)
        elif len(cols_posibles) == 1:
            df_next.rename(columns={cols_posibles[0]: col}, inplace=True)
        else:
            df_next[col] = None

    # 8️⃣ Asignar microciclos
    df_next["microciclo_actual"] = microciclo_actual
    df_next["microciclo_next"] = microciclo_next

    # 9️⃣ Aplicar factor de intensidad diaria a features fisiológicos
    if "tipo_dia_next" in df_next.columns:
        intensidad_factor = df_next["tipo_dia_next"]
        for col in [
            "CT_total_actual", "CE_total_actual", "CS_total_actual", "CR_total_actual",
            "jugador_mean_dist", "jugador_mean_load", "jugador_mean_acc", "jugador_mean_dec"
        ]:
            if col in df_next.columns:
                df_next[col] = df_next[col] * (1 + 0.1 * intensidad_factor)
        print("🧠 Intensidad diaria aplicada a features de entrada.")
    else:
        print("⚠️ No se aplicó ajuste de intensidad (columna tipo_dia_next ausente).")

    # 🔹 Asegurar una sola fila por jugador y fecha (por si se duplica en merges)
    df_next = df_next.groupby(["id_jugador", "Fecha"], as_index=False).first()

    # 10️⃣ Mostrar resumen de columnas clave
    print(f"Columnas finales disponibles: {list(df_next.columns)}")
    print(f"📆 Fechas únicas detectadas: {df_next['Fecha'].nunique()}")
    print(df_next[["Fecha", "Tipo_Dia", "tipo_dia_next"]].drop_duplicates().sort_values("Fecha").head(10))
    print("✅ Datos de entrada listos para el pipeline de predicción.")

    return df_next, microciclo_actual


# ================================================
# 🧮 Pipeline de predicción jerárquica
# ================================================
def predict_pipeline(df):
    print("🚀 Iniciando flujo de predicción jerárquica...\n")
    fecha_original = df["Fecha"].copy()

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
    df["tipo_semana_next"] = df["tipo_semana_pred"]
    print("✅ Tipo de semana predicha")

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

    X_dist = df[[c for c in FEATURES_DIST if c in df.columns]].copy()
    expected_cols = list(scaler.feature_names_in_)
    for c in expected_cols:
        if c not in X_dist.columns:
            X_dist[c] = 0.0
    X_dist = X_dist[expected_cols]

    df["Distancia_total_pred"] = model.predict(scaler.transform(X_dist))
    print("✅ Distancia total predicha")

    # 3️⃣ Predicción Carga Explosiva y Sostenida
    for name, folder, m, s in [
        ("CE", "modelo_clas_CE", "model_rf_CE_tendencias.pkl", "scaler_CE.pkl"),
        ("CS", "modelo_clas_CS", "model_rf_CS_tendencias.pkl", "scaler_CS.pkl")
    ]:
        model, scaler = load_model(os.path.join(REGISTRY_DIR, folder), m, s)
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
        expected_cols = list(scaler.feature_names_in_)
        for c in expected_cols:
            if c not in X_cargas.columns:
                X_cargas[c] = 0.0
        X_cargas = X_cargas[expected_cols]
        df[f"{name}_pred"] = model.predict(scaler.transform(X_cargas))
        print(f"✅ {name} predicha correctamente")

    # ⚙️ Ajuste fisiológico post-predicción
    print("\n🧠 Aplicando ajuste fisiológico por riesgo o carga alta...")
    df["ajuste_por_riesgo"] = (
        (df["CT_total_actual"] > 850) | (df["riesgo_suavizado_3d_actual"] > 0.7)
    ).astype(int)

    for var, factor in {
        "Distancia_total_pred": 0.95,
        "CE_pred": 0.9,
        "CS_pred": 0.9,
    }.items():
        df[var] = np.where(df["ajuste_por_riesgo"] == 1, df[var] * factor, df[var])

    print("✅ Ajuste fisiológico aplicado correctamente.")

    # ⚖️ Ajuste de continuidad semanal por jugador (máx +10% sobre su promedio actual)
    print("\n⚖️ Aplicando ajuste de continuidad fisiológica por jugador...")
    for var in ["Distancia_total_pred", "CE_pred", "CS_pred"]:
        base_var = var.replace("_pred", "_actual")
        if base_var in df.columns:
            limites = (
                df.groupby("id_jugador")[base_var]
                .transform(lambda x: x.mean() * 1.10)
            )
            df[var] = np.where(df[var] > limites, limites, df[var])
    print("✅ Ajuste de continuidad individual aplicado correctamente.")


    # 4️⃣ Predicción de métricas micro
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
        expected_cols = list(scaler.feature_names_in_)
        for c in expected_cols:
            if c not in X_micro.columns:
                X_micro[c] = 0.0
        X_micro = X_micro[expected_cols]
        df[f"{col}_pred"] = model.predict(scaler.transform(X_micro))
        print(f"✅ {col} predicha correctamente")

    df["Fecha"] = pd.to_datetime(fecha_original).dt.date
    print("📅 Columna Fecha preservada para visualización en Power BI.")
    return df


# ================================================
# 💤 Agregar días de descanso (relleno con 0)
# ================================================
def agregar_dias_descanso(df_result, conn, microciclo_predicho):
    print("🧩 Integrando días de descanso al resultado final...")
    fecha_original = df_result["Fecha"].copy()  # 💡 guardamos la columna para restaurarla después


    # 1️⃣ Asegurar que exista la columna Fecha en df_result
    if "Fecha_x" in df_result.columns:
        df_result.rename(columns={"Fecha_x": "Fecha"}, inplace=True)
    if "Fecha_y" in df_result.columns:
        df_result.drop(columns=["Fecha_y"], inplace=True, errors="ignore")

    if "Fecha" not in df_result.columns:
        raise ValueError("❌ df_result no contiene columna Fecha.")

    df_result["Fecha"] = pd.to_datetime(df_result["Fecha"]).dt.normalize()

    # 2️⃣ Cargar planificación semanal (DB_MicrociclosExcel)
    df_plan = pd.read_sql(f"""
        SELECT Fecha, Tipo_Dia, Intensidad
        FROM DB_MicrociclosExcel
        WHERE Microciclo_Num = {microciclo_predicho}
    """, conn)
    df_plan["Fecha"] = pd.to_datetime(df_plan["Fecha"]).dt.normalize()

    # 3️⃣ Generar estructura base: jugador × fecha_plan
    jugadores = df_result["id_jugador"].unique()
    fechas_plan = df_plan["Fecha"].unique()
    df_grid = pd.DataFrame(
        [(j, f) for j in jugadores for f in fechas_plan],
        columns=["id_jugador", "Fecha"]
    )

    # 4️⃣ Unir planificación (Tipo_Dia, Intensidad)
    df_final = df_grid.merge(df_plan, on="Fecha", how="left")

    # 5️⃣ Unir predicciones por jugador y fecha (❗la corrección clave)
    df_result["Fecha"] = pd.to_datetime(df_result["Fecha"]).dt.normalize()
    df_final = df_final.merge(
        df_result,
        on=["id_jugador", "Fecha"],
        how="left",
        suffixes=("", "_pred")
    )

    # 6️⃣ Rellenar días sin datos con 0 (descanso o partido)
    metricas = [
        "Distancia_total_pred", "CE_pred", "CS_pred",
        "Acc_3_pred", "Dec_3_pred", "HMLD_m_pred",
        "HSR_abs_m_pred", "Sprint_vel_max_kmh_pred"
    ]
    for m in metricas:
        if m in df_final.columns:
            mask = df_final["Tipo_Dia"].isin(["DESCANSO", "PARTIDO"])
            df_final.loc[mask, m] = 0
            df_final[m] = df_final[m].fillna(0)

    # 7️⃣ Mantener contexto y limpiar duplicados
    df_final["microciclo_next"] = microciclo_predicho
    if "tipo_semana_pred" in df_result.columns:
        df_final["tipo_semana_pred"] = df_result["tipo_semana_pred"].iloc[0]

    # 8️⃣ Limpieza final: eliminar columnas duplicadas o *_pred innecesarias
    drop_cols = [c for c in df_final.columns if c.endswith("_pred") and c not in metricas]
    df_final.drop(columns=drop_cols, inplace=True, errors="ignore")

    df_final = df_final.loc[:, ~df_final.columns.duplicated()]

    print(f"✅ Días de descanso integrados correctamente ({len(df_final)} filas, {len(df_final['Fecha'].unique())} fechas únicas).")
    
    # 💾 Restaurar columna de fecha para análisis temporal
    if "Fecha" not in df_final.columns:
        df_final["Fecha"] = fecha_original

    # 🧹 Normalizamos formato de fecha (sin hora)
    df_final["Fecha"] = pd.to_datetime(df_final["Fecha"]).dt.date
    print("📅 Columna Fecha normalizada (sin horas).")

    print("📅 Columna Fecha preservada para visualización en Power BI.")
    
    return df_final




# ================================================
# 🧩 Ejecución principal (llamada desde pipeline)
# ================================================
def ejecutar_prediccion_microciclo():
    conn = sqlite3.connect(DB_PATH)
    df_input, microciclo_actual = preparar_datos(conn)
    print(f"🎯 Prediciendo el microciclo {microciclo_actual + 1}...\n")
    print(df_input)

    print("\n🧩 Diagnóstico de duplicación en df_input:")
    duplicados = df_input.groupby(["id_jugador", "Fecha"]).size().reset_index(name="repeticiones")
    print(duplicados[duplicados["repeticiones"] > 1].head(10))
    print(f"Total de combinaciones duplicadas: {len(duplicados[duplicados['repeticiones'] > 1])}")
    print(f"Columnas actuales: {list(df_input.columns)}")


    df_result = predict_pipeline(df_input)

    print(df_result.columns)
    print("\n🔍 Chequeo de variación real entre días (post-predict):")
    print(df_result.groupby('tipo_dia_next')[['Distancia_total_pred', 'CE_pred', 'CS_pred']].mean().round(2))

    df_final = agregar_dias_descanso(df_result, conn, microciclo_actual + 1)
    print(df_result.columns)
    print(df_final["Fecha"])
    # ======================================================
    # 🧹 DEPURACIÓN FINAL DE COLUMNAS (para vista Power BI)
    # ======================================================
    columnas_finales = [
        # Identificación y contexto
        "id_jugador", "Fecha", "microciclo_actual", "microciclo_next",'tipo_semana_next',

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

    #Reemplazamos valores numéricos por categóricos:
    df_final['tipo_semana_next'] = df_final['tipo_semana_next'].replace({0: 'BAJA', 1: 'MEDIA', 2: 'ALTA'})

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