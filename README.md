<<<<<<< HEAD
# Chivas ML — Monitoreo de Carga Física

Proyecto final — **Nicolás Di Bartolo**

En conjunto con el preparador físico de **Chivas de Guadalajara**, este proyecto desarrolla un **Data Warehouse** y **dashboards en Power BI** para centralizar datos de **GPS WIMU**.
Además, incluye un **modelo de Machine Learning** que recomienda ajustes de carga semanal tras cada partido, clasificando el rendimiento físico en bajo, intermedio o alto y sugiriendo carga explosiva, sostenida o regenerativa.


## Estructura
```
chivas-ml/
├─ src/chivas_ml/                 # Código fuente (ETL, features, modelos)
│  ├─ etl/                        # Pipelines de ingestión
│  ├─ features/                   # Ingeniería de variables
│  └─ models/                     # Entrenamiento/serving
├─ notebooks/                     # Experimentos y análisis
├─ data/
│  ├─ raw/
│  │   ├─ entrenamientos/         # Archivos de entrenamientos (Excel del PF)
│  │   └─ partidos/               # Archivos de partidos jugados
│  ├─ ref/                        # Datos auxiliares (calendario, jugadores, etc.)
│  ├─ processed/                  # Archivos ya procesados (backup histórico)
│  ├─ external/                   # Datos externos adicionales
│  └─ chivas_dw.sqlite            # Base SQLite del Data Warehouse
├─ dashboards/powerbi/            # Archivos PBIX / artefactos de Power BI
├─ reports/figures/               # Figuras para informes
└─ docs/                          # Documentación, diagramas, PDFs, PPT

```

## Entorno virtual
```# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r chivas-ml/requirements.txt

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r chivas-ml/requirements.txt

```

## Variables de entorno (opcional)
Crear un archivo `.env` en la raíz si querés credenciales o rutas personalizadas.
Ejemplo:
```
DATABASE_URL=sqlite:///data/chivas_dw.sqlite
```

## Uso rápido
1. Guardá los archivos en las carpetas correspondientes:
2. `data/raw/entrenamientos/` → entrenamientos
3. `data/raw/partidos/` → partidos jugados
4. `data/ref/` → calendario, jugadores y datos auxiliares
5. Corré el pipeline desde `main.py` para cargar datos al DW (`chivas_dw.sqlite`).
6. Revisá los Jupyter Notebooks en `notebooks/` para ejemplos de ETL, EDA y validaciones.
7. Diseñá dashboards en `dashboards/powerbi/` usando como fuente `data/chivas_dw.sqlite`.


## Git LFS (recomendado para .pptx y .sqlite)
```bash
git lfs install
git lfs track "*.pptx" "*.sqlite" "*.pbix" "*.pdf" "docs/*"
git add .gitattributes
```

---
=======
# ✨ Chivas ML 
Sistema de análisis de carga física y rendimiento para fútbol profesional.
Integra datos de GPS WIMU en un Data Warehouse, genera reportes en Power BI y aplica Machine Learning para apoyar decisiones de planificación de entrenamientos.
>>>>>>> e829c2a (Initial commit)