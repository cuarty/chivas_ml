<<<<<<< HEAD
# Chivas ML — Monitoreo de Carga Física

Proyecto final — **Nicolás Di Bartolo**

En conjunto con el preparador físico de **Chivas de Guadalajara**, este proyecto desarrolla un **Data Warehouse** y **dashboards en Power BI** para centralizar datos de **GPS WIMU**. 
Además, incluye un **modelo de Machine Learning** que recomienda ajustes de carga semanal tras cada partido.

## Estructura
```
chivas-ml/
├─ src/chivas_ml/                 # Código fuente (ETL, features, modelos)
│  ├─ etl/                        # Pipelines de ingestión
│  ├─ features/                   # Ingeniería de variables
│  └─ models/                     # Entrenamiento/serving
├─ notebooks/                     # Experimentos y análisis
├─ data/
│  ├─ raw/                        # Archivos crudos (Excel del PF, etc.)
│  ├─ external/                   # Datos externos/auxiliares
│  └─ chivas_dw.sqlite            # Base SQLite del DW
├─ dashboards/powerbi/            # PBIX / artefactos de Power BI
├─ reports/figures/               # Figuras para informes
└─ docs/                          # PPT, diagramas, PDFs
```

## Entorno virtual
```bash
# Windows (PowerShell)
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
1. Colocá tus Excels en `data/raw/` (ej.: `DB_Jugadores.xlsx`, `Carga Guadalajara.xlsx`).  
2. Abrí `notebooks/01_etl_db_jugadores.ipynb` y corré el ETL de ejemplo.  
3. Abrí `notebooks/02_exploracion.ipynb` para EDA y validaciones.  
4. Diseñá los dashboards en `dashboards/powerbi/` usando `data/chivas_dw.sqlite` como fuente.

## Git LFS (recomendado para .pptx y .sqlite)
```bash
git lfs install
git lfs track "*.pptx" "*.sqlite" "*.pbix" "*.pdf" "docs/*"
git add .gitattributes
```

---
=======
# chivas-ml
Sistema de análisis de carga física y rendimiento para fútbol profesional. Incluye Data Warehouse, dashboards en Power BI y un modelo de Machine Learning con datos GPS WIMU, desarrollado en conjunto con el preparador físico del Club Chivas de Guadalajara.
>>>>>>> e829c2a (Initial commit)
