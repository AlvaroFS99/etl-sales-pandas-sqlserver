# ETL Pipeline with Pandas/NumPy + SQL Server

> Compact ETL pipeline that loads multiple CSV files, cleans and validates sales data, generates a monthly summary, exports results to CSV, and loads them into SQL Server with structured logging.

---

## ✨ Features

* **Batch CSV ingestion** from a folder, automatically adding **`Audit_Date`** from each filename.
* **Two cleaning flows**:

  * `Valid_Sales` (normalized fields, nulls/duplicates removed).
  * `Invalid_Sales` with column `Reason`: `N` (nulls), `A` (invalid amount), `D` (duplicates).
* **Currency normalization**: EUR → USD (×0.85 multiplier as coded).
* **Monthly summary** grouped by `Month` × `Product` with total, count, and minimum amount.
* **CSV export** to `/Results` and **load into SQL Server** (three tables).
* **Structured logging** in `logfile.log`.

---

## 📂 Project Structure

```
.
├── .venv/
├── csv/                      # Input CSV files (filenames contain dates)
├── Results/                  # Output processed CSVs
├── __pycache__/
├── main.py                   # Main script (runs the ETL)
├── etl_utils.py              # Utility functions (logging, IO, DB connection, batch loader)
├── logfile.log               # Execution log (auto-generated)
└── requirements.txt          # Python dependencies
```

---

## 🔁 ETL Flow

1. **Extract**

   * `cargar_ficheros_en_dataframe(ruta_csv)` reads all CSV files in `/csv` and adds `Audit_Date` from the filename.
2. **Transform**

   * `limpiar_ventas_validas(df)`:

     * `Sale_ID` uppercased, nulls/duplicates removed.
     * `Product`: last token after `-`, uppercased, nulls removed.
     * `Amount`: strip `USD/EUR`, cast to float, **if EUR ×0.85**, round to 2 decimals, drop nulls.
     * `Date` and `Audit_Date` converted to `datetime`, nulls removed.
   * `limpiar_ventas_invalidas(df)`:

     * Collects invalid rows with column `Reason`:

       * `N`: row has any nulls.
       * `A`: `Amount` text does not contain `USD` or `EUR`.
       * `D`: duplicated `Sale_ID` (among non-null rows with valid currency).
   * `generar_ventas_resumen_mensual(df_validas)`:

     * Adds `Month = Date.strftime('%m/%Y')`.
     * Aggregates `Amount` by `Month`×`Product` → sum, count, min.
3. **Load**

   * **CSV outputs** (`/Results`): `Valid_Sales.csv`, `Invalid_Sales.csv`, `Monthly_Summary.csv`.
   * **SQL Server**: same datasets as tables: `Valid_Sales`, `Invalid_Sales`, `Monthly_Summary`.

---

## 🛠️ Requirements

* **Python** ≥ 3.10
* **Packages**: `pandas`, `numpy`, `SQLAlchemy`, `pyodbc`, `logging` (stdlib).
* **ODBC driver**: *ODBC Driver 17 for SQL Server*.

Install dependencies:

```bash
pip install -r requirements.txt
```

> If `pyodbc` or driver installation fails on Windows, install **ODBC Driver 17 for SQL Server** from Microsoft and add it to PATH.

---

## ⚙️ Configuration

Edit the config block in `main.py`:

```python
configurar_logging('logfile.log')

# Input CSV folder
ruta_csv = r"C:\\path\\to\\project\\csv"

# Target SQL Server database
nombre_bbdd = "MyDatabase"

# Create engine via SQLAlchemy
engine = crear_conexion(nombre_bbdd)
```

**Notes**

* The connection uses **Windows Trusted Authentication** with *ODBC Driver 17*.
* Ensure your Windows user has permissions on `nombre_bbdd`.

---

## ▶️ How to Run

From the project root:

```bash
python main.py
```

**Process**

1. Initialize logging → `logfile.log`.
2. Read all CSVs from `ruta_csv` and add `Audit_Date`.
3. Build `df_validas`, `df_invalidas`, `df_resumen`.
4. Format dates (`Date`, `Audit_Date`) to `YYYY-MM-DD` before export/load.
5. Export to `/Results` and load three tables into SQL Server.
6. Close connection and log process end.

---

## 📤 Outputs

* **CSVs** in `/Results/`:

  * `Valid_Sales.csv`
  * `Invalid_Sales.csv`
  * `Monthly_Summary.csv`
* **SQL Server tables**:

  * `Valid_Sales`
  * `Invalid_Sales`
  * `Monthly_Summary`
* **Log**: `logfile.log`

---

## 🧱 Modules

* `etl_utils.py`

  * `configurar_logging(nombre_archivo)`
  * `crear_conexion(nombre_bbdd)` (SQLAlchemy + Trusted Connection)
  * `leer_datos(ruta_csv, convertir_fecha=False, columna_fecha='Date')`
  * `exportar_a_csv(df, ruta_csv)` (Excel-friendly encoding)
  * `cargar_en_bdd(df, nombre_tabla, engine, modo='replace')`
  * `cargar_ficheros_en_dataframe(ruta_directorio)` (batch load + `Audit_Date`)
* `main.py`

  * Orchestrates ETL; defines:

    * `limpiar_ventas_validas`, `limpiar_ventas_invalidas`, `generar_ventas_resumen_mensual`.
  * Applies `formatear_fechas` before export/load.

---

## ⚠️ Edge Cases

* **`Audit_Date`** parsed from filename. Non-date filenames → `NaT` → rows removed from valid pipeline.
* **Invalid `Amount`**: if not containing `USD` or `EUR`, row marked with `Reason = A`.
* **DB load mode**: default `replace`. Switch to `append` for incremental loads.

---

## 🧪 Quick Checks

* Verify row counts in log messages.
* Ensure three tables exist in DB.
* Open `/Results` CSVs to check date formats and normalized amounts.

---

## 🧩 Troubleshooting

* **`pyodbc.InterfaceError` / driver not found** → install *ODBC Driver 17 for SQL Server* and restart.
* **Permissions/DB not found** → check `nombre_bbdd` exists and your Windows user has rights.
* **Empty outputs** → verify `/csv` contains files and filenames allow valid `Audit_Date` parsing.

---

**Author**: Álvaro F. — Exam project with **Pandas + NumPy + SQLAlchemy**.

---

## 🇪🇸 Resumen en Español

Este proyecto implementa un flujo ETL sencillo con **Pandas y NumPy**:

* Carga múltiples CSV añadiendo una columna `Audit_Date`.
* Limpia ventas válidas e inválidas (motivos N/A/D).
* Normaliza monedas (EUR → USD).
* Genera un resumen mensual agregado.
* Exporta resultados a CSV y a tablas en SQL Server.

Incluye **logging**, modularización en `etl_utils.py`, y está preparado para ejecutarse directamente con `python main.py`.
