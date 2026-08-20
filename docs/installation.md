# Instal.lacio

## Requisits

- Python 3.9 o superior.
- Per al mode real: ODBC Driver 18 for SQL Server.
- Per al mode real: permisos d'acces a SQL Server / Azure Synapse.

El mode `--sample` no necessita ODBC ni credencials.

## Windows

```powershell
git clone https://github.com/<user>/data-retrieval.git
cd data-retrieval
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python setup.py
```

## Linux / macOS

```bash
git clone https://github.com/<user>/data-retrieval.git
cd data-retrieval
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python setup.py
```

## Validacio sense base de dades

```bash
python run_pipeline.py --sample --all
python -m pytest -q
```

La sortida esperada del primer comandament es crea a:

```text
data/sample/output/
```

## Configuracio real

Copia la plantilla:

```powershell
Copy-Item .env.example .env
```

o en Linux/macOS:

```bash
cp .env.example .env
```

Edita els valors principals:

```env
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_SERVER=synw-aquas.sql.azuresynapse.net
DB_PORT=1433
DB_DATABASE=aquas
AUTH_MODE=ActiveDirectoryIntegrated
BASE_DIR=.
UP_RS_FILE=UPperRS.xlsx
SELECTED_RS_FILE=selections/selected_rs.csv
SELECTED_UP_FILE=selections/selected_up.csv
SELECTED_DIAGNOSIS_CODES_FILE=selections/selected_diagnosis_codes.csv
MAX_DIAGNOSIS_FEATURES=200000
LOG_LEVEL=INFO
```

`UP_RS_FILE` apunta a l'Excel de mapping UP-RS. Per defecte el repo inclou `UPperRS.xlsx`, amb el full `UP per RS` i les columnes `Codi UP` i `RS`. Si vols usar un mapping alternatiu local, posa'n la ruta a `.env`.
