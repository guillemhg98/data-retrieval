# Execucio

## Ordre recomanat

1. Instal.lar dependencias.
2. Executar sample.
3. Revisar sortides Parquet.
4. Configurar `.env` i `UP_RS_FILE`.
5. Executar demanda i diagnostics reals.
6. Fer join final.

## Comandes principals

Executar demanda i diagnostics, sense join:

```bash
python run_pipeline.py
```

Executar demanda:

```bash
python run_pipeline.py --demand
```

Executar diagnostics:

```bash
python run_pipeline.py --diagnosis
```

Executar demanda, diagnostics i join:

```bash
python run_pipeline.py --all
```

Fer nomes el join de finals existents:

```bash
python run_pipeline.py --join-final
```

<<<<<<< HEAD
=======
Allargar finals locals sense connexio al servidor:

```bash
python run_pipeline.py --offline-impute --end-date 2026-09-14
```

>>>>>>> 47892ac (Initial project upload)
## Dates

Per defecte, el pipeline es incremental:

- si hi ha metadata o final anterior, continua des del dia seguent a l'ultim dia real processat;
- si no hi ha estat anterior, comenca a `2008-01-01`;
<<<<<<< HEAD
- mai escriu files amb `timestamp` posterior a avui.
=======
- en execucio normal amb base de dades, no escriu files amb `timestamp` posterior a avui.
>>>>>>> 47892ac (Initial project upload)

Rang manual, dates incloses:

```bash
python run_pipeline.py --all --start-date 2024-01-01 --end-date 2024-12-31
```

<<<<<<< HEAD
Si la font real acaba abans del dia objectiu, el final es completa amb imputacio fins a `--end-date` o fins avui.
=======
Si la font real acaba abans del dia objectiu, el final es completa amb imputacio fins a `--end-date` o fins avui. En execucio normal, les dates futures es retallen per evitar publicar futur accidentalment.

## Imputacio offline sense servidor

Quan no hi ha connexio al servidor pero cal preparar una sortida fins a una data concreta, utilitza el mode explicit `--offline-impute`:

```powershell
cd C:\Users\Guillem\Desktop\data-retrieval
.\.venv\Scripts\activate
python run_pipeline.py --offline-impute --end-date 2026-09-14
```

Aquest mode:

- no obre connexio ODBC ni consulta Synapse;
- llegeix `data/demand_pipeline/finals/demand_final.parquet`;
- llegeix `data/diagnosis_pipeline/finals/diagnosis_final.parquet`;
- elimina imputacions anteriors, conserva les files reals i torna a imputar la cua fins a `--end-date`;
- permet escriure dates futures nomes en aquest mode explicit;
- reconstrueix `data/finals/demand_diagnosis_joined.parquet`.

Les files creades en aquest mode son estimacions, no dades observades. Queden marcades amb:

```text
__is_imputed=True
__imputation_method=same_month_day_mean
__imputation_source_last_date=<ultim dia real disponible>
__imputation_created_at=<moment de creacio>
```

Per imputar nomes un dels dos finals:

```powershell
python run_pipeline.py --offline-impute --demand --end-date 2026-09-14
python run_pipeline.py --offline-impute --diagnosis --end-date 2026-09-14
```

Per refer nomes el join despres d'haver imputat els finals:

```powershell
python run_pipeline.py --offline-impute --join-final --end-date 2026-09-14
```

Quan torni a haver-hi connexio al servidor, executa el pipeline normal:

```powershell
python run_pipeline.py --all --end-date 2026-09-14
```

Les files imputades no avancen el cursor real. El pipeline repren des de l'ultim dia observat, descarta les imputacions quan fusiona el final existent, i substitueix els dies solapats pels valors reals recuperats de Synapse.
>>>>>>> 47892ac (Initial project upload)

## Sample

```bash
python run_pipeline.py --sample --all
```

Entrades per defecte:

```text
data/sample/input/
```

Sortides per defecte:

```text
data/sample/output/
```

Amb rutes alternatives:

```bash
python run_pipeline.py --sample --all --sample-input-dir data/sample/input --sample-output-dir data/sample/output
```

Generar i processar dades sintetiques locals:

```bash
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-31
python run_pipeline.py --sample --all --sample-input-dir data/synthetic/input --sample-output-dir data/synthetic/output
```

## Utilitats Parquet

Convertir Parquet a CSV:

```bash
python run_pipeline.py --convert-parquet data/sample/output/finals/demand_diagnosis_joined.parquet --to csv
```

Mostrar files:

```bash
python run_pipeline.py --show-parquet data/sample/output/finals/demand_diagnosis_joined.parquet --start-date 2026-01-01 --end-date 2026-01-03
```

Comprovar imputacio:

```bash
python run_pipeline.py --check-imputation data/sample/output/finals/demand_diagnosis_joined.parquet
```

<<<<<<< HEAD
=======
Si s'ha usat `--offline-impute` amb una data posterior a avui, `--check-imputation` pot avisar que hi ha files futures. En aquest cas l'avis es esperat sempre que aquestes files tinguin `__is_imputed=True` i arribin fins a la data objectiu.

>>>>>>> 47892ac (Initial project upload)
Eliminar un rang de dates d'un Parquet:

```bash
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28 --dry-run
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28
```

Sense `--dry-run`, es crea un backup abans de sobreescriure.
