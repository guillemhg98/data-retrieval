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

## Dates

Per defecte, el pipeline es incremental:

- si hi ha metadata o final anterior, continua des del dia seguent a l'ultim dia real processat;
- si no hi ha estat anterior, comenca a `2008-01-01`;
- mai escriu files amb `timestamp` posterior a avui.

Rang manual, dates incloses:

```bash
python run_pipeline.py --all --start-date 2024-01-01 --end-date 2024-12-31
```

Si la font real acaba abans del dia objectiu, el final es completa amb imputacio fins a `--end-date` o fins avui.

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

Eliminar un rang de dates d'un Parquet:

```bash
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28 --dry-run
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28
```

Sense `--dry-run`, es crea un backup abans de sobreescriure.
