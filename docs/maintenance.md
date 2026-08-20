# Manteniment

## Validacio local

```bash
python run_pipeline.py --help
python run_pipeline.py --sample --all
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-10
python run_pipeline.py --sample --all --sample-input-dir data/synthetic/input --sample-output-dir data/synthetic/output
python -m pytest -q
python -m compileall -q run_pipeline.py run_pipeline_optimized.py config pipelines scripts validate_project.py
```

Per publicar o actualitzar el repo, consulta [Git workflow](git-workflow.md).

## Regenerar mostra multi-any

Per crear nomes inputs sintetiques amb el format esperat pel mode sample:

```bash
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-31
```

Per crear una mostra multi-any i materialitzar finals:

```bash
python -B scripts/create_multiyear_sample.py
```

Per canviar el rang:

```bash
python -B scripts/create_multiyear_sample.py --start 2008-01-01 --end 2012-12-31
```

Les sortides es creen a `data/sample/multiyear_output/` i no es versionen.

## Revisar metadata d'upload

El script seguent nomes fa consultes de lectura:

```bash
python scripts/check_source_upload_metadata.py
```

Si no tens permisos per mirar DMVs:

```bash
python scripts/check_source_upload_metadata.py --skip-dmv
```

## Afegir noves variables

Per demanda:

1. Afegir o normalitzar columna a `prepare_visits_chunk()`.
2. Incloure-la a les funcions d'agregacio de demanda.
3. Afegir test de naming i de sortida sample.

Per diagnostics:

1. Afegir codis o grups a `selections/selected_diagnosis_codes.csv`.
2. Mantenir `MAX_DIAGNOSIS_FEATURES` com a proteccio.
3. Afegir test si hi ha nova regla de rang, alias o territori.

## Reprocessar dies

Per substituir dies antics amb dades reals noves:

```bash
python run_pipeline.py --all --start-date 2026-05-01 --end-date 2026-05-31
```

El final substitueix completament els dies solapats i evita doble comptatge.
