# Analisi funcional

## Visio general

El projecte esta organitzat en cinc blocs:

| Bloc | Funcio |
| --- | --- |
| `run_pipeline.py` | Punt d'entrada estable i compatible. |
| `run_pipeline_optimized.py` | CLI complet, execucio de pipelines, mode sample i utilitats Parquet. |
| `config/` | Configuracio per entorn i per pipeline. |
| `pipelines/` | Transformacio, agregacio, incrementals, imputacio i join final. |
| `scripts/` | Eines auxiliars per generar mostres i inspeccionar metadata de la font. |

## CLI principal

`run_pipeline_optimized.py` exposa:

| Funcio | Que fa |
| --- | --- |
| `main()` | Parseja arguments i decideix si executar demanda, diagnostics, join o utilitats Parquet. |
| `run_demand_pipeline_optimized()` | Executa el pipeline real de demanda. |
| `run_diagnosis_pipeline_optimized()` | Executa el pipeline real de diagnostics. |
| `join_final_outputs()` | Uneix finals de demanda i diagnostics per `timestamp`. |
| `run_demand_sample_pipeline()` | Executa demanda amb CSVs locals sintetiques. |
| `run_diagnosis_sample_pipeline()` | Executa diagnostics amb CSVs locals sintetiques. |
| `join_sample_final_outputs()` | Uneix els finals generats pel mode sample. |
| `convert_parquet_file()` | Converteix Parquet a CSV o Excel. |
| `print_parquet_rows()` | Mostra files d'un Parquet, opcionalment filtrades per dates i columnes. |
| `delete_parquet_rows()` | Elimina un rang de dates d'un Parquet i crea backup. |
| `check_parquet_imputation()` | Resumeix i valida columnes d'imputacio. |
| `write_parquet_imputation_metadata()` | Escriu sidecars JSON/CSV d'imputacio per a un Parquet existent. |

`run_pipeline.py` reexporta les funcions principals i permet mantenir la comanda curta:

```bash
python run_pipeline.py --all
```

## Configuracio

`config/config.py` defineix:

| Classe / funcio | Que fa |
| --- | --- |
| `Config` | Llegeix variables comunes: base dir, database, auth, seleccions, logging. |
| `DemandConfig` | Defineix taula, columna de data, rutes i Excel UP-RS de demanda. |
| `DiagnosisConfig` | Defineix taula, columnes UP/diagnostic, seleccions i rutes de diagnostics. |
| `get_config()` | Retorna la configuracio de `demand` o `diagnosis`. |
| `resolve_up_rs_file()` | Busca l'Excel de mapping UP-RS i dona un error accionable si falta. |

## Demanda

`pipelines/demand/transformations.py`:

| Funcio | Que fa |
| --- | --- |
| `prepare_visits_chunk()` | Normalitza dates, UP, RS, comptador per visita i tipus de visita agrupat. |

`pipelines/demand/incremental_optimized.py`:

| Funcio | Que fa |
| --- | --- |
| `_load_selected_rs()` / `_load_selected_up()` | Carreguen seleccions territorials compartides. |
| `_filter_if_selected()` | Filtra RS/UP nomes si hi ha seleccio activa. |
| `run_incremental_pipeline_optimized()` | Consulta la font per anys, transforma, agrega i escriu incrementals Parquet. |
| `run_demand_pipeline_main_optimized()` | Connecta `DemandConfig` amb el runner incremental. |

`pipelines/demand/aggregation_optimized.py`:

| Funcio | Que fa |
| --- | --- |
| `build_daily_total_cat_optimized()` | Calcula `DEMAND__TOTAL` per dia. |
| `build_daily_features_global_optimized()` | Calcula variables globals per categories de visita. |
| `build_daily_features_by_group_optimized()` | Calcula variables per RS o UP. |
| `aggregate_final_optimized()` | Fusiona incrementals, substitueix solapaments i desa final. |
| `refresh_final_imputation()` | Refresca imputacions si no hi ha dades noves reals. |

## Diagnostics

`pipelines/diagnosis/incremental_optimized.py`:

| Funcio | Que fa |
| --- | --- |
| `validate_table_columns()` | Comprova que la taula origen te les columnes requerides. |
| `get_diagnosis_data_for_year_optimized()` | Consulta SQL agregada per dia, UP i prefix ICD10. |
| `_expand_diagnosis_code_spec()` | Expandeix codis i rangs ICD10, per exemple `J00-J06`. |
| `_load_selected_codes()` | Carrega codis/grups de `selected_diagnosis_codes.csv`. |
| `_expand_selected_code_aliases()` | Duplica contribucions quan un codi pertany a mes d'un grup. |
| `_load_selected_rs()` / `_load_selected_up()` | Carreguen subsets territorials. |
| `run_incremental_diagnosis_pipeline_optimized()` | Consulta, normalitza, agrega i escriu incrementals de diagnostics. |
| `run_diagnosis_pipeline_main_optimized()` | Connecta `DiagnosisConfig` amb el runner incremental. |

`pipelines/diagnosis/aggregation_optimized.py`:

| Funcio | Que fa |
| --- | --- |
| `build_daily_total_general_optimized()` | Calcula `DIAGNOSIS__TOTAL` amb tots els diagnostics. |
| `build_daily_total_by_group_optimized()` | Calcula totals reals per RS/UP. |
| `build_daily_diagnosis_counts_optimized()` | Calcula comptatges per codi/grup seleccionat. |
| `build_daily_diagnosis_by_group_optimized()` | Calcula comptatges de codi/grup per RS/UP. |
| `build_diagnosis_wide_format_optimized()` | Pivota codis diagnostics a format ample. |
| `aggregate_diagnosis_final_optimized()` | Fusiona incrementals i desa `diagnosis_final.parquet`. |
| `_validate_feature_count()` | Evita matrius massa amples si falta una seleccio. |

## Shared

| Modul | Funcions principals |
| --- | --- |
| `shared/db.py` | Construeix connexio ODBC, accepta `DB_CONNECTION_STRING`, retries i autenticacio Azure. |
| `shared/utils.py` | Calcula finestres incrementals, rangs anuals, consultes SQL i estat legacy. |
| `shared/parquet_storage.py` | Gestiona incrementals, finals, metadata i filtrat de dates futures. |
| `shared/imputation.py` | Completa dies pendents fins a data objectiu amb mitjana historica i marca files imputades. |
| `shared/final_joiner.py` | Carrega finals, canonitza columnes i fa join columnwise per `timestamp`. |
| `shared/naming.py` | Defineix gramatica `DOMAIN__VARIABLE__CATEGORY__GEO__ID` i migracio de noms antics. |
| `shared/logging_config.py` | Configura logging. |

## Scripts

| Script | Que fa |
| --- | --- |
| `scripts/create_synthetic_inputs.py` | Genera inputs CSV sintetiques amb l'esquema esperat pel mode `--sample`. |
| `scripts/create_multiyear_sample.py` | Genera una mostra sintetica multi-any i materialitza finals Parquet/CSV. |
| `scripts/check_source_upload_metadata.py` | Inspecciona metadata de Synapse per trobar pistes de carrega o ingestio. |

## Tests

Els tests cobreixen:

- finestres incrementals i tall de dates futures;
- naming canonical de demanda, diagnostics i join;
- expansio de codis/rangs ICD10;
- seleccions territorials;
- imputacio i metadata sidecar;
- utilitats Parquet de visualitzacio i eliminacio.
