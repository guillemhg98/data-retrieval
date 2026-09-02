# Sortides

## Estructura

| Ruta | Contingut |
| --- | --- |
| `data/demand_pipeline/incremental/*.parquet` | Blocs incrementals agregats de demanda. |
| `data/demand_pipeline/incremental/metadata.parquet` | Cursor incremental de demanda. |
| `data/demand_pipeline/finals/demand_final.parquet` | Final de demanda, una fila per dia. |
| `data/diagnosis_pipeline/incremental/*.parquet` | Blocs incrementals agregats de diagnostics. |
| `data/diagnosis_pipeline/incremental/metadata.parquet` | Cursor incremental de diagnostics. |
| `data/diagnosis_pipeline/finals/diagnosis_final.parquet` | Final de diagnostics, una fila per dia. |
| `data/finals/demand_diagnosis_joined.parquet` | Demanda i diagnostics units per `timestamp`. |

Els outputs no es versionen.

## Format

- Format principal: Parquet amb compressio `snappy`.
- Clau temporal: columna `timestamp`, normalitzada a dia.
- Frequencia: diaria.
- Tipus de columnes: comptatges numerics i columnes de control d'imputacio.

## Imputacio

Quan la font real encara no te els ultims dies, el pipeline completa el final fins al dia objectiu amb estimacions.

Columnes de control:

| Columna | Descripcio |
| --- | --- |
| `__is_imputed` | `True` si la fila es estimada. |
| `__imputation_method` | Metode aplicat, actualment `same_month_day_mean`. |
| `__imputation_source_last_date` | Ultim dia real disponible quan es va estimar. |
| `__imputation_created_at` | Moment de creacio de la imputacio. |

Cada final tambe escriu sidecars:

| Fitxer | Contingut |
| --- | --- |
| `*_imputation_metadata.json` | Resum global i dates imputades. |
| `*_imputed_rows.csv` | Una fila per data imputada. |

Quan la base de dades publica dades reals mes tard, les files imputades no compten com a processades i son substituides pels valors reals.

<<<<<<< HEAD
## Regla de dates futures

Les consultes i l'escriptura Parquet exclouen timestamps posteriors a avui. Si la font conte dates corruptes o futures, es descarten abans de guardar.
=======
El mode `--offline-impute` aplica la mateixa semantica sense connexio al servidor: fa servir els Parquet finals locals com a base, conserva les files reals, elimina imputacions anteriors i torna a crear la cua imputada fins a `--end-date`.

Exemple:

```bash
python run_pipeline.py --offline-impute --end-date 2026-09-14
```

La sortida fins al `2026-09-14` queda marcada com a imputada. Quan posteriorment s'executa `python run_pipeline.py --all --end-date 2026-09-14` amb connexio a Synapse, els dies imputats es recalculen amb dades reals i reemplacen les estimacions.

## Regla de dates futures

En execucio normal, les consultes i l'escriptura Parquet exclouen timestamps posteriors a avui. Si la font conte dates corruptes o futures, es descarten abans de guardar.

L'unica excepcio es `--offline-impute`: quan l'usuari dona explicitament `--end-date` posterior a avui, el mode offline pot escriure files futures, pero sempre com a imputades i amb les columnes de control anteriors.
>>>>>>> 47892ac (Initial project upload)
