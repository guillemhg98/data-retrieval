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

## Regla de dates futures

Les consultes i l'escriptura Parquet exclouen timestamps posteriors a avui. Si la font conte dates corruptes o futures, es descarten abans de guardar.
