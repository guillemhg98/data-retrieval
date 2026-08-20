# Data policy

Aquest repo ha de contenir codi, configuracio, tests, documentacio i fixtures sintetiques petites. No ha de contenir dades reals ni sortides generades.

## Que es versiona

| Ruta | Motiu |
| --- | --- |
| `data/sample/input/*.csv` | Fixtures sintetiques petites per validar un clone net. |
| `selections/*.csv` | Plantilles de seleccio de diagnostics, RS i UP. |
| `UPperRS.xlsx` | Mapping de referencia UP -> RS necessari per executar el pipeline real. |
| `config/`, `pipelines/`, `scripts/`, `tests/`, `docs/` | Codi i documentacio. |

## Que no es versiona

| Ruta o patro | Motiu |
| --- | --- |
| `data/demand_pipeline/` | Sortides reals o locals de demanda. |
| `data/diagnosis_pipeline/` | Sortides reals o locals de diagnostics. |
| `data/finals/` | Join final generat. |
| `data/synthetic/` | Dades sintetiques generades localment. |
| `*.parquet`, `*.csv` fora de les excepcions | Evita publicar datasets accidentalment. |
| `.env`, `.env.local` | Secrets i configuracio local. |
| `*.xlsx`, `*.xls`, excepte `UPperRS.xlsx` | Excels locals no declarats. |

La `.gitignore` esta preparada per bloquejar aquests fitxers. Abans de publicar, revisa sempre:

```bash
git status --short --ignored
```

## Crear dades reals localment

1. Configura `.env`.
2. Per defecte, `UP_RS_FILE=UPperRS.xlsx` usa el mapping inclos al repo.
3. Si vols usar un mapping alternatiu local, canvia `UP_RS_FILE` a `.env`.
4. Executa:

```bash
python run_pipeline.py --all
```

Les dades reals quedaran sota `data/demand_pipeline/`, `data/diagnosis_pipeline/` i `data/finals/`. Aquestes carpetes son ignorades per Git.

## Crear dades sintetiques localment

Per generar inputs sintetiques amb el format esperat:

```bash
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-31
```

Per executar el pipeline amb aquestes inputs:

```bash
python run_pipeline.py --sample --all --sample-input-dir data/synthetic/input --sample-output-dir data/synthetic/output
```

Tant `data/synthetic/input/` com `data/synthetic/output/` son ignorades per Git.
