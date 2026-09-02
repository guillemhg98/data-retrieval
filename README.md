# data-retrieval

Pipelines Python per recuperar dades de SQL Server / Azure Synapse, transformar-les i generar matrius diaries en Parquet per a demanda assistencial i diagnostics.

El repo esta preparat per funcionar en tres modes:

- `sample`: executa tot el flux amb CSVs sintetiques incloses al repo, sense credencials ni ODBC.
- `production`: consulta les taules reals, crea incrementals Parquet i reconstrueix finals.
- `offline-impute`: sense connexio al servidor, allarga els finals Parquet locals fins a una data objectiu amb files imputades i marcades com a tals.

## Que genera

| Pipeline | Font real | Sortida |
| --- | --- | --- |
| Demanda | `z_inv.P1038_visites` | `data/demand_pipeline/finals/demand_final.parquet` |
| Diagnostics | `z_inv.P1038_prstb015r_filtrat` | `data/diagnosis_pipeline/finals/diagnosis_final.parquet` |
| Join final | Finals de demanda i diagnostics | `data/finals/demand_diagnosis_joined.parquet` |

Les columnes segueixen la gramatica canonica:

```text
DEMAND__TOTAL
DEMAND__SERVEI_CODI__INF__RS__RS_64
DIAGNOSIS__TOTAL
DIAGNOSIS__ICD10_3__G01__UP__MICRO_01
```

## Instal.lacio rapida

```powershell
git clone https://github.com/<user>/data-retrieval.git
cd data-retrieval
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python setup.py
```

Prova local sense base de dades:

```powershell
python run_pipeline.py --sample --all
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-31
python run_pipeline.py --sample --all --sample-input-dir data/synthetic/input --sample-output-dir data/synthetic/output
python run_pipeline.py --check-imputation data/sample/output/finals/demand_diagnosis_joined.parquet
python -m pytest -q
```

Per executar contra Synapse:

```powershell
Copy-Item .env.example .env
# editar .env amb servidor, auth i UP_RS_FILE
python run_pipeline.py --all
```

Per allargar els finals locals sense connexio al servidor, per exemple fins al `2026-09-14`:

```powershell
cd C:\Users\Guillem\Desktop\data-retrieval
.\.venv\Scripts\activate
python run_pipeline.py --offline-impute --end-date 2026-09-14
```

Aquest mode no recupera dades reals: carrega `demand_final.parquet` i `diagnosis_final.parquet`, afegeix les dates que falten fins a `--end-date` amb imputacio, i reconstrueix `data/finals/demand_diagnosis_joined.parquet`. Les files imputades queden marcades amb `__is_imputed=True`, `__imputation_method`, `__imputation_source_last_date` i `__imputation_created_at`.

Quan es torni a executar el pipeline normal amb connexio:

```powershell
python run_pipeline.py --all --end-date 2026-09-14
```

les files imputades no compten com a reals ni com a estat definitiu: el pipeline repren des de l'ultim dia real, descarta les imputacions en fusionar, i substitueix els dies solapats pels valors reals de la base de dades.

Flux Git, push/pull i validacions completes:

```powershell
git status --short --ignored
git add .
git commit -m "Initial clean data-retrieval release"
git remote add origin https://github.com/<user>/data-retrieval.git
git push -u origin main
git pull --ff-only origin main
```

Consulta [docs/git-workflow.md](docs/git-workflow.md) per les ordres completes de test toy, test sintetic generat i test real.

## Documentacio

- [Analisi funcional](docs/function-analysis.md)
- [Politica de dades](docs/data-policy.md)
- [Git workflow](docs/git-workflow.md)
- [Instal.lacio](docs/installation.md)
- [Dades d'entrada](docs/input-data.md)
- [Execucio](docs/execution.md)
- [Sortides](docs/outputs.md)
- [Seleccions i naming](docs/selections-and-naming.md)
- [Llicencies i publicacio](docs/licensing.md)
- [Manteniment](docs/maintenance.md)

## Que no es versiona

El repo exclou dades reals extretes, dades sintetiques generades, sortides generades, fitxers Parquet, Excels locals no declarats, `.env`, caches i artefactes de documentacio. Es versionen les fixtures sintetiques petites de `data/sample/input/`, les plantilles de `selections/` i el mapping de referencia `UPperRS.xlsx`.

## Estat de llicencia

No he assignat una llicencia open source per tu. Abans de publicar el repo, decideix si vols MIT, Apache-2.0, GPL, o mantenir-lo sense llicencia publica. Consulta [docs/licensing.md](docs/licensing.md).
