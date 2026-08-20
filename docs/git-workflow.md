# Git workflow

Aquest document assumeix que el repositori local es diu `data-retrieval` i que el remot de GitHub sera `https://github.com/<user>/data-retrieval.git`.

## Primera publicacio

Des de l'arrel del repo:

```powershell
cd C:\Users\Guillem\Desktop\AQUAS_DATA_RETRIEVAL-git\data-retrieval
git status --short --ignored
git add .
git commit -m "Initial clean data-retrieval release"
git branch -M main
git remote add origin https://github.com/<user>/data-retrieval.git
git push -u origin main
```

Abans del `git add .`, comprova que `git status --short --ignored` no mostra dades reals com a fitxers no versionats pendents d'afegir. Han de sortir ignorats, no com `??`.

## Actualitzar des de GitHub

```powershell
cd C:\Users\Guillem\Desktop\AQUAS_DATA_RETRIEVAL-git\data-retrieval
git pull --ff-only origin main
```

Si tens canvis locals pendents:

```powershell
git status --short
git add README.md docs scripts pipelines tests config selections .gitignore
git commit -m "Describe local changes"
git pull --rebase origin main
```

## Pujar canvis nous

```powershell
git status --short --ignored
python run_pipeline.py --sample --all
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-10
python run_pipeline.py --sample --all --sample-input-dir data/synthetic/input --sample-output-dir data/synthetic/output
python -m pytest -q
git status --short --ignored
git add .
git commit -m "Describe change"
git push
```

Despres de les proves, les carpetes `data/sample/output/` i `data/synthetic/` poden existir localment, pero han de sortir com ignorades o no sortir al commit.

## Comprovar que no puges dades

Fitxers que es poden commitejar:

```text
UPperRS.xlsx
data/sample/input/*.csv
selections/*.csv
README.md
docs/
config/
pipelines/
scripts/
tests/
```

Fitxers que no han d'entrar al commit:

```text
.env
data/demand_pipeline/
data/diagnosis_pipeline/
data/finals/
data/synthetic/
data/sample/output/
*.parquet
```

Comanda de control:

```powershell
git status --short --ignored
```

Si veus dades reals amb prefix `??`, atura't abans del commit i revisa `.gitignore`.

## Test toy

Aquest test valida el clone net amb les fixtures versionades:

```powershell
python setup.py
python run_pipeline.py --sample --all
python run_pipeline.py --show-parquet data/sample/output/finals/demand_diagnosis_joined.parquet --parquet-limit 5
python run_pipeline.py --check-imputation data/sample/output/finals/demand_diagnosis_joined.parquet
```

Sortides esperades:

```text
data/sample/output/demand_pipeline/finals/demand_final.parquet
data/sample/output/diagnosis_pipeline/finals/diagnosis_final.parquet
data/sample/output/finals/demand_diagnosis_joined.parquet
```

## Test sintetic generat

Aquest test valida que el repo pot crear dades sintetiques noves amb el format correcte i processar-les:

```powershell
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-31
python run_pipeline.py --sample --all --sample-input-dir data/synthetic/input --sample-output-dir data/synthetic/output
python run_pipeline.py --show-parquet data/synthetic/output/finals/demand_diagnosis_joined.parquet --parquet-limit 5
```

Les inputs i outputs de `data/synthetic/` son locals i ignorades per Git.

## Test real

Prerequisits:

- ODBC Driver 18 for SQL Server instal.lat.
- Acces a SQL Server / Azure Synapse.
- `.env` configurat.
- `UPperRS.xlsx` present a l'arrel del repo.

Preparacio:

```powershell
Copy-Item .env.example .env
notepad .env
python validate_project.py
```

Execucio curta per validar connexio i format:

```powershell
python run_pipeline.py --all --start-date 2026-01-01 --end-date 2026-01-07
python run_pipeline.py --show-parquet data/finals/demand_diagnosis_joined.parquet --parquet-limit 5
python run_pipeline.py --check-imputation data/finals/demand_diagnosis_joined.parquet
```

Execucio incremental normal:

```powershell
python run_pipeline.py --all
```

Les sortides reals queden sota `data/demand_pipeline/`, `data/diagnosis_pipeline/` i `data/finals/`, totes ignorades per Git.

## Problema de safe.directory

Si Git mostra un error tipus `detected dubious ownership`, marca el repo com a segur:

```powershell
git config --global --add safe.directory C:/Users/Guillem/Desktop/AQUAS_DATA_RETRIEVAL-git/data-retrieval
```
