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

Allargar finals locals sense connexio al servidor:

```bash
python run_pipeline.py --offline-impute --end-date 2026-09-14
```

## Dates

Per defecte, el pipeline es incremental:

- continua des del dia seguent a l'ultim dia observat del final publicat; les metadades dels fragments pendents no avancen aquest cursor;
- si no hi ha estat anterior, comenca a `2008-01-01`;
- en execucio normal amb base de dades, no escriu files amb `timestamp` posterior a avui.

Rang manual, dates incloses:

```bash
python run_pipeline.py --all --start-date 2024-01-01 --end-date 2024-12-31
```

Si la font real acaba abans del dia objectiu, el final es completa amb imputacio fins a `--end-date` o fins avui. En execucio normal, les dates futures es retallen per evitar publicar futur accidentalment.

## Revisio eficient de dades existents

Les execucions de produccio tambe revisen els agregats locals: dates absents o imputades, caigudes per sota del 10% d'una referencia setmanal amb almenys quatre observacions, i mesos que han perdut el contrast habitual entre laborables i caps de setmana. La referencia inclou el mateix dia de setmana de l'any anterior per detectar caigudes sostingudes. Un festiu pot quedar assenyalat: el detector indica sospita, no incompletesa demostrada.

Es comproven com a maxim **120 dies existents per pipeline i execucio**, incloent els set dies mes recents disponibles. Es fa una unica consulta agrupada a SQL, amb filtres de dates, `COUNT_BIG(*)` i nombre d'UP diferents. Nomes es reextreuen els dies amb recomptes diferents, cobertura modificada respecte d'una comprovacio anterior, dades absents/imputades amb observacions disponibles, o canvis coneguts de configuracio. Els dies nous es processen amb el flux incremental habitual i no consumeixen aquest pressupost de revisio.

Per revisar explicitament el tram de juliol a setembre sense reconstruir tot l'historic:

```powershell
python run_pipeline.py --all --review-period 2026-07-01 2026-09-26
```

`--review-period START END` es repetible i te prioritat sobre els candidats automatics. `--review-max-days 30` permet reduir el pressupost. Els pendents automatics continuen en execucions posteriors; les revisions explicites s'han de repetir si excedeixen el pressupost. Els dies recents es tornen a comprovar al cap d'un dia i els sospitosos antics al cap de set dies. La comprovacio es desa en `*_integrity.json` nomes despres de publicar correctament el final. Si s'edita el total local, la comprovacio anterior deixa de servir per ometre'l.

La signatura dels inputs inclou la font, el mapping UP-RS, les seleccions i la versio d'agregacio. Un canvi conegut posa les dates anteriors en una cua de reconstruccio, amb el mateix limit per execucio. La primera execucio estableix aquesta signatura; no pot deduir amb quina configuracio es van generar fitxers antics sense metadades.

Recomptes iguals no demostren contingut identic ni carrega completa: una modificacio que conservi el total i el nombre d'UP pot passar desapercebuda. Per a aquests casos cal una marca de versio de la font o una reconstruccio explicita amb `--start-date`. No s'executa una consulta per dia ni es descarrega tot l'historic per validar-lo.

Els fragments repetits substitueixen snapshots anteriors; no se sumen. Si un fragment es corrupte, s'atura la publicacio i es conserven els fragments. Si un pipeline falla, `--all` no publica un join nou.

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
__imputation_method=recent_same_weekday_mean
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

### Comprovacio local amb informe i PNG

Des de PowerShell, a l'arrel del repositori, actualitza la branca i instal·la les dependencies del visor:

```powershell
git switch incremental-selection-refresh
git pull --ff-only
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest -q
```

La comprovacio local **no consulta SQL ni modifica els finals**. Genera l'informe abans de recuperar dades:

```powershell
.\.venv\Scripts\python.exe scripts/check_data_quality.py --start-date 2026-07-01 --output-dir data/quality_before
```

Despres, amb l'ODBC Driver 18 i l'autenticacio de SQL configurats, executa la revisio del periode i actualitza els dies nous. Atura't si el pipeline retorna un error:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --all --review-period 2026-07-01 2026-09-26 --review-max-days 120
if ($LASTEXITCODE -ne 0) { throw "El pipeline ha fallat; revisa el log abans de continuar" }
.\.venv\Scripts\python.exe scripts/check_data_quality.py --start-date 2026-07-01 --output-dir data/quality_after
Invoke-Item .\data\quality_after\demand_quality.png
Invoke-Item .\data\quality_after\diagnosis_quality.png
Get-Content .\data\quality_after\summary.txt
```

Si nomes vols generar el visor del tram concret, afegeix `--end-date 2026-09-26`. Sense dates, el visor mostra els ultims 365 dies emmagatzemats. Per inspeccionar un ZIP, extreu-lo en una carpeta separada i passa `--data-root ruta/a/la/carpeta/data`; no cal substituir les dades de produccio.

Cada informe conte:

- `demand_quality.png` i `diagnosis_quality.png`: observacions blaves, imputacions taronges, sospites amb creus vermelles i referencia observada de fa 52 setmanes. El segon panell mostra les medianes per dia de setmana per detectar aplanaments.
- `summary.txt` i `summary.json`: dates, qualitat numerica, comprovacions del join, discrepàncies entre CSV i Parquet i verificacio del manifest SHA-256 quan existeix.
- `flagged_days.csv`: dates assenyalades i motiu. Les estructures numeriques es comproven sobre tot el fitxer; les dates del comandament limiten els grafics i la llista de sospites.

Interpretacio del resultat:

- `OK`: les comprovacions locals passen i no s'han trobat avisos al periode mostrat. No demostra que SQL sigui complet ni valida el model predictiu.
- `REVIEW`: hi ha imputacions, dies sospitosos, procedencia desconeguda o revisions pendents. Un festiu pot quedar assenyalat legitimament. Comprova el CSV de dates i el registre `*_integrity.json`.
- `ERROR`: falta un fitxer, hi ha dates duplicades/invalides, valors negatius/no finits, totals incoherents, o el CSV/join/manifest no coincideixen. Cal resoldre-ho abans d'utilitzar la sortida.

Codis de sortida del visor: `0` sense errors estructurals (pot haver-hi `REVIEW`), `2` amb `ERROR`. Amb `--strict`, un `REVIEW` retorna `1`. Per tant, no et basïs nomes en el codi zero: llegeix el resultat del resum. No s'han de convertir les sospites en imputacions per aconseguir un informe verd.

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

Si s'ha usat `--offline-impute` amb una data posterior a avui, `--check-imputation` pot avisar que hi ha files futures. En aquest cas l'avis es esperat sempre que aquestes files tinguin `__is_imputed=True` i arribin fins a la data objectiu.

Eliminar un rang de dates d'un Parquet:

```bash
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28 --dry-run
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28
```

Sense `--dry-run`, es crea un backup abans de sobreescriure.
