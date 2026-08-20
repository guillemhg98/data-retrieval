# Dades d'entrada

## Mode sample

El repo inclou CSVs sintetiques petites a:

```text
data/sample/input/
```

| Fitxer | Columnes | Us |
| --- | --- | --- |
| `up_rs.csv` | `Codi UP`, `RS` | Mapping UP a regio sanitaria per a les dades sample. |
| `demand_visits.csv` | `DATA_VISITA`, `UP`, `VISI_LLOC_VISITA`, `VISI_SITUACIO_VISITA`, `SERVEI_CODI`, `TIPUS_CLASS`, `VISI_TIPUS_VISITA` | Visites sintetiques. |
| `diagnosis_visits.csv` | `data_visita`, `up_c`, `problema_salut_c` | Diagnostics sintetiques. |
| `selected_codes.csv` | codi diagnostic, opcionalment alias | Fallback local per diagnostics sample. |

Aquestes dades serveixen per comprovar instal.lacio, naming i join sense tocar dades reals.

## Dades sintetiques generades

Per crear inputs sintetiques noves amb el mateix format:

```bash
python scripts/create_synthetic_inputs.py --start 2026-01-01 --end 2026-01-31
```

Per defecte s'escriuen a:

```text
data/synthetic/input/
```

I es poden processar amb:

```bash
python run_pipeline.py --sample --all --sample-input-dir data/synthetic/input --sample-output-dir data/synthetic/output
```

Aquestes carpetes son locals i ignorades per Git.

## Mode production

Fonts configurades per defecte:

| Pipeline | Taula | Data | Altres columnes necessaries |
| --- | --- | --- | --- |
| Demanda | `z_inv.P1038_visites` | `DATA_VISITA` | `UP`, `VISI_LLOC_VISITA`, `VISI_SITUACIO_VISITA`, `SERVEI_CODI`, `TIPUS_CLASS`, `VISI_TIPUS_VISITA` |
| Diagnostics | `z_inv.P1038_prstb015r_filtrat` | `data_visita` | `up_c`, `problema_salut_c` |

El pipeline de diagnostics consulta dades ja agregades en SQL per dia, UP i prefix ICD10 de tres caracters. Aixo redueix memoria i ample de transferencia.

## Excel UP-RS

El repo inclou `UPperRS.xlsx` com a mapping de referencia. El fitxer indicat per `UP_RS_FILE` ha de tenir:

| Columna | Exemple | Descripcio |
| --- | --- | --- |
| `Codi UP` | `00348` | Codi UP normalitzat a 5 digits. |
| `RS` | `CATALUNYA CENTRAL` | Regio sanitaria font. |

El full esperat es `UP per RS`.

## Seleccions

Els fitxers versionats a `selections/` controlen quines columnes territorials i diagnostics es generen. Els totals generals continuen calculant-se amb totes les files encara que hi hagi seleccions.

Consulta [Seleccions i naming](selections-and-naming.md).
