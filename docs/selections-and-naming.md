# Seleccions i naming

## Naming canonical

Format general:

```text
{DOMAIN}__{VARIABLE}__{CATEGORY}
{DOMAIN}__{VARIABLE}__{CATEGORY}__RS__{GEO}
{DOMAIN}__{VARIABLE}__{CATEGORY}__UP__{GEO}
{DOMAIN}__TOTAL
{DOMAIN}__TOTAL__RS__{GEO}
{DOMAIN}__TOTAL__UP__{GEO}
```

Dominis:

- `DEMAND`
- `DIAGNOSIS`

Exemples:

```text
DEMAND__TOTAL
DEMAND__SERVEI_CODI__INF
DEMAND__TIPUS_VISITA_AGRUPAT__PRESENCIAL__RS__RS_64
DIAGNOSIS__TOTAL
DIAGNOSIS__ICD10_3__G01
DIAGNOSIS__ICD10_3__D08__UP__MICRO_08
```

## Seleccio de RS

`selections/selected_rs.csv`:

```csv
geo_id,RS
RS_64,GIRONA
RS_61,LLEIDA
```

- `RS` filtra el valor font.
- `geo_id` es el sufix estable al nom de columna.
- Si el fitxer falta o no te valors, s'inclouen totes les RS.

## Seleccio de UP

`selections/selected_up.csv`:

```csv
geo_id,UP,name
MICRO_01,00348,CAP Bages / Manresa
MICRO_08,06311,CUAP Cotxeres
```

- `UP` es normalitza a 5 digits.
- `geo_id` es el sufix estable al nom de columna.
- `name` es descriptiu i no entra al naming.

## Seleccio de diagnostics

`selections/selected_diagnosis_codes.csv`:

```csv
ICD10_3,feature_name,definition_ca
J00-J06,G01,Infeccions respiratories agudes
U07.1,D08,COVID-19
I10,D09,Hipertensio essencial
```

Regles:

- es llegeix el prefix ICD10 de 3 caracters;
- `U07.1` es normalitza a `U07`;
- rangs com `J00-J06` s'expandeixen;
- `feature_name` permet agrupar molts codis en una mateixa columna;
- un mateix codi pot contribuir a mes d'un grup si apareix en diverses files.

Diagnostics sempre calcula totals reals amb tots els codis:

```text
DIAGNOSIS__TOTAL
DIAGNOSIS__TOTAL__RS__RS_64
DIAGNOSIS__TOTAL__UP__MICRO_01
```

Les seleccions de diagnostics nomes limiten les columnes `DIAGNOSIS__ICD10_3__...`.
