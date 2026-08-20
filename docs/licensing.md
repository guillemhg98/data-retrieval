# Llicencies i publicacio

## Estat actual

Aquest repo no declara cap llicencia open source. Aixo es intencionat: escollir una llicencia es una decisio legal i de projecte.

Abans de publicar a GitHub, decideix una d'aquestes opcions:

| Opcio | Efecte |
| --- | --- |
| Sense llicencia | El codi es visible, pero no queda concedit dret public de copia, modificacio o redistribucio. |
| MIT | Permissiva, simple, habitual per eines internes que es volen compartir. |
| Apache-2.0 | Permissiva, inclou clausula explicita de patents. |
| GPL-3.0 | Copyleft: derivats distribuïts han de mantenir la mateixa llicencia. |

Quan ho decideixis, afegeix un fitxer `LICENSE` a l'arrel.

## Dades

No publiquis:

- dades reals de pacients o visites;
- sortides Parquet/CSV generades a partir de dades reals;
- `.env`, credencials, tokens o connection strings;
- Excels locals no declarats;
- logs amb fragments de dades o errors de connexio sensibles.

El repo nomes conserva:

- fixtures sintetiques petites a `data/sample/input/`;
- plantilles de seleccio a `selections/`;
- mapping de referencia `UPperRS.xlsx`;
- codi i tests.

## Dependencies

Les dependencies Python s'instal.len des de `requirements.txt`. Abans de fer una release publica, genera un informe amb:

```bash
pip install pip-licenses
pip-licenses --format=markdown --with-urls --output-file THIRD_PARTY_LICENSES.md
```

Aixo crea un inventari de llicencies efectives segons les versions instal.lades al teu entorn.

## Checklist abans de publicar

- `git status` no mostra `.env`, Parquet, Excels reals ni dades reals.
- `python run_pipeline.py --sample --all` funciona en un clone net.
- `python -m pytest -q` passa.
- El README apunta al repo `data-retrieval`.
- Has triat o documentat explicitament la llicencia del codi.
