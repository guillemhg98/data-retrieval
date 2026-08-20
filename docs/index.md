# data-retrieval

Aquest projecte conte pipelines de recuperacio i transformacio de dades AQUAS/PREDAP.

Objectiu del repo:

- poder clonar-lo i provar-lo localment amb dades sintetiques;
- poder configurar-lo contra SQL Server / Azure Synapse;
- mantenir fora de Git les dades reals i les sortides generades;
- documentar formats, ordre d'execucio, naming, sortides i criteris de llicencia.

Entrada principal:

```bash
python run_pipeline.py
```

`run_pipeline.py` delega a `run_pipeline_optimized.py`, que conte el CLI actiu.

## Lectura recomanada

1. [Analisi funcional](function-analysis.md)
2. [Politica de dades](data-policy.md)
3. [Git workflow](git-workflow.md)
4. [Instal.lacio](installation.md)
5. [Dades d'entrada](input-data.md)
6. [Execucio](execution.md)
7. [Sortides](outputs.md)
8. [Seleccions i naming](selections-and-naming.md)
9. [Llicencies i publicacio](licensing.md)
10. [Manteniment](maintenance.md)
