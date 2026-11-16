# Evidencia batch a Bronze — 16/11/2026

Se ejecutó `scripts/ingest_batch.py` en el runtime Docker de la sección 1 con
los cuatro CSV del paquete original. No se modificaron las entradas.

| Fuente | Filas de entrada | Filas Bronze | Duplicados retirados | Filas con errores de conversión/clave |
| --- | ---: | ---: | ---: | ---: |
| customers_orgs | 80 | 80 | 0 | 0 |
| users | 800 | 800 | 0 | 0 |
| billing_monthly | 240 | 240 | 0 | 0 |
| resources | 400 | 400 | 0 | 0 |
| Total | 1.520 | 1.520 | 0 | 0 |

Los [resultados iniciales](first.json) registran `created`; la
[segunda ejecución](repeat.json), `reused`. Sus manifiestos son idénticos,
incluidos batch_id, ingest_ts, conteos, esquemas y checksums de las salidas.
Los cuatro hashes de entrada coinciden con `evidence/profile.json`.
Cada fuente se escribió en un archivo Parquet particionado por fecha de ingesta
y se verificó igualdad de registros antes y después de leerlo.

La ausencia de errores de conversión no implica calidad de negocio completa:
Bronze conserva subtotales negativos, créditos vacíos y otros nulos. Su
tratamiento y las relaciones entre fuentes corresponden a Silver.

## Reproducir

```sh
docker compose run --rm runtime python scripts/ingest_batch.py --report-name first
docker compose run --rm runtime python scripts/ingest_batch.py --report-name repeat
docker compose run --rm runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

En un volumen que ya contiene este lote, ambas ejecuciones devolverán `reused`.
No eliminar el Lake sólo para obtener `created`: las pruebas usan directorios
temporales aislados y demuestran creación y recuperación sin tocar los originales.

El [log de pruebas](tests.log) registra 13 pruebas aprobadas: las siete del
entorno y seis de batch. Incluyen duplicados exactos y contradictorios, claves
ausentes, errores de tipos, decimales fuera de precisión, negativos y vacíos,
CSV mal estructurado y campos entrecomillados/multilínea. La prueba integrada
simula un fallo de publicación, recupera el lote, verifica la reejecución, crea
un snapshot distinto y detecta una salida alterada. Se suprimen únicamente los
ResourceWarning de sockets internos de PySpark en este comando de pruebas.

Esta evidencia no valida streaming, reglas Silver, quarantine, Gold ni carga
de marts en AstraDB. Los archivos raw y tipificados permanecen en el volumen
Docker; Git contiene sólo esquemas, hashes, conteos y pruebas con datos sintéticos.
