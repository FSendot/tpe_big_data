# Evidencia de Silver — 16/11/2026

Spark 3.5.8 en Docker procesó los seis Parquet comprometidos en el archivo
Bronze de `usage-v1` y un único snapshot batch. Los hashes de los 120 JSONL
y cuatro CSV coinciden con el ZIP del campus. La relectura del archivo
Bronze también verificó los 120 pares de archivo y hash de origen.
[Verificaciones](checks.json).

| Salida | Filas | Bytes en el volumen, incluidos metadatos de Spark |
| --- | ---: | ---: |
| Eventos Silver | 43.200 | 8.115.631 |
| Organizaciones Silver | 80 | 19.078 |
| Recursos Silver | 400 | 46.361 |
| Quarantine | 3.131 | 499.579 |

Silver tiene 43.200 IDs únicos (10.800 v1 y 32.400 v2). Quarantine tiene
216 incidencias de costo negativo, 2.038 de unidad ausente con valor
presente y 877 de valor ausente. Son 3.122 IDs que también están en Silver,
con nueve casos de incidencia en costo y consumo. De los 2.915 eventos con
incidencia de consumo, 2.906 conservaron el costo. Dos filas se conservaron
para cobertura con todas sus medidas inutilizables o ausentes. El corte
real pasó las validaciones de identidad y maestros.

| Medida | Válida | En quarantine | Ausente |
| --- | ---: | ---: | ---: |
| Costo | 42.984 | 216 | 0 |
| Consumo | 40.285 | 2.915 | 0 |
| Carbono | 32.400 | 0 | 10.800 |
| Tokens | 3.132 | 0 | 40.068 |

Las columnas de consumo tienen 18.215 valores de requests, 9.995 de CPU y
12.075 de almacenamiento. Los ejemplos de quarantine sin valores de origen
se pueden verificar en `manifest.quality.quarantine_reason_combinations`
del [informe de ejecución](first.json). Los detalles de origen y payloads
permanecen en Parquet en el volumen.

## Reejecución y pruebas

La [primera ejecución](first.json) creó la publicación y la
[segunda](repeat.json) verificó y reutilizó el mismo ID. Se comprobó
igualdad del manifiesto completo, incluidos conteos y hashes de todos los
archivos publicados.

El [log de pruebas](tests.log) registra 18 pruebas aprobadas. El caso
integrado de Silver usa un Lake temporal y dos micro-lotes sintéticos.
Verifica la recuperación de un evento excluido por watermark, la exclusión
de un Parquet sin commit, el rechazo de un corte anterior al último micro-lote,
la resolución de repeticiones equivalentes, las contradicciones por medida y
por dimensiones, el enriquecimiento y los motivos y linaje de las incidencias.
Simula un fallo tras escribir las tablas y antes de promoverlas, verifica
el reintento y comprueba que la verificación detecta un Parquet alterado.

```sh
docker compose run --rm --no-deps runtime python scripts/build_silver.py --stream-id usage-v1 --inspect-cut
BATCH_ID=bbdccc35eab9288ce7d018bca0ae19440c9d551ec18e55a882617b7336851dc4
CUT_ID=7eb4fadcd7f2ca4d9748439a4fb72782eb17b419a32e56a82ad35dc448b50cc2
docker compose run --rm --no-deps runtime python scripts/build_silver.py --stream-id usage-v1 --batch-id "$BATCH_ID" --archive-cut "$CUT_ID" --report-name first
docker compose run --rm --no-deps runtime python scripts/build_silver.py --stream-id usage-v1 --batch-id "$BATCH_ID" --archive-cut "$CUT_ID" --report-name repeat
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

Los IDs del ejemplo corresponden al volumen validado. En otro volumen
usar el `batch_id` de su ingesta batch y el `cut_id` de su inspección.
