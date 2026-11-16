# Evidencia de streaming — 16/11/2026

Sesión validada: `usage-v1`, Spark 3.5.8, watermark de 60 días y máximo 20
archivos por trigger. Los JSONL originales se ofrecieron en dos tandas de 60
archivos. Cada comando inició procesos Spark nuevos con los mismos checkpoints.

| Ejecución | Archivos disponibles | Eventos acumulados en Bronze | Candidatos acumulados |
| --- | ---: | ---: | ---: |
| [Primera](first.json) | 60 | 21.600 | 21.600 |
| [Reanudación](resumed.json) | 120 | 43.200 | 43.200 |
| [Repetición sin novedades](repeat.json) | 120 | 43.200 | 43.200 |

Los IDs de las dos queries coinciden entre las tres ejecuciones. La repetición
registra cero filas nuevas de entrada en ambas consultas. La auditoría final
coincide entre reanudación y repetición. Los 120 hashes de entrada coinciden
con el perfilado original de la primera entrega.

El resultado contiene 10.800 eventos v1 y 32.400 v2; no hubo errores de parseo,
IDs conflictivos, repeticiones equivalentes ni contribuciones elegibles sin
emitir en este paquete. Esto no valida calidad de negocio: negativos, nulos y
cobertura parcial de medidas se conservan para Silver. Los campos numéricos
aceptan también la notación científica que utiliza el lector JSON de Spark.

## Pruebas con casos que no están en la muestra

El [log](tests.log) registra 17 pruebas aprobadas: 13 de entorno/batch y cuatro
de streaming. Las nuevas pruebas verifican:

- Superset v1/v2, números como texto y en notación científica, equivalencia
  UTC de offsets, preservación de JSON malformado y versiones desconocidas.
- Replay sin cambios en el archivo y rechazo de modificaciones en copias ya
  ofrecidas o identificadores de sesión que escapen del directorio asignado.
- Reinicio con los mismos IDs de query, deduplicación y recuperación tras una
  interrupción simulada entre el archivo completo y la consulta deduplicada.
- Con watermark de un día, un evento dentro del margen se acepta y un evento
  demasiado viejo se descarta del candidato, pero sigue en Bronze. Las métricas
  de Spark confirman un descarte por watermark. Una variante contradictoria
  queda preservada y el ID completo se excluye del conteo sin conflictos.
- Un lote compuesto sólo por registros inválidos conserva ambos registros y
  genera cero candidatos. Se bloquea reiniciar una salida cuyo checkpoint falta.

El caso integrado termina con ocho filas archivadas, tres candidatos, un ID
conflictivo y dos candidatos sin conflictos. Dos contribuciones elegibles no
fueron emitidas: el evento tardío y la variante contradictoria. Estos datos son
sintéticos y no se mezclan con la sesión del paquete real.

## Reproducción

```sh
docker compose run --rm runtime python scripts/replay_usage.py --limit 60
docker compose run --rm runtime python scripts/ingest_stream.py --report-name first
docker compose run --rm runtime python scripts/replay_usage.py
docker compose run --rm runtime python scripts/ingest_stream.py --report-name resumed
docker compose run --rm runtime python scripts/ingest_stream.py --report-name repeat
docker compose run --rm runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

Si `usage-v1` ya está consumida, los comandos retoman su estado sin volver a
sumar datos. Para repetir la demostración desde cero usar otro `--stream-id`
en replay e ingesta; no eliminar checkpoints de salidas existentes. El replay
publica copias completas, sin ordenar timestamps. Su informe más reciente
es [replay-usage-v1.json](replay-usage-v1.json).

Los archivos de Bronze, candidatos, checkpoints y auditorías están en el volumen
Docker, fuera de Git. Los informes contienen hashes y métricas, no eventos raw.
Esta etapa no acredita latencia continua de 60 segundos, un detector de anomalías,
Silver, quarantine de negocio, agregados ni carga de marts a Astra.
