# Validación integrada — 16/11/2026

Se usó el dataset configurado en Docker y el destino Astra real. El Lake
de validación quedó en `/lake/integration/section-7-v1/lake`, con sesión
`integration-section-7-v1`. Los informes completos de los jobs quedaron
en el volumen, bajo `runs/<informe>/job-reports` del directorio de
validación. Los [informes compactos](first.json) conservan los comandos,
IDs y hashes que encadenan las etapas.

## Datos reales

| Contrato | Resultado |
| --- | ---: |
| Bronze batch | 1.520 filas en cuatro fuentes |
| Bronze streaming | 120 JSONL y 43.200 eventos archivados |
| Silver | 43.200 eventos, 80 organizaciones y 400 recursos con claves únicas |
| Quarantine | 3.131 incidencias |
| Gold FinOps | 11.050 claves únicas y seis medidas conciliadas con Silver |
| Anomalías | 11.050 claves únicas, 9.003 evaluadas y 243 señales |
| Astra | 11.050 filas leídas en 524 particiones e iguales a Gold |
| Consultas CQL | Siete filas del rango y una fila del detalle, iguales a Gold |

La consulta de detalle devolvió 40,4290 USD y 506 requests el 03/08/2025.
El costo quedó parcial con cinco aportes válidos de seis elegibles.
El [segundo intento](repeat.json) conservó los IDs de queries y publicación,
los hashes de manifiesto y el hash de los resultados CQL. Hubo cero
archivos nuevos, cero filas nuevas para ambas queries y cero upserts.

La [comparación de arranque limpio](fresh-comparison.json) verificó los
manifiestos y archivos de Gold y anomalías de las secciones anteriores y
de esta validación. Con los IDs de linaje excluidos de la comparación,
`exceptAll` en ambos sentidos devolvió cero diferencias. Las publicaciones
de una corrida nueva tienen otros IDs y el mismo contenido de negocio.

## Recuperación verificada

| Destino | Interrupción y resultado |
| --- | --- |
| Streaming sintético | Se interrumpió la ejecución después de archivar un nuevo evento y antes de actualizar los candidatos. El reintento conservó los dos IDs de query. La primera query recibió cero filas nuevas y la segunda procesó la fila pendiente. |
| Silver sintético | Se inyectó un fallo en el rename de promoción. El directorio publicado quedó vacío y el reintento creó y verificó la salida. |
| Astra real | Se confirmó y releyó una fila antes de interrumpir la carga de la publicación aislada. El marcador quedó `loading` con conteo verificado nulo y el comando oficial devolvió `publication_not_ready`. El reintento verificó 11.050 filas, pasó a `complete` y habilitó las consultas. |

Los fallos se inyectaron en puntos concretos del código. El
[log local](tests.log) registra 30 pruebas aprobadas y no incluye la
ejecución de Astra. La prueba real y su recuperación están en
[`first.json`](first.json).

## Comandos

```sh
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --validation-id section-7-v1 --report-name first --exercise-serving-recovery
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --validation-id section-7-v1 --report-name repeat --compare-to first
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

Para otra corrida desde cero, usar un ID de validación y nombres de informe
nuevos. El reintento debe usar el ID de la corrida inicial y su informe
como base.
