# Validación integrada — 16/11/2026

Se validó el dataset del campus en Docker con AstraDB. La sesión
`integration-section-7-v1` usa `/lake/integration/section-7-v1/lake`.
Los informes completos quedan bajo `runs/<informe>/job-reports` dentro
del directorio de validación. [Comandos, IDs y hashes](first.json).

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

El detalle del 03/08/2025 devolvió 40,4290 USD y 506 requests. El costo
quedó parcial, con cinco aportes válidos de seis elegibles.

El reintento conservó los IDs de consultas y publicación y los hashes de
manifiestos y resultados CQL. Hubo cero archivos nuevos, filas nuevas
y upserts. [Informe del reintento](repeat.json).

En el arranque limpio se verificaron los manifiestos y archivos de Gold y
anomalías. Al excluir los IDs de linaje, `exceptAll` devolvió cero
diferencias en ambos sentidos. [Comparación entre publicaciones](fresh-comparison.json).

## Recuperación verificada

| Destino | Interrupción y resultado |
| --- | --- |
| Streaming sintético | Se interrumpió la ejecución después de archivar un nuevo evento y antes de actualizar los candidatos. El reintento conservó los dos IDs de query. La primera query recibió cero filas nuevas y la segunda procesó la fila pendiente. |
| Silver sintético | Se inyectó un fallo en el rename de promoción. El directorio publicado quedó vacío y el reintento creó y verificó la salida. |
| Astra real | Se confirmó y releyó una fila antes de interrumpir la carga de la publicación aislada. El marcador quedó `loading` con conteo verificado nulo y el comando oficial devolvió `publication_not_ready`. El reintento verificó 11.050 filas, pasó a `complete` y habilitó las consultas. |

Se inyectaron fallos en las etapas indicadas. El log local registra
30 pruebas aprobadas. La prueba de Astra se registra en el informe de la
corrida. [Log local](tests.log) e [informe integrado](first.json).

## Comandos

```sh
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --validation-id section-7-v1 --report-name first --exercise-serving-recovery
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --validation-id section-7-v1 --report-name repeat --compare-to first
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

Usar un ID de validación y nombres de informe nuevos para empezar de cero.
Para reintentar, usar el ID de la primera corrida y su informe como base.
