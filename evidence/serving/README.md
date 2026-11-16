# Evidencia de serving en AstraDB — 16/11/2026

Se cargaron **11.050 filas** de una publicación Gold en AstraDB. Se
releyeron **524 particiones** y se cotejó cada fila con Parquet antes
de marcar `complete`. Coincidieron decimales, nulos, ceros y coberturas.
El resultado incluye seis costos cero y 213 costos parciales.
[Esquema](../../config/finops-serving.cql) e [informe de carga](first.json).

El reintento verificó las mismas filas y particiones con cero upserts.
[Informe del reintento](repeat.json).

## Consultas verificadas

Antes de consultar se verifica el estado en `finops_publications`.
Ambas consultas fijan el ID de publicación, la organización sintética y
`compute`. [CQL, parámetros y resultados comparados con Gold](queries.json).

**Rango inclusivo del 29/07 al 04/08/2025.** El driver leyó una partición
de julio y otra de agosto con `event_date >= ? AND event_date <= ?`.

| Fecha UTC | Costo USD | Requests | Cobertura de costo |
| --- | ---: | ---: | --- |
| 2025-07-29 | 46,1992 | 503 | Completa |
| 2025-07-30 | 60,1359 | 603 | Completa |
| 2025-07-31 | 54,6660 | 615 | Completa |
| 2025-08-01 | 35,5324 | 363 | Completa |
| 2025-08-02 | 1,9700 | Nulo | Completa |
| 2025-08-03 | 40,4290 | 506 | Parcial |
| 2025-08-04 | 225,2637 | 112 | Completa |

**Detalle del 03/08/2025.** Con `event_date = ?` se leyó una fila de agosto.
El costo de 40,4290 USD usa cinco aportes válidos de seis. Un aporte queda
en quarantine y la cobertura es `partial`. La fila tiene 506 requests,
11,452 GB-horas y 0,12949 kg de carbono. CPU y tokens quedan nulos.

## Reproducción

```sh
GOLD_ID=ad9dcfe3db7eda46accbaa3c8a384b11c44589472ae8f13fd91d6c9efa931d52
docker compose run --rm --no-deps -e SERVING_MODE=astra runtime python scripts/serve_finops.py load --gold-publication-id "$GOLD_ID" --report-name first
docker compose run --rm --no-deps -e SERVING_MODE=astra runtime python scripts/serve_finops.py load --gold-publication-id "$GOLD_ID" --report-name repeat
docker compose run --rm --no-deps -e SERVING_MODE=astra runtime python scripts/serve_finops.py query --gold-publication-id "$GOLD_ID" --org-id org_he7ur23g --service compute --start 2025-07-29 --end 2025-08-04 --detail 2025-08-03 --report-name queries
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

En el mismo destino, la primera carga devuelve `loaded` y el reintento
`reused`. En otro volumen, usar el ID de su informe Gold.

Se probaron interrupciones, aislamiento, filas inesperadas y preservación
de tipos con datos sintéticos. En Astra se cargó una sola publicación real.
[Suite de 28 pruebas, tres de serving](tests.log).
