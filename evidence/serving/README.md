# Evidencia de serving en AstraDB — 16/11/2026

Se usó el keyspace Astra configurado en la sección 1 y el
[esquema versionado](../../config/finops-serving.cql). La
[carga real](first.json) escribió **11.050 filas** de una publicación Gold.
Se releyeron **524 particiones** y se comparó cada fila con el Parquet
antes de registrar `complete`. Coincidieron los decimales, nulos, ceros y
coberturas, incluidos seis costos cero y 213 costos parciales.
La [reejecución](repeat.json) verificó las mismas filas y particiones,
con cero upserts adicionales.

## Consultas verificadas

El [informe real](queries.json) incluye las sentencias preparadas, sus
parámetros y los resultados CQL comprobados contra Gold. Ambas consultas
usan el ID explícito de la publicación, una organización sintética y
`compute`. El comando comprueba primero el estado en `finops_publications`.

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

**Detalle del 03/08/2025.** Con `event_date = ?` se recuperó una fila de la
partición de agosto. El costo de 40,4290 USD usa cinco aportes válidos de
seis elegibles. Un aporte está en quarantine y la cobertura es `partial`.
La fila conserva 506 requests, 11,452 GB-horas, 0,12949 kg de carbono y
valores nulos para CPU y tokens.

## Reproducción

```sh
GOLD_ID=ad9dcfe3db7eda46accbaa3c8a384b11c44589472ae8f13fd91d6c9efa931d52
docker compose run --rm --no-deps -e SERVING_MODE=astra runtime python scripts/serve_finops.py load --gold-publication-id "$GOLD_ID" --report-name first
docker compose run --rm --no-deps -e SERVING_MODE=astra runtime python scripts/serve_finops.py load --gold-publication-id "$GOLD_ID" --report-name repeat
docker compose run --rm --no-deps -e SERVING_MODE=astra runtime python scripts/serve_finops.py query --gold-publication-id "$GOLD_ID" --org-id org_he7ur23g --service compute --start 2025-07-29 --end 2025-08-04 --detail 2025-08-03 --report-name queries
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

Los estados `loaded` y `reused` corresponden a la primera carga y a sus
reintentos en el mismo destino. En otro volumen, usar el ID de su informe
Gold. La [suite de 28 pruebas](tests.log) incluye tres pruebas de serving.
Los escenarios de interrupción, aislamiento entre publicaciones, detección
de filas inesperadas y preservación de tipos se probaron con datos
sintéticos. En Astra se cargó una sola publicación real.
