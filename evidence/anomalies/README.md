# Evidencia de anomalías de costo — 16/11/2026

Spark 3.5.8 en Docker evaluó una [publicación Gold](../gold/first.json)
con 60 fechas UTC. Se usó una ventana de 28 días anteriores, siete
observaciones mínimas y el umbral 3,5 del score z modificado unilateral.

| Resultado | Filas |
| --- | ---: |
| Total del mart | 11.050 |
| Evaluables | 9.003 |
| Por encima del umbral | 243 |
| En el umbral o por debajo | 8.760 |
| No evaluables | 2.047 |
| Costo parcial | 213 |
| Historial insuficiente | 1.834 |

En este corte no hubo bloqueos por MAD cero ni exclusiones sin ubicación.
Las 243 señales son casos para investigar. El dataset carece de etiquetas
para estimar precisión, recall o calibrar el umbral.

## Verificación y reproducción

La [primera corrida](first.json) creó el mart y la [segunda](repeat.json)
reutilizó la publicación con el mismo manifiesto. La relectura comprobó
11.050 claves únicas, costos y coberturas idénticos a Gold y cero diferencias
en el conteo histórico al contrastarlo con un join por días calendario.
Se verificaron también los checksums de Gold después de la ejecución.
[Resultados de la relectura](checks.json).

El [log](tests.log) registra 25 pruebas aprobadas. Las pruebas de esta
sección cubren un pico conocido, independencia de fechas futuras, el
límite de la ventana, días faltantes, ceros válidos, cobertura parcial,
historial insuficiente, MAD cero, umbral estricto, exclusiones sin ubicación,
configuración, reejecución y detección de archivos Gold alterados.

```sh
GOLD_ID=ad9dcfe3db7eda46accbaa3c8a384b11c44589472ae8f13fd91d6c9efa931d52
docker compose run --rm --no-deps runtime python scripts/build_cost_anomalies.py --gold-publication-id "$GOLD_ID" --report-name first
docker compose run --rm --no-deps runtime python scripts/build_cost_anomalies.py --gold-publication-id "$GOLD_ID" --report-name repeat
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

En un volumen distinto, usar el `manifest.publication_id` de su informe
de Gold. La evidencia versionada contiene métricas agregadas y metadatos
de publicación.
