# Evidencia de Gold — 16/11/2026

Spark 3.5.8 en Docker publicó `org_daily_usage_by_service` desde el
[snapshot real de Silver](../silver/first.json). La relectura encontró
**11.050 claves únicas**, 80 organizaciones, seis servicios y 60 fechas UTC
entre el 3 de julio y el 31 de agosto de 2025. El mart ocupa 2.630.976 bytes
de datos Parquet. La comparación independiente por clave, de las seis
sumas y del conteo de eventos, encontró cero diferencias.
[Verificación de la relectura](checks.json).

## Conciliación

Los conteos de aportes válidos y los totales coincidieron exactamente.
Los grupos Gold representan los 43.200 eventos Silver.

| Medida | Aportes válidos en cada capa | Total Silver | Total Gold |
| --- | ---: | ---: | ---: |
| Costo incremental (USD) | 42.984 | 148.351,6009 | 148.351,6009 |
| Requests | 18.215 | 2.186.404 | 2.186.404 |
| CPU (horas) | 9.995 | 18.880,2712 | 18.880,2712 |
| Almacenamiento (GB-horas) | 12.075 | 124.661,374 | 124.661,374 |
| Carbono (kg) | 32.400 | 367,502378 | 367,502378 |
| Tokens | 3.132 | 2.558.359 | 2.558.359 |

Hubo 213 grupos con costo parcial y 2.790 con carbono ausente. Los 2.915
eventos con incidencias de consumo tenían una métrica identificable para su
denominador. El corte real tenía cero exclusiones de evento completo.
Los estados de cobertura y los decimales a escala completa están en el
[manifiesto del informe](first.json).

## Reproducción

```sh
SILVER_ID=190b2e8ebbeb35f1eef60e5a45da48e343e0e119aab4451056862ffb0509daac
docker compose run --rm --no-deps runtime python scripts/build_gold.py --silver-publication-id "$SILVER_ID" --report-name first
docker compose run --rm --no-deps runtime python scripts/build_gold.py --silver-publication-id "$SILVER_ID" --report-name repeat
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

En un volumen distinto, usar el ID del informe de Silver de ese volumen.
La [primera ejecución](first.json) creó la publicación y la
[segunda](repeat.json) reutilizó el mismo ID con un manifiesto idéntico.
Pasaron las [20 pruebas](tests.log). Los casos sintéticos validan sumas
exactas y nulas, ceros válidos, cobertura parcial e incidencias ambiguas,
conflictos en varios grupos, variantes sin ubicación, reejecución, detección
de Parquet alterado y bloqueo por overflow decimal. Una suma de tokens
por encima de int64 conserva su valor exacto.
