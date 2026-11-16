# Muestra reducida y reinicio — 16/11/2026

Se validó `repro-demo-01` con datos sintéticos en AstraDB. El manifiesto
registra las fechas y los casos de la muestra, junto con los SHA-256
de los cuatro CSV y dos JSONL.
[Entrada de la demo](dataset.json).
Se generó la muestra en dos directorios y se compararon los bytes, los
manifiestos y los contratos. [Pruebas](tests.log).

| Control | Resultado esperado y observado |
| --- | ---: |
| Bronze batch | 6 filas en cuatro fuentes |
| Bronze streaming | 17 filas archivadas, 16 IDs únicos |
| Silver | 16 eventos, una organización y tres recursos |
| Quarantine | 2 incidencias, costo negativo y unidad ausente |
| Gold y Astra | 14 filas iguales en 4 particiones CQL |
| Anomalías | 4 evaluables, 1 señal y 10 no evaluables |
| Consultas de rango y detalle | 6 filas y 1 fila, respectivamente |

El pico del 01/08/2025 fue de 100 USD y se evaluó con ocho observaciones.
El costo cero quedó sin evaluar por historial insuficiente. El detalle
del 02/08 devolvió 10 USD y 112 requests. El costo quedó parcial, con un
aporte válido de dos elegibles, y quedó sin score. [Casos observados](checks.json)
y [conciliación de seis medidas y consultas](first.json).

El reintento mantuvo los IDs, manifiestos y resultados. Hubo cero archivos
nuevos, cero filas reingeridas y cero upserts. [Informe del reintento](repeat.json).

La limpieza retiró el directorio `work` de la demo. Rechazó una ruta con
`../` y el ID de una integración ajena. Se cotejaron los hashes de los
127 archivos de Landing, además de la evidencia, el checkpoint y el
manifiesto de la corrida real. Se conservaron el dataset y los informes
compactos de demo. La publicación previa seguía en `complete` en Astra
y devolvió el mismo detalle. [Comprobaciones](checks.json).

El arranque posterior usó el mismo manifiesto. Creó checkpoints y
publicaciones con IDs nuevos. Se repitieron los conteos, la conciliación,
los motivos de evaluación y ambas consultas. Las dos publicaciones siguen
en Astra. [Informe del reinicio](restart.json) y
[evidencia del dataset completo](../integration/README.md).

## Comandos

```sh
docker compose run --rm --no-deps runtime python scripts/demo_sample.py generate --demo-id repro-demo-01
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --demo-id repro-demo-01 --report-name first
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --demo-id repro-demo-01 --report-name repeat --compare-to first
docker compose run --rm --no-deps runtime python scripts/demo_sample.py clean --demo-id repro-demo-01 --dry-run
docker compose run --rm --no-deps runtime python scripts/demo_sample.py clean --demo-id repro-demo-01 --apply
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --demo-id repro-demo-01 --report-name restart
docker compose run --rm --no-deps runtime python -W ignore::ResourceWarning -m unittest discover -s tests -v
```

Usar un ID nuevo para empezar otra demo.
[Procedimiento de reinicio](../../docs/segunda-entrega.md#muestra-reducida-y-reinicio-seguro).
