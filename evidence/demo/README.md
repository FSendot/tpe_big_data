# Muestra reducida y reinicio — 16/11/2026

Validación con datos sintéticos y AstraDB real para el ID `repro-demo-01`.
La [entrada](dataset.json) identifica la semilla fija y los SHA-256 de los
cuatro CSV y dos JSONL. No se copiaron filas de Landing. La [suite de
pruebas](tests.log) generó la muestra en dos directorios independientes y
comparó bytes, manifiestos y contratos.

| Control | Resultado esperado y observado |
| --- | ---: |
| Bronze batch | 6 filas en cuatro fuentes |
| Bronze streaming | 17 filas archivadas, 16 IDs únicos |
| Silver | 16 eventos, una organización y tres recursos |
| Quarantine | 2 incidencias, costo negativo y unidad ausente |
| Gold y Astra | 14 filas iguales en 4 particiones CQL |
| Anomalías | 4 evaluables, 1 señal y 10 no evaluables |
| Consultas de rango y detalle | 6 filas y 1 fila, respectivamente |

Los [casos observados](checks.json) incluyen el pico de 100 USD el
01/08/2025 (ocho observaciones históricas), el costo cero con historial
insuficiente y el costo parcial sin score. El detalle del 02/08 devolvió
10 USD y 112 requests, con un aporte de costo válido de dos elegibles.
La conciliación de seis medidas y ambas consultas están en la
[primera corrida](first.json).

El [reintento](repeat.json) mantuvo los IDs, manifiestos y resultados,
con cero archivos nuevos, cero filas reingeridas y cero upserts.
El plan se validó sobre el propio estado descartable. Se rechazaron un
ID con traversal y el ID de una integración ajena. La aplicación retiró
el directorio `work`; se cotejaron los hashes de los 127 archivos de Landing
original y de evidencia, checkpoint y manifiesto de la corrida real. El
dataset y los informes compactos de demo se conservaron. Se comprobó que
la publicación de demo anterior seguía en `complete` en Astra y devolvía
el mismo detalle.

El [arranque posterior](restart.json) creó checkpoints y publicaciones
nuevos a partir del mismo manifiesto de entrada. Reprodujo los conteos,
la conciliación, los motivos de evaluación y ambas consultas con otro
ID Gold. La limpieza es local; las publicaciones de ambas corridas
siguen en Astra. Este ensayo cubre una muestra fija, con una señal
construida, y se conserva separado del [dataset completo](../integration/README.md).

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

Elegir un ID nuevo para repetir la prueba sin afectar el workspace
registrado. Ver [muestra reducida y reinicio seguro](../../docs/segunda-entrega.md#muestra-reducida-y-reinicio-seguro).
