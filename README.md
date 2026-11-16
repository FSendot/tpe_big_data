# Cloud Provider Analytics

Proyecto integrador de Big Data, ITBA, segundo cuatrimestre de 2026.

Integrantes: Francisco Sendot y Santos Galarraga.
Francisco Sendot hizo el aporte principal al diseño, la documentación y la
evidencia reproducible de la primera entrega.

Se ingieren CSV y JSONL con PySpark en Docker. Bronze, Silver, quarantine
y Gold quedan en Parquet local. Se calculan señales de costo y se publica
el mart diario de FinOps en AstraDB. Para este proyecto usamos Docker
como entorno equivalente a Colab.

La [segunda entrega](docs/segunda-entrega.md) describe el flujo implementado
y los resultados. La [primera entrega](docs/primera-entrega.md) conserva el
diseño histórico con HDFS, operación horaria y reportes HTML.

## Artefactos de la primera entrega

| Artefacto | Ubicación |
| --- | --- |
| Documento de diseño | [Primera entrega](docs/primera-entrega.md) |
| Diagrama de arquitectura v1 | [Arquitectura](docs/primera-entrega.md#arquitectura-v1) |
| Matriz requisito–componente | [Matriz y relación con las 5V](docs/primera-entrega.md#matriz-requisitocomponente) |
| Plan inicial | [Supuestos, riesgos, roles, recursos y esfuerzo](docs/primera-entrega.md#plan-inicial) |
| Registro de decisiones | [Decisiones y tradeoffs](docs/primera-entrega.md#registro-de-decisiones) |
| Datos y evidencia | [Acceso y reproducción](data/README.md), [resultado de la lectura](evidence/README.md) |

## Estructura

```text
docs/                     Diseño histórico y documentación del MVP
src/cloud_analytics/      Ingesta, transformaciones, analítica y serving
scripts/                  Comandos por etapa y validación integrada
config/                   Runtime y esquema CQL
tests/                    Pruebas de calidad, publicación y recuperación
data/README.md            Obtención y uso del paquete del campus
evidence/                 Resultados verificables por etapa
CONTEXT.md                Estado técnico y diseño productivo propuesto
```

## Reproducir la exploración

Descargar `cloud_provider_challenge_dataset_v1` del campus. Extraerlo fuera
del repositorio y ejecutar desde la raíz con Python 3.9 o posterior.

```sh
python3 scripts/profile_data.py /ruta/cloud_provider_challenge_dataset_v1 --output evidence/profile-local.json
```

El script verifica las ocho fuentes en solo lectura. La salida guarda
estadísticas y hashes. [Instrucciones de datos](data/README.md).

## Quickstart

1. Abrir Docker Desktop en modo Linux con Compose v2. Copiar `.env.example`
   a `.env` si el archivo falta. Apuntar `DATASET_PATH` al paquete extraído.
   [Puesta en marcha](docs/segunda-entrega.md#puesta-en-marcha).
2. Preparar un keyspace en Astra. Guardar el token y el bundle en la
   configuración local. [Configuración Astra](docs/segunda-entrega.md#preparar-astradb).
3. Construir el runtime desde la raíz. Usar un ID y un nombre de informe
   libres. El comando carga el mart y verifica las dos consultas en Astra.

```sh
docker compose build runtime
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --validation-id cierre-01 --report-name cierre-first
```

- Chequeo del entorno, prueba y reintento. [Guion de demo](docs/segunda-entrega.md#guion-de-demo).
- Recorrido por etapa y resultados. [Arquitectura](docs/segunda-entrega.md#arquitectura-ejecutada)
  y [evidencia integrada](evidence/integration/README.md).
- Requisitos verificados y tareas pendientes. [Matriz de cumplimiento](docs/segunda-entrega.md#matriz-de-cumplimiento)
  y [backlog de la final](docs/segunda-entrega.md#backlog-para-la-entrega-final).

### Demo reducida

Construir el runtime y configurar Astra antes de ejecutar desde la raíz.
La muestra y sus informes quedan en `/lake/demos/finops-demo/`.

```sh
docker compose run --rm --no-deps runtime python scripts/demo_sample.py generate --demo-id finops-demo
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --demo-id finops-demo --report-name first
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --demo-id finops-demo --report-name repeat --compare-to first
```

Usar un ID nuevo para empezar otra demo. Para reiniciar el mismo ID, usar
la limpieza local. [Plan y comandos de limpieza](docs/segunda-entrega.md#muestra-reducida-y-reinicio-seguro).

## Convenciones del repositorio

- Usar `snake_case` y conservar los identificadores de origen.
- Usar UTC para eventos y agregaciones. Separar costos de uso y revenue.
- Versionar código, diseño y evidencia agregada. Mantener fuera de Git los
  originales, credenciales, entornos, checkpoints y salidas del Lake.
- Registrar las decisiones en la entrega y las notas internas en `CONTEXT.md`.
