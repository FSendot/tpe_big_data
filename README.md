# Cloud Provider Analytics

Proyecto integrador de Big Data, ITBA, segundo cuatrimestre de 2026.

Integrantes: Francisco Sendot y Santos Galarraga. La primera entrega contó
con la contribución principal de Francisco Sendot en el diseño, la documentación
y la evidencia reproducible. La segunda entrega desarrolla esa base del proyecto.

El MVP del segundo parcial ingiere CSV y JSONL con PySpark en Docker.
Publica Bronze, Silver, quarantine y Gold en Parquet sobre un volumen
local. Calcula señales estadísticas de costo y publica el mart diario
FinOps en AstraDB para consultas CQL. El usuario autorizó Docker como
entorno equivalente a Colab.

La [segunda entrega](docs/segunda-entrega.md) documenta el flujo ejecutado.
La [primera entrega](docs/primera-entrega.md) conserva el diseño histórico
con HDFS, operación horaria y reportes HTML.

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

Descargar `cloud_provider_challenge_dataset_v1` del campus y extraerlo fuera
del repo. Con Python 3.9 o posterior, desde la raíz del repositorio:

```sh
python3 scripts/profile_data.py /ruta/cloud_provider_challenge_dataset_v1 --output evidence/profile-local.json
```

El script verifica las ocho fuentes y no modifica los originales. La salida
contiene estadísticas y hashes, sin filas de clientes, emails ni comentarios.
Las [instrucciones de datos](data/README.md) detallan qué comprueba.

## Quickstart

1. Con Docker Desktop en modo Linux y Compose v2, extraer el paquete del
   campus fuera del repositorio. Configurar `DATASET_PATH` en `.env` a
   partir de `.env.example`. [Puesta en marcha](docs/segunda-entrega.md#puesta-en-marcha).
2. Preparar un keyspace Astra y guardar el token y bundle en la
   configuración local. [Configuración Astra](docs/segunda-entrega.md#preparar-astradb).
3. Desde la raíz, construir el runtime y validar el flujo con un ID y un
   nombre de informe libres. El comando ingiere el dataset, encadena los
   IDs de publicación, carga el mart y verifica dos consultas en Astra.

```sh
docker compose build runtime
docker compose run --rm --no-deps runtime python scripts/validate_integration.py --validation-id cierre-01 --report-name cierre-first
```

El [guion de demo](docs/segunda-entrega.md#guion-de-demo) incluye el chequeo de
entorno, una demostración con publicaciones existentes, pruebas y reintento.
Para una verificación por etapa, consultar la
[documentación técnica](docs/segunda-entrega.md#arquitectura-ejecutada).

La [evidencia integrada](evidence/integration/README.md) registra 43.200
eventos Silver, 11.050 filas Gold y Astra y una reejecución sin nuevas
filas ni upserts. La [matriz de cumplimiento](docs/segunda-entrega.md#matriz-de-cumplimiento)
separa lo verificado de las decisiones pendientes. El
[backlog de la final](docs/segunda-entrega.md#backlog-para-la-entrega-final)
prioriza las cuatro consultas obligatorias restantes y los artefactos de defensa.

## Convenciones del repositorio

- Código y nombres derivados en `snake_case`; conservar los identificadores de origen.
- Fechas de eventos y agregaciones en UTC; costos de uso separados de revenue.
- Versionar código, diseño y evidencia agregada. Mantener fuera de Git los
  originales, credenciales, entornos, checkpoints y salidas del Lake.
- Registrar decisiones de diseño en la entrega y detalles internos en
  `CONTEXT.md`; no duplicar el documento en otros formatos.
