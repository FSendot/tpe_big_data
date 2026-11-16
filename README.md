# Cloud Provider Analytics

Proyecto integrador de Big Data, ITBA, segundo cuatrimestre de 2026.

Análisis de costos y consumo de un proveedor cloud, con foco en FinOps y
métricas de Soporte y Producto. El diseño inicial combina PySpark, HDFS y
Cassandra, con consultas por CLI y reportes HTML. La segunda entrega revisa
el almacenamiento para comenzar con un entorno local reproducible.

La [primera entrega](docs/primera-entrega.md) reúne el diseño completo.
Entrega: **05/10/2026**. La exploración inicial se puede ejecutar; el pipeline
queda diseñado para las próximas etapas.

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
docs/primera-entrega.md    Diseño, diagrama, matriz, decisiones y plan
data/README.md            Obtención y uso del paquete del campus
scripts/profile_data.py   Exploración con Python estándar
evidence/                 Resultado agregado y hashes de las entradas
CONTEXT.md                Notas internas para la implementación posterior
```

## Reproducir la exploración

Descargar `cloud_provider_challenge_dataset_v1` del campus y extraerlo fuera
del repo. Con Python 3.9 o posterior, desde la raíz del repositorio:

```sh
python3 scripts/profile_data.py /ruta/cloud_provider_challenge_dataset_v1 --output evidence/profile.json
```

El script verifica las ocho fuentes y no modifica los originales. La salida
contiene estadísticas y hashes, sin filas de clientes, emails ni comentarios.
Las [instrucciones de datos](data/README.md) detallan qué comprueba.

## Correcciones posteriores a la evaluación

La [respuesta al feedback](docs/primera-entrega.md#correcciones-posteriores-al-feedback)
registra las acciones, responsables y evidencias. Se mantiene HDFS como decisión
de diseño, con alternativas y límites explícitos; se precisan retención,
operación y prioridades de validación. El responsable es Santos Galarraga.
Estos ajustes corresponden a la primera entrega: no implementan el pipeline
ni adelantan el segundo parcial. La reproducción con el paquete correcto se
documenta en [evidencias](evidence/README.md).

## Segunda entrega

La [sección 1: entorno y configuración](docs/segunda-entrega.md) prepara Spark
en Docker, Parquet en un volumen local y pruebas de conexión a serving.
Incluye instrucciones para Cassandra local y AstraDB, configuración sin
secretos y evidencia de ejecución. Docker está permitido como entorno
equivalente a Colab; la conexión CQL a Astra fue verificada. El pipeline aún
no está implementado; HDFS se revisa a favor de almacenamiento local para
el flujo mínimo, según la decisión documentada allí.

## Convenciones del repositorio

- Código y nombres derivados en `snake_case`; conservar los identificadores de origen.
- Fechas de eventos y agregaciones en UTC; costos de uso separados de revenue.
- Versionar código, diseño y evidencia agregada. Mantener fuera de Git los
  originales, credenciales, entornos, checkpoints y salidas del Lake.
- Registrar decisiones de diseño en la entrega y detalles internos en
  `CONTEXT.md`; no duplicar el documento en otros formatos.
