# Primera entrega: diseño y fundación de datos

Fecha límite actualizada: 05/10/2026, 18:30 h.
Prórroga informada el 04/10; se mantiene el horario de la consigna original.
Fuente: consigna v1.0, secciones 5 y 9.1, páginas 7, 8 y 15.

Este archivo registra los requisitos de la entrega. El [diseño consolidado](diseno-primera-entrega.md)
documenta las decisiones de la entrevista y está listo para revisión. Las marcas
de diseño indican contenido redactado, no implementación ni aprobación final.

## Alcance obligatorio

- [x] Explicar problema, usuarios, preguntas principales y objetivos medibles.
- [x] Justificar Big Data mediante volumen, velocidad, variedad, veracidad y valor.
- [ ] Inventariar y perfilar fuentes: grano, frecuencia, tipos, calidad, trazabilidad y riesgos.
- [x] Diagramar arquitectura v1 con fuentes, ingesta, Data Lake, procesamiento, serving y consumo.
- [x] Incluir gobierno, calidad, seguridad, metadatos y observabilidad en el diseño.
- [x] Justificar el patrón arquitectónico y distinguir los recorridos batch y streaming.
- [x] Relacionar requisitos y 5V con componentes y decisiones de arquitectura.
- [x] Diseñar zonas, formatos, particiones, naming, retención, metadatos y promoción del Data Lake.
- [x] Describir los flujos batch y streaming con herramientas específicas.
- [x] Expresar un flujo batch mediante MapReduce o su equivalente conceptual.
- [x] Registrar supuestos, riesgos, mitigaciones y decisiones abiertas.
- [x] Estimar esfuerzo, roles y recursos.
- [ ] Completar el README y las convenciones con evidencia mínima reproducible de datos.

## Artefactos a entregar

| Artefacto | Contenido mínimo | Estado |
| --- | --- | --- |
| Documento de diseño | PDF o Markdown conciso con diagramas, tablas y decisiones | Consolidado; revisión final pendiente |
| Repositorio | README, estructura, muestras o instrucciones, evidencias y decisiones | Inicializado; incompleto |
| Diagrama v1 | Componentes, flujos, zonas, patrón y capacidades transversales | Incluido en el diseño |
| Matriz requisito-componente | Trazabilidad entre objetivos, requisitos y componentes | Incluida en el diseño |
| Plan inicial | Supuestos, riesgos, mitigaciones, esfuerzo y próximos pasos | Incluido en el diseño |

## Restricciones comunes que condicionan el diseño

La solución del proyecto completo usa PySpark, Structured Streaming, Parquet y
Cassandra/AstraDB. Debe contemplar eventos por streaming y maestros/facturación
por batch, cualquiera sea el patrón elegido. La opción batch mencionada en la
sección 5.2 no elimina ese requisito invariable de la sección 4.3.

El entorno previsto es Google Colab o un equivalente validado. La elección sigue
abierta. Landing conserva los archivos originales. El diseño debe prever
compatibilidad entre eventos v1 y v2, tipos ambiguos, nulos y anomalías.

Para esta entrega se requiere una arquitectura viable y evidencia mínima de
datos. El pipeline integrado, las cargas a Cassandra y las consultas ejecutadas
son trabajo de las etapas siguientes.

HDFS es la elección acordada para el Lake. La consigna no exige diseñar su
despliegue en esta entrega. Hosting, topología, instalación y aprovisionamiento
quedan pendientes. Sí se incluyen estimaciones preliminares de recursos,
§5.2.11, y las instrucciones para reproducir la evidencia efectivamente
entregada, §8.2, sin incorporar una instalación de HDFS que esa evidencia no use.

Las secciones 5.2.9 y 5.4 piden describir cómo se resolvería el flujo batch;
no exigen ejecutar la comparación diaria propuesta para FinOps. La evidencia
ejecutada puede limitarse a lectura y perfilado inicial reproducible de las
fuentes. Un documento Markdown conciso es un formato admitido por la sección
5.3, sin obligación de generar también un PDF.

La arquitectura global debe contemplar capacidad cercana al tiempo real para
métricas operativas, sección 2.1. El análisis diario de FinOps no satisface por
sí solo esa parte del diseño.

## Rúbrica

| Dimensión | Peso |
| --- | ---: |
| Comprensión y justificación | 15 % |
| Arquitectura | 25 % |
| Data Lake | 20 % |
| Datos | 15 % |
| Procesamiento batch | 10 % |
| Ingeniería y documentación | 10 % |
| Defensa y respuesta al feedback | 5 % |

## Cierre de la entrega

Verificar que problema y éxito sean medibles, que arquitectura y Data Lake
respondan a los datos, y que el flujo batch tenga una correspondencia clara con
MapReduce. Los riesgos necesitan mitigaciones y el repositorio debe permitir
continuar la implementación sobre la misma base.

La versión evaluada es la disponible a la hora límite. Después del feedback se
debe versionar un plan de correcciones con prioridad, responsable, fecha objetivo
y evidencia esperada.
