# Primera entrega: diseño y fundación de datos

Fecha límite: 28/09/2026, 18:30 h.
Fuente: consigna v1.0, secciones 5 y 9.1, páginas 7, 8 y 15.

Este archivo registra los requisitos de la entrega. El documento de diseño se
redactará con las decisiones acordadas durante la entrevista.

## Alcance obligatorio

- [ ] Explicar problema, usuarios, preguntas principales y objetivos medibles.
- [ ] Justificar Big Data mediante volumen, velocidad, variedad, veracidad y valor.
- [ ] Inventariar y perfilar fuentes: grano, frecuencia, tipos, calidad, trazabilidad y riesgos.
- [ ] Diagramar arquitectura v1 con fuentes, ingesta, Data Lake, procesamiento, serving y consumo.
- [ ] Incluir gobierno, calidad, seguridad, metadatos y observabilidad en el diseño.
- [ ] Justificar el patrón arquitectónico y distinguir los recorridos batch y streaming.
- [ ] Relacionar requisitos y 5V con componentes y decisiones de arquitectura.
- [ ] Diseñar zonas, formatos, particiones, naming, retención, metadatos y promoción del Data Lake.
- [ ] Describir los flujos batch y streaming con herramientas específicas.
- [ ] Expresar un flujo batch mediante MapReduce o su equivalente conceptual.
- [ ] Registrar supuestos, riesgos, mitigaciones y decisiones abiertas.
- [ ] Estimar esfuerzo, roles y recursos.
- [ ] Completar el README y las convenciones con evidencia mínima reproducible de datos.

## Artefactos a entregar

| Artefacto | Contenido mínimo | Estado |
| --- | --- | --- |
| Documento de diseño | PDF o Markdown conciso con diagramas, tablas y decisiones | Pendiente |
| Repositorio | README, estructura, muestras o instrucciones, evidencias y decisiones | Inicializado; incompleto |
| Diagrama v1 | Componentes, flujos, zonas, patrón y capacidades transversales | Pendiente |
| Matriz requisito-componente | Trazabilidad entre objetivos, requisitos y componentes | Pendiente |
| Plan inicial | Supuestos, riesgos, mitigaciones, esfuerzo y próximos pasos | Pendiente |

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
