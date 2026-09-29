# Fundamentos de las lectures para la primera entrega

Revisión del 28/09/2026. Las lectures fundamentan decisiones; la consigna y el
[alcance acordado](alcance-primera-entrega.md) determinan qué entregar.
HDFS fue elegido en D5; proveedor y entorno de ejecución siguen abiertos.

## Fuentes y cobertura

Las referencias usan la página del PDF, contada desde 1. Esto evita confundir
el número físico con pies repetidos o incompletos dentro de las diapositivas.

| Referencia | Archivo local | Páginas revisadas |
| --- | --- | --- |
| L1 | `Clase_01_Introduccion_a_Big_Data_ITBA.pdf` | 1–31 |
| L2 | `Clase_02_Framework_Hadoop_Intro.pdf` | 1–16 |
| L3 | `Clase_03_Procesamiento_ de_datos_distribuidos_con_Spark.pdf` | 1–152 |
| L4 | `04-HDFS.pdf` | 1–31 |
| C | `Consigna_Proyecto_Integrador_Big_Data_ITBA_2C_2026.pdf` | Contraste con §§2–5, 8 y 9.1 |

Se leyó el texto extraíble completo y se renderizaron las 230 páginas de las
lectures. Se inspeccionaron directamente las capturas relevantes, en particular
borrado/Trash, almacenamiento, DataFrames, metastore y persistencia. La
extracción no recupera el contenido de algunas imágenes; el OCR local falló.
Por eso las búsquedas de texto se complementaron con lectura visual y no se
presentan como una transcripción íntegra de las diapositivas.

Los PDF se consultan en el material de la materia y no se versionan aquí.

## Retención: resultado para D3

**No se encontró un plazo académico de retención ni una recomendación de días
que cambie la propuesta de cierre de evaluación más 30 días.** Es un resultado
de la revisión de L1–L4, con la cobertura indicada arriba, no una política
docente explícita. Los pasajes cercanos al tema tratan conceptos distintos:

| Hecho de la lecture | Fuente exacta | Qué no permite concluir |
| --- | --- | --- |
| Enseña eliminación por archivo, patrón y directorio; explica Trash como papelera y su vaciado para borrado permanente. No establece un plazo. | L4, p. 21, captura; repetido en L3, p. 87 | No define cuándo eliminar Landing, Silver, evidencia ni copias de trabajo. Tampoco especifica una tarea de expiración automática. |
| Distingue cache, persistencia, lineage y checkpoint. Lineage permite recomputar particiones perdidas; checkpoint materializa y corta una cadena larga. | L3, p. 24 | No fija retención del histórico ni un TTL del estado de streaming. |
| Cache/persist conservan cálculos intermedios para reutilizarlos en acciones posteriores. | L3, p. 151, captura | Reutilizar un cálculo no establece una política de archivo del proyecto. |
| Presenta tablas/metadatos persistentes que sobreviven a una sesión Spark. | L3, pp. 147–148 | Sobrevivir al cierre de sesión no significa conservar indefinidamente ni especifica días. |
| La palabra retención aparece como resultado de negocio en medios/plataformas. | L1, p. 25 | Es retención de usuarios, no de archivos. |

**Requisito de la consigna:** justificar retención y promoción entre zonas,
sin fijar una cantidad de días. C, §4.2, p. 5, y §5.2.7, p. 7.

**Consecuencia propuesta:** no hay un indicio en estas lectures que desplace el
margen de 30 días autorizado condicionalmente por el usuario. Ese margen sigue
siendo una decisión del proyecto. Se aplica a las copias de trabajo después
del cierre de evaluación registrado, con las excepciones de originales,
documentación y evidencia descritas en [D3](decisiones-de-diseno.md).
No equivale a borrar 30 días después del timestamp de un evento de 2025 ni
habilita ejecutar borrados ahora. La elección del mecanismo y la coordinación
con checkpoints, reprocesamientos y serving siguen siendo cuestiones de diseño.

## Fundamentos que sirven al diseño

En la segunda columna se resume lo enseñado. La tercera contiene inferencias
para este proyecto; no son nuevas decisiones aceptadas por el usuario.

| Tema | Hecho de las lectures y fuente | Aplicación propuesta a la primera entrega |
| --- | --- | --- |
| Batch y streaming | Separar necesidades de latencia justifica un híbrido; Spark reúne batch, SQL y streaming. Structured Streaming se presenta como un modelo incremental sobre DataFrames e incluye lectura JSON desde directorio. L1, pp. 16 y 18; L3, pp. 5 y 44. | Fundamentar recorridos distintos para operación horaria y análisis diario, sin añadir un broker. No deducir del material una ventana, watermark o latencia concretos. |
| DataFrames y SQL | DataFrame es la opción principal en PySpark; RDD queda para control de bajo nivel y aprendizaje. SQL y DataFrames comparten optimizador. L1, p. 20; L3, pp. 25 y 33. | Proponer reglas declarativas y funciones nativas para tipificar, integrar y agregar. MapReduce conceptual no obliga a implementar con RDD. |
| Parquet y particiones | Parquet se describe como columnar y eficiente para analítica; particionar permite podar lecturas, con advertencia contra cardinalidades muy altas. L3, pp. 28 y 38. El número de particiones de ejecución tiene costos de paralelismo y tareas pequeñas. L3, p. 23. | Justificar el formato requerido y proponer particiones según consultas y volumen. Distinguir directorios particionados del Lake de particiones de ejecución Spark. No copiar el número de particiones de un ejemplo como configuración definitiva. |
| HDFS y almacenamiento de objetos | Hadoop se presenta como modular, incluyendo sustituir HDFS por S3. L2, p. 5. El mapa Spark incluye HDFS, S3, GCS y ADLS. L3, p. 8. L4 muestra NameNode/DataNodes y bloques, pp. 5–11, y particiones de tablas Hive en subdirectorios, p. 31. | HDFS es una alternativa estudiada, no una dependencia obligatoria. D5 elige HDFS; distinguir almacenamiento, motor y gestor de recursos. |
| MapReduce | Map produce resultados parciales, shuffle/sort reagrupa por clave y reduce combina. L1, p. 17. Spark muestra agregación por claves y el costo de shuffle. L3, p. 31. | Explicar el batch diario como proyección/validación de eventos, agrupación por organización-servicio-fecha y suma de medidas válidas con incidencias; después asociar días consecutivos. Es una correspondencia conceptual, no un pipeline ejecutado. |
| Reproceso y trazabilidad | Lineage permite recomputar particiones; las recomendaciones ETL incluyen esquema explícito, separación de etapas, métricas e idempotencia. L3, pp. 24 y 39. | Fundamentar conservar datos y reglas suficientes para reconstruir resultados. El lineage de ejecución no reemplaza el archivo original, el detalle Silver ni el registro de incidencias acordados en D1/D2. |
| 5V y escala | Las 5V distinguen volumen, velocidad, variedad, veracidad y valor; no toda analítica necesita Big Data. La elección debe ponderar latencia, escala, gobierno y costo. L1, pp. 6–10 y 27–28; L3, p. 7. | Separar el tamaño observado de la muestra de un supuesto de crecimiento del proveedor. Justificar el aprendizaje y la arquitectura exigida sin afirmar que el dataset académico, por sí solo, requiere un clúster. |
| Arquitectura y gobierno | Se separan fuentes, ingesta, almacenamiento, procesamiento y consumo, con calidad, seguridad, gobierno y observabilidad transversales. L1, pp. 22–24. | Incluir responsables, trazabilidad y calidad en el diagrama y matriz. El principio de simplicidad de L1, p. 27, respalda evitar infraestructura sin requisito. |

## Límites al trasladar ejemplos al proyecto

- **La consigna prevalece sobre ejemplos de las clases.** L3, p. 37, describe
  Bronze de forma general y sitúa tipos correctos en Silver. C, §4.2, p. 5,
  requiere ya en Bronze tipificación explícita, grano original, `ingest_ts` y
  `source_file`. El ejemplo con `inferSchema` de L3, p. 28, no sustituye los
  esquemas explícitos exigidos en C, §4.4, p. 5.
- **No imponer tecnologías por aparecer en una práctica.** L3, p. 27, muestra
  Colab en modo local; pp. 119–120 proponen otra actividad con EMR/Hive. Son
  ejemplos docentes. C, §2.2, p. 4, fija Colab o equivalente validado y
  Cassandra/AstraDB para el proyecto. Esta revisión no resuelve el entorno
  pendiente ni incorpora EMR, Hive, Delta o Iceberg al alcance. HDFS se eligió en D5.
- **No convertir una ilustración de rendimiento en un compromiso.** L2,
  p. 16, ilustra escalabilidad lineal; L3, pp. 23 y 32, también explica costos
  de particiones, skew y joins. La estimación del proyecto debe declarar
  supuestos y validaciones pendientes, sin prometer tiempos por multiplicar
  recursos.
- **No extrapolar detalles de runtime sin validarlos.** Las versiones,
  valores por defecto y comparaciones simplificadas de las diapositivas no
  se adoptan como configuración. Esta revisión usa los fundamentos y no
  sustituye la verificación de compatibilidad cuando se elija el entorno.
- **Primera entrega sigue siendo diseño y evidencia mínima.** C, §5.1,
  §5.2.9 y §5.2.12, p. 7, y §9.1, p. 15, piden arquitectura viable, flujo
  batch conceptual y lectura/exploración de datos. Las lectures no añaden
  una obligación de desplegar la arquitectura o ejecutar los marts ahora.
