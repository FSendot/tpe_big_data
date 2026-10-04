# Alcance de la primera entrega

Estado: alcance acordado el 24/09/2026 y actualizado con D1–D16 el 28/09/2026.
El diseño está consolidado para revisión. La evidencia reproducible de la
primera entrega sigue pendiente de preparación.

## Objetivo y límite

Preparar el diseño y la fundación de datos exigidos por las secciones 5 y 9.1
de la consigna, con el mínimo trabajo ejecutable necesario para sustentar el
diseño. Fecha de entrega prorrogada al 05/10/2026, según lo informado el 04/10.
Se mantiene el horario de las 18:30 de la consigna original.

El caso principal explica las variaciones diarias del costo de uso por
organización y servicio para FinOps del proveedor, mostrando impacto en USD
y variación porcentual. El diseño global conserva Soporte y Producto/Usage
porque la consigna los requiere.

## Trabajo incluido

1. Documento de diseño conciso en Markdown: problema, usuarios, objetivos
   medibles, 5V, arquitectura y justificación del patrón, capacidades
   transversales, supuestos y decisiones abiertas.
2. Inventario y perfil inicial de las ocho fuentes: grano, frecuencia, tipos,
   calidad, trazabilidad y riesgos, con evidencia reproducible de lectura y
   exploración e instrucciones de acceso a los datos.
3. Diseño del Data Lake: Landing original, Bronze, Silver y Gold; formatos,
   particiones, nombres, metadatos, retención y reglas de promoción.
4. Diseño de ambos recorridos, batch y streaming, con herramientas específicas,
   salidas, serving y consumo previstos. Incluir las tecnologías requeridas
   por la consigna: PySpark, Structured Streaming, Parquet y Cassandra/AstraDB.
5. Descripción del cálculo batch diario y su equivalente conceptual MapReduce.
   El diseño streaming contempla esquema, deduplicación, ventanas, datos
   tardíos, watermark y checkpointing.
6. Diagrama v1, matriz requisito-componente, reglas de calidad, seguridad,
   metadatos y observabilidad; riesgos, mitigaciones, esfuerzo y plan inicial.
7. README, glosario y decisiones mantenidos en el repositorio. Crear ADR
   únicamente cuando exista un trade-off duradero que justifique el registro.

La evidencia ejecutada se limita a lectura/perfilado inicial. El cálculo
analítico se explica en el diseño. El volumen académico observado se distingue
de cualquier escenario hipotético de escala usado para justificar las 5V.

## Acuerdos de negocio y calidad

- Comparar costos agregados por organización, servicio y día UTC contra el
  día calendario anterior. Mostrar ambos costos, diferencia en USD y porcentaje.
- Permitir ordenar por impacto en USD o por variación relativa.
- Si el día anterior no tiene registros o su costo es cero, mostrar el caso
  sin base comparable fuera del ranking porcentual. No imputar cero a la
  ausencia de registros ni usar la última fecha con actividad como sustituto.
- Conservar los negativos separados para validación. Marcar los totales
  afectados como parciales y excluir esas comparaciones de ambos rankings.
- Conservar costos altos válidos; la magnitud no demuestra un error.
- Permitir corregir resultados por eventos tardíos, con fecha de actualización.
  Corregir también las comparaciones que dependan del día modificado.
- Diseñar una salida operativa por hora UTC, organización y servicio, con costo
  acumulado y consumo separado por métrica y unidad, actualizada con cada lote.
  La hora del evento determina la ventana. La ventana no fija la latencia.
- Mantener facturación en el inventario y la arquitectura. Conciliar facturas
  y eventos no forma parte del problema principal.
- Objetivo de frescura operativa: hasta 60 segundos desde que el dato está
  disponible en la entrada hasta que el resultado actualizado puede consultarse.
  Es un objetivo de diseño aceptado, todavía no medido ni garantizado. La
  validación bajo carga y la disponibilidad del servicio quedan para la
  implementación posterior.
- Excepción aceptada en D11: las horas operativas afectadas por eventos fuera
  del margen garantizado del streaming o por conflictos sobre eventos ya
  contabilizados se reconstruyen en la siguiente corrida horaria, más su
  tiempo de procesamiento. Se identifican como pendientes de recálculo.
  Esta recuperación incluye horas del día actual y no promete 60 segundos;
  el análisis diario sigue limitado a días cerrados.
- D12 mantiene los grupos horarios que necesitaron reparación bajo autoridad
  batch durante esta reproducción. También sus eventos nuevos válidos esperan
  la siguiente corrida horaria más procesamiento. Los demás grupos mantienen
  el recorrido streaming ordinario.
- D15 conserva la última publicación terminada ante una actualización fallida,
  con su fecha y las incidencias conocidas. Una publicación incompleta no la
  sustituye; sin publicación previa se informa no disponible, no cero.
- D16 fija revenue como subtotal menos créditos más impuestos, convertido con
  el FX provisto salvo USD, cuyo factor es 1 y cuya inconsistencia se registra.
  Créditos vacíos y subtotales negativos quedan pendientes. Facturas sin importe
  calculable no se convierten en cero y se explicita cobertura al agregarlas.

## Fuera de esta entrega

- Pipeline integrado batch/streaming, cargas a Cassandra y consultas ejecutadas.
- Dashboard, aplicación web, despliegue de servicios o infraestructura permanente.
- Diseño de despliegue de HDFS: hosting, topología, instalación y
  aprovisionamiento. Se mantienen la estimación preliminar de recursos y las
  instrucciones para reproducir la evidencia mínima entregada.
- Detector estadístico de anomalías, conciliación contable o análisis de margen.
- Benchmarks de latencia y escala, generación masiva de datos o SLA demostrado.
- Un ejemplo analítico ejecutable adicional al perfilado y un PDF duplicado
  del documento Markdown.

## Decisiones abiertas y límites de validación

- D8 fija una corrida batch cada hora para publicar días UTC cerrados pendientes
  y corregir los que tengan novedades. La espera hasta el siguiente corte se suma
  al tiempo de procesamiento; no es una latencia medida. El grano sigue siendo
  diario y la herramienta de programación queda pendiente.
- Mecanismo de ingesta: la sección 4.4 exige Structured Streaming desde un
  directorio JSONL. El diseño describe cómo presentar los archivos para simular
  llegadas; no se agrega Kafka ni otro broker. El trigger y la latencia total
  deben distinguirse en el diseño.
- Entorno de ejecución: queda sin definir por decisión expresa del usuario,
  que prefiere evitar Colab. La restricción de Colab o equivalente validado
  sigue vigente para el pipeline del proyecto completo. La primera evidencia
  debe usar un entorno documentado y reproducible; esa elección no valida por
  sí sola una alternativa para el pipeline posterior. No se fija Colab como
  opción por defecto ni se agrega una migración de entorno a esta entrega.
- Consumo: D13 adopta un CLI/script que genere
  tablas y un reporte HTML con gráficos para cubrir visualización. En esta
  entrega sólo se diseña, sin construir la interfaz ni las consultas ejecutadas.
  Jupyter no es obligatorio. Docker Compose es una posibilidad posterior y
  no modifica la exclusión del diseño de despliegue.
- D14 limita el consumo al análisis agregado y sus indicadores de calidad;
  el responsable de datos conserva acceso al detalle e incidencias para
  investigar. No se incorpora navegación por eventos individuales al consumo.
- El diseño técnico documenta retención, particiones, recuperación, accesos
  y demás mecanismos necesarios. Son contenido de la primera entrega, no
  autorizaciones para implementarlos ni nuevos entregables.

## Cierre de la definición

La definición del alcance queda cerrada con los acuerdos de la entrevista,
incluidas sus exclusiones y las decisiones expresamente abiertas. El diseño
consolidado queda para revisión y la evidencia reproducible sigue pendiente;
esto no equivale a la primera entrega terminada ni incluye trabajo de las
etapas posteriores.

## Referencias

- Consigna local, secciones 2.2, 4, 5, 8.2 y 9.1. Detalle en
  [el checklist](primera-entrega.md).
- [Decisiones técnicas y tradeoffs](decisiones-de-diseno.md).
- [Spark Structured Streaming: fuentes y triggers](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html).
- [Colab: entorno de notebooks y ciclo de vida de las máquinas](https://research.google.com/colaboratory/faq.html).
