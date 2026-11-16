# Contexto de implementación

Notas de trabajo. La [segunda entrega](docs/segunda-entrega.md) documenta el
MVP ejecutado y su backlog. La [primera entrega](docs/primera-entrega.md)
conserva el diseño histórico y las correcciones derivadas del feedback.

## MVP ejecutado

El usuario autorizó Docker. El runtime usa Python 3.11.15, Java 17 y Spark
3.5.8 en local[2], con Landing de solo lectura y Parquet en un volumen Docker.
El flujo usa cuatro CSV batch y dos consultas streaming `availableNow`
con checkpoints. Silver toma el archivo Bronze completo y el snapshot
batch explícito, conforma organizaciones y recursos y separa incidencias en
quarantine. Gold agrega el uso diario. Las anomalías quedan en Parquet como
señales estadísticas. La aceptación docente como equivalente a ML sigue
sin confirmación.

El mart diario fue cargado y verificado en Astra. Su clave de partición
es `(publication_id, org_id, service, usage_month)` y su clustering es
`event_date ASC`. El marcador `finops_publications` distingue cargas
`loading` y `complete`. El comando oficial usa IDs explícitos y sólo acepta
publicaciones completas. [Esquema ejecutado](config/finops-serving.cql).

La validación aislada y su reintento enlazan IDs, manifiestos, consultas y
recuperación. `usage-v1` corresponde a la prueba por etapas y
`integration-section-7-v1` a la integración registrada. La sesión
`usage` fue una prueba preliminar del parser.
[Evidencia integrada](evidence/integration/README.md).

Santos Galarraga figura como responsable de diseño, gobierno, reproducción
y revisión. La política por zona, el cierre registrado y la limpieza
controlada están en la primera entrega. La limpieza automática y los
controles de producción quedan pendientes.
[Retención y operación](docs/primera-entrega.md#retención-y-operación).

## Vocabulario

- Costo de uso: importe incremental de los eventos, distinto de facturación,
  revenue, margen o costo interno del proveedor.
- Aumento absoluto: diferencia en USD. Aumento relativo: porcentaje respecto
  del costo de referencia.
- Día completo: día UTC terminado; puede recibir correcciones y no garantiza
  que hayan llegado todos sus eventos.
- Sin base comparable: día anterior ausente o costo cero; no hay porcentaje.
- Pendiente de validación: valor cuyo significado o validez no se resolvió,
  como costo negativo o contribución contradictoria. No equivale a error
  confirmado ni a outlier estadístico.
- Total parcial: excluye contribuciones pendientes de una medida.
- Pendiente de recálculo: la publicación no refleja todas las novedades.
  Recalcular puede eliminar la espera y conservar la parcialidad.
- Revenue mensual: subtotal menos créditos más impuestos, convertido a USD.
  La fórmula es una convención, no conciliación con el costo de uso.

## Exploración y validaciones posteriores

La exploración registra archivos, tamaños, SHA256, columnas, tipos JSON,
vacíos, claves, duplicados, referencias y distribuciones temporales. Silver
implementa el conformado de organizaciones, recursos y eventos de uso.
Las reglas de facturación y recuperación de agregados quedan pendientes.
Comando, versión de Python y resultados están versionados sin datos personales.
Los conteos de diseño son referencias para investigar diferencias, no filtros
que permitan forzar el resultado esperado.

Datos adicionales a los resumidos en la entrega:

| Fuente | Bytes observados |
| --- | ---: |
| customers_orgs.csv | 7.746 |
| users.csv | 70.683 |
| resources.csv | 36.703 |
| support_tickets.csv | 70.945 |
| marketing_touches.csv | 101.998 |
| nps_surveys.csv | 3.977 |
| billing_monthly.csv | 15.854 |
| 120 JSONL | 12.927.515 |

- 33.953 timestamps distintos, intervalos intradía observados de 1–21 minutos.
  No son una frecuencia de llegada.
- Costos presentes y finitos; todos los timestamps interpretables. Servicios
  y regiones ya estaban en minúsculas y sin espacios extremos.
- Servicios: compute 12.498, database 7.419, networking 6.921, analytics 4.590,
  genai 4.158 y storage 7.614. Las tres métricas value/unit aparecen en todos.
- `genai_tokens` es un campo independiente de metric. Sus 3.132 valores
  observados son enteros positivos y corresponden a v2 y service=genai.
- Billing no repite invoice_id ni org-mes. FX USD observado: 0,85463–1,11791.
- Tickets: 56 critical; sla_breached contiene 95 true y 905 false. Usar ese
  indicador, sin inventar duraciones contractuales de SLA.
- No hay generador, diccionario separado ni revisión de eventos en el paquete.
  Su README, líneas 16–19, describe defectos deliberados pero no los resuelve.

## Contratos de transformación

Los montos y consumos de Silver usan decimal(38,12) y los tokens bigint.
Gold ensancha tokens a decimal(38,0) antes de sumar y rechaza desbordes.
Bronze conserva el original interpretado, `ingest_ts`, `source_file`,
`source_hash`, `source_record_hash`, el contrato y el ID batch o sesión
según la ingesta. Silver usa el superset v1/v2 y fija los maestros en el
manifiesto. Quarantine particiona por ingesta para admitir fechas inválidas.

Equivalencia de eventos excluye metadatos de ingesta y aplica sólo casts y
normalizaciones admitidos. Se compara contra el historial retenido para
reconocer conflictos entre lotes. Si identidad, fecha y dimensiones
coinciden, se evalúa cada medida por separado. Si discrepan, las variantes
pueden afectar varios grupos o quedar sin ubicación confiable. El archivo
completo permite reconstruir desde el corte seleccionado.

## Diseño operativo propuesto

Las notas siguientes describen la evolución del MVP. HDFS, la tercera
consulta de agregación horaria, la selección de publicación activa por
generación y el objetivo operativo de 60 segundos pertenecen al diseño.
El contrato de rename actual está validado en el volumen local.

En ese diseño, un cambio de día, organización o servicio invalidaría
las asignaciones anterior y nueva. Una corrección diaria actualizaría
comparaciones adyacentes y las ventanas de 14 días que incluyan el día.
Se podrían reutilizar agregados coherentes de días sin cambios. CSV y
dimensiones invalidarían resultados dependientes del snapshot.

### Streaming y recuperación operativa

1. Un script de reproducción ofrecería copias de los JSONL originales completos
   al directorio de entrada. No reordena eventos ni modifica Landing. La tasa
   de presentación y los archivos por lote serán parámetros documentados; el
   objetivo de frescura se mide desde esa disponibilidad, no desde la fecha 2025.
2. Ingesta con readStream y esquema explícito; foreachBatch conserva Bronze,
   clasifica calidad/conflictos y publica un nuevo manifiesto Silver con sus
   incidencias. Un cambio que necesita reparación bloquea la autoridad streaming
   del grupo antes de permitir otra publicación. Una caída se recupera por ID
   de query, lote y archivos, sin duplicar la contribución.
3. Sólo se emiten como novedades operativas nuevas contribuciones canónicas.
   Una query separada deduplica por event_id con watermark de 60 días; se prevé
   reutilizar `dropDuplicatesWithinWatermark` del MVP. Su salida
   es append a Parquet. Conflictos y eventos fuera de margen quedan preservados
   antes de este operador y registrados para recuperación.
4. Otra query agrega esas novedades por org-servicio-ventana de una hora UTC,
   con watermark de 60 días y salida Update mediante foreachBatch al publicador.
   Se usan checkpoints distintos. Separar deduplicación y agregación evita
   depender de una cadena de operadores con estado no admitida en Update.
5. El publicador verifica autoridad y generación del grupo. Los grupos
   transferidos a batch se reconstruyen sólo en el batch horario desde Silver,
   incluidas nuevas contribuciones válidas. No se devuelve autoridad a un
   acumulador que no haya sido reconciliado.

Antes de promover Silver, cada lote guarda un plan recuperable con su corte,
archivos, variantes, invalidaciones y todas las salidas esperadas. El reintento
completa salidas faltantes desde ese plan aunque sus IDs ya figuren en Silver.
Así, una caída entre actualizar Silver y emitir novedades no pierde eventos
operativos. El lote no se considera terminado hasta verificar sus salidas.
Para esta muestra, la ingesta publica un archivo de novedades por lote: lo
escribe y cierra fuera del directorio observado y lo promueve completo con
nombre determinista. Si ya existe, verifica el contenido lógico esperado.
Se usará rename dentro del mismo HDFS y límites compatibles, comprobando el
resultado; esto no se generaliza a otros backends. [Contrato de filesystem](https://apache.github.io/hadoop/hadoop-project-dist/hadoop-common/filesystem/filesystem.html).

Propuesta inicial de triggers: 10 s por query. Presupuesto ilustrativo extremo:
hasta 30 s de espera entre tres etapas y 30 s para lectura, validación,
procesamiento y publicación. Es un presupuesto a comprobar, no una medición
ni un SLA garantizado. Si hay backlog, se expone y se revisan tamaños de lote,
paralelismo y costo de identidad antes de declarar alcanzado el objetivo.

El margen de 60 días cubre el intervalo de la muestra si no se mezclan eventos
posteriores ajenos a ella. No garantiza una configuración eficiente a escala.
Se publican resultados provisionales sin esperar cierre de ventanas. Una
entrada finita no hace avanzar por sí sola el watermark hasta cerrarlas todas.
La recuperación identifica todas las horas fuera del margen garantizado,
aunque Spark hubiera incorporado algunas: reemplazar desde detalle evita
sumarlas otra vez. Esta reparación no depende de inferir un descarte exacto.

Las garantías de operadores y limitaciones de consultas están documentadas por
[Spark 3.5.8](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html).
foreachBatch puede reintentarse; la aplicación debe hacer sus escrituras
idempotentes. Los archivos de novedades, snapshots y manifiestos comprometidos
son inmutables. Repetir un lote verifica/reutiliza su salida. Los operadores
adicionales, la compatibilidad de checkpoints al migrar y el tráfico
concurrente del operativo requerirán validación propia.

### Publicación y consistencia operativa

- Cada resultado lleva source_cut, publication_id, schema_version, estado de
  calidad y generated_at. generated_at no decide autoridad sobre otro escritor.
- Batch produce una versión completa de los agregados que el CLI consultará
  juntos. Puede reutilizar agregados no modificados al materializarla, sin
  releer detalle que no cambió. Se escriben Gold y las filas de serving bajo
  un nuevo publication_id; se verifican conteos y luego se activa su referencia.
- El CLI fija una publicación batch al iniciar la consulta/reporte. Todas sus
  tablas, comparaciones y rankings usan ese ID. Filas omitidas en la nueva
  versión no sobreviven accidentalmente como restos de la anterior.
- Para el operativo, cada grupo horario publica un conjunto de medidas bajo
  un ID inmutable. Su referencia de control contiene autoridad stream/batch,
  generación y estado. Un conflicto incrementa la generación y cambia la
  autoridad a batch antes de autorizar una reparación. La activación de una
  salida comprueba autoridad/generación y el predecesor esperado.
- Toda activación, también batch, exige que la referencia activa siga siendo
  el predecesor contra el que se calculó y que el corte no retroceda en ninguna
  entrada relevante. Se comprueban snapshots/progreso, no hora de escritura.
  Si otra publicación ganó, se rechaza el candidato o se recalcula; no se
  reintenta el CAS ignorando la nueva base. Si el mismo ID ya está activo,
  el reintento sólo verifica su finalización. Esto evita retrocesos dentro
  de una misma autoridad y generación.
- Un mismo responsable lógico de publicación aplica esas reglas. El mecanismo
  previsto usa filas de control con actualización condicional, no un nuevo
  servicio. Los cálculos pueden escribir candidatos; sólo el control autoriza
  hacerlos visibles. Los candidatos rechazados quedan sin referencia activa.
- Fallo antes de activar: sigue la publicación anterior, señalada como pendiente
  o fallida. No se la considera confiable si ya hay incidencias conocidas.
  Estas se consultan junto a la versión y excluyen de rankings los grupos
  afectados; si no se puede comprobar su estado, la consulta lo informa y no
  los presenta como elegibles. Sin publicación anterior: no disponible.

El estado de calidad se publica también como agregado por grupo/período, con
revisión, elegibilidad y actualización pendiente. No expone variantes ni PII.
El CLI fija su revisión además del publication_id y verifica al finalizar que
no haya cambiado; si cambió, rehace la evaluación o declara que no pudo obtener
una vista estable, sin presentar un ranking como vigente. Las correcciones no
borran incidencias ni pendientes posteriores a su corte. Quarantine continúa
restringida al responsable de datos.

Las actualizaciones condicionales de Cassandra permiten compare-and-set en
una partición; no convierten una carga de varias tablas en una transacción
aislada global. Por eso se proponen datos versionados más referencia activa.
Se deben validar niveles de lectura/escritura compatibles antes de activar
referencias, junto con conteos y cobertura. [Semántica CQL](https://cassandra.apache.org/doc/stable/cassandra/developing/cql/dml.html).

Batch y operativo tienen cortes distintos, explícitos en el reporte. En el
operativo, la coherencia se garantiza por grupo horario; no se promete que
todas las horas cambien simultáneamente. Calidad y estado de actualización
son ejes distintos: una publicación terminada puede contener un total parcial.
La retención conserva las versiones activas y las que estén siendo leídas o
reprocesadas. Tras cierre y margen de 30 días se coordina limpieza de Lake y serving.

### Claves de serving propuestas

`pub` identifica la publicación batch fijada por el CLI.

| Tabla propuesta | Partition key | Clustering / orden | Consulta y contenido |
| --- | --- | --- | --- |
| top_services_14d | org_id, reference_date, pub | rank ASC, service | Top-N por costo acumulado de los 14 días calendario terminados en reference_date; importe, cobertura y elegibilidad |
| tickets_by_org_date | org_id, severity, month, pub | date ASC | Conteos, breached_count, indicador válido, tasa y CSAT; critical y rango de 30 días |
| revenue_by_org_month | org_id, year, pub | month ASC | Importe USD o no calculable, componentes, FX original/aplicado e incidencias |
| genai_tokens_by_org_date | org_id, month, pub | date ASC | Tokens disponibles, costo registrado GenAI y cobertura de cada medida |
| finops_daily_comparison | org_id, service, month, pub | date ASC | D/D−1, delta USD, porcentaje, base comparable y calidad |
| finops_ranking_by_date | date, ranking_type, pub | rank ASC, org_id, service | Priorización entre organizaciones, por aumento USD o relativo; sólo comparaciones elegibles |
| cost_anomaly_mart | org_id, service, month, pub | date ASC | Flag/score, método, parámetros, cobertura y motivo de evaluación |
| hourly_head_by_org_day | org_id, date | hour ASC, service | Referencia activa por grupo, autoridad, generación, estado y corte |
| hourly_usage_version | org_id, service, hour, version | metric, unit | Medidas de la versión horaria y su calidad; se consulta la versión resuelta por head |
| publication_head | dataset_id | — | Publicación batch activa, corte, estado y generación; actualizaciones condicionales |
| quality_status_by_period | domain, period, quality_revision | org_id, service, hour_scope, measure | Estado agregado, elegibilidad, motivos resumidos y recálculo pendiente; sin variantes originales |
| quality_head | dataset_id | — | Revisión de calidad activa que el reporte fija y vuelve a verificar |

### Validaciones de la implementación posterior

- Reintentar un lote en cada punto de fallo: tras promover Silver, antes de
  emitir novedades y durante la carga de Cassandra. Comprobar todas las salidas.
- Repeticiones equivalentes y contradictorias, incluidas las de otros lotes.
- Costo válido con consumo inválido; medida sin contribuciones válidas;
  importe cero frente a ausencia; FX USD inconsistente y factura no calculable.
- Corregir D y verificar D/D−1, D+1/D, rankings y ventanas de 14 días.
- Intentar publicar desde un acumulador obsoleto después de reparar un grupo.
- Competencia entre candidatos del mismo escritor/generación: el corte activo
  no retrocede aunque haya reintento de CAS.
- Cambiar calidad durante la lectura de un reporte: reintentar o informar que
  no se obtuvo una vista estable, sin mostrar rankings como válidos.
- Medir frescura ordinaria, recuperación, bytes leídos, tamaño del estado y
  número de archivos; contrastar recálculo por grupos o deltas contra el día
  completo antes de sustituirlo.

Mantener fuera de Git originales, credenciales, entornos locales, checkpoints
y datos generados del Lake. Crear carpetas de código y pruebas cuando tengan
contenido. No duplicar la entrega en PDF ni crear nuevos registros de decisiones
si basta con actualizar el diseño y estas notas.
