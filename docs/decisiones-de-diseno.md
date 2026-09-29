# Decisiones de diseño

D1–D16 aceptadas entre el 24 y el 28/09/2026. Este registro conserva las
alternativas y sus costos; el [diseño consolidado](diseno-primera-entrega.md)
define los contratos técnicos propuestos. La implementación y la evidencia
reproducible siguen pendientes.

## D1. Calcular los días desde Silver

Usar detalle canónico e incidencias para reconstruir los resultados diarios.
Sumar agregados horarios leería menos, pero exigiría garantizar que todas las
horas estén presentes, corregidas y con calidad propagada. Conservar y releer
el detalle permite corregir sin depender de agregados provisionales.

## D2. Validez por medida

Un costo utilizable participa aunque su consumo esté incompleto. Excluir la
medida inválida, registrar la incidencia y marcar su agregado parcial; no
imputar ceros ni unidades. Convertir strings numéricos con trazabilidad.
Descartar toda la fila simplificaría la validación, pero excluiría costos
utilizables: se observaron 2.906 eventos con ese problema.

## D3. Retención y expiración

Conservar copias de trabajo durante el proyecto y las devoluciones. Hacerlas
elegibles para limpieza automática 30 días después del cierre de evaluación
registrado; preservar originales, código, documentación y evidencia en archivo
separado. Proteger checkpoints, versiones y reprocesamientos activos; coordinar
la limpieza del serving. No usar la fecha histórica del evento como vencimiento.

Las [lectures](referencia-lectures.md) no fijan un plazo: los 30 días son una
decisión del proyecto. HDFS requiere una tarea de limpieza; S3 Lifecycle fue
una alternativa evaluada antes de elegir almacenamiento. Tras expirar el
detalle, una corrección exige reconstruirlo desde originales. No se ejecuta
ningún borrado de datos en esta entrega.

## D4. Particionar eventos Silver por fecha UTC

Organización y servicio permanecen como columnas. Dividir también por esas
claves podría reducir lectura a mayor escala, pero fragmentaría la muestra de
720 eventos diarios. La partición diaria responde al acceso y al requisito
académico; no demuestra una mejora medida ni exige un archivo por día.

## D5. HDFS; despliegue diferido

Elegir HDFS para las zonas del Lake. Hosting, topología e instalación quedan
fuera del diseño de esta entrega; sí se estiman recursos y se documentará la
reproducción de la evidencia. No se fija un clúster de un nodo ni un proveedor.

Archivos locales reducen la preparación del perfilado; HDFS pseudodistribuido
permite experimentar con sus procesos; EMR administra el aprovisionamiento,
con recursos cloud y conservación externa al terminar el clúster. Referencias:
[Hadoop pseudodistribuido](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation),
[EMR y HDFS](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview.html),
[terminación de EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-termination.html).
La elección de HDFS no valida un entorno equivalente a Colab.

## D6. Recalcular el día completo

Reconstruir cada día afectado desde Silver e incidencias, reemplazar su versión
y actualizar D frente a D−1 y D+1 frente a D, rankings y ventanas dependientes.

| Alternativa | Beneficio | Costo o condición |
| --- | --- | --- |
| Día completo, elegida | Regla uniforme y verificable para totales y calidad | Relee eventos y publica grupos sin cambios |
| Sólo grupos organización-servicio-día afectados | Reduce agregación y escritura cuando cambian pocos grupos | Requiere identificar todos los grupos; particionar por fecha no garantiza menor lectura física |
| Deltas sobre totales | Puede evitar releer histórico | Exige conocer contribuciones anteriores, cambios aplicados, incidencias y recuperación ante reintentos |

Reintentar un delta de USD 7 sobre USD 100 no debe producir USD 114. Recalcular
simplifica ese control, pero sigue requiriendo detalle deduplicado y publicación
coherente. Las tres opciones deben retirar grupos que desaparezcan y corregir
ambas asignaciones si un cambio autorizado mueve fecha, organización o servicio.

Los 720 eventos por día favorecen simplicidad, sin probar duración. Medir bytes,
archivos, días y grupos leídos, filas publicadas y latencia. Revisar si no se
sostiene la cadencia, se acumulan pendientes o se supera el presupuesto.
Evaluar primero recálculo por grupos; deltas cuando domine la lectura histórica
y existan contratos de cambios y reintentos. Contrastar cualquier alternativa
contra el recálculo completo. [Filtros Parquet en Spark](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
no garantizan un ahorro concreto sin medirlo.

## D7. Agrupar correcciones por corrida

Reunir días afectados y recalcular cada uno una vez por corte consistente.
Cambios posteriores esperan la siguiente corrida. Recalcular por evento
repetiría trabajo, especialmente porque cada JSONL mezcla 59–60 fechas.

## D8. Batch cada hora

Publicar días UTC cerrados pendientes y corregir los que tengan novedades.
El cierre habilita la primera publicación aunque no lleguen más eventos.
Una corrida diaria reduciría oportunidades de trabajo, pero podría demorar
correcciones casi un día. La opción horaria acepta hasta el siguiente corte
más ejecución; no promete una latencia medida. El programador queda abierto.

## D9. Duplicados y conflictos

Comparar variantes antes de deduplicar: equivalentes cuentan una vez;
contradictorias quedan trazadas como incidencia. Excluir la contribución
disputada, marcar parcial y retirar comparaciones afectadas de rankings.
Si ya se publicó, corregir sus grupos. Mantener validez por medida cuando
identidad, fecha y dimensiones coincidan; si discrepan, considerar todos los
grupos potencialmente afectados.

Elegir primera o última llegada sería simple, pero dependería del orden de
recepción. Ni `timestamp`, ni `ingest_ts`, ni `schema_version` acreditan revisión.
Sólo una revisión ordenable y garantizada por la fuente permitiría desempatar;
una fecha empatada con contenidos distintos tampoco basta. El paquete no
aporta ese contrato; sus 43.200 IDs son únicos y el conflicto es preventivo.

## D10. Watermark de 60 días para esta muestra

Cubre el intervalo observado de 59 días, 23 horas y 56 minutos si no entran
timestamps posteriores ajenos a la muestra. Publicar actualizaciones
provisionales por lote, sin esperar 60 días. No es retención del Lake ni SLA
del proveedor; el cierre de todas las ventanas no está garantizado con entrada
histórica finita. [Semántica de watermark de Spark](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#handling-late-data-and-watermarking).

Un margen corto reduce estado y exige más recuperación batch; ordenar un replay
reduce desorden pero agrega preparación y cambia la simulación. Se acepta más
estado para reproducir los archivos originales; su costo está sin medir.
Preservar eventos e incidencias antes de cualquier descarte temporal.

## D11. Reparar horas en el batch horario

Reconstruir horas afectadas por tardanza o conflictos, incluidas las del día
actual, desde Silver e incidencias. Marcar pendiente de recálculo; ese estado
puede desaparecer y mantenerse un total parcial por calidad. Esta excepción
espera el siguiente corte horario más ejecución. Reparar en cada micro-lote
reduciría espera a cambio de más lecturas y coordinación. El objetivo ordinario
sigue siendo 60 segundos desde entrada disponible hasta serving consultable.

## D12. Autoridad batch sobre grupos reparados

El grupo organización-servicio-hora reparado permanece bajo batch durante la
reproducción: bloquear sus publicaciones streaming y controlar generación y
corte. También sus eventos nuevos válidos esperan el batch horario.
Recalcular en cada micro-lote cuesta más lecturas; devolver autoridad al stream
exigiría reconstruir o reconciliar su estado. Una corrección publicada no
repara por sí sola el acumulador streaming.

## D13. CLI/script, tablas y HTML

Consultar Cassandra/AstraDB y generar tablas reproducibles y un reporte Plotly
HTML autocontenido, con corte, fecha y calidad. Es una foto; volver a ejecutar
actualiza el reporte. Evita instalar Jupyter u hospedar una aplicación.
La consigna admite scripts (§1.3 y §6.3); [Plotly documenta la exportación](https://plotly.com/python/interactive-html-export/).

Diseñar las cinco consultas obligatorias, los rankings FinOps y el operativo.
Las ventanas históricas usan una fecha de referencia explícita. El mart futuro
de anomalías conserva método/calibración pendientes; variaciones e incidencias
no equivalen a detección estadística. CLI, detector y Docker Compose no se
implementan ahora. Colab o equivalente validado sigue sin definir.

## D14. Consumo agregado y detalle restringido

FinOps consulta agregados entre organizaciones y calidad; el responsable de
datos investiga detalle e incidencias. Evitar emails y texto libre innecesarios.
Navegar desde agregados a eventos facilitaría investigar dentro del CLI, pero
agrega consultas y permisos. Los roles son lógicos; no se incorpora login.

## D15. Conservar la última publicación terminada

Ante falla o carga incompleta, consultar la versión anterior con fecha, corte,
calidad y estado. Sin versión previa, informar no disponible. Ocultarla por
completo interrumpiría consultas que pueden usar una versión identificable.
Finalizar una publicación no garantiza calidad completa: mantener incidencias
conocidas y excluir rankings incompatibles. Fijar una versión coherente y evitar
que una publicación antigua reactive resultados invalidados.

## D16. Revenue mensual

Convención aceptada: `(subtotal - credits + taxes) × factor_usd`. USD usa 1 y
registra FX provisto inconsistente; otras monedas usan el factor positivo
provisto. Conservar montos, moneda, FX original y aplicado. No incorporar
cotizaciones externas ni conciliar con eventos.

Créditos vacíos y subtotales negativos quedan pendientes, sin imputar cero.
Una factura no calculable deja sin importe a su organización-mes; agregaciones
que la excluyan son parciales y muestran cobertura. Interpretar vacíos como
cero y negativos como ajustes aumentaría cobertura con supuestos contables
no confirmados por el paquete.
