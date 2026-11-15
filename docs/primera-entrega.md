# Cloud Provider Analytics

Primera evaluación parcial. Diseño y fundación de datos. 05/10/2026.

[Problema](#problema-y-objetivos) · [Datos y 5V](#datos-y-exploración-inicial) ·
[Arquitectura v1](#arquitectura-v1) · [Matriz requisito–componente](#matriz-requisitocomponente) ·
[Data Lake](#organización-del-data-lake) · [Decisiones](#registro-de-decisiones) ·
[Plan inicial](#plan-inicial)

## Problema y objetivos

Un proveedor cloud necesita entender cómo cambia el consumo de sus clientes
y qué explica los aumentos de costo. FinOps es el usuario principal: debe
poder revisar una organización y servicio, comparar días consecutivos y
priorizar tanto aumentos porcentuales como diferencias en dólares. Soporte
necesita seguir tickets críticos e incumplimientos de SLA; Producto, analizar
uso de servicios y GenAI.

Las preguntas principales son qué organizaciones y servicios aumentaron su
costo respecto del día anterior, cuánto aumentaron en dólares y en porcentaje,
y qué parte del resultado puede compararse sin problemas de calidad. Soporte
consultará los tickets críticos y el SLA de los últimos 30 días; Producto,
los servicios de mayor costo en 14 días y el uso de GenAI.

El costo registrado en los eventos y el ingreso por facturación son medidas
distintas. Se analizarán por separado, sin asumir que deben coincidir ni
calcular un margen a partir de ellas.

En esta entrega diseñamos el recorrido de los CSV y JSONL desde Landing hasta
las consultas. Elegimos HDFS y Parquet para el almacenamiento, PySpark para
procesar los datos y Cassandra/AstraDB para servir resultados. El CLI mostrará
tablas y un reporte HTML con Plotly. La ejecución completa del pipeline queda
para las próximas entregas.

| Objetivo | Criterio de evaluación |
| --- | --- |
| Explicar aumentos de costo | Comparar días UTC consecutivos por organización y servicio, mostrando ambos costos, diferencia en USD y porcentaje |
| Priorizar casos para FinOps | Permitir ordenar por aumento absoluto o relativo y excluir comparaciones con costos parciales |
| Consultar consumo operativo | Agregar por organización, servicio y hora UTC, separando métrica y unidad |
| Actualizar el operativo | Objetivo de hasta 60 segundos desde que el archivo está disponible hasta que el resultado puede consultarse |
| Corregir resultados históricos | Recalcular cada hora los períodos afectados y mostrar fecha, cobertura y estado de actualización |

El objetivo de 60 segundos debe validarse con mediciones. Los grupos que
necesiten reparación pasarán al batch horario, también para sus novedades
posteriores, y esperarán el siguiente corte más su tiempo de procesamiento.
El reporte HTML representa el corte consultado; para actualizarlo se ejecuta
de nuevo el CLI.

## Datos y exploración inicial

El paquete sintético `cloud_provider_challenge_dataset_v1`, disponible en el
campus, contiene siete CSV y 120 archivos JSONL dentro de `datalake/landing/`.
El script [profile_data.py](../scripts/profile_data.py) lee los CSV y cada
línea JSON sin modificar los originales. La [evidencia](../evidence/profile.json)
registra conteos, nulos, tipos JSON, claves, referencias, fechas y SHA256 por
archivo. Las [instrucciones de datos](../data/README.md) permiten repetir la
lectura con Python estándar. Los resultados siguientes coinciden con esa ejecución.

| Fuente | Registros | Grano y clave observada | Campos principales y calidad |
| --- | ---: | --- | --- |
| customers_orgs.csv | 80 | Organización, org_id | Categorías, plan, signup_date y NPS; 11 NPS vacíos |
| users.csv | 800 | Usuario, user_id | org_id, active, created_at, last_login y email; 139 last_login vacíos |
| resources.csv | 400 | Recurso, resource_id | Organización, servicio, región, estado y tags_json; 83 tags vacíos |
| support_tickets.csv | 1.000 | Ticket, ticket_id | Severidad, fechas, sla_breached y CSAT; 240 cierres y 254 CSAT vacíos |
| marketing_touches.csv | 1.500 | Interacción, touch_id | Organización, campaña, canal, timestamp, clicked y converted |
| nps_surveys.csv | 92 | Encuesta por organización y fecha | NPS y comentario; 19 NPS y 10 comentarios vacíos, sin survey_id |
| billing_monthly.csv | 240 | Factura, invoice_id; una por organización-mes | Montos, moneda y tipo de cambio; 137 créditos vacíos y 13 subtotales negativos |
| usage_events_stream/*.jsonl | 43.200 | Evento, event_id | Timestamp, organización, recurso, servicio, métrica, valor, unidad, costo y versión de esquema |

La muestra ocupa aproximadamente 13,24 MB sin comprimir. Los eventos van del
03/07/2025 a las 00:02 al 31/08/2025 a las 23:58 UTC, con 720 eventos por día.
Cada archivo mezcla 59 o 60 fechas, por lo que el orden de los archivos no
representa el orden temporal de los eventos. La facturación cubre junio,
julio y agosto de 2025.

Los 43.200 identificadores de evento son únicos. Todas las referencias a
organizaciones y recursos existen, y sus asociaciones de organización,
servicio y región coinciden. Hay 216 costos negativos, 877 valores de consumo
nulos, 1.309 valores como texto numérico y 2.075 unidades ausentes. En 2.906
eventos el consumo está incompleto pero el costo es no negativo: descartar
la fila entera también eliminaría costos utilizables.

La versión v1 aporta 10.800 eventos y v2, 32.400. Todos los v2 incluyen
`carbon_kg`; 3.132 incluyen `genai_tokens`. Las versiones representan cambios
de esquema, no revisiones de un mismo evento. De 262 combinaciones de
organización y servicio, 259 tienen algún día sin registros. Esa ausencia
no demuestra consumo cero.

En facturación hay 160 filas USD, 51 ARS y 29 EUR. Todos los tipos de cambio
son positivos, pero los 160 correspondientes a USD difieren de 1. El paquete
no aclara si un crédito vacío significa cero ni si un subtotal negativo es
un ajuste. Estas ambigüedades requieren reglas de calidad explícitas.

Los CSV se tratarán como snapshots: revisión diaria de nuevas entregas de
maestros y actividad, y facturación mensual con posibles rectificaciones.
Es una frecuencia propuesta; las fechas de las filas no prueban la frecuencia
de llegada. Los atributos de los maestros describen su snapshot, sin asumir
que representan el estado histórico de cada cliente.

Los identificadores y categorías se interpretarán como texto; fechas y tiempos,
como date o timestamp UTC; importes y medidas, como decimales; conteos y tokens,
como enteros; active, clicked, converted y sla_breached, como booleanos. Los CSV
llegan como texto y requieren casts explícitos. tags_json contiene JSON anidado.
Los tipos observados del JSONL y los vacíos por columna están en la evidencia;
esos tipos no sustituyen el contrato de validación de Silver.

### Las 5V en este caso

| Dimensión | Implicación para el diseño |
| --- | --- |
| Volumen | La muestra cabe en un equipo local. Spark y HDFS permiten estudiar el procesamiento distribuido; como escenario de crecimiento, 1.000 veces los eventos daría 43,2 millones en 60 días |
| Velocidad | Separar la actualización operativa por micro-lotes del análisis diario y las correcciones batch |
| Variedad | Integrar CSV, JSONL v1/v2 y JSON anidado en tags con esquemas explícitos |
| Veracidad | Conservar nulos, conflictos, tipos ambiguos y problemas de moneda con trazabilidad y calidad por medida |
| Valor | Explicar variaciones de costo, priorizar su revisión y relacionar consumo con información de Soporte y Producto |

## Arquitectura v1

Se propone un patrón híbrido. Structured Streaming procesa los eventos para
el operativo; el batch incorpora CSV, calcula resultados diarios y corrige
períodos anteriores. Ambos usan el detalle validado del Lake. Así, una
corrección histórica no depende de conservar todo el estado del streaming.

```mermaid
flowchart TD
    F["7 CSV: snapshots"] --> L
    E["120 JSONL: reproducción de eventos"] --> L
    L --> I["Ingesta PySpark: lectura batch CSV y readStream JSONL"]
    I --> B
    subgraph H["Data Lake en HDFS"]
      L["Landing: originales"]
      B["Bronze: tipos y trazabilidad"] -->|"PySpark: validación por medida"| S["Silver: detalle validado"]
      B -.-> Q["Quarantine: incidencias y variantes"]
      S -.-> Q
      G["Gold: resultados analíticos"]
    end
    S --> N["Nuevas contribuciones válidas"]
    N --> ST["Structured Streaming: ventanas de una hora UTC"]
    S --> BA["PySpark batch: días cerrados y reparaciones cada hora"]
    Q -.-> BA
    BA --> G
    G --> P["Publicación de versiones completas"]
    ST --> P
    P --> C["Cassandra/AstraDB: tablas por consulta"]
    C --> R["CLI: tablas y reporte HTML con Plotly"]
    T["Esquemas, permisos, calidad, manifiestos y métricas"] -.-> H
    T -.-> I
    T -.-> ST
    T -.-> BA
    T -.-> P
```

Usaremos Parquet en las capas transformadas porque las consultas leen columnas
y períodos concretos. PySpark calculará los agregados y Cassandra guardará
tablas preparadas para esas consultas. Las novedades operativas viajarán en
archivos derivados internos. Todavía falta validar las versiones y el entorno
de ejecución.

### Elección del almacenamiento

HDFS se mantiene como decisión de diseño para estudiar almacenamiento
distribuido y un escenario de crecimiento; los 13,3 MB de la muestra no lo
requieren. Su despliegue queda sujeto a validar el aporte frente a su costo
operativo. Una instalación en un único equipo no tolera la pérdida de ese equipo.

| Alternativa | Aporte y costo para el proyecto |
| --- | --- |
| Archivos locales | Suficientes para el perfilado actual, sin servicios adicionales; no demuestran almacenamiento distribuido |
| HDFS | Permite estudiar bloques y replicación; exige operar NameNode/DataNodes, permisos y recuperación |
| Almacenamiento de objetos | Alternativa para separar cómputo y almacenamiento; requiere configurar acceso y revisar costos y contratos de publicación |

Si HDFS dificulta la reproducción sin aportar una ventaja demostrable, se
revisará la elección, el diagrama y las garantías de publicación. Parquet y
las zonas no dependen de HDFS, pero el rename previsto sí requiere revisión
al cambiar de backend. La decisión se apoya en Hadoop, p. 5; HDFS, pp. 8–11;
e Introducción, p. 27.

### Elección del patrón

| Patrón | Evaluación para este caso |
| --- | --- |
| Batch | Alcanza para reportes diarios, pero con corridas horarias no cumple el objetivo operativo de 60 segundos |
| Lambda | Separa vistas rápidas y batch que luego deben conciliarse; mantener dos cálculos completos agrega trabajo para esta muestra |
| Kappa | Centra el cálculo en el stream y sus replays; integrar snapshots CSV y reparar períodos históricos resulta menos directo |
| Híbrido, elegido | Streaming para horas operativas y batch para snapshots, días cerrados y reparaciones, con detalle Silver compartido y reglas de publicación explícitas |

La combinación se parece a Lambda en su separación de latencias. Aquí el batch
tiene responsabilidades propias y reconstruye desde Silver; el consumidor no
fusiona una vista rápida con otra histórica para responder una misma consulta.

## Matriz requisito–componente

| Requisito u objetivo | 5V relacionadas | Componentes y decisión | Cómo se comprobará |
| --- | --- | --- | --- |
| Comparar costos D/D−1 y priorizar aumentos | Valor, veracidad | PySpark batch sobre Silver, tablas FinOps en Cassandra y CLI; separar diferencia USD y porcentaje | Casos con base cero, fecha ausente y costo parcial; excluir rankings no comparables |
| Consultar operativo en hasta 60 s | Velocidad | Reproducción JSONL, Structured Streaming, ventanas horarias y serving por grupo | Medir desde archivo disponible hasta resultado consultable; reparaciones se miden aparte |
| Recalcular historia y repetir lotes | Veracidad, velocidad | HDFS conserva detalle; batch horario reconstruye días y activa versiones completas | Reintentar una corrida y corregir D sin duplicar aportes; verificar también D+1 y ventanas de 14 días |
| Integrar CSV y JSONL v1/v2 | Variedad | Esquemas explícitos en Bronze y normalización por medida en Silver | Perfil de columnas, nulos, casts, referencias y cobertura de campos v2 |
| Leer períodos y columnas concretos al crecer | Volumen | HDFS, Parquet y particiones diarias; recálculo completo para los 720 eventos diarios actuales | Medir bytes leídos, duración, archivos y memoria antes de aumentar particiones o recursos |
| Conservar origen y explicar exclusiones | Veracidad | Hashes, manifiestos, Quarantine e indicadores de calidad; detalle restringido | Trazar una salida a archivo y reglas; conteos por motivo sin exponer datos personales |
| Consultas de Soporte, Producto y FinOps | Valor, variedad | Marts Gold, tablas Cassandra por consulta y reporte HTML con Plotly | Verificar las consultas, su período, corte y cobertura sin joins en serving |

## Organización del Data Lake

Las rutas parten de `/cloud_provider/`. Se conservan los identificadores de
origen, se usan nombres derivados en snake_case y se normalizan tiempos a UTC.
Landing guarda CSV/JSONL; las capas de transformación usan Parquet.

| Zona y partición | Contenido y condición de promoción |
| --- | --- |
| `landing/<fuente>/ingest_date=.../` | Archivos originales completos e inmutables, con ruta, tamaño y hash |
| `bronze/<fuente>/ingest_date=.../batch_id=.../` | Grano original, esquema explícito y valores crudos junto a los interpretados; añade ingest_ts y source_file |
| `silver/usage_events/event_date=.../snapshot_id=.../` | Eventos normalizados, referencias verificadas y validez por medida; un manifiesto selecciona la versión vigente de cada día |
| `silver/<csv>/snapshot_date=.../snapshot_id=.../` | Maestros y otras fuentes tipificados y versionados; facturación puede subdividirse por mes |
| `quarantine/ingest_date=.../source=.../` | Errores, variantes contradictorias y medidas pendientes, vinculados al original y a los grupos afectados |
| `gold/<mart>/period=.../publication_id=.../` | Agregados de FinOps, Soporte y Producto con cobertura y corte de datos; período diario o mensual |
| control/ y checkpoints/ | Manifiestos, progreso y checkpoint independiente por consulta |

Particionar eventos Silver sólo por fecha permite reconstruir días sin crear
numerosos archivos por organización y servicio. Con 720 eventos diarios,
agregar esas divisiones fragmentaría demasiado los datos. Los snapshots
versionados permiten leer un corte estable mientras se prepara otro.

Antes de pasar eventos a Silver comprobaremos el identificador, la fecha, las
referencias a organización y recurso, y la validez de costo y consumo por
separado. Guardaremos las filas con problemas junto con el archivo y la regla
que los detectó. Un manifiesto identificará las salidas de cada promoción.

### Retención y operación

Un integrante registrará el cierre al concluir la evaluación y sus
correcciones. Desde esa fecha se cuentan los 30 días de conservación adicional,
una decisión del proyecto. El watermark no determina la retención del Lake.

| Activos | Conservación y condición de limpieza |
| --- | --- |
| Landing original, código, documentación y evidencia | Se archivan por separado; originales privados y evidencia sin datos personales. Sin borrado programado: su conservación se revisa al cierre |
| Bronze, Silver, Gold, novedades y serving | Elegibles desde cierre + 30 días, protegiendo versiones activas, lectores y reprocesos; limpieza coordinada entre Lake y serving |
| Quarantine | Mismo plazo, siempre que las incidencias estén resueltas |
| Control, manifiestos y checkpoints | Mismo plazo, sólo cuando no sean necesarios para reanudar procesos o reconstruir resultados |

Santos Galarraga autorizará y ejecutará la limpieza: revisar candidatos y
dependencias, verificar archivos de respaldo y hashes, y registrar rutas,
fecha, motivo y resultado. Las dependencias activas posponen el borrado.
No hay limpieza automática implementada.

## Reglas de calidad y cálculo

La validez se evalúa por medida. Un evento puede aportar costo aunque falten
valor o unidad de consumo. Los costos negativos quedan pendientes de
validación, separados del total utilizable; los costos altos válidos se
conservan. Un total es parcial si excluye contribuciones pendientes. Una suma
sin valores utilizables se muestra sin importe, nunca como cero.

Antes de deduplicar por `event_id` se comparan sus variantes, también contra
el historial retenido. Repeticiones equivalentes cuentan una vez. Si hay
contenidos contradictorios, se conserva la incidencia y se excluye la
contribución disputada. Ni la llegada más reciente ni `schema_version`
identifican una corrección autorizada. Un desempate necesitaría una secuencia
de revisión garantizada por la fuente, que este paquete no ofrece.

| Medida | Cálculo |
| --- | --- |
| Costo diario u horario | Suma de cost_usd_increment utilizable por organización, servicio y período UTC |
| Requests | Suma de value con metric=requests y unit=count |
| CPU | Suma de value con metric=cpu_hours y unit=hours |
| Almacenamiento | Suma de value con metric=storage_gb_hours y unit=gb_hours |
| GenAI | Suma de genai_tokens disponibles y costo de service=genai por separado; no se infiere una tarifa por token |
| Carbono | Suma de carbon_kg válido, con cobertura; su ausencia en v1 no equivale a cero |
| Soporte | Tickets por fecha de creación y severidad; tasa sla_breached sobre indicadores válidos y CSAT promedio con cobertura |
| Producto | Uso por servicio, recursos y usuarios por snapshot, interacciones clicked/converted y NPS por período |
| Revenue mensual | (subtotal − credits + taxes) × factor de conversión a USD |

Las métricas de consumo se separan por nombre y unidad, independientemente del
servicio. No se deduce una unidad ausente ni se suman unidades incompatibles.

FinOps compara el día D con D−1. La diferencia absoluta es `costo_D − costo_D−1`
y la relativa es esa diferencia dividida por `costo_D−1`, multiplicada por 100.
Si no hay registros previos o su costo es cero, el caso queda sin base
porcentual. Un cero real permite calcular diferencia absoluta; un importe
ausente, no. Las comparaciones con costos parciales quedan fuera de ambos
rankings. No se reemplaza el día anterior por la última fecha con actividad.

Para revenue, USD usa factor 1 y registra la inconsistencia del FX original;
las demás monedas usan el factor positivo provisto. Se conservan moneda,
componentes y tipos de cambio original y aplicado. Créditos vacíos y subtotales
negativos quedan pendientes. Si la única factura de una organización-mes no
es calculable, su importe queda ausente; una suma que la excluya es parcial.
La fórmula es una convención del proyecto, no una regla contable confirmada
por el README del paquete.

El mart de anomalías incluirá organización, servicio, fecha, flag o score y
contexto. Se evaluará un umbral por percentil del histórico comparable, sin
suponer normalidad. Ventana, percentil y mínimo de observaciones requieren
calibración; sin base suficiente se mostrará no evaluable. Un aumento o una
incidencia de calidad no constituyen por sí solos una anomalía de costo.

## Procesamiento y correcciones

### Streaming

Un script presentará archivos JSONL completos al directorio de entrada, a una
tasa configurable. La ingesta usará esquema explícito, conservará Bronze y
validará Silver antes de emitir nuevas contribuciones operativas. Los
conflictos se detectan antes de cualquier descarte por duplicación o tardanza.

Se separan tres consultas con checkpoints propios: ingesta y validación,
deduplicación con watermark, y agregación por ventanas horarias. Las dos
últimas se conectan mediante archivos Parquet añadidos de forma inmutable;
no se observan como entradas nuevas las particiones Silver reemplazadas.
La agregación publica resultados provisionales por micro-lote.

El watermark de 60 días cubre el desorden de esta muestra, cuyo intervalo es
de 59 días, 23 horas y 56 minutos, siempre que no se mezclen timestamps
posteriores ajenos a ella. Evita preparar un replay ordenado, a cambio de
conservar más estado. Un margen corto reduciría ese estado y aumentaría la
recuperación batch. Su costo se medirá antes de extrapolarlo a otra escala.

Como punto de partida se proponen triggers de 10 segundos por consulta:
hasta 30 segundos de espera entre etapas y otros 30 para procesar y publicar.
Ese presupuesto debe contrastarse con lectura, validación, backlog y tamaño
del estado. Los resultados no esperan 60 días para publicarse; una entrada
finita tampoco garantiza cerrar todas las ventanas.

### Batch y equivalente MapReduce

Cada hora se toma un corte de Silver, incidencias y snapshots CSV. Se agrupan
los días UTC cerrados pendientes y se reconstruye cada día una vez. El cierre
de un día habilita su primera publicación aunque no lleguen eventos nuevos.
Las novedades posteriores al corte esperan la siguiente corrida.

```text
Map:     validar y proyectar contribuciones por medida e incidencias
         clave = organización, servicio, fecha UTC
Shuffle: reunir los valores de cada clave
Reduce:  sumar importes, consumos y conteos de calidad
Después: unir D con D−1, calcular diferencias y ordenar rankings elegibles
```

Este flujo se implementará con DataFrames/SQL. La base del cálculo es el
detalle Silver, para no depender de que todos los agregados horarios estén
presentes y corregidos. Cambiar D exige actualizar D/D−1, D+1/D y las ventanas
de 14 días que lo incluyen. Cambios de snapshots CSV o dimensiones también
invalidan los resultados que dependan de ellos.

| Estrategia de corrección | Ventaja | Costo |
| --- | --- | --- |
| Día completo, elegida | Totales y calidad se reconstruyen con una regla verificable | Relee eventos y publica grupos sin cambios |
| Sólo grupos afectados | Reduce agregación y escritura si cambian pocos grupos | Requiere localizar todos los afectados; filtrar dentro de una partición diaria no garantiza menos lectura física |
| Deltas | Puede evitar releer el histórico | Debe recordar contribuciones anteriores y evitar aplicar dos veces un cambio al reintentar |

Con 720 eventos por día se prioriza el recálculo completo. Se revisará si la
duración impide sostener la cadencia o se acumulan correcciones. Recalcular
por grupos sería la primera alternativa; los deltas necesitan contratos más
estrictos de cambios y reintentos.

El batch también repara horas afectadas por conflictos o eventos fuera del
margen garantizado, incluidas horas del día actual. Cada grupo reparado queda
bajo autoridad batch durante la reproducción. Se bloquean sus escrituras
streaming para evitar que un acumulador anterior vuelva a publicar un total
incorrecto. Esto aumenta la espera de sus novedades, pero evita reconstruir
y reconciliar el estado streaming en esta etapa.

### Publicación y recuperación ante fallos

Los resultados se escriben bajo una versión nueva y se verifican antes de
activar su referencia en Cassandra. El reporte fija una publicación para
leer juntos totales, comparaciones y rankings. Si falla una carga, sigue
disponible la última versión terminada con su fecha y estado; sin una versión
previa, se informa que el resultado no está disponible.

Cada publicación identifica su corte de entradas. El control comprueba el
predecesor esperado y que el corte no retroceda; para horas operativas comprueba
además autoridad y generación. Un candidato obsoleto se rechaza o recalcula.
Los reintentos completan las salidas previstas del lote sin duplicar aportes.
Las actualizaciones condicionales de Cassandra no son una transacción global
entre tablas: se usarán datos inmutables y referencias activas, con niveles
de consistencia a validar.

Las incidencias conocidas siguen visibles aunque el valor publicado sea
anterior. El reporte verifica una revisión estable de calidad antes de
presentar rankings. Un resultado pendiente de recálculo necesita actualizarse;
un total parcial tiene contribuciones no utilizables. Son estados distintos,
y terminar una publicación no elimina sus problemas de calidad.

## Consultas y consumo

Cassandra se modela a partir de las consultas. Spark prepara las agregaciones;
el CLI lee particiones acotadas, sin joins ni escaneos globales. Las tablas
batch incluyen la versión de publicación y acotan el tiempo por mes, año o
fecha de referencia.

| Consulta | Tabla y clave de partición propuesta | Orden o rango |
| --- | --- | --- |
| Costo y requests diarios por organización y servicio | org_daily_usage_by_service: org_id, service, month, publication_id | Fecha ascendente |
| Top-N de servicios por costo acumulado en 14 días de una organización | top_services_14d: org_id, reference_date, publication_id | Ranking, servicio |
| Tickets críticos y evolución de SLA en 30 días | tickets_by_org_date: org_id, severity, month, publication_id | Fecha ascendente |
| Revenue mensual en USD | revenue_by_org_month: org_id, year, publication_id | Mes |
| Tokens y costo GenAI diarios | genai_tokens_by_org_date: org_id, month, publication_id | Fecha |
| Variaciones FinOps y sus rankings | finops_daily_comparison: org_id, service, month, publication_id; finops_ranking_by_date: date, ranking_type, publication_id | Fecha o ranking, con desempate estable por organización y servicio |
| Anomalías de costo | cost_anomaly_mart: org_id, service, month, publication_id | Fecha, flag/score y contexto |
| Operativo horario | Referencias por org_id y date; medidas por org_id, service, hour y version | Hora y servicio; luego métrica y unidad |

El CLI recibirá organización, servicio, rango de fechas y N. Las ventanas se
anclan en una fecha explícita para consultar el histórico de 2025: 14 días
significa desde reference_date−13 hasta reference_date. Los días sin registros
se reflejan en cobertura y no se sustituyen por días anteriores.

El reporte HTML reunirá costo y variaciones, top-N, tickets/SLA, revenue y
GenAI. Mostrará el corte consultado y la calidad de cada medida. El operativo
puede tener un corte distinto del batch; su coherencia se controla por grupo
horario. Las tablas de control y el detalle de eventos quedan fuera de la
interfaz de consumo.

## Gobierno y observabilidad

El consumidor tiene lectura de agregados e indicadores de calidad. El
responsable de datos accede a originales e incidencias para investigar.
El proceso de publicación escribe versiones y referencias de control.
Un único integrante asume estas responsabilidades en el proyecto.

Esquemas, reglas y manifiestos se versionan junto al código. Cada resultado
registra entradas, hashes, snapshot de dimensiones y versión de reglas.
Un cambio de interpretación requiere reproceso trazable. Emails, responsables
comerciales y texto libre se restringen al detalle cuando sean necesarios;
reportes, logs y evidencia omiten datos personales y credenciales.

Registraremos cuántas filas entran y cuántas quedan fuera de cada cálculo, con
su motivo. También mediremos la duración de los lotes y los días pendientes de
reparación. Estas salidas servirán para comprobar las reglas de calidad durante
la implementación.

## Registro de decisiones

Las decisiones vigentes y su costo se resumen aquí. Las reglas completas están
en las secciones de calidad, procesamiento y consumo; los cambios se conservan
en el historial de Git.

| Decisión | Motivo y costo aceptado |
| --- | --- |
| Reconstruir días desde Silver y agrupar correcciones por corrida | Evita depender de horas provisionales; relee el día completo y actualiza comparaciones y ventanas afectadas |
| Validar por medida y comparar variantes antes de deduplicar | Conserva costos utilizables con consumo incompleto; exige propagar parcialidad e incidencias |
| HDFS y partición diaria, con despliegue pendiente | Permite trabajar con las zonas del Lake sin fragmentar la muestra por organización y servicio |
| Conservar copias hasta cierre más 30 días | Permite revisión y reproceso; exige limpieza coordinada y protección de versiones en uso |
| Batch horario y autoridad batch sobre horas reparadas | Evita que un acumulador streaming obsoleto reemplace una corrección; sus novedades también esperan la próxima corrida |
| Watermark de 60 días para la muestra | Admite el desorden de los archivos originales; retiene más estado que un margen corto |
| CLI y HTML sobre agregados | Permite consultas y reportes reproducibles sin hospedar una aplicación; actualizar el HTML requiere otra ejecución |
| Mantener la última publicación completa y su calidad | Sostiene las consultas ante fallos; se debe mostrar antigüedad y excluir resultados invalidados |
| Revenue separado del costo de uso; USD con factor 1 | Evita una conciliación sin respaldo; créditos vacíos y subtotales negativos reducen la cobertura hasta validar su significado |

## Plan inicial

### Supuestos y decisiones abiertas

- Los JSONL simulan llegadas mediante reproducción; no existe una fuente en vivo
  ni una frecuencia de llegada observada. Los 60 segundos son un objetivo a medir.
- Los CSV representan snapshots. La revisión diaria y el batch horario son
  decisiones de diseño, no frecuencias garantizadas por el proveedor.
- Un `event_id` identifica el evento; v2 cambia su esquema. Falta un contrato de
  revisión que permita resolver versiones contradictorias automáticamente.
- Los 30 días de retención después del cierre y la fórmula de revenue son
  convenciones del proyecto. Los vacíos y negativos no tienen interpretación
  contable confirmada por el paquete.

Siguen abiertos el entorno validado para ejecutar el pipeline, versiones,
topología de HDFS, scheduler y calibración de anomalías. Se resolverán al
preparar la implementación posterior, antes de medir latencias o publicar resultados.

### Riesgos y mitigaciones

| Riesgo | Mitigación |
| --- | --- |
| Estado grande y archivos pequeños | Medir memoria, bytes y cantidad de archivos; revisar watermark y particiones al crecer |
| Doble contribución o conflictos ocultos | Comparar variantes antes de deduplicar y verificar reintentos |
| Una escritura streaming reemplaza una reparación | Autoridad batch por grupo y control de generación y predecesor |
| Caída a mitad de carga o novedades durante una corrida | Corte estable, versiones completas y pendientes posteriores conservados |
| Interpretación contable o histórica incorrecta | Componentes originales, cobertura y versiones de maestros visibles |
| APIs, driver o consistencia incompatibles | Fijar versiones y comprobar recuperación y publicación antes de integrar |
| Entorno alternativo sin validar | Resolver la condición de Colab o equivalente validado antes de implementar el pipeline |

### Roles y recursos

Un integrante cubre tres responsabilidades: análisis de negocio para preguntas
y reglas; ingeniería de datos para ingesta, calidad y publicación; y validación
para evidencia, reproducción y medición. En esta etapa sólo se ejecuta la
exploración inicial.

El perfilado usa Python 3.9 o posterior con biblioteca estándar en un equipo
local. Recorre los JSONL por línea y conserva claves, fechas y los CSV en memoria;
no es un perfilador distribuido. Como referencia de planificación: 2 CPU
lógicas, 4 GB de RAM y 200 MB libres para entradas y resultados, además del
entorno instalado. El dimensionamiento del pipeline requerirá medir bytes por
capa, versiones retenidas, réplicas, checkpoints y memoria del estado.

### Esfuerzo y próximos pasos

El plan inicial estimaba 6–11 h para diseño, perfilado y evidencia, ya disponibles.
Para las correcciones se estiman 3–4,5 h adicionales; no son horas registradas.
Santos Galarraga es responsable de todas las tareas siguientes.

| Tarea | Esfuerzo | Dependencia | Criterio de cierre |
| --- | --- | --- | --- |
| Registrar feedback y justificar HDFS | 1–1,5 h | Devolución y arquitectura | Observaciones trazables; alternativas y límites explícitos |
| Precisar retención | 0,5–1 h | Zonas y publicación | Plazos, responsable y protecciones definidos |
| Reproducir perfilado | 0,5–1 h | Dataset y script | Hashes y estadísticas comparados; diferencias explicadas |
| Priorizar validaciones y sincronizar documentos | 1 h | Tareas anteriores | Plan, README y contexto consistentes |

Las correcciones documentales y la reproducción están realizadas. Las próximas
validaciones se priorizan así; quedan pendientes para la implementación y no
forman parte del trabajo ejecutado en esta entrega:

| Orden | Validación | Criterio de aceptación |
| --- | --- | --- |
| 1 | Entorno y versiones | Colab o equivalente validado, ejecución mínima y versiones registradas |
| 2 | Fuentes y calidad | Casos válidos e inválidos, compatibilidad v1/v2 y reglas independientes por medida |
| 3 | Recorrido mínimo hasta serving | Conexión comprobada y salida trazable con conteos e importes conciliados |
| 4 | Reinicio e idempotencia | Repetición y recuperación sin duplicar filas ni contribuciones |
| 5 | Latencia y operación | Medir frescura, backlog, memoria, bytes y archivos antes de afirmar el objetivo de 60 segundos |

## Correcciones posteriores al feedback

Revisión del 15/11/2026. Los ajustes se integraron en las secciones existentes;
la versión evaluada se conserva en Git. No se implementa el segundo parcial.

| Observación | Resolución |
| --- | --- |
| Complejidad de HDFS | Alternativas y condición de revisión en Arquitectura |
| Retención y operación | Plazos, protecciones y responsable en Organización del Data Lake |
| Responsabilidades y prioridades | Tareas asignadas y orden de validación en Plan inicial |
| Reproducibilidad | Perfilado repetido: hashes de entradas y estadísticas coinciden; diferencias de entorno en [evidencias](../evidence/README.md) |

## Referencias

- Consigna del Proyecto Integrador Big Data ITBA, segundo cuatrimestre de 2026; dataset del campus y su README.
- `Clase_01_Introduccion_a_Big_Data_ITBA.pdf`, pp. 6–10, 16–17 y 22–24: 5V, batch/streaming, MapReduce y arquitectura.
- `Clase_02_Framework_Hadoop_Intro.pdf`, p. 5, y `04-HDFS.pdf`, pp. 5–11 y 21: almacenamiento HDFS y eliminación de archivos. El margen de retención de 30 días es una decisión del proyecto.
- `Clase_03_Procesamiento_ de_datos_distribuidos_con_Spark.pdf`, pp. 25, 28, 38–39 y 44: DataFrames, Parquet, particiones, ETL y streaming. Las páginas citadas corresponden al PDF.
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html): operadores, watermark, modos de salida y recuperación.
- [Spark y Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html): lectura columnar y filtros.
- [Modelo de datos de Cassandra](https://cassandra.apache.org/doc/stable/cassandra/developing/data-modeling/intro.html) y [semántica CQL](https://cassandra.apache.org/doc/stable/cassandra/developing/cql/dml.html): tablas por consulta y actualizaciones condicionales.
- [Exportación HTML de Plotly](https://plotly.com/python/interactive-html-export/): reportes autocontenidos.
