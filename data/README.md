# Datos del proyecto

Las ocho fuentes de la consigna están presentes en el paquete
`cloud_provider_challenge_dataset_v1`, descargado del campus por el usuario.
Su `README.txt` lo identifica como un dataset sintético.

| Fuente | Contenido anunciado |
| --- | --- |
| `customers_orgs.csv` | Organizaciones, industria, región, plan y NPS |
| `users.csv` | Usuarios, roles, actividad y timestamps |
| `resources.csv` | Recursos cloud por servicio |
| `support_tickets.csv` | Tickets, severidad, SLA, CSAT y estado |
| `marketing_touches.csv` | Canales y conversiones de marketing |
| `nps_surveys.csv` | Encuestas NPS por organización y momento |
| `billing_monthly.csv` | Tres meses de facturación, impuestos, créditos y monedas |
| `usage_events_stream/*.jsonl` | Fragmentos de eventos para simular micro-lotes |

## Estado de acceso

Obtener el paquete `cloud_provider_challenge_dataset_v1` del campus y
extraerlo fuera del repositorio. Las ocho fuentes están en `datalake/landing/`
dentro del paquete. Los originales permanecen sin modificar y fuera de Git.
El script de perfilado pendiente recibirá la ruta del paquete como parámetro.

## Aclaraciones del paquete

El `README.txt`, líneas 16–19, describe inconsistencias deliberadas: nulos,
números como texto, costos negativos, outliers y variaciones de regiones y
servicios. La consigna, sección 3.2, página 4, también las contempla. Estas
fuentes no establecen que todo costo negativo sea inválido ni que todo pico
sea un error. D2/D9 conservan costos pendientes separados de los utilizables;
D16 define la calidad de facturación. Ver el [diseño consolidado](../docs/diseno-primera-entrega.md).

El paquete no aporta reglas contables que demuestren equivalencia entre
eventos de uso y facturas. Esa equivalencia no se asumirá en el análisis.

## Verificación inicial

Conteos obtenidos leyendo los CSV con `csv.DictReader` y los JSONL como objetos
JSON, sin modificar los originales. Las filas CSV excluyen el encabezado.

| Fuente | Registros | Bytes |
| --- | ---: | ---: |
| `customers_orgs.csv` | 80 | 7.746 |
| `users.csv` | 800 | 70.683 |
| `resources.csv` | 400 | 36.703 |
| `support_tickets.csv` | 1.000 | 70.945 |
| `marketing_touches.csv` | 1.500 | 101.998 |
| `nps_surveys.csv` | 92 | 3.977 |
| `billing_monthly.csv` | 240 | 15.854 |
| `usage_events_stream/`, 120 archivos | 43.200 | 12.927.515 |

Los eventos cubren desde el 03/07/2025 a las 00:02 UTC hasta el 31/08/2025
a las 23:58 UTC. Hay 10.800 eventos v1 y 32.400 v2. `carbon_kg` aparece
con valor en los 32.400 eventos v2 y `genai_tokens` en 3.132 eventos v2.
La facturación contiene junio, julio y agosto de 2025.

### Frecuencia temporal verificada el 24/09/2026

- Los eventos son intradía: hay 33.953 timestamps distintos, con intervalos
  observados de 1 a 21 minutos entre timestamps consecutivos de una misma fecha.
  Esto no establece una frecuencia de llegada.
- Las 60 fechas UTC consecutivas contienen 720 eventos cada una, incluidos
  el primer y último día. Este conteo no demuestra completitud del consumo.
- Cada archivo JSONL mezcla eventos de 59 o 60 fechas. Un archivo no equivale
  a un día ni representa una partición temporal; sirve para simular micro-lotes.
- Con los identificadores y servicios crudos hay 262 combinaciones de
  organización y servicio. De ellas, 259 tienen alguna fecha sin registros.
  Aún no se normalizaron servicios ni se evaluó calidad para este conteo.
  Ausencia de registros no demuestra consumo cero.

Comparar totales diarios requiere elegir una agregación por fecha del evento;
no se desprende de la frecuencia de los archivos. Esta comprobación puntual
no sustituye el perfilado reproducible pendiente.

Todos los JSONL se pudieron interpretar y los CSV tienen filas con el número
esperado de columnas. Esto confirma disponibilidad y lectura básica; quedan
pendientes el perfilado de nulos, tipos, claves, duplicados, relaciones y
anomalías, así como un comando versionado que regenere la evidencia.

### Comprobación acotada para el diseño de calidad

En la sesión de diseño del 24/09/2026 se revisaron los eventos, las claves de
organizaciones y las asociaciones de recursos. Esta lectura puntual no es
todavía el perfilado reproducible de las ocho fuentes.

- Todos los `org_id` y `resource_id` de eventos existen en sus maestros.
  No se observaron discrepancias de organización, servicio o región entre
  eventos y recursos. Los servicios y regiones ya están en minúsculas y sin
  espacios extremos.
- Todos los timestamps son interpretables y todos los costos son números
  finitos presentes. Hay 216 costos negativos.
- `value` tiene 877 nulos y 1.309 strings numéricos convertibles a números
  finitos. `unit` tiene 2.075 nulos.
- Hay 2.906 eventos con consumo incompleto por valor o unidad ausentes cuyo
  costo es no negativo. Descartar esos eventos completos también excluiría
  sus costos, aunque el defecto observado esté en otra medida.

Ejemplos de campos técnicos: un evento `requests` tiene `value=null`,
`unit=count` y `cost_usd_increment=0.9561`; otro `storage_gb_hours` tiene
`value=9.3311`, `unit=null` y `cost_usd_increment=0.6275`.

No se encontró un diccionario separado ni un generador dentro del paquete.
Los archivos y el README alcanzan para iniciar el inventario y la exploración
de la primera entrega. Los hashes y el mecanismo de reproducción quedan
pendientes de documentar.

### Identidad de eventos, comprobación del 28/09/2026

La lectura exploratoria de los 120 JSONL encontró 43.200 eventos con 43.200
`event_id` distintos, todos strings presentes y no vacíos. No se observaron
IDs repetidos con contenido idéntico ni conflictivo. Esta comprobación tampoco
sustituye el perfilado reproducible pendiente.

La consigna exige deduplicación por `event_id`, §4.4, p. 5. La unicidad global
es un hecho de esta muestra; el README del paquete no declara un contrato
adicional de identidad ni una clave compuesta. Sus líneas 12 y 17 describen
v2 como incorporación de campos, no como rectificación de eventos anteriores.
No se observaron campos de revisión, secuencia de actualización ni `updated_at`
en los eventos. No hay una regla documentada para resolver un mismo ID con
contenido distinto; el diseño debe tratar ese caso como un supuesto preventivo.

Los originales se mantienen fuera de Git. El script de evidencia previsto
recibirá la ruta del paquete como parámetro, sin depender de Downloads ni
modificarlo. `data/landing/` es una ubicación local opcional, excluida de Git;
el Lake del pipeline futuro usa HDFS según D5. La evidencia deberá registrar
identidad y hashes de las entradas, sin publicar PII ni credenciales.

### Facturación, comprobación del 28/09/2026

Para diseñar la consulta obligatoria de revenue mensual se revisó
`billing_monthly.csv`. Campos: `invoice_id`, `org_id`, `month`, `subtotal`,
`credits`, `taxes`, `currency`, `exchange_rate_to_usd`.

- Hay 240 filas, una por organización-mes, sin repeticiones de `invoice_id`
  ni de organización-mes. Las monedas son USD en 160 filas, ARS en 51 y EUR en 29.
- Todas las filas tienen tipo de cambio numérico, finito y positivo. En las
  160 filas USD difiere de 1: varía entre 0,85463 y 1,11791.
- Hay 137 créditos vacíos y 13 subtotales negativos. El README no define si
  un crédito vacío significa cero ni si esos negativos representan ajustes.
- No hay una fuente adicional de cotizaciones ni una fórmula contable
  documentada. El FX provisto permite diseñar la conversión sin buscar otra
  fuente, sujeto a una política explícita para sus inconsistencias.

Son comprobaciones exploratorias para D16; no sustituyen la evidencia
reproducible pendiente. La facturación sigue separada del costo de los eventos.
