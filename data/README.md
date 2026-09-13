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

Paquete localizado el 13/09/2026 en
`~/Downloads/cloud_provider_challenge_dataset_v1/`. Las fuentes se encuentran
en `datalake/landing/`, dentro del paquete. La revisión fue de solo lectura.

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

Todos los JSONL se pudieron interpretar y los CSV tienen filas con el número
esperado de columnas. Esto confirma disponibilidad y lectura básica; quedan
pendientes el perfilado de nulos, tipos, claves, duplicados, relaciones y
anomalías, así como un comando versionado que regenere la evidencia.

No se encontró un diccionario separado ni un generador dentro del paquete.
Los archivos y el README alcanzan para iniciar el inventario y la exploración
de la primera entrega. Los hashes y el mecanismo de reproducción quedan
pendientes de documentar.

Los datos de trabajo se mantendrán en `data/landing/`, excluido de Git. La
estrategia para compartir una muestra reproducible se decidirá al revisar las
fuentes y sus condiciones de uso.
