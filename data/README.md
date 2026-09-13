# Datos del proyecto

La consigna enumera las siguientes fuentes. Esta lista describe lo anunciado;
todavía no constituye un inventario verificado sobre archivos reales.

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

El 13/09/2026 se buscó el dataset y un posible generador en Downloads y en los
repositorios locales del ITBA. Solo se encontró la consigna. Falta identificar
la ubicación o el mecanismo de descarga del material provisto.

Cuando se obtengan los archivos, registrar procedencia, instrucciones de acceso,
tamaños, hashes y resultados del perfilado. Conservar los originales inmutables.
No atribuir a los datos cantidades, esquemas completos ni tasas de error sin
medirlos. Toda muestra sintética debe identificarse como tal.

Los datos de trabajo se mantendrán en `data/landing/`, excluido de Git. La
estrategia para compartir una muestra reproducible se decidirá al revisar las
fuentes y sus condiciones de uso.
