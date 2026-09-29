# Cloud Provider Analytics

Proyecto integrador de Big Data, ITBA, segundo cuatrimestre de 2026.
Repositorio personal: [FSendot/tpe_big_data](https://github.com/FSendot/tpe_big_data).

## Primera entrega

Fecha límite: **28/09/2026 a las 18:30 h**.

El trabajo actual cubre diseño y fundación de datos. Debe explicar el problema,
proponer una arquitectura viable y aportar evidencia mínima de lectura,
generación o exploración de datos. La implementación completa de batch,
streaming y serving corresponde a las instancias posteriores.

Estado: alcance y decisiones D1–D16 acordados, con foco en FinOps.
El diseño está consolidado para revisión; la evidencia reproducible sigue pendiente.
El entorno de ejecución queda abierto; Colab no está elegido.
HDFS es el almacenamiento elegido para el Lake. El diseño de despliegue queda
fuera de esta entrega; se mantienen los supuestos de recursos y la reproducción
de la evidencia mínima.

- [Alcance y checklist de la entrega](docs/primera-entrega.md).
- [Alcance acordado](docs/alcance-primera-entrega.md).
- [Diseño consolidado: arquitectura, contratos, consultas y plan](docs/diseno-primera-entrega.md).
- [Decisiones D1–D16 y tradeoffs](docs/decisiones-de-diseno.md).
- [Lectures: fundamentos y referencias para el diseño](docs/referencia-lectures.md).
- [Glosario del dominio](CONTEXT.md).
- [Disponibilidad de los datos](data/README.md).

El siguiente trabajo ejecutable de esta entrega es el script de lectura y
perfilado de las ocho fuentes y su evidencia. El pipeline, el CLI de consumo
con reporte HTML y las cargas a Cassandra están diseñados, no implementados.

## Convenciones iniciales

- Documentación en español, con referencias a las secciones de la consigna.
- Identificadores de fuentes y campos conservados tal como aparecen en los datos.
- Distinguir hechos observados, requisitos, supuestos y decisiones aceptadas.
- Mantener los archivos originales de Landing sin modificaciones.
- Guardar resultados reproducibles y sus comandos en `evidence/` cuando existan.
- Excluir del control de versiones credenciales, entornos locales y datos generados.
- Actualizar `CONTEXT.md` al acordar términos del dominio. Reservar
  `docs/adr/` para decisiones con alternativas y consecuencias duraderas.
- Crear carpetas de código, configuración y pruebas cuando haya contenido real.

## Referencia

`Consigna_Proyecto_Integrador_Big_Data_ITBA_2C_2026.pdf`, versión 1.0,
3 de agosto de 2026. Primera entrega: secciones 5 y 9.1. Requisitos comunes:
secciones 2, 3, 4 y 8.
