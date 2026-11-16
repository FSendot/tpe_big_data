# Segunda entrega: implementación

## 1. Entorno y configuración

Se usa Docker Desktop con contenedores Linux y Compose: Python 3.11.15,
Java 17 y PySpark 3.5.8. Las dependencias Python están fijadas con hashes en
`requirements.txt`; las imágenes base, por digest. El parche de Java proviene
de los repositorios Debian y su versión efectiva se registra en la evidencia.
No se promete una reconstrucción binaria idéntica de los paquetes del sistema.

Docker aísla el runtime del Java instalado en Windows. Se adopta como entorno
equivalente a Colab: Santos Galarraga confirmó el 15/11/2026 que Docker está
permitido. La evidencia de ejecución comprueba su funcionamiento técnico.

### Decisión de almacenamiento

Para el flujo mínimo se revisa la elección de HDFS de la primera entrega:
Spark usará Parquet en un volumen local persistente, sin NameNode ni DataNodes.
La muestra no requiere almacenamiento distribuido y se prioriza reproducir el
flujo antes de agregar servicios. HDFS queda como alternativa de crecimiento.
Este entorno no demuestra alta disponibilidad ni tolerancia a la pérdida del
equipo. Los contratos de publicación dependientes de HDFS del diseño inicial
deberán adaptarse y probarse antes de implementarlos.

```mermaid
flowchart LR
    D[Dataset original: solo lectura] --> R[Runtime Docker: Python, Java, Spark local]
    R --> V[Volumen local: prueba Parquet]
    R --> E[Evidencia agregada]
    R --> C[Cassandra local opcional: consulta CQL de lectura]
    R --> A[AstraDB: conexión CQL verificada]
```

Este diagrama representa la prueba de entorno de la sección 1. Su prueba
Parquet usa dos filas sintéticas; de las fuentes verifica acceso, inventario
y hashes. La ingesta batch real se describe en la sección 2.

### Puesta en marcha

Requisitos: Docker Desktop en modo Linux y Compose v2. Presupuesto inicial:
6 GB de RAM disponibles para Docker y 5 GB de disco para imágenes y volúmenes.
Es una referencia de instalación, no un dimensionamiento productivo.

1. Extraer el ZIP del campus en una carpeta que contenga `datalake/landing`.
2. Copiar `.env.example` a `.env` y ajustar `DATASET_PATH` a esa carpeta.
   En Windows usar barras `/`; no apuntar al ZIP ni directamente a `landing`.
3. Crear una carpeta vacía `secrets/` en la raíz. Los secretos no se versionan.
4. Ejecutar desde la raíz del repositorio:

```sh
docker compose build runtime
docker compose run --rm runtime python -m unittest discover -s tests -v
docker compose run --rm runtime python scripts/check_environment.py
```

El primer build descarga Spark y Java. La configuración no secreta está en
`config/runtime.toml`. El dataset se monta sólo para lectura; Lake y evidencia
tienen rutas separadas. `lake` es un volumen administrado por Docker: no usarlo
como ubicación de los originales. No se necesita instalar Python/Java en Windows.

Para probar serving sin cuenta cloud, dejar `SERVING_MODE=local` en `.env`:

```sh
docker compose --profile local-serving up -d --wait --wait-timeout 360 cassandra
docker compose run --rm runtime python scripts/check_environment.py --serving
docker compose --profile local-serving stop cassandra
```

Cassandra sólo es accesible desde la red de Compose, sin puertos publicados al
host. Es un servicio de desarrollo sin autenticación; no debe exponerse a una
red compartida. La prueba consulta `system.local`; no crea keyspaces, tablas
ni datos de negocio. La carga del mart corresponde a la sección de serving.

### Preparar AstraDB

1. Crear una cuenta en [Astra](https://astra.datastax.com/) y una base compatible
   con conexiones CQL mediante driver; no alcanza con disponer de una colección
   para una API vectorial. Revisar las condiciones del plan antes de crearla.
2. Crear un keyspace, por ejemplo `cloud_provider`, desde la plataforma. En Astra
   la creación del keyspace se gestiona allí, no mediante `CREATE KEYSPACE` CQL.
3. Descargar el Secure Connect Bundle y guardarlo, sin extraer, como
   `secrets/secure-connect.zip`. Generar un token con acceso al keyspace.
4. Completar localmente en `.env`: `SERVING_MODE=astra`, `ASTRA_KEYSPACE` y
   `ASTRA_TOKEN`. No pegar el token en el chat, logs o documentación.
5. Ejecutar el mismo comando `check_environment.py --serving`. Para Astra no
   hace falta iniciar Cassandra local.

Sin esos datos, el chequeo informa `pending` y termina con código 1; no declara
una conexión exitosa. Un fallo de conexión informa sólo el tipo de error para
evitar volcar secretos. Revisar modo, base activa, permisos, bundle y salida
de red antes de reintentar. Nunca compartir el resultado de `docker compose
config` si contiene credenciales interpoladas.

### Evidencia y límites

Los comandos generan `evidence/environment/spark.json` y `serving.json`:
versiones efectivas, hashes de entradas, conteo y suma del round-trip Parquet,
y resultado de la consulta CQL. `--serving` exige que todos los chequeos pasen;
sin ese flag, serving queda explícitamente como no comprobado. Cada ejecución
actualiza su informe y escribe Parquet en un directorio nuevo del volumen.

La [ejecución verificada](../evidence/environment/README.md) completó siete
pruebas, el round-trip Parquet y una consulta real a AstraDB. También se había
comprobado la conexión a Cassandra local. El driver extrae el bundle en el
directorio temporal del contenedor mediante `use_default_tempdir`, conservando
el montaje de secretos en solo lectura.

La sección 1 está verificada y el uso de Docker está confirmado. La prueba
de conexión no acredita permisos de escritura ni carga desde Spark.

Para detener el entorno conservando los datos: `docker compose --profile
local-serving down`. Los volúmenes persisten; no usar `down -v` para un reinicio
normal. Si falta el dataset, corregir `.env`; si Cassandra aún inicia, esperar
su estado saludable. Si falta memoria, revisar los recursos de Docker antes
de aumentar los límites del proyecto.

Fuentes: [compatibilidad de Spark 3.5.8](https://spark.apache.org/docs/3.5.8/)
y [conexión Python a Astra](https://docs.datastax.com/en/astra-db-serverless/drivers/python-driver.html).

## 2. Contratos e ingesta batch a Bronze

Se incorporan customers_orgs, users y billing_monthly para cubrir el mínimo,
y resources para el enriquecimiento posterior. Los contratos versionados están
en `src/cloud_analytics/contracts.py`; no se infieren tipos. Todos los campos
no indicados en la tabla se conservan como string, incluidos IDs y `tags_json`.

| Fuente y clave | Tipos explícitos |
| --- | --- |
| customers_orgs: org_id | is_enterprise: boolean; signup_date: date; nps_score: decimal(38,12) |
| users: user_id | active: boolean; created_at y last_login: date |
| billing_monthly: invoice_id | month: date; subtotal, credits, taxes y exchange_rate_to_usd: decimal(38,12) |
| resources: resource_id | created_at: date |

Las fechas de estos CSV son `YYYY-MM-DD`, incluida la fecha del mes de billing.
Los booleanos admiten true/false sin distinguir mayúsculas. Los decimales
admiten signo y hasta 12 cifras fraccionarias; overflow, precisión excedida y
formatos no admitidos quedan nulos y señalados, sin redondeo silencioso. Vacíos
no equivalen a errores de conversión ni a cero. NPS decimal conserva los valores
de origen sin imponer todavía rango o escala de negocio.

### Lectura y trazabilidad

Un preflight secuencial comprueba encabezados exactos, cantidad de columnas y
archivos no vacíos. Spark lee los CSV con esquema string explícito y luego los
tipifica. Se admiten campos entrecomillados, comas, comillas escapadas y saltos
de línea dentro de un campo. La lectura conserva `raw`, una estructura con los
campos interpretados por el lector CSV: el original byte a byte permanece en
Landing. El preflight evita que filas cortas/largas se acepten silenciosamente.

Cada registro añade `ingest_ts` UTC, `ingest_date`, `source_file`, `source_hash`,
`batch_id`, `contract_version`, `source_record_hash`, `source_occurrences`,
`key_variants` y `parse_errors`. El hash de registro identifica su contenido CSV
interpretado, no un número de línea física. El hash de archivo permite volver
al original, incluso cuando un campo ocupa varias líneas.

Se colapsan sólo registros idénticos tras la lectura CSV, guardando el número
de copias en `source_occurrences`. Variantes distintas de una misma clave se
conservan y marcan `conflicting_key`; claves ausentes se marcan `missing_key`.
Los errores de tipos se registran por campo. Estas filas siguen en Bronze con
sus valores raw: no se descartan costos negativos ni se imputan créditos vacíos.
Normalización, referencias, reglas de negocio y quarantine corresponden a Silver.

### Publicación y reejecución

```text
/lake/bronze/batch_id=<id>/
  manifest.json
  customers_orgs/ingest_date=YYYY-MM-DD/*.parquet
  users/ingest_date=YYYY-MM-DD/*.parquet
  billing_monthly/ingest_date=YYYY-MM-DD/*.parquet
  resources/ingest_date=YYYY-MM-DD/*.parquet
```

Se adapta el orden de rutas del diseño inicial para publicar las cuatro fuentes
juntas. El manifiesto identifica cada fuente, esquema, entradas, conteos, errores
y archivos de salida con hashes. Leer cada fuente por su ruta del manifiesto;
no leer toda la raíz Bronze como si las cuatro fuentes tuvieran el mismo esquema.

El ID depende de hashes de entradas, contrato, implementación y versión de Spark.
Una repetición verifica archivos y checksums y devuelve `reused`, conservando
la fecha de ingesta original. Un cambio produce otro lote: no debe acumularse
como si fuera otro fragmento del mismo snapshot. La selección de snapshots
para Silver se implementará en su sección.

Se escribe primero en `.bronze-staging`, se verifica el round-trip Parquet y
que las entradas no hayan cambiado, y se promueve el directorio completo con
rename dentro del volumen local Linux. Un lock impide escritores concurrentes.
Las excepciones normales limpian el staging; una terminación forzada puede
dejar temporales sin publicar, que no deben consumirse. Estas garantías no se
extienden a otros backends. Un lote publicado alterado se rechaza, no se sobrescribe.

Se usa un archivo Parquet por fuente y fecha de ingesta para esta muestra,
sin fragmentar por organización. El preflight lee secuencialmente y Spark
procesa en local[2]; no se afirma escalabilidad distribuida medida.

### Ejecución y verificación

Con la configuración de la sección 1, ejecutar desde la raíz:

```sh
docker compose run --rm runtime python scripts/ingest_batch.py --report-name first
docker compose run --rm runtime python scripts/ingest_batch.py --report-name repeat
docker compose run --rm runtime python -m unittest discover -s tests -v
```

Los informes agregados quedan en `evidence/batch/first.json` y `repeat.json`;
no contienen filas, emails ni valores de campos personales. Los datos raw y
tipificados sólo quedan en el volumen local. `evidence_root` configura la raíz
común: los chequeos del entorno siguen escribiendo en su subcarpeta `environment`.

El comando devuelve código 1 ante fallos, con tipo de error y código seguro
cuando corresponde. Encabezado o estructura inválidos bloquean todo el lote;
errores de conversión no impiden conservar la fila en Bronze. Ver los conteos
y la comparación entre ejecuciones en [evidencia batch](../evidence/batch/README.md).

## Secciones siguientes

Pendientes, en orden: 3. streaming a Bronze;
4. Silver, calidad y quarantine; 5. features, Gold y analítica; 6. serving del
mart y consultas; 7. integración e idempotencia; 8. documentación final y backlog.
