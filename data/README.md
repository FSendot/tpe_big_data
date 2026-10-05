# Datos

Descargar `cloud_provider_challenge_dataset_v1` del campus de la materia y
extraerlo fuera del repositorio. Es un paquete sintético con siete CSV y
120 JSONL en `datalake/landing/`. Se usa completo, sin editar sus archivos.
El [inventario y perfil inicial](../docs/primera-entrega.md#datos-y-exploración-inicial)
describe las ocho fuentes.

Desde la raíz del repositorio, con Python 3.9 o posterior:

```sh
python3 scripts/profile_data.py /ruta/cloud_provider_challenge_dataset_v1 --output evidence/profile.json
```

La ruta es la carpeta que contiene `datalake/`, no el ZIP ni `landing/`.
No hacen falta paquetes adicionales ni servicios externos. El script exige
los siete CSV y al menos un JSONL; registra la cantidad real de archivos sin
forzar los conteos esperados de la muestra.

La lectura registra columnas y vacíos CSV, claves repetidas y referencias a
organizaciones. Para los eventos registra tipos JSON, campos ausentes, fechas,
identidad, referencias a recursos y defectos de costo y consumo. Los duplicados
se comparan con el primer contenido observado, sin normalización de negocio.
La clave organización–fecha de NPS es una candidata de exploración, no un ID
garantizado por la fuente. El perfil no implementa todas las reglas de Silver.

La [evidencia](../evidence/profile.json) incluye la versión de Python, el hash
del script y un manifiesto con ruta relativa, bytes y SHA256 de cada entrada.
No contiene filas originales ni valores de campos personales. Para comparar
ejecuciones con otra versión de Python, revisar los conteos y hashes de entrada;
el campo `python` puede cambiar. Una salida dentro del paquete se rechaza para
proteger los originales.
