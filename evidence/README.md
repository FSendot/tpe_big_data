# Evidencia de lectura

[profile.json](profile.json) fue generado el 05/10/2026 con Python 3.9.6 mediante
el [script de exploración](../scripts/profile_data.py), sobre el paquete del
campus. El [comando de reproducción](../data/README.md) sólo necesita Python.

La ejecución leyó 127 archivos, 13.235.421 bytes: siete CSV con 4.112 filas y
120 JSONL con 43.200 eventos. Confirmó 43.200 IDs de evento únicos, sin referencias
desconocidas a organizaciones o recursos. Las ocho fuentes y sus hallazgos se
describen en el [documento de diseño](../docs/primera-entrega.md#datos-y-exploración-inicial).

El JSON conserva los conteos y SHA256 por archivo para comprobar que otra
ejecución usa las mismas entradas. El script vuelve a verificar los hashes al
terminar la lectura. No limpia los datos ni corrige sus defectos; tampoco mide
latencia, rendimiento distribuido o resultados de consultas del pipeline.

## Reproducción posterior al feedback — 15/11/2026

Se extrajo `cloud_provider_challenge_dataset_v1.zip` en
`tmp/feedback-dataset/` (directorio ignorado por Git) y se ejecutó el script
existente con Python 3.11.15, sin modificarlo ni modificar las entradas:

```sh
python scripts/profile_data.py tmp/feedback-dataset --output tmp/profile-feedback.json
```

El resultado se conserva en [profile-feedback.json](profile-feedback.json).
Se compararon ambos JSON: todos los campos coinciden excepto `python` y
`script_sha256`. Coinciden los 127 hashes de archivos de entrada, los tamaños,
conteos y estadísticas de las ocho fuentes, incluidos los 43.200 eventos.

El hash original del script es
`a119d31197d77f7b3971f583b267f5e7f0640d8cd7739a2e110331b8efa2397e`.
El de esta ejecución es
`3a3c419d9243d8c51dca3b3cf37b74362336801894a0847eb0732593b9c1c451`.
Se comprobó que convertir los 210 finales de línea CRLF de la copia de Windows
a LF reproduce exactamente el hash original. La diferencia no es un cambio
de lógica. Se preserva la evidencia original y se registra la nueva por separado.

Para repetir la comparación desde la raíz del repositorio:

```sh
python -c "import json; from pathlib import Path; a=json.loads(Path('evidence/profile.json').read_text(encoding='utf-8')); b=json.loads(Path('evidence/profile-feedback.json').read_text(encoding='utf-8')); [x.pop(k) for x in (a,b) for k in ('python','script_sha256')]; assert a == b; print('Entradas y estadisticas coinciden')"
```

Esta verificación cierra la reproducción del perfilado de la primera entrega;
no valida Spark, HDFS, Cassandra ni un pipeline distribuido.
