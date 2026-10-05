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
