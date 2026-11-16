# Verificación del entorno — 15/11/2026

Se construyó el runtime Docker y se ejecutaron:

```sh
docker compose build runtime
docker compose run --rm runtime python -m unittest discover -s tests -v
docker compose run --rm runtime python scripts/check_environment.py --serving
```

Resultado actualizado: siete pruebas aprobadas y los tres chequeos de integración aprobados.
El [informe generado](serving.json) registra las versiones efectivas y los hashes.

- Dataset: 127 archivos, 120 JSONL, 13.235.421 bytes. Los 127 hashes coinciden
  con el manifiesto de `evidence/profile.json` de la primera entrega.
- Parquet: dos filas sintéticas, particionadas por fecha, recuperadas sin
  diferencias y con suma decimal `3.75`.
- Serving: consulta real `SELECT release_version FROM system.local` contra
  AstraDB, conectando al keyspace configurado. La ejecución previa también
  comprobó Cassandra local 5.0.6. El informe actual corresponde a Astra.
  No se crearon tablas ni se cargaron datos de negocio.
- Configuración: se rechazan rutas solapadas y particiones inválidas; se
  comprueba el caso de credenciales Astra ausentes y la omisión de secretos
  en errores de conexión.

La prueba no acredita permisos de escritura, despliegue HDFS ni pipeline.
Cassandra local se detuvo
al terminar y conserva su volumen para la próxima ejecución.

Santos Galarraga confirmó el 15/11/2026 que Docker está permitido. El informe
actual registra `docker_allowed_confirmed_by_user` y `serving.mode=astra`.
Para conectar se ajustó la ruta local del bundle y se configuró el driver con
`use_default_tempdir=True`: los archivos temporales se extraen fuera del montaje
de secretos de solo lectura. Una prueba de regresión verifica esa configuración.
