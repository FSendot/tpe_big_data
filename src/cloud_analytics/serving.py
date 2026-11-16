"""Prueba CQL de sólo lectura. Nunca imprime credenciales ni endpoints."""

import logging
import os
from pathlib import Path


def check_serving():
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    mode = os.environ.get("SERVING_MODE", "local")
    if mode not in {"local", "astra"}:
        raise ValueError("SERVING_MODE debe ser local o astra.")
    if mode == "astra":
        token = os.environ.get("ASTRA_TOKEN", "")
        bundle = os.environ.get("ASTRA_BUNDLE", "")
        keyspace = os.environ.get("ASTRA_KEYSPACE", "")
        if not token or not keyspace or not Path(bundle).is_file():
            return {"status": "pending", "mode": mode, "reason": "Faltan token, bundle o keyspace de Astra."}

    # Los errores del driver pueden contener endpoints. El informe sólo expone
    # la clase del error; no serializa excepciones ni variables de entorno.
    logger = logging.getLogger("cassandra")
    old_level = logger.level
    logger.setLevel(logging.CRITICAL)
    cluster = None
    try:
        if mode == "astra":
            cluster = Cluster(
                cloud={"secure_connect_bundle": bundle, "use_default_tempdir": True},
                auth_provider=PlainTextAuthProvider("token", token),
                connect_timeout=15,
                control_connection_timeout=15,
            )
        else:
            cluster = Cluster([os.environ.get("CASSANDRA_HOST", "cassandra")], connect_timeout=10)
        session = cluster.connect(keyspace if mode == "astra" else None)
        row = session.execute("SELECT release_version FROM system.local", timeout=15).one()
        if row is None:
            raise RuntimeError("No se recibió respuesta CQL.")
        return {"status": "passed", "mode": mode, "release_version": row.release_version,
                "query": "SELECT release_version FROM system.local", "read_only": True}
    except Exception as error:
        return {"status": "failed", "mode": mode, "error_type": type(error).__name__}
    finally:
        if cluster is not None:
            cluster.shutdown()
        logger.setLevel(old_level)
