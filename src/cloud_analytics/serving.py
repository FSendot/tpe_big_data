"""Conexión CQL del runtime. Nunca imprime credenciales ni endpoints."""

from contextlib import contextmanager
import logging
import os
from pathlib import Path


class MissingAstraConfiguration(ValueError):
    pass


@contextmanager
def serving_session(mode):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    if mode not in {"local", "astra"}:
        raise ValueError("SERVING_MODE debe ser local o astra.")
    if mode == "astra":
        token = os.environ.get("ASTRA_TOKEN", "")
        bundle = os.environ.get("ASTRA_BUNDLE", "")
        keyspace = os.environ.get("ASTRA_KEYSPACE", "")
        if not token or not keyspace or not Path(bundle).is_file():
            raise MissingAstraConfiguration("Faltan token, bundle o keyspace de Astra.")

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
        yield cluster.connect(keyspace if mode == "astra" else None)
    finally:
        if cluster is not None:
            cluster.shutdown()
        logger.setLevel(old_level)


def check_serving():
    mode = os.environ.get("SERVING_MODE", "local")
    if mode not in {"local", "astra"}:
        raise ValueError("SERVING_MODE debe ser local o astra.")
    try:
        with serving_session(mode) as session:
            row = session.execute("SELECT release_version FROM system.local", timeout=15).one()
            if row is None:
                raise RuntimeError("No se recibió respuesta CQL.")
            return {"status": "passed", "mode": mode, "release_version": row.release_version,
                    "query": "SELECT release_version FROM system.local", "read_only": True}
    except MissingAstraConfiguration as error:
        return {"status": "pending", "mode": mode, "reason": str(error)}
    except Exception as error:
        return {"status": "failed", "mode": mode, "error_type": type(error).__name__}
