"""Configuración no secreta del runtime; credenciales sólo por entorno."""

from dataclasses import dataclass
from pathlib import Path
import os
import tomllib


@dataclass(frozen=True)
class Settings:
    dataset_root: Path
    lake_root: Path
    evidence_root: Path
    master: str
    driver_memory: str
    shuffle_partitions: int
    timezone: str


def load_settings(path=None):
    with Path(path or os.environ.get("APP_CONFIG", "config/runtime.toml")).open("rb") as source:
        raw = tomllib.load(source)
    paths = [Path(raw[key]).resolve() for key in ("dataset_root", "lake_root", "evidence_root")]
    for index, left in enumerate(paths):
        for right in paths[index + 1:]:
            if left == right or left in right.parents or right in left.parents:
                raise ValueError("Dataset, Lake y evidencia deben tener rutas separadas, sin anidamiento.")
    spark = raw["spark"]
    if spark["master"] != "local[2]" or spark["timezone"] != "UTC":
        raise ValueError("Este entorno está validado sólo para local[2] y UTC.")
    if type(spark["shuffle_partitions"]) is not int or spark["shuffle_partitions"] < 1:
        raise ValueError("shuffle_partitions debe ser un entero positivo.")
    return Settings(*paths, **spark)
