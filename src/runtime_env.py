from __future__ import annotations

import os
from dataclasses import dataclass


def quote_ident(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


@dataclass(frozen=True)
class NamespaceConfig:
    env: str
    use_unity_catalog: bool
    database: str | None = None
    catalog: str | None = None
    schema: str | None = None
    external_location: str | None = None
    layer: str | None = None

    @property
    def namespace_display(self) -> str:
        if self.use_unity_catalog:
            assert self.catalog is not None
            assert self.schema is not None
            return f"{self.catalog}.{self.schema}"
        assert self.database is not None
        return self.database

    def fq_table(self, table_name: str) -> str:
        if self.use_unity_catalog:
            assert self.catalog is not None
            assert self.schema is not None
            return (
                f"{quote_ident(self.catalog)}."
                f"{quote_ident(self.schema)}."
                f"{quote_ident(table_name)}"
            )
        assert self.database is not None
        return f"{quote_ident(self.database)}.{quote_ident(table_name)}"


def resolve_runtime_environment(cfg: dict) -> str:
    return os.getenv("WATER_QUALITY_ENV", cfg["environment"]).strip().lower()


def build_namespace_config(cfg: dict, layer: str | None = None) -> NamespaceConfig:
    env = resolve_runtime_environment(cfg)

    if env == "azure":
        uc_cfg = cfg.get("unity_catalog", {})
        catalog = uc_cfg.get("catalog")
        schemas = uc_cfg.get("schemas", {})
        external_locations = uc_cfg.get("external_locations", {})
        schema = schemas.get(layer) if layer else None
        if not catalog or not schema:
            raise KeyError(
                "Configuration unity_catalog incomplète pour l'environnement azure. "
                f"catalog={catalog!r}, schema[{layer!r}]={schema!r}"
            )
        return NamespaceConfig(
            env=env,
            use_unity_catalog=True,
            catalog=str(catalog),
            schema=str(schema),
            external_location=str(external_locations.get(layer)) if external_locations.get(layer) else None,
            layer=layer,
        )

    return NamespaceConfig(
        env=env,
        use_unity_catalog=False,
        database=str(cfg["database"]["name"]),
        layer=layer,
    )


def initialize_namespace(spark, namespace: NamespaceConfig) -> None:
    if namespace.use_unity_catalog:
        assert namespace.catalog is not None
        assert namespace.schema is not None
        spark.sql(f"USE CATALOG {quote_ident(namespace.catalog)}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(namespace.schema)}")
        spark.sql(f"USE {quote_ident(namespace.schema)}")
        return

    if namespace.env != "local":
        assert namespace.database is not None
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {quote_ident(namespace.database)}")
        spark.sql(f"USE {quote_ident(namespace.database)}")
