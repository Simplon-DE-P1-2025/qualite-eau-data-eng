import threading
from pathlib import Path
from typing import Any

import duckdb
import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder


def resolve_project_root() -> Path:
    candidates = []

    try:
        candidates.append(Path(__file__).resolve().parents[2])
    except NameError:
        pass

    cwd = Path.cwd().resolve()
    candidates.extend([cwd, cwd.parent])

    seen = set()
    for candidate in candidates:
        root = candidate.resolve()
        if str(root) in seen:
            continue
        seen.add(str(root))
        if (root / "config" / "config.yml").exists() or (root / "config.yml").exists():
            return root

    searched = "\n - ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "config.yml introuvable pour l'API Gold. Emplacements testes :\n - " + searched
    )


PROJECT_ROOT = resolve_project_root()
CONFIG_PATH = (
    PROJECT_ROOT / "config" / "config.yml"
    if (PROJECT_ROOT / "config" / "config.yml").exists()
    else PROJECT_ROOT / "config.yml"
)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)


def resolve_gold_root() -> Path:
    local_cfg = cfg["storage"]["local"]["gold"]
    configured = (PROJECT_ROOT / local_cfg).resolve()
    fallback = (PROJECT_ROOT / "gold").resolve()

    if configured.exists():
        return configured
    if fallback.exists():
        return fallback

    raise FileNotFoundError(
        "Impossible de trouver la couche Gold locale. "
        f"Chemins testes : {configured} ; {fallback}"
    )


GOLD_ROOT = resolve_gold_root()
TABLES = {
    "gold_conformite_commune": GOLD_ROOT / "conformite_commune",
    "gold_evolution_parametres": GOLD_ROOT / "evolution_parametres",
    "gold_qualite_region": GOLD_ROOT / "qualite_region",
    "gold_top10_communes": GOLD_ROOT / "top10_communes",
    "gold_non_conformites": GOLD_ROOT / "non_conformites",
}

TABLE_ENDPOINTS = {
    "conformite_commune": "gold_conformite_commune",
    "evolution_parametres": "gold_evolution_parametres",
    "qualite_region": "gold_qualite_region",
    "top10_communes": "gold_top10_communes",
    "non_conformites": "gold_non_conformites",
}

TABLE_ORDERS = {
    "gold_conformite_commune": "annee_prelevement DESC, taux_conformite_pct DESC, nom_commune_norm ASC",
    "gold_evolution_parametres": "annee_prelevement DESC, mois_prelevement DESC, libelle_parametre_norm ASC",
    "gold_qualite_region": "annee_prelevement DESC, taux_conformite_pct DESC, code_region_geo ASC",
    "gold_top10_communes": "classement_type ASC, rang ASC",
    "gold_non_conformites": "annee_prelevement DESC, nb_prelevements_non_conformes DESC, nom_commune_norm ASC",
}


def build_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    for table_name, table_path in TABLES.items():
        parquet_glob = (table_path / "**" / "*.parquet").as_posix()
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT *
            FROM read_parquet('{parquet_glob}', hive_partitioning = true)
            """
        )
    return con


CON = build_connection()
CON_LOCK = threading.Lock()


app = FastAPI(
    title="Water Quality Gold API",
    version="1.0.0",
    description=(
        "API locale d'exposition des tables Gold produites par le pipeline "
        "qualite de l'eau. Les donnees sont lues directement depuis les "
        "datasets parquet locaux."
    ),
)


def fetch_rows(
    table_name: str,
    filters: list[tuple[str, str, Any]],
    limit: int,
    offset: int,
) -> dict[str, Any]:
    where_clauses = []
    params: list[Any] = []

    for column_name, operator, value in filters:
        if value is None:
            continue
        where_clauses.append(f"{column_name} {operator} ?")
        params.append(value)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    base_sql = f"SELECT * FROM {table_name}{where_sql}"
    count_sql = f"SELECT COUNT(*) AS row_count FROM ({base_sql}) AS q"
    data_sql = (
        f"{base_sql} ORDER BY {TABLE_ORDERS[table_name]} "
        "LIMIT ? OFFSET ?"
    )

    with CON_LOCK:
        total_rows = CON.execute(count_sql, params).fetchone()[0]
        cursor = CON.execute(data_sql, params + [limit, offset])
        columns = [item[0] for item in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    rows = jsonable_encoder(rows)
    return {
        "table": table_name,
        "gold_root": GOLD_ROOT.as_posix(),
        "limit": limit,
        "offset": offset,
        "row_count": total_rows,
        "rows": rows,
    }


def get_table_counts() -> dict[str, int]:
    counts = {}
    with CON_LOCK:
        for table_name in TABLES:
            counts[table_name] = CON.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
    return counts


def run_query_rows(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    with CON_LOCK:
        cursor = CON.execute(sql, params or [])
        columns = [item[0] for item in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return jsonable_encoder(rows)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "project_root": PROJECT_ROOT.as_posix(),
        "gold_root": GOLD_ROOT.as_posix(),
        "tables": get_table_counts(),
    }


@app.get("/tables")
def list_tables() -> dict[str, Any]:
    return {
        "available_tables": [
            {
                "endpoint_name": endpoint_name,
                "table_name": table_name,
                "path": TABLES[table_name].as_posix(),
                "row_count": count,
            }
            for endpoint_name, table_name in TABLE_ENDPOINTS.items()
            for count in [get_table_counts()[table_name]]
        ]
    }


@app.get("/gold/conformite-commune")
def gold_conformite_commune(
    annee_prelevement: int | None = None,
    code_departement: int | None = None,
    code_commune: str | None = None,
    code_region_geo: str | None = None,
    min_taux_conformite_pct: float | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    payload = fetch_rows(
        table_name="gold_conformite_commune",
        filters=[
            ("annee_prelevement", "=", annee_prelevement),
            ("code_departement", "=", code_departement),
            ("code_commune", "=", code_commune),
            ("code_region_geo", "=", code_region_geo),
            ("taux_conformite_pct", ">=", min_taux_conformite_pct),
        ],
        limit=limit,
        offset=offset,
    )
    payload["filters"] = {
        "annee_prelevement": annee_prelevement,
        "code_departement": code_departement,
        "code_commune": code_commune,
        "code_region_geo": code_region_geo,
        "min_taux_conformite_pct": min_taux_conformite_pct,
    }
    return payload


@app.get("/gold/evolution-parametres")
def gold_evolution_parametres(
    annee_prelevement: int | None = None,
    mois_prelevement: int | None = Query(default=None, ge=1, le=12),
    code_departement: int | None = None,
    code_commune: str | None = None,
    code_parametre: str | None = None,
    libelle_parametre_norm: str | None = None,
    categorie_parametre: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    payload = fetch_rows(
        table_name="gold_evolution_parametres",
        filters=[
            ("annee_prelevement", "=", annee_prelevement),
            ("mois_prelevement", "=", mois_prelevement),
            ("code_departement", "=", code_departement),
            ("code_commune", "=", code_commune),
            ("code_parametre", "=", code_parametre),
            ("libelle_parametre_norm", "=", libelle_parametre_norm),
            ("categorie_parametre", "=", categorie_parametre),
        ],
        limit=limit,
        offset=offset,
    )
    payload["filters"] = {
        "annee_prelevement": annee_prelevement,
        "mois_prelevement": mois_prelevement,
        "code_departement": code_departement,
        "code_commune": code_commune,
        "code_parametre": code_parametre,
        "libelle_parametre_norm": libelle_parametre_norm,
        "categorie_parametre": categorie_parametre,
    }
    return payload


@app.get("/gold/qualite-region")
def gold_qualite_region(
    annee_prelevement: int | None = None,
    code_region_geo: str | None = None,
    min_taux_conformite_pct: float | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    payload = fetch_rows(
        table_name="gold_qualite_region",
        filters=[
            ("annee_prelevement", "=", annee_prelevement),
            ("code_region_geo", "=", code_region_geo),
            ("taux_conformite_pct", ">=", min_taux_conformite_pct),
        ],
        limit=limit,
        offset=offset,
    )
    payload["filters"] = {
        "annee_prelevement": annee_prelevement,
        "code_region_geo": code_region_geo,
        "min_taux_conformite_pct": min_taux_conformite_pct,
    }
    return payload


@app.get("/gold/top10-communes")
def gold_top10_communes(
    classement_type: str | None = Query(default=None, pattern="^(PLUS_CONFORME|MOINS_CONFORME)$"),
    code_region_geo: str | None = None,
    limit: int = Query(default=20, ge=1, le=20),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    payload = fetch_rows(
        table_name="gold_top10_communes",
        filters=[
            ("classement_type", "=", classement_type),
            ("code_region_geo", "=", code_region_geo),
        ],
        limit=limit,
        offset=offset,
    )
    payload["filters"] = {
        "classement_type": classement_type,
        "code_region_geo": code_region_geo,
    }
    return payload


@app.get("/gold/non-conformites")
def gold_non_conformites(
    annee_prelevement: int | None = None,
    code_departement: int | None = None,
    code_commune: str | None = None,
    code_parametre: str | None = None,
    categorie_parametre: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    payload = fetch_rows(
        table_name="gold_non_conformites",
        filters=[
            ("annee_prelevement", "=", annee_prelevement),
            ("code_departement", "=", code_departement),
            ("code_commune", "=", code_commune),
            ("code_parametre", "=", code_parametre),
            ("categorie_parametre", "=", categorie_parametre),
        ],
        limit=limit,
        offset=offset,
    )
    payload["filters"] = {
        "annee_prelevement": annee_prelevement,
        "code_departement": code_departement,
        "code_commune": code_commune,
        "code_parametre": code_parametre,
        "categorie_parametre": categorie_parametre,
    }
    return payload


@app.get("/gold/dashboard-meta")
def gold_dashboard_meta(
    annee_prelevement: int | None = None,
    code_departement: int | None = None,
) -> dict[str, Any]:
    filters: list[str] = []
    params: list[Any] = []

    if annee_prelevement is not None:
        filters.append("annee_prelevement = ?")
        params.append(annee_prelevement)
    if code_departement is not None:
        filters.append("code_departement = ?")
        params.append(code_departement)

    where_sql = f" WHERE {' AND '.join(filters)}" if filters else ""

    years = run_query_rows(
        (
            "SELECT DISTINCT annee_prelevement "
            "FROM gold_conformite_commune "
            "WHERE annee_prelevement IS NOT NULL "
            "ORDER BY annee_prelevement DESC"
        )
    )
    parameters = run_query_rows(
        (
            "SELECT DISTINCT libelle_parametre_norm "
            "FROM gold_evolution_parametres"
            f"{where_sql} "
            "AND libelle_parametre_norm IS NOT NULL "
            "ORDER BY libelle_parametre_norm ASC"
            if where_sql
            else
            "SELECT DISTINCT libelle_parametre_norm "
            "FROM gold_evolution_parametres "
            "WHERE libelle_parametre_norm IS NOT NULL "
            "ORDER BY libelle_parametre_norm ASC"
        ),
        params[:],
    )
    communes = run_query_rows(
        (
            "SELECT DISTINCT code_commune, nom_commune_norm "
            "FROM gold_conformite_commune"
            f"{where_sql} "
            "AND code_commune IS NOT NULL AND nom_commune_norm IS NOT NULL "
            "ORDER BY nom_commune_norm ASC"
            if where_sql
            else
            "SELECT DISTINCT code_commune, nom_commune_norm "
            "FROM gold_conformite_commune "
            "WHERE code_commune IS NOT NULL AND nom_commune_norm IS NOT NULL "
            "ORDER BY nom_commune_norm ASC"
        ),
        params[:],
    )

    return {
        "filters": {
            "annee_prelevement": annee_prelevement,
            "code_departement": code_departement,
        },
        "years": [row["annee_prelevement"] for row in years],
        "parameters": [row["libelle_parametre_norm"] for row in parameters],
        "communes": communes,
    }


@app.get("/gold/top-communes-parametre")
def gold_top_communes_parametre(
    libelle_parametre_norm: str,
    annee_prelevement: int | None = None,
    code_departement: int | None = None,
    limit: int = Query(default=20, ge=1, le=20),
) -> dict[str, Any]:
    filters = ["libelle_parametre_norm = ?"]
    params: list[Any] = [libelle_parametre_norm]

    if annee_prelevement is not None:
        filters.append("annee_prelevement = ?")
        params.append(annee_prelevement)
    if code_departement is not None:
        filters.append("code_departement = ?")
        params.append(code_departement)

    sql = f"""
        SELECT
            code_commune,
            nom_commune_norm,
            code_departement,
            code_region_geo,
            libelle_unite_norm,
            SUM(COALESCE(nb_mesures, 0)) AS nb_mesures,
            ROUND(
                SUM(COALESCE(valeur_moyenne, 0) * COALESCE(nb_mesures, 0))
                / NULLIF(SUM(COALESCE(nb_mesures, 0)), 0),
                3
            ) AS valeur_moyenne_ponderee,
            MAX(valeur_max) AS valeur_max,
            MIN(valeur_min) AS valeur_min
        FROM gold_evolution_parametres
        WHERE {" AND ".join(filters)}
        GROUP BY
            code_commune,
            nom_commune_norm,
            code_departement,
            code_region_geo,
            libelle_unite_norm
        HAVING SUM(COALESCE(nb_mesures, 0)) > 0
        ORDER BY
            valeur_moyenne_ponderee DESC NULLS LAST,
            nb_mesures DESC,
            nom_commune_norm ASC
        LIMIT ?
    """
    rows = run_query_rows(sql, params + [limit])
    return {
        "table": "gold_top_communes_parametre",
        "gold_root": GOLD_ROOT.as_posix(),
        "row_count": len(rows),
        "rows": rows,
        "filters": {
            "libelle_parametre_norm": libelle_parametre_norm,
            "annee_prelevement": annee_prelevement,
            "code_departement": code_departement,
        },
    }


@app.get("/gold/{table_name}")
def unknown_table(table_name: str) -> dict[str, Any]:
    allowed = ", ".join(TABLE_ENDPOINTS)
    raise HTTPException(
        status_code=404,
        detail=f"Table Gold inconnue: {table_name}. Endpoints disponibles: {allowed}",
    )
