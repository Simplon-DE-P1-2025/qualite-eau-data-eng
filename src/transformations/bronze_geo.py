from __future__ import annotations

import json
from typing import Any


def normalize_code_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, int):
        return [f"{value:02d}"]
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return []
        if "," in cleaned:
            return [item.strip() for item in cleaned.split(",") if item.strip()]
        return [cleaned]
    if isinstance(value, (list, tuple, set)):
        result = []
        for item in value:
            result.extend(normalize_code_list(item))
        return result
    return [str(value)]


def strip_geo_request_params(params: dict) -> dict:
    request_params = dict(params)
    request_params.pop("codeDepartement", None)
    request_params.pop("codeRegion", None)
    request_params.pop("centre", None)
    return request_params


def build_department_communes_url(api_url: str, department_code: str) -> str:
    base = api_url.rstrip("/")
    marker = "/communes"
    if base.endswith(marker):
        base = base[: -len(marker)]
    return f"{base}/departements/{department_code}/communes"


def build_region_departements_url(api_url: str, region_code: str) -> str:
    base = api_url.rstrip("/")
    marker = "/communes"
    if base.endswith(marker):
        base = base[: -len(marker)]
    return f"{base}/regions/{region_code}/departements"


def build_departements_url(api_url: str) -> str:
    base = api_url.rstrip("/")
    marker = "/communes"
    if base.endswith(marker):
        base = base[: -len(marker)]
    return f"{base}/departements"


def serialize_nested_fields(records: list[dict], nested: dict) -> list[dict]:
    for field, strategy in nested.items():
        if strategy == "json_string":
            for rec in records:
                if field in rec and not isinstance(rec[field], str):
                    rec[field] = json.dumps(rec[field], ensure_ascii=False)
    return records


def extract_geojson_records(raw: dict, nested: dict) -> list[dict]:
    records = []
    for feat in raw.get("features", []):
        row = dict(feat.get("properties", {}))
        geometry = feat.get("geometry")
        coordinates = geometry.get("coordinates") if geometry else None
        if coordinates:
            row["longitude"] = coordinates[0]
            row["latitude"] = coordinates[1]
        else:
            row["longitude"] = None
            row["latitude"] = None
        records.append(row)
    return serialize_nested_fields(records, nested)
