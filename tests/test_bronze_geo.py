from __future__ import annotations

from src.transformations import bronze_geo as bronze_geo_tf


def test_normalize_code_list_supports_string_int_and_csv():
    assert bronze_geo_tf.normalize_code_list("64") == ["64"]
    assert bronze_geo_tf.normalize_code_list(64) == ["64"]
    assert bronze_geo_tf.normalize_code_list("64,69") == ["64", "69"]
    assert bronze_geo_tf.normalize_code_list(["64", 69]) == ["64", "69"]


def test_strip_geo_request_params_removes_department_and_region_filters():
    params = {
        "codeDepartement": "64",
        "codeRegion": "75",
        "geometry": "centre",
        "format": "geojson",
        "fields": "nom,code",
    }
    stripped = bronze_geo_tf.strip_geo_request_params(params)

    assert "codeDepartement" not in stripped
    assert "codeRegion" not in stripped
    assert stripped["geometry"] == "centre"
    assert stripped["format"] == "geojson"


def test_build_department_communes_url_targets_department_endpoint():
    url = bronze_geo_tf.build_department_communes_url(
        "https://geo.api.gouv.fr/communes",
        "64",
    )
    assert url == "https://geo.api.gouv.fr/departements/64/communes"


def test_extract_geojson_records_maps_coordinates_and_nested_lists():
    raw = {
        "features": [
            {
                "properties": {
                    "nom": "Pau",
                    "code": "64445",
                    "codesPostaux": ["64000"],
                },
                "geometry": {
                    "coordinates": [-0.3708, 43.2951],
                },
            }
        ]
    }

    records = bronze_geo_tf.extract_geojson_records(
        raw,
        {"codesPostaux": "json_string"},
    )

    assert records == [
        {
            "nom": "Pau",
            "code": "64445",
            "codesPostaux": '["64000"]',
            "longitude": -0.3708,
            "latitude": 43.2951,
        }
    ]
