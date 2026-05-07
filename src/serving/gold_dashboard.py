from __future__ import annotations

from html import escape
from typing import Any

import plotly.graph_objects as go
import plotly.io as pio
import requests
from flask import Flask, request


app = Flask(__name__)

DEFAULT_API_URL = "http://127.0.0.1:8000"


def api_get(base_url: str, path: str, **params: Any) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_rows(base_url: str, path: str, **params: Any) -> list[dict[str, Any]]:
    payload = api_get(base_url, path, **params)
    return payload.get("rows", [])


def fetch_all_rows(base_url: str, path: str, batch_size: int = 1000, **params: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        payload = api_get(
            base_url,
            path,
            limit=batch_size,
            offset=offset,
            **params,
        )
        batch = payload.get("rows", [])
        rows.extend(batch)

        if not batch or len(rows) >= int(payload.get("row_count", len(rows))):
            break
        offset += batch_size

    return rows


def fig_to_html(fig: go.Figure, include_js: bool = False) -> str:
    return pio.to_html(
        fig,
        full_html=False,
        include_plotlyjs="inline" if include_js else False,
        config={"displayModeBar": False, "responsive": True},
    )


def empty_figure(message: str, height: int = 360) -> go.Figure:
    fig = go.Figure()
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(
        height=height,
        title=message,
        margin=dict(l=10, r=10, t=70, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def sort_rows(rows: list[dict[str, Any]], keys: list[tuple[str, bool]]) -> list[dict[str, Any]]:
    sorted_rows = rows[:]
    for key, reverse in reversed(keys):
        sorted_rows.sort(key=lambda row: (row.get(key) is None, row.get(key)), reverse=reverse)
    return sorted_rows


def weighted_average(pairs: list[tuple[float, float]]) -> float | None:
    valid = [(value, weight) for value, weight in pairs if value is not None and weight is not None and weight > 0]
    if not valid:
        return None
    numerator = sum(value * weight for value, weight in valid)
    denominator = sum(weight for _, weight in valid)
    if denominator == 0:
        return None
    return numerator / denominator


def format_int(value: float | int) -> str:
    return f"{int(value):,}".replace(",", " ")


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.strip()
    return int(cleaned) if cleaned.isdigit() else None


def sum_field(rows: list[dict[str, Any]], field: str) -> float:
    return sum(float(row.get(field) or 0) for row in rows)


def filter_rows_by_communes(rows: list[dict[str, Any]], commune_codes: list[str] | None) -> list[dict[str, Any]]:
    if not commune_codes:
        return rows[:]
    selected = set(commune_codes)
    return [row for row in rows if row.get("code_commune") in selected]


def pick_default_communes(rows: list[dict[str, Any]], limit: int = 5) -> list[str]:
    scores: dict[str, float] = {}
    labels: dict[str, str] = {}

    for row in rows:
        code_commune = row.get("code_commune")
        nom_commune = row.get("nom_commune_norm")
        if not code_commune or not nom_commune:
            continue
        scores[code_commune] = scores.get(code_commune, 0) + float(row.get("nb_mesures") or 0)
        labels[code_commune] = nom_commune

    return sorted(scores, key=lambda code: (-scores[code], labels.get(code, code)))[:limit]


def build_coord_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        code_commune = row.get("code_commune")
        if not code_commune:
            continue
        if row.get("longitude") is None or row.get("latitude") is None:
            continue
        if code_commune not in lookup:
            lookup[code_commune] = {
                "code_commune": code_commune,
                "nom_commune_norm": row.get("nom_commune_norm"),
                "code_region_geo": row.get("code_region_geo"),
                "code_departement": row.get("code_departement"),
                "longitude": row.get("longitude"),
                "latitude": row.get("latitude"),
                "population": row.get("population") or 0,
            }
    return lookup


def build_conformite_map_rows(conformite_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    coord_lookup = build_coord_lookup(conformite_rows)
    map_rows: list[dict[str, Any]] = []

    for row in conformite_rows:
        code_commune = row.get("code_commune")
        coords = coord_lookup.get(code_commune)
        if not code_commune or not coords:
            continue
        map_rows.append(
            {
                **coords,
                "metric_value": round(float(row.get("taux_conformite_pct") or 0), 3),
                "size_value": round(float(row.get("nb_prelevements_total") or 0), 2),
            }
        )

    return sort_rows(map_rows, [("metric_value", True), ("size_value", True)])


def build_france_map_figure(map_rows: list[dict[str, Any]], selected_year: int | None) -> go.Figure:
    if not map_rows:
        return empty_figure("Aucune donnee cartographique disponible pour ce filtre", height=620)

    fig = go.Figure(
        data=[
            go.Scattergeo(
                lon=[row["longitude"] for row in map_rows],
                lat=[row["latitude"] for row in map_rows],
                text=[row["nom_commune_norm"] for row in map_rows],
                customdata=[
                    [
                        row["code_commune"],
                        row["code_departement"],
                        row["code_region_geo"],
                        row["metric_value"],
                        row["size_value"],
                    ]
                    for row in map_rows
                ],
                mode="markers",
                marker=dict(
                    size=[max(12, min(38, int((row["size_value"] ** 0.5) * 1.6))) for row in map_rows],
                    color=[row["metric_value"] for row in map_rows],
                    colorscale=[[0, "#f4b942"], [0.5, "#13a3bf"], [1, "#0f2d3a"]],
                    line=dict(color="rgba(255,255,255,0.78)", width=1.2),
                    showscale=True,
                    colorbar=dict(
                        title="Taux de conformite",
                        x=0.98,
                        y=0.52,
                        bgcolor="rgba(255,255,255,0.7)",
                    ),
                ),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "Code commune: %{customdata[0]}<br>"
                    "Departement: %{customdata[1]}<br>"
                    "Region: %{customdata[2]}<br>"
                    "Taux de conformite: %{customdata[3]} %<br>"
                    "Prelevements: %{customdata[4]}<extra></extra>"
                ),
            )
        ]
    )
    fig.update_geos(
        scope="europe",
        center=dict(lat=46.4, lon=2.2),
        projection_type="mercator",
        lataxis_range=[41.0, 51.8],
        lonaxis_range=[-5.8, 9.8],
        showcountries=True,
        countrycolor="rgba(15,45,58,0.26)",
        showland=True,
        landcolor="rgba(244, 239, 228, 0.9)",
        showocean=True,
        oceancolor="rgba(19, 163, 191, 0.10)",
        showlakes=True,
        lakecolor="rgba(19, 163, 191, 0.08)",
        coastlinecolor="rgba(15,45,58,0.18)",
        subunitcolor="rgba(15,45,58,0.08)",
        bgcolor="rgba(0,0,0,0)",
    )
    year_suffix = f" - {selected_year}" if selected_year is not None else ""
    fig.update_layout(
        title=f"Carte de conformite par commune{year_suffix}",
        height=620,
        margin=dict(l=10, r=10, t=66, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def build_evolution_figure(
    rows: list[dict[str, Any]],
    selected_parameter: str,
    selected_year: int | None,
) -> go.Figure:
    if not rows:
        return empty_figure("Aucune donnee d'evolution disponible pour ce filtre", height=460)

    grouped: dict[str, list[dict[str, Any]]] = {}
    unit_label = next((row.get("libelle_unite_norm") for row in rows if row.get("libelle_unite_norm")), "")

    for row in rows:
        grouped.setdefault(row.get("nom_commune_norm") or "INCONNUE", []).append(row)

    fig = go.Figure()
    for commune, commune_rows in sorted(grouped.items()):
        ordered = sort_rows(commune_rows, [("annee_prelevement", False), ("mois_prelevement", False)])
        periods = [
            f"{int(row.get('annee_prelevement'))}-{int(row.get('mois_prelevement')):02d}"
            for row in ordered
            if row.get("annee_prelevement") is not None and row.get("mois_prelevement") is not None
        ]
        fig.add_trace(
            go.Scatter(
                x=periods,
                y=[row.get("valeur_moyenne") for row in ordered],
                mode="lines+markers",
                name=commune,
                customdata=[
                    [
                        row.get("nb_mesures"),
                        row.get("valeur_min"),
                        row.get("valeur_max"),
                        row.get("libelle_unite_norm"),
                    ]
                    for row in ordered
                ],
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "Periode: %{x}<br>"
                    "Valeur moyenne: %{y}<br>"
                    "Mesures: %{customdata[0]}<br>"
                    "Min: %{customdata[1]}<br>"
                    "Max: %{customdata[2]}<br>"
                    "Unite: %{customdata[3]}<extra></extra>"
                ),
            )
        )

    title_suffix = f" - {selected_year}" if selected_year is not None else " - toutes annees"
    unit_suffix = f" - {unit_label}" if unit_label else ""
    fig.update_layout(
        title=f"Evolution temporelle - {selected_parameter}{title_suffix}{unit_suffix}",
        height=460,
        margin=dict(l=10, r=10, t=66, b=10),
        legend_title="Communes",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(title="Periode")
    fig.update_yaxes(title="Valeur moyenne")
    return fig


def build_top20_parameter_figure(
    rows: list[dict[str, Any]],
    selected_parameter: str,
    selected_year: int | None,
) -> go.Figure:
    if not rows:
        return empty_figure("Aucune donnee disponible pour construire le Top 20", height=620)

    ordered = sort_rows(rows, [("valeur_moyenne_ponderee", True), ("nb_mesures", True)])[:20]
    ordered = list(reversed(ordered))
    unit_label = next((row.get("libelle_unite_norm") for row in rows if row.get("libelle_unite_norm")), "")

    fig = go.Figure(
        data=[
            go.Bar(
                x=[row.get("valeur_moyenne_ponderee") for row in ordered],
                y=[row.get("nom_commune_norm") for row in ordered],
                orientation="h",
                text=[row.get("nb_mesures") for row in ordered],
                marker=dict(
                    color=[row.get("valeur_moyenne_ponderee") or 0 for row in ordered],
                    colorscale="Tealgrn",
                ),
                customdata=[
                    [
                        row.get("code_commune"),
                        row.get("nb_mesures"),
                        row.get("valeur_min"),
                        row.get("valeur_max"),
                        row.get("libelle_unite_norm"),
                    ]
                    for row in ordered
                ],
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Code commune: %{customdata[0]}<br>"
                    "Valeur moyenne ponderee: %{x}<br>"
                    "Mesures: %{customdata[1]}<br>"
                    "Min: %{customdata[2]}<br>"
                    "Max: %{customdata[3]}<br>"
                    "Unite: %{customdata[4]}<extra></extra>"
                ),
            )
        ]
    )
    title_suffix = f" - {selected_year}" if selected_year is not None else " - toutes annees"
    unit_suffix = f" - {unit_label}" if unit_label else ""
    fig.update_layout(
        title=f"Top 20 des communes - {selected_parameter}{title_suffix}{unit_suffix}",
        height=620,
        margin=dict(l=10, r=10, t=66, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(title="Valeur moyenne ponderee")
    fig.update_yaxes(title="")
    return fig


def metric_card(label: str, value: Any) -> str:
    return f"""
    <div class="metric-card">
        <div class="metric-label">{escape(str(label))}</div>
        <div class="metric-value">{escape(str(value))}</div>
    </div>
    """


def build_summary_cards(
    conformite_rows: list[dict[str, Any]],
    parameter_rows: list[dict[str, Any]],
    selected_year: int | None,
) -> str:
    communes_count = len(conformite_rows)
    prelevements_total = sum_field(conformite_rows, "nb_prelevements_total")
    mesures_total = sum_field(parameter_rows, "nb_mesures")
    taux_conformite = weighted_average(
        [
            (
                float(row.get("taux_conformite_pct") or 0),
                float(row.get("nb_prelevements_total") or 0),
            )
            for row in conformite_rows
        ]
    )
    taux_label = f"{taux_conformite:.2f} %" if taux_conformite is not None else "N/A"

    return "".join(
        [
            metric_card("Annee analysee", selected_year if selected_year is not None else "Toutes"),
            metric_card("Communes chargees", format_int(communes_count)),
            metric_card("Prelevements charges", format_int(prelevements_total)),
            metric_card("Taux de conformite", taux_label),
            metric_card("Mesures chargees", format_int(mesures_total)),
        ]
    )


def render_hidden_inputs(state: dict[str, Any], exclude: set[str] | None = None) -> str:
    exclude = exclude or set()
    html_parts: list[str] = []

    for key, value in state.items():
        if key in exclude:
            continue
        if isinstance(value, list):
            for item in value:
                html_parts.append(
                    f'<input type="hidden" name="{escape(key)}" value="{escape(str(item))}">'
                )
        elif value not in (None, ""):
            html_parts.append(
                f'<input type="hidden" name="{escape(key)}" value="{escape(str(value))}">'
            )

    return "".join(html_parts)


def render_select_options(
    options: list[tuple[str, str]],
    selected_value: str | None,
    include_all: bool = False,
    all_label: str = "Toutes",
) -> str:
    html_parts: list[str] = []
    if include_all:
        selected_attr = " selected" if selected_value in (None, "") else ""
        html_parts.append(f'<option value=""{selected_attr}>{escape(all_label)}</option>')

    for value, label in options:
        selected_attr = " selected" if selected_value == value else ""
        html_parts.append(
            f'<option value="{escape(value)}"{selected_attr}>{escape(label)}</option>'
        )
    return "".join(html_parts)


def render_commune_options(
    communes_catalog: list[dict[str, Any]],
    selected_values: list[str],
) -> str:
    html_parts: list[str] = []
    selected = set(selected_values)

    for commune in communes_catalog:
        code_commune = commune.get("code_commune")
        nom_commune = commune.get("nom_commune_norm")
        if not code_commune or not nom_commune:
            continue
        selected_attr = " selected" if code_commune in selected else ""
        html_parts.append(
            f'<option value="{escape(code_commune)}"{selected_attr}>'
            f"{escape(nom_commune)} ({escape(code_commune)})</option>"
        )

    return "".join(html_parts) or '<option value="">Aucune commune</option>'


def build_dashboard(
    api_base_url: str,
    map_year: int | None,
    summary_year: int | None,
    summary_communes: list[str],
    evolution_year: int | None,
    evolution_parameter: str | None,
    evolution_communes: list[str],
    top_year: int | None,
    top_parameter: str | None,
) -> str:
    global_meta = api_get(api_base_url, "/gold/dashboard-meta")
    years = [int(year) for year in global_meta.get("years", []) if year is not None]
    default_year = max(years) if years else None

    map_year = default_year if map_year is None else map_year
    summary_year = default_year if summary_year is None else summary_year
    top_year = default_year if top_year is None else top_year

    summary_meta = api_get(api_base_url, "/gold/dashboard-meta", annee_prelevement=summary_year)
    evolution_meta = api_get(
        api_base_url,
        "/gold/dashboard-meta",
        annee_prelevement=evolution_year,
    ) if evolution_year is not None else global_meta
    top_meta = api_get(api_base_url, "/gold/dashboard-meta", annee_prelevement=top_year)

    global_parameter_list = global_meta.get("parameters", [])
    summary_parameter = (
        "NITRATES (EN NO3)"
        if "NITRATES (EN NO3)" in summary_meta.get("parameters", [])
        else (summary_meta.get("parameters", [None])[0])
    )
    evolution_parameters = evolution_meta.get("parameters") or global_parameter_list
    top_parameters = top_meta.get("parameters") or global_parameter_list

    if evolution_parameter not in evolution_parameters:
        evolution_parameter = (
            "NITRATES (EN NO3)"
            if "NITRATES (EN NO3)" in evolution_parameters
            else (evolution_parameters[0] if evolution_parameters else None)
        )
    if top_parameter not in top_parameters:
        top_parameter = (
            "NITRATES (EN NO3)"
            if "NITRATES (EN NO3)" in top_parameters
            else (top_parameters[0] if top_parameters else None)
        )

    map_rows_raw = fetch_all_rows(
        api_base_url,
        "/gold/conformite-commune",
        annee_prelevement=map_year,
    )
    summary_rows_raw = fetch_all_rows(
        api_base_url,
        "/gold/conformite-commune",
        annee_prelevement=summary_year,
    )
    summary_parameter_rows = (
        fetch_all_rows(
            api_base_url,
            "/gold/evolution-parametres",
            annee_prelevement=summary_year,
            libelle_parametre_norm=summary_parameter,
        )
        if summary_parameter
        else []
    )
    evolution_rows_raw = (
        fetch_all_rows(
            api_base_url,
            "/gold/evolution-parametres",
            annee_prelevement=evolution_year,
            libelle_parametre_norm=evolution_parameter,
        )
        if evolution_parameter
        else []
    )
    top_rows = (
        fetch_rows(
            api_base_url,
            "/gold/top-communes-parametre",
            annee_prelevement=top_year,
            libelle_parametre_norm=top_parameter,
            limit=20,
        )
        if top_parameter
        else []
    )

    summary_available_codes = {row.get("code_commune") for row in summary_meta.get("communes", [])}
    summary_selected = [code for code in summary_communes if code in summary_available_codes]
    summary_rows = filter_rows_by_communes(summary_rows_raw, summary_selected)
    summary_parameter_rows = filter_rows_by_communes(summary_parameter_rows, summary_selected)

    evolution_available_codes = {row.get("code_commune") for row in evolution_meta.get("communes", [])}
    evolution_manual = [code for code in evolution_communes if code in evolution_available_codes]
    evolution_auto = pick_default_communes(evolution_rows_raw, limit=5) if not evolution_manual else []
    evolution_selected = evolution_manual or evolution_auto
    evolution_rows = filter_rows_by_communes(evolution_rows_raw, evolution_selected)

    map_rows = build_conformite_map_rows(map_rows_raw)
    fig_map_html = fig_to_html(build_france_map_figure(map_rows, map_year), include_js=True)
    fig_evolution_html = fig_to_html(
        build_evolution_figure(evolution_rows, evolution_parameter or "Parametre", evolution_year)
    )
    fig_top_html = fig_to_html(
        build_top20_parameter_figure(top_rows, top_parameter or "Parametre", top_year)
    )

    years_options = [(str(year), str(year)) for year in years]
    map_year_options = render_select_options(years_options, str(map_year) if map_year is not None else None)
    summary_year_options = render_select_options(years_options, str(summary_year) if summary_year is not None else None)
    evolution_year_options = render_select_options(
        years_options,
        str(evolution_year) if evolution_year is not None else "",
        include_all=True,
        all_label="Toutes les annees",
    )
    top_year_options = render_select_options(years_options, str(top_year) if top_year is not None else None)

    evolution_parameter_options = render_select_options(
        [(value, value) for value in evolution_parameters],
        evolution_parameter,
    )
    top_parameter_options = render_select_options(
        [(value, value) for value in top_parameters],
        top_parameter,
    )
    summary_commune_options = render_commune_options(summary_meta.get("communes", []), summary_selected)
    evolution_commune_options = render_commune_options(
        evolution_meta.get("communes", []),
        evolution_selected,
    )

    summary_scope_note = (
        f"{len(summary_selected)} commune(s) selectionnee(s)."
        if summary_selected
        else "Aucune commune filtree : les chiffres couvrent toutes les communes chargees."
    )
    evolution_scope_note = (
        f"{len(evolution_manual)} commune(s) selectionnee(s)."
        if evolution_manual
        else "Aucune commune forcee : affichage automatique des 5 communes les plus mesurees."
    )

    state = {
        "api_base_url": api_base_url,
        "map_year": map_year or "",
        "summary_year": summary_year or "",
        "summary_commune": summary_selected,
        "evolution_year": evolution_year or "",
        "evolution_parameter": evolution_parameter or "",
        "evolution_commune": evolution_manual,
        "top_year": top_year or "",
        "top_parameter": top_parameter or "",
    }

    summary_cards_html = build_summary_cards(summary_rows, summary_parameter_rows, summary_year)

    return f"""
    <!doctype html>
    <html lang="fr">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Dashboard Gold - Qualite de l'eau</title>
        <style>
            :root {{
                --ink: #0f2d3a;
                --muted: #607981;
                --sea: #13a3bf;
                --sand: #f0b14a;
                --paper: rgba(255,255,255,0.84);
                --line: rgba(15,45,58,0.08);
                --shadow: 0 18px 38px rgba(15,45,58,0.08);
                --radius-xl: 28px;
                --radius-lg: 22px;
                --radius-md: 16px;
            }}
            * {{
                box-sizing: border-box;
            }}
            body {{
                margin: 0;
                font-family: "Segoe UI", "Trebuchet MS", sans-serif;
                background:
                    radial-gradient(circle at top right, rgba(19, 163, 191, 0.16), transparent 28%),
                    radial-gradient(circle at top left, rgba(240, 177, 74, 0.14), transparent 26%),
                    linear-gradient(180deg, #f4efe4 0%, #fbfaf6 40%, #f3f6f1 100%);
                color: var(--ink);
            }}
            .page {{
                max-width: 1460px;
                margin: 0 auto;
                padding: 28px 24px 42px;
            }}
            .hero {{
                background: linear-gradient(135deg, rgba(15,45,58,0.98), rgba(27,112,138,0.92));
                border-radius: var(--radius-xl);
                padding: 28px 30px;
                color: #f7f8f3;
                box-shadow: 0 24px 48px rgba(15,45,58,0.16);
            }}
            .hero-top {{
                display: flex;
                justify-content: space-between;
                gap: 24px;
                align-items: flex-start;
                flex-wrap: wrap;
            }}
            .hero h1 {{
                margin: 0;
                font-size: 2.45rem;
                letter-spacing: -0.03em;
            }}
            .hero p {{
                margin: 10px 0 0;
                line-height: 1.65;
                max-width: 900px;
                color: rgba(247,248,243,0.92);
            }}
            .hero-api {{
                min-width: 320px;
                max-width: 420px;
                width: 100%;
            }}
            .hero-api form {{
                display: grid;
                grid-template-columns: 1fr auto;
                gap: 10px;
                align-items: end;
            }}
            .hero-api label,
            .panel-toolbar label {{
                display: block;
                font-size: 0.78rem;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 6px;
            }}
            .hero-api label {{
                color: #d3e9ef;
            }}
            .hero-api input,
            .hero-api button,
            .panel-toolbar select,
            .panel-toolbar button {{
                border-radius: 14px;
                border: 1px solid rgba(255,255,255,0.16);
                padding: 12px 14px;
                font: inherit;
            }}
            .hero-api input {{
                width: 100%;
                background: rgba(255,255,255,0.12);
                color: white;
            }}
            .hero-api button,
            .panel-toolbar button {{
                border: 0;
                background: var(--sand);
                color: #173946;
                font-weight: 700;
                cursor: pointer;
            }}
            .stack {{
                display: grid;
                gap: 22px;
                margin-top: 22px;
            }}
            .panel {{
                background: var(--paper);
                border: 1px solid var(--line);
                border-radius: var(--radius-lg);
                padding: 22px;
                box-shadow: var(--shadow);
            }}
            .panel-header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                gap: 18px;
                margin-bottom: 18px;
                flex-wrap: wrap;
            }}
            .panel-copy {{
                max-width: 620px;
            }}
            .panel h2 {{
                margin: 0;
                font-size: 1.18rem;
                letter-spacing: -0.02em;
            }}
            .panel-meta {{
                margin: 8px 0 0;
                color: var(--muted);
                font-size: 0.95rem;
                line-height: 1.55;
            }}
            .panel-toolbar {{
                display: flex;
                gap: 12px;
                align-items: end;
                flex-wrap: wrap;
                justify-content: flex-end;
            }}
            .toolbar-field {{
                min-width: 170px;
            }}
            .toolbar-field.wide {{
                min-width: 280px;
            }}
            .toolbar-field.medium {{
                min-width: 220px;
            }}
            .panel-toolbar select {{
                width: 100%;
                background: rgba(15,45,58,0.05);
                color: var(--ink);
                border: 1px solid rgba(15,45,58,0.12);
            }}
            .panel-toolbar select[multiple] {{
                min-height: 132px;
            }}
            .panel-toolbar option {{
                color: var(--ink);
            }}
            .scope-note {{
                margin: 0 0 14px;
                color: var(--muted);
                font-size: 0.92rem;
            }}
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(5, minmax(0, 1fr));
                gap: 16px;
            }}
            .metric-card {{
                border-radius: 18px;
                padding: 16px 18px;
                background: rgba(255,255,255,0.92);
                border: 1px solid rgba(15,45,58,0.08);
            }}
            .metric-label {{
                font-size: 0.8rem;
                text-transform: uppercase;
                color: #55717b;
                letter-spacing: 0.08em;
            }}
            .metric-value {{
                margin-top: 8px;
                font-size: 1.9rem;
                font-weight: 700;
            }}
            .grid-2 {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 22px;
            }}
            .viz-shell {{
                border-radius: 18px;
                background: rgba(255,255,255,0.55);
                padding: 8px;
            }}
            @media (max-width: 1260px) {{
                .metrics-grid {{
                    grid-template-columns: repeat(3, minmax(0, 1fr));
                }}
            }}
            @media (max-width: 1080px) {{
                .grid-2 {{
                    grid-template-columns: 1fr;
                }}
                .metrics-grid {{
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }}
                .panel-toolbar {{
                    justify-content: stretch;
                }}
            }}
            @media (max-width: 760px) {{
                .page {{
                    padding: 18px 14px 32px;
                }}
                .hero {{
                    padding: 22px 18px;
                }}
                .hero h1 {{
                    font-size: 2rem;
                }}
                .hero-api form {{
                    grid-template-columns: 1fr;
                }}
                .panel {{
                    padding: 18px;
                }}
                .metrics-grid {{
                    grid-template-columns: 1fr;
                }}
                .toolbar-field,
                .toolbar-field.medium,
                .toolbar-field.wide {{
                    min-width: 100%;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="page">
            <section class="hero">
                <div class="hero-top">
                    <div>
                        <h1>Dashboard Gold - Qualite de l'eau</h1>
                        <p>
                            Chaque cartouche a maintenant ses propres filtres. On garde une lecture
                            simple : carte de conformite, chiffres cles, evolution temporelle du
                            parametre choisi et Top 20 des communes.
                        </p>
                    </div>
                    <div class="hero-api">
                        <form method="get">
                            <div>
                                <label for="api_base_url">URL API Gold</label>
                                <input id="api_base_url" name="api_base_url" value="{escape(api_base_url)}">
                                {render_hidden_inputs(state, exclude={"api_base_url"})}
                            </div>
                            <button type="submit">Connecter</button>
                        </form>
                    </div>
                </div>
            </section>

            <div class="stack">
                <section class="panel">
                    <div class="panel-header">
                        <div class="panel-copy">
                            <h2>Carte de conformite</h2>
                            <p class="panel-meta">
                                Une cartouche dediee a la carte, avec son filtre d'annee. Elle reste
                                volontairement lisible et se concentre sur la conformite territoriale.
                            </p>
                        </div>
                        <form method="get" class="panel-toolbar">
                            {render_hidden_inputs(state, exclude={"map_year"})}
                            <div class="toolbar-field">
                                <label for="map_year">Annee de la carte</label>
                                <select id="map_year" name="map_year">{map_year_options}</select>
                            </div>
                            <button type="submit">Mettre a jour</button>
                        </form>
                    </div>
                    <div class="viz-shell">{fig_map_html}</div>
                </section>

                <section class="panel">
                    <div class="panel-header">
                        <div class="panel-copy">
                            <h2>Chiffres cles charges</h2>
                            <p class="panel-meta">
                                Cette cartouche garde les volumes charges par annee et par communes.
                                Tu peux cibler un sous-ensemble de communes sans toucher aux autres vues.
                            </p>
                        </div>
                        <form method="get" class="panel-toolbar">
                            {render_hidden_inputs(state, exclude={"summary_year", "summary_commune"})}
                            <div class="toolbar-field">
                                <label for="summary_year">Annee des chiffres</label>
                                <select id="summary_year" name="summary_year">{summary_year_options}</select>
                            </div>
                            <div class="toolbar-field wide">
                                <label for="summary_commune">Communes des chiffres</label>
                                <select id="summary_commune" name="summary_commune" multiple>{summary_commune_options}</select>
                            </div>
                            <button type="submit">Appliquer</button>
                        </form>
                    </div>
                    <p class="scope-note">{escape(summary_scope_note)}</p>
                    <div class="metrics-grid">{summary_cards_html}</div>
                </section>

                <div class="grid-2">
                    <section class="panel">
                        <div class="panel-header">
                            <div class="panel-copy">
                                <h2>Evolution temporelle d'un parametre</h2>
                                <p class="panel-meta">
                                    Cette cartouche a ses propres listes deroulantes : annee, parametre
                                    et communes. Si tu ne choisis pas de commune, on affiche les plus mesurees.
                                </p>
                            </div>
                            <form method="get" class="panel-toolbar">
                                {render_hidden_inputs(state, exclude={"evolution_year", "evolution_parameter", "evolution_commune"})}
                                <div class="toolbar-field">
                                    <label for="evolution_year">Annee</label>
                                    <select id="evolution_year" name="evolution_year">{evolution_year_options}</select>
                                </div>
                                <div class="toolbar-field medium">
                                    <label for="evolution_parameter">Parametre</label>
                                    <select id="evolution_parameter" name="evolution_parameter">{evolution_parameter_options}</select>
                                </div>
                                <div class="toolbar-field wide">
                                    <label for="evolution_commune">Communes</label>
                                    <select id="evolution_commune" name="evolution_commune" multiple>{evolution_commune_options}</select>
                                </div>
                                <button type="submit">Tracer</button>
                            </form>
                        </div>
                        <p class="scope-note">{escape(evolution_scope_note)}</p>
                        <div class="viz-shell">{fig_evolution_html}</div>
                    </section>

                    <section class="panel">
                        <div class="panel-header">
                            <div class="panel-copy">
                                <h2>Top 20 des communes</h2>
                                <p class="panel-meta">
                                    Cartouche independante pour le classement. Ici, on choisit uniquement
                                    l'annee et le parametre qui servent a construire l'histogramme.
                                </p>
                            </div>
                            <form method="get" class="panel-toolbar">
                                {render_hidden_inputs(state, exclude={"top_year", "top_parameter"})}
                                <div class="toolbar-field">
                                    <label for="top_year">Annee du Top 20</label>
                                    <select id="top_year" name="top_year">{top_year_options}</select>
                                </div>
                                <div class="toolbar-field medium">
                                    <label for="top_parameter">Parametre du Top 20</label>
                                    <select id="top_parameter" name="top_parameter">{top_parameter_options}</select>
                                </div>
                                <button type="submit">Classer</button>
                            </form>
                        </div>
                        <div class="viz-shell">{fig_top_html}</div>
                    </section>
                </div>
            </div>
        </div>
    </body>
    </html>
    """


@app.route("/")
def home() -> tuple[str, int]:
    api_base_url = request.args.get("api_base_url", DEFAULT_API_URL)

    try:
        html = build_dashboard(
            api_base_url=api_base_url,
            map_year=parse_int(request.args.get("map_year")),
            summary_year=parse_int(request.args.get("summary_year")),
            summary_communes=request.args.getlist("summary_commune"),
            evolution_year=parse_int(request.args.get("evolution_year")),
            evolution_parameter=request.args.get("evolution_parameter") or None,
            evolution_communes=request.args.getlist("evolution_commune"),
            top_year=parse_int(request.args.get("top_year")),
            top_parameter=request.args.get("top_parameter") or None,
        )
        return html, 200
    except Exception as exc:
        return (
            f"""
            <html>
            <body style="font-family:Segoe UI,sans-serif;padding:24px;background:#faf6ef;">
                <h1 style="color:#7a2b18;">Dashboard indisponible</h1>
                <p>Impossible de joindre ou d'exploiter l'API Gold locale.</p>
                <pre style="background:#fff;border:1px solid #ddd;padding:16px;border-radius:12px;">{escape(str(exc))}</pre>
                <p>Verifie que l'API tourne sur {escape(api_base_url)}.</p>
            </body>
            </html>
            """,
            500,
        )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8501, debug=True)
