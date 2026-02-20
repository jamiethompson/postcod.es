"""Locked SQL contracts for Phase 2 Voronoi clipping and enumeration."""

from __future__ import annotations

from pipeline.config import VORONOI_HULL_BUFFER_M

# Phase 2 lock: no inline literals for the buffer. Runtime must bind
# `hull_buffer_m` explicitly so the governing constant is always traceable.
VORONOI_CLIP_EXPR_SQL = (
    "ST_Buffer(ST_ConvexHull(ST_Collect(seed_geom_bng)), %(hull_buffer_m)s)"
)

VORONOI_CLIP_GEOMETRY_SQL_TEMPLATE = """
WITH seed_points AS (
    {seed_points_sql}
),
clip_geom AS (
    SELECT
        ST_SetSRID({clip_expr}, 27700) AS gb_clip_geom
    FROM seed_points
)
SELECT gb_clip_geom
FROM clip_geom;
""".strip()

VORONOI_CELL_CTE_SQL_TEMPLATE = """
WITH seed_points AS (
    {seed_points_sql}
),
clip_geom AS (
    SELECT
        ST_SetSRID({clip_expr}, 27700) AS gb_clip_geom
    FROM seed_points
),
cell_geoms AS (
    SELECT
        (ST_Dump(
            ST_VoronoiPolygons(
                ST_Collect(seed_geom_bng),
                0.0,
                (SELECT gb_clip_geom FROM clip_geom)
            )
        )).geom AS cell_geom
    FROM seed_points
)
""".strip()

VORONOI_CELL_SQL_TEMPLATE = """
{cell_cte_sql}
SELECT cell_geom
FROM cell_geoms;
""".strip()


def voronoi_sql_params(hull_buffer_m: float | None = None) -> dict[str, float]:
    """Return the bound parameter dict for Voronoi clipping SQL."""

    value = VORONOI_HULL_BUFFER_M if hull_buffer_m is None else float(hull_buffer_m)
    if value <= 0:
        raise ValueError("hull_buffer_m must be greater than zero")
    return {"hull_buffer_m": value}


def render_voronoi_clip_geometry_sql(seed_points_sql: str) -> str:
    """Render SQL that computes the clipped Voronoi boundary geometry."""

    if not seed_points_sql.strip():
        raise ValueError("seed_points_sql must be non-empty")
    return VORONOI_CLIP_GEOMETRY_SQL_TEMPLATE.format(
        seed_points_sql=seed_points_sql.strip(),
        clip_expr=VORONOI_CLIP_EXPR_SQL,
    )


def render_voronoi_cell_sql(seed_points_sql: str) -> str:
    """Render SQL that computes clipped Voronoi cell polygons."""

    if not seed_points_sql.strip():
        raise ValueError("seed_points_sql must be non-empty")
    cell_cte_sql = VORONOI_CELL_CTE_SQL_TEMPLATE.format(
        seed_points_sql=seed_points_sql.strip(),
        clip_expr=VORONOI_CLIP_EXPR_SQL,
    )
    return VORONOI_CELL_SQL_TEMPLATE.format(cell_cte_sql=cell_cte_sql)


def render_voronoi_cell_cte_sql(seed_points_sql: str) -> str:
    """Render SQL CTE block for building clipped Voronoi cells."""

    if not seed_points_sql.strip():
        raise ValueError("seed_points_sql must be non-empty")
    return VORONOI_CELL_CTE_SQL_TEMPLATE.format(
        seed_points_sql=seed_points_sql.strip(),
        clip_expr=VORONOI_CLIP_EXPR_SQL,
    )
