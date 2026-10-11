"""PorterFlow ops dashboard (Streamlit).

    streamlit run dashboard/app.py

Reads only the *serving* DuckDB file produced by the publish task (never the
warehouse dbt is writing to). The connection is reopened whenever the file is
replaced, so a new publish shows up on the next interaction.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA_DIR = Path(os.environ.get("PORTERFLOW_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
SERVING = DATA_DIR / "serving" / "porterflow_serving.duckdb"

# Categorical order is fixed per city (colour follows the entity, never its rank).
CITY_COLORS = {"BLR": "#2a78d6", "MUM": "#eb6834", "DEL": "#1baf7a", "HYD": "#eda100", "PUN": "#e87ba4",
               "CHE": "#6250d6"}
STATUS = {"PASS": ("#008300", "✔ PASS"), "WARN": ("#c98500", "▲ WARN"), "FAIL": ("#e34948", "✖ FAIL")}
BLUES = ["#eef4fc", "#c9dcf5", "#94bbec", "#5b95de", "#2a78d6", "#1a56a3", "#103a70"]

st.set_page_config(page_title="PorterFlow · Ops dashboard", page_icon="🚚", layout="wide")


@st.cache_resource
def _connect(path: str, mtime: float) -> duckdb.DuckDBPyConnection:  # mtime in the key -> reconnect after publish
    return duckdb.connect(path, read_only=True)


def query(sql: str, params: list | None = None) -> pd.DataFrame:
    con = _connect(str(SERVING), SERVING.stat().st_mtime)
    return con.execute(sql, params or []).df()


CITY_ORDER = {"city_code": list(CITY_COLORS)}


def layout(fig: go.Figure, height: int = 340) -> go.Figure:
    fig.update_layout(height=height, margin=dict(l=8, r=8, t=36, b=8), legend_title_text="",
                      hovermode="x unified", plot_bgcolor="rgba(0,0,0,0)")
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.18)", zeroline=False)
    fig.update_traces(selector=dict(type="scatter"), line=dict(width=2))
    return fig


if not SERVING.exists():
    st.title("PorterFlow")
    st.info("No published data yet. The Airflow DAG `porterflow_daily` publishes after its first successful run "
            "(about 1-2 minutes per simulated day). This page refreshes on the next interaction.")
    st.stop()

publish = query("select run_date, published_at_utc from ops.publish_log")
bounds = query("select min(booked_date_ist) lo, max(booked_date_ist) hi from marts.agg_city_daily")
lo, hi = bounds.loc[0, "lo"], bounds.loc[0, "hi"]

st.title("🚚 PorterFlow · Intra-city logistics ops")
st.caption(f"Data through **{pd.Timestamp(hi):%d %b %Y}** · last published for run "
           f"{pd.Timestamp(publish.loc[0, 'run_date']):%d %b %Y} at "
           f"{publish.loc[0, 'published_at_utc']:%Y-%m-%d %H:%M} UTC · simulated data")

c1, c2, c3 = st.columns([2, 3, 2])
date_range = c1.date_input("Booking date (IST)", value=(lo, hi), min_value=lo, max_value=hi)
cities = c2.multiselect("Cities", list(CITY_COLORS), default=list(CITY_COLORS))
vtypes_all = query("select vehicle_type from marts.dim_vehicle group by 1 order by 1")["vehicle_type"].tolist()
vtypes = c3.multiselect("Vehicle types", vtypes_all, default=vtypes_all)
if not isinstance(date_range, tuple) or len(date_range) != 2 or not cities or not vtypes:
    st.warning("Pick a start and end date, at least one city and one vehicle type.")
    st.stop()
d0, d1 = date_range
F = [d0, d1, cities, vtypes]
TRIP_FILTER = "booked_date_ist between ? and ? and list_contains(?, city_code) and list_contains(?, vehicle_type)"

tab_overview, tab_cancel, tab_util, tab_dead, tab_health = st.tabs(
    ["Overview", "Cancellations", "Driver utilisation", "Dead km", "Data health"])

# ----------------------------------------------------------------------------------- overview
with tab_overview:
    k = query(f"""
        select count(*) booked,
               count(*) filter (where trip_status = 'completed') completed,
               count(*) filter (where trip_status = 'cancelled') cancelled,
               sum(fare_amount) filter (where trip_status = 'completed') gmv,
               median(time_to_assign_sec) tta,
               avg(dead_km) filter (where trip_status = 'completed') dead_km
        from marts.fct_trips where {TRIP_FILTER} and booked_date_ist >= (select min(booked_date_ist)
              from marts.agg_city_daily)""", F).iloc[0]
    finished = (k.completed + k.cancelled) or 1
    m = st.columns(5)
    m[0].metric("Trips booked", f"{int(k.booked):,}")
    m[1].metric("Completion rate", f"{k.completed / finished:.1%}", help="completed / (completed + cancelled)")
    m[2].metric("GMV", f"₹{(k.gmv or 0) / 1e7:.2f} Cr")
    m[3].metric("Median time to assign", f"{(k.tta or 0):.0f} s")
    m[4].metric("Avg dead km / trip", f"{(k.dead_km or 0):.2f} km")

    daily = query(f"""
        select booked_date_ist, city_code,
               count(*) filter (where trip_status = 'completed') * 1.0
                 / nullif(count(*) filter (where trip_status in ('completed', 'cancelled')), 0) completion_rate,
               count(*) trips
        from marts.fct_trips where {TRIP_FILTER} group by 1, 2 order by 1""", F)
    l, r = st.columns(2)
    fig = px.line(daily, x="booked_date_ist", y="completion_rate", color="city_code", markers=True,
                  color_discrete_map=CITY_COLORS, category_orders=CITY_ORDER, title="Completion rate by city",
                  labels={"booked_date_ist": "", "completion_rate": "", "city_code": "City"})
    fig.update_yaxes(tickformat=".0%")
    l.plotly_chart(layout(fig), width="stretch")
    fig = px.line(daily, x="booked_date_ist", y="trips", color="city_code", markers=True,
                  color_discrete_map=CITY_COLORS, category_orders=CITY_ORDER, title="Trips booked by city",
                  labels={"booked_date_ist": "", "trips": "", "city_code": "City"})
    r.plotly_chart(layout(fig), width="stretch")
    st.caption("Recent days settle over the next 3 days as late events arrive (in-progress trips are excluded "
               "from completion rate). Dips on 10 Sep (Mumbai) and 23 Sep (Bengaluru) are simulated heavy-rain days.")

# ------------------------------------------------------------------------------- cancellations
with tab_cancel:
    canc = query(f"""
        select cancellation_reason, cancelled_by, cancellation_stage, count(*) n
        from marts.fct_trips where {TRIP_FILTER} and trip_status = 'cancelled' group by all order by n desc""", F)
    owners = query("select * from reference.cancellation_reasons")
    canc = canc.merge(owners[["cancellation_reason", "owner_team", "is_controllable"]], how="left")
    total = canc["n"].sum() or 1
    l, r = st.columns([3, 2])
    fig = px.bar(canc.sort_values("n"), x="n", y="cancellation_reason", orientation="h", color="cancelled_by",
                 color_discrete_map={"customer": "#2a78d6", "system": "#eb6834", "driver": "#1baf7a"},
                 title="Cancellations by reason", labels={"n": "Cancelled trips", "cancellation_reason": "",
                                                          "cancelled_by": "Cancelled by"})
    fig.update_layout(hovermode="closest")
    fig.update_yaxes(categoryorder="total ascending")
    l.plotly_chart(layout(fig, 420), width="stretch")
    by_owner = canc.groupby("owner_team", as_index=False)["n"].sum().sort_values("n", ascending=False)
    by_owner["share"] = 100 * by_owner["n"] / total
    r.markdown("**Who owns the fix**")
    r.dataframe(by_owner.rename(columns={"owner_team": "Owner team", "n": "Cancellations", "share": "Share"}),
                hide_index=True, width="stretch",
                column_config={"Share": st.column_config.ProgressColumn(format="%.0f%%", min_value=0, max_value=100)})
    ctrl = canc.loc[canc["is_controllable"] == True, "n"].sum() / total  # noqa: E712
    r.metric("Controllable cancellations", f"{ctrl:.0%}", help="Reasons the business can act on (supply, "
             "dispatch, pricing, driver quality) vs. customer intent.")
    hourly = query(f"""
        select booked_hour_ist as "hour",
               count(*) filter (where cancellation_stage = 'before_assignment') * 1.0 / count(*) before_assign,
               count(*) filter (where cancellation_stage = 'after_assignment') * 1.0 / count(*) after_assign
        from marts.fct_trips where {TRIP_FILTER} group by 1 order by 1""", F)
    fig = go.Figure([go.Scatter(x=hourly.hour, y=hourly.before_assign, name="Before assignment (supply)",
                                line=dict(color="#eb6834")),
                     go.Scatter(x=hourly.hour, y=hourly.after_assign, name="After assignment",
                                line=dict(color="#2a78d6"))])
    fig.update_layout(title="Cancellation rate by booking hour (IST)")
    fig.update_yaxes(tickformat=".0%")
    fig.update_xaxes(dtick=2, title="Hour of day")
    st.plotly_chart(layout(fig), width="stretch")

# --------------------------------------------------------------------------------- utilisation
with tab_util:
    heat = query("""
        select city_code, hour_of_day, sum(busy_seconds) / nullif(sum(online_seconds), 0) utilisation,
               count(*) filter (where online_seconds > 0) / count(distinct activity_date_ist) drivers_online
        from marts.fct_driver_hourly_utilisation
        where activity_date_ist between ? and ? and list_contains(?, city_code) and list_contains(?, vehicle_type)
        group by 1, 2""", F)
    if heat.empty:
        st.info("No utilisation data for this selection.")
    else:
        piv = heat.pivot(index="city_code", columns="hour_of_day", values="utilisation").reindex(
            [c for c in CITY_COLORS if c in cities])
        fig = px.imshow(piv, color_continuous_scale=BLUES, zmin=0, zmax=0.8, aspect="auto",
                        labels=dict(x="Hour of day (IST)", y="", color="Utilisation"),
                        title="Driver utilisation: busy time ÷ logged-in time (PySpark job)")
        fig.update_traces(hovertemplate="%{y} %{x}:00 · utilisation %{z:.0%}<extra></extra>")
        fig.update_xaxes(dtick=2)
        st.plotly_chart(layout(fig, 300), width="stretch")
    sd = query(f"""
        select booked_hour_ist as "hour", count(*) / count(distinct booked_date_ist) bookings_per_day,
               count(*) filter (where cancellation_reason in ('no_driver_found', 'long_wait_for_assignment'))
                 * 1.0 / count(*) supply_cancel_rate
        from marts.fct_trips where {TRIP_FILTER} group by 1 order by 1""", F)
    on = heat.groupby("hour_of_day", as_index=False)["drivers_online"].sum() if not heat.empty else None
    l, r = st.columns(2)
    if on is not None:
        fig = go.Figure([go.Bar(x=sd.hour, y=sd.bookings_per_day, name="Bookings / day", marker_color="#94bbec"),
                         go.Scatter(x=on.hour_of_day, y=on.drivers_online, name="Drivers online (avg)",
                                    line=dict(color="#103a70"))])
        fig.update_layout(title="Demand vs supply by hour", barmode="overlay")
        fig.update_xaxes(dtick=2, title="Hour of day (IST)")
        l.plotly_chart(layout(fig), width="stretch")
    fig = px.line(sd, x="hour", y="supply_cancel_rate", title="Bookings lost to supply (no driver / long wait)",
                  labels={"hour": "Hour of day (IST)", "supply_cancel_rate": ""})
    fig.update_traces(line_color="#eb6834")
    fig.update_yaxes(tickformat=".0%")
    r.plotly_chart(layout(fig), width="stretch")
    dist = query("""
        select utilisation from marts.agg_driver_daily
        where activity_date_ist between ? and ? and list_contains(?, city_code) and list_contains(?, vehicle_type)
          and online_minutes >= 120""", F)
    fig = px.histogram(dist, x="utilisation", nbins=40, title="Driver-day utilisation (drivers online ≥ 2 h)",
                       labels={"utilisation": "Utilisation"})
    fig.update_traces(marker_color="#2a78d6", marker_line_width=0)
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(layout(fig, 300), width="stretch")

# ------------------------------------------------------------------------------------- dead km
with tab_dead:
    dk = query(f"""
        select vehicle_type, city_code, avg(dead_km) avg_dead_km,
               sum(dead_km) / nullif(sum(dead_km) + sum(distance_km), 0) dead_share
        from marts.fct_trips where {TRIP_FILTER} and trip_status = 'completed' group by 1, 2""", F)
    l, r = st.columns(2)
    fig = px.bar(dk, x="vehicle_type", y="avg_dead_km", color="city_code", barmode="group",
                 color_discrete_map=CITY_COLORS, category_orders=CITY_ORDER, title="Average dead km per completed trip",
                 labels={"vehicle_type": "", "avg_dead_km": "km", "city_code": "City"})
    fig.update_layout(hovermode="closest")
    l.plotly_chart(layout(fig), width="stretch")
    fig = px.bar(dk, x="vehicle_type", y="dead_share", color="city_code", barmode="group",
                 color_discrete_map=CITY_COLORS, category_orders=CITY_ORDER, title="Empty running: dead km ÷ total km",
                 labels={"vehicle_type": "", "dead_share": "", "city_code": "City"})
    fig.update_yaxes(tickformat=".0%")
    fig.update_layout(hovermode="closest")
    r.plotly_chart(layout(fig), width="stretch")
    trend = query(f"""
        select booked_date_ist, avg(dead_km) avg_dead_km from marts.fct_trips
        where {TRIP_FILTER} and trip_status = 'completed' group by 1 order by 1""", F)
    fig = px.line(trend, x="booked_date_ist", y="avg_dead_km", markers=True, title="Dead km per trip, network",
                  labels={"booked_date_ist": "", "avg_dead_km": "km"})
    fig.update_traces(line_color="#2a78d6")
    st.plotly_chart(layout(fig, 280), width="stretch")
    st.caption("Dead km = driver's distance from the assignment location to the pickup × 1.35 road factor. "
               "Trips with a missing GPS fix at assignment are excluded rather than counted as zero.")

# --------------------------------------------------------------------------------- data health
with tab_health:
    dq = query("select * from ops.dq_check_results order by run_date desc, layer, check_name")
    latest = dq[dq.run_date == dq.run_date.max()]
    s = st.columns(3)
    for col, status in zip(s, ["PASS", "WARN", "FAIL"]):
        col.metric(STATUS[status][1], int((latest.status == status).sum()),
                   help=f"Checks with status {status} on the latest run ({dq.run_date.max()})")
    grid = dq.pivot_table(index="check_name", columns="run_date", values="status", aggfunc="first")
    score = grid.replace({"PASS": 0, "WARN": 1, "FAIL": 2}).astype(float)
    fig = go.Figure(go.Heatmap(z=score.values, x=[str(c) for c in score.columns], y=score.index,
                               zmin=0, zmax=2, showscale=False, text=grid.values, xgap=2, ygap=2,
                               colorscale=[[0, "#cfe8cf"], [0.5, "#f6dfa6"], [1, "#f2b8b8"]],
                               hovertemplate="%{y}<br>%{x}: %{text}<extra></extra>"))
    fig.update_layout(title="Quality checks per run (green PASS · amber WARN · red FAIL)")
    st.plotly_chart(layout(fig, 40 + 22 * len(score)), width="stretch")
    show = latest[["layer", "check_name", "status", "observed", "threshold", "details"]].copy()
    show["status"] = show["status"].map(lambda x: STATUS[x][1])
    st.dataframe(show, hide_index=True, width="stretch")
    q = query("""
        select booked_date_ist, sum(late_event_count) * 1.0 / sum(event_count) late_share,
               sum(gps_missing_event_count) * 1.0 / sum(event_count) gps_missing_share,
               avg(is_speed_anomaly::int) speed_anomaly_share, avg(is_vehicle_mismatch::int) vehicle_mismatch_share
        from marts.fct_trips where booked_date_ist >= (select min(booked_date_ist) from marts.agg_city_daily)
        group by 1 order by 1""")
    long = q.melt("booked_date_ist", var_name="defect", value_name="share")
    fig = px.line(long, x="booked_date_ist", y="share", color="defect", markers=True,
                  color_discrete_sequence=["#2a78d6", "#eb6834", "#1baf7a", "#eda100"],
                  title="Injected defects caught downstream (share of events / trips)",
                  labels={"booked_date_ist": "", "share": "", "defect": ""})
    fig.update_yaxes(tickformat=".1%")
    st.plotly_chart(layout(fig), width="stretch")
