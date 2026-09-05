"""Shared as-of decision features for historical replay and live scanning.

Every view registered here is built from a DuckDB connection that exposes the
canonical ``eod`` (and optionally ``intraday_1hour``) views, whether those come
from the research runner's explicit input files or from a future scanner's
nightly snapshot. The same SQL therefore produces the same feature values in
both settings, which is the D-036 requirement that replay and scanning share
one feature path.

Causality contract (research-protocol.md):

- Every rolling window ends at the *prior* completed session
  (``ROWS BETWEEN n PRECEDING AND 1 PRECEDING``). No feature reads the
  decision session's close, high, low, or volume.
- The decision session's raw and adjusted ``open`` are exposed explicitly as
  the only same-session inputs; they are observed at the open and are not an
  assumed executable entry price.
- Cross-session ratios (gap, prior returns, volatility) use adjusted prices.
  Same-session intraday returns downstream must use the raw open on the same
  raw basis as intraday bars; the raw open is carried for that purpose.
- Intraday bar density is measured from the IEX hourly feed over prior
  sessions only, so a study can screen thin IEX coverage without peeking.
"""

from __future__ import annotations

from datetime import date

import duckdb
import polars as pl

from marketdata.calendar import expected_intraday_labels, session_schedule

DEFAULT_LOOKBACK_SESSIONS = 20
_HOURLY_BARS_PER_SESSION = 6


def register_eod_decision_features(
    con: duckdb.DuckDBPyConnection,
    *,
    view_name: str = "eod_decision_features",
    eod_view: str = "eod",
    lookback_sessions: int = DEFAULT_LOOKBACK_SESSIONS,
) -> str:
    """Create a per-(instrument, session) as-of feature view over EOD bars.

    Columns:

    - ``instrument_id``, ``date`` — the decision session.
    - ``open_raw``, ``adj_open`` — the decision session's opening print.
    - ``prior_date``, ``prior_close_raw``, ``prior_adj_close`` — the last
      completed session before the decision session.
    - ``lookback_start_date`` — the first of the ``lookback_sessions`` prior
      completed sessions present in storage; ``prior_sessions_present`` counts
      them (a caller requires the full count for a complete window).
    - ``adv_dollars`` — mean composite ``close * volume`` over those sessions.
    - ``realized_vol`` — sample standard deviation of prior adjusted-close log
      returns over the window.
    - ``prior_5_return`` — adjusted close five sessions before the prior
      close to the prior close.
    - ``gap_return`` — ``adj_open / prior_adj_close - 1``.
    - ``gap_vol_normalized`` — ``gap_return / realized_vol`` (null when the
      volatility is zero or unavailable).
    - ``event_day_corporate_action`` — the decision session carries a cash
      dividend or a non-unit split factor, so raw and adjusted bases differ.
    """
    if lookback_sessions < 2:
        raise ValueError("lookback_sessions must be at least 2")
    n = int(lookback_sessions)
    con.execute(
        f"""CREATE OR REPLACE VIEW {view_name} AS
            WITH ordered AS (
                SELECT instrument_id, date, open, adj_open, close, adj_close,
                       volume, div_cash, split_factor,
                       lag(date) OVER w AS prior_date,
                       lag(close) OVER w AS prior_close_raw,
                       lag(adj_close) OVER w AS prior_adj_close,
                       lag(adj_close, 6) OVER w AS adj_close_6_back,
                       lag(date, {n}) OVER w AS lookback_start_date,
                       count(*) OVER (
                           PARTITION BY instrument_id ORDER BY date
                           ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING
                       ) AS prior_sessions_present,
                       avg(CASE WHEN volume >= 0 THEN close * volume END) OVER (
                           PARTITION BY instrument_id ORDER BY date
                           ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING
                       ) AS adv_dollars,
                       ln(adj_close / lag(adj_close) OVER w) AS log_return
                FROM {eod_view}
                WINDOW w AS (PARTITION BY instrument_id ORDER BY date)
            ),
            vol AS (
                SELECT *,
                       stddev_samp(log_return) OVER (
                           PARTITION BY instrument_id ORDER BY date
                           ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING
                       ) AS realized_vol
                FROM ordered
            )
            SELECT instrument_id, date,
                   open AS open_raw, adj_open,
                   prior_date, prior_close_raw, prior_adj_close,
                   lookback_start_date, prior_sessions_present,
                   adv_dollars, realized_vol,
                   adj_close_6_back,
                   CASE WHEN adj_close_6_back IS NOT NULL AND adj_close_6_back > 0
                        THEN prior_adj_close / adj_close_6_back - 1.0 END
                       AS prior_5_return,
                   CASE WHEN prior_adj_close IS NOT NULL AND prior_adj_close > 0
                        THEN adj_open / prior_adj_close - 1.0 END AS gap_return,
                   CASE WHEN prior_adj_close IS NOT NULL AND prior_adj_close > 0
                             AND realized_vol IS NOT NULL AND realized_vol > 0
                        THEN (adj_open / prior_adj_close - 1.0) / realized_vol END
                       AS gap_vol_normalized,
                   (coalesce(div_cash, 0) > 0 OR coalesce(split_factor, 1) <> 1)
                       AS event_day_corporate_action
            FROM vol"""
    )
    return view_name


def register_session_opens(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    view_name: str = "session_opens",
) -> str:
    """Register the XNYS session schedule so decision timestamps come from it."""
    schedule = session_schedule(start, end)
    con.register(f"_{view_name}_frame", schedule)
    con.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM _{view_name}_frame"
    )
    return view_name


def register_hourly_density_features(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    view_name: str = "hourly_density_features",
    hourly_view: str = "intraday_1hour",
    eod_view: str = "eod",
    lookback_sessions: int = DEFAULT_LOOKBACK_SESSIONS,
) -> str:
    """Create a per-(instrument, session) prior-window IEX hourly density view.

    ``hourly_density`` is the share of the expected regular-session direct
    hourly labels (six per full session from 10:00 New York time) over the
    prior ``lookback_sessions`` EOD sessions that are stored with non-zero IEX
    volume. Sessions with no hourly rows count as zero, so a thin or missing
    IEX history lowers the density instead of hiding. The window ends at the
    prior session; the decision session's bars are never read.
    """
    if lookback_sessions < 1:
        raise ValueError("lookback_sessions must be at least 1")
    n = int(lookback_sessions)
    schedule = session_schedule(start, end)
    first_open = schedule["session_open"].min()
    last_close = schedule["session_close"].max()
    if first_open is None or last_close is None:
        labels = pl.DataFrame(schema={"ts": pl.Datetime("us", "UTC")})
    else:
        labels = expected_intraday_labels(first_open, last_close, freq="1hour")
    labels = labels.with_columns(
        pl.col("ts")
        .dt.convert_time_zone("America/New_York")
        .dt.date()
        .alias("session_date")
    )
    con.register(f"_{view_name}_labels", labels)
    con.execute(
        f"""CREATE OR REPLACE VIEW {view_name} AS
            WITH expected AS (
                SELECT session_date, count(*) AS expected_bars
                FROM _{view_name}_labels GROUP BY session_date
            ),
            per_session AS (
                SELECT bars.instrument_id, labels.session_date,
                       count(*) FILTER (WHERE bars.volume > 0) AS traded_bars
                FROM {hourly_view} AS bars
                JOIN _{view_name}_labels AS labels ON labels.ts = bars.ts
                GROUP BY bars.instrument_id, labels.session_date
            ),
            joined AS (
                SELECT eod.instrument_id, eod.date,
                       coalesce(per_session.traded_bars, 0) AS traded_bars,
                       coalesce(expected.expected_bars, {_HOURLY_BARS_PER_SESSION})
                           AS expected_bars
                FROM {eod_view} AS eod
                LEFT JOIN per_session
                  ON per_session.instrument_id = eod.instrument_id
                 AND per_session.session_date = eod.date
                LEFT JOIN expected ON expected.session_date = eod.date
            )
            SELECT instrument_id, date,
                   sum(traded_bars) OVER w AS prior_traded_bars,
                   sum(expected_bars) OVER w AS prior_expected_bars,
                   CASE WHEN sum(expected_bars) OVER w > 0
                        THEN sum(traded_bars) OVER w * 1.0 / sum(expected_bars) OVER w
                        END AS hourly_density
            FROM joined
            WINDOW w AS (
                PARTITION BY instrument_id ORDER BY date
                ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING
            )"""
    )
    return view_name
