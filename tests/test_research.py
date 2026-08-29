import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import polars as pl
import pytest

import marketdata.query as query_mod
from marketdata.config import Config
from marketdata.errors import DataDirectoryBusyError
from marketdata.locking import DataDirectoryLock
from marketdata.query import connect_research, load_research_observations
from marketdata.research import (
    INPUT_MANIFEST_SCHEMA,
    ResearchMetric,
    ResearchOutput,
    reconcile_research_state,
    run_research_publication,
    verify_research_input_fingerprint,
)
from marketdata.research_layout import research_run_layout
from marketdata.store.bars import EOD_SCHEMA, BarStore
from marketdata.store.meta import MetaStore


def _eod(ticker: str, *, days: int = 2) -> pl.DataFrame:
    dates = [date(2024, 1, 2) + timedelta(days=offset) for offset in range(days)]
    return pl.DataFrame(
        {
            "ticker": [ticker] * days,
            "date": dates,
            "open": [100.0] * days,
            "high": [102.0] * days,
            "low": [99.0] * days,
            "close": [101.0] * days,
            "volume": [1000] * days,
            "adj_open": [100.0] * days,
            "adj_high": [102.0] * days,
            "adj_low": [99.0] * days,
            "adj_close": [101.0] * days,
            "adj_volume": [1000] * days,
            "div_cash": [0.0] * days,
            "split_factor": [1.0] * days,
        }
    ).cast(EOD_SCHEMA)


def _config(tmp_path) -> Config:
    config = Config(data_dir=tmp_path, tiingo_token=None)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        meta.upsert_instrument("apple-id")
        meta.activate_canonical_generation()
    BarStore(tmp_path).publish_eod({"apple-id": _eod("AAPL")})
    return config


def _output(value: float = 0.05) -> ResearchOutput:
    return ResearchOutput(
        observations=pl.DataFrame(
            {
                "instrument_id": ["apple-id"],
                "event_date": [date(2024, 1, 3)],
                "horizon": ["10:00"],
                "value": [value],
            }
        ),
        metrics=(
            ResearchMetric(
                "mean_return",
                value,
                dimensions={"gap_bucket": "large", "horizon": "10:00"},
                unit="return",
            ),
        ),
    )


def _publish(config: Config, *, version: int = 1, value: float = 0.05):
    return run_research_publication(
        config,
        study_name="fixture-study",
        study_schema_version=version,
        parameters={
            "enabled": True,
            "gap_threshold": -0.05,
            "horizons": ["10:00", "11:00"],
            "optional": None,
        },
        input_globs=["bars/eod/bucket=*/bars.parquet"],
        evaluate=lambda context: _output(value),
        source_revision="deadbeef",
    )


def test_research_publication_catalogs_typed_metadata_and_manifest(tmp_path):
    config = _config(tmp_path)
    seen = {}

    def evaluate(context):
        seen["paths"] = context.input_files
        seen["fingerprint"] = context.input_fingerprint
        return _output()

    published = run_research_publication(
        config,
        study_name="fixture-study",
        study_schema_version=1,
        parameters={
            "enabled": True,
            "gap_threshold": -0.05,
            "horizons": ["10:00", "11:00"],
            "optional": None,
        },
        input_globs=["bars/eod/bucket=*/bars.parquet"],
        evaluate=evaluate,
        source_revision="deadbeef",
    )

    assert len(seen["paths"]) == 1
    assert seen["fingerprint"] == published.input_fingerprint
    assert published.observation_count == 1
    assert published.observation_path == (
        tmp_path
        / "results"
        / "fixture-study"
        / published.run_id
        / "observations.parquet"
    )
    observations = pl.read_parquet(published.observation_path)
    assert observations["run_id"].to_list() == [published.run_id]
    assert observations["instrument_id"].to_list() == ["apple-id"]

    manifest = pl.read_parquet(published.manifest_path)
    assert manifest.schema == pl.Schema(INPUT_MANIFEST_SCHEMA)
    assert manifest["input_patterns_json"].unique().to_list() == [
        '["bars/eod/bucket=*/bars.parquet"]'
    ]
    assert manifest["input_metadata_json"].unique().to_list() == ["{}"]
    assert manifest["relative_path"].to_list()[0].startswith("bars/eod/bucket=")
    assert manifest["first_date"].to_list() == [date(2024, 1, 2)]
    assert manifest["last_date"].to_list() == [date(2024, 1, 3)]

    with MetaStore(config.meta_path) as meta:
        run = meta.research_run(published.run_id)
        assert run["status"] == "succeeded"
        assert (
            run["observation_path"]
            == published.observation_path.relative_to(tmp_path).as_posix()
        )
        assert meta.research_parameters(published.run_id) == {
            "enabled": True,
            "gap_threshold": -0.05,
            "horizons": ["10:00", "11:00"],
            "optional": None,
        }
        metric = meta.research_metrics(published.run_id)[0]
        assert metric["metric_name"] == "mean_return"
        assert metric["dimensions_json"] == ('{"gap_bucket":"large","horizon":"10:00"}')
        assert metric["unit"] == "return"


def test_input_fingerprint_ignores_mtime_and_detects_content_changes(tmp_path):
    config = _config(tmp_path)
    published = _publish(config)
    input_path = next((tmp_path / "bars" / "eod").glob("bucket=*/bars.parquet"))

    status = verify_research_input_fingerprint(config, published.run_id)
    assert status.matches
    os.utime(input_path, (input_path.stat().st_atime, input_path.stat().st_mtime + 10))
    assert verify_research_input_fingerprint(config, published.run_id).matches

    BarStore(tmp_path).publish_eod({"apple-id": _eod("AAPL", days=3)})
    changed = verify_research_input_fingerprint(config, published.run_id)
    assert not changed.matches
    assert not changed.metadata_changed
    assert changed.added_files == ()
    assert changed.changed_files == (input_path.relative_to(tmp_path).as_posix(),)


def test_input_fingerprint_detects_new_files_matching_recorded_globs(tmp_path):
    config = _config(tmp_path)
    published = _publish(config)
    existing = next((tmp_path / "bars" / "eod").glob("bucket=*/bars.parquet"))
    added = tmp_path / "bars" / "eod" / "bucket=added" / "bars.parquet"
    added.parent.mkdir()
    pl.read_parquet(existing).write_parquet(added)

    status = verify_research_input_fingerprint(config, published.run_id)

    assert not status.matches
    assert status.added_files == (added.relative_to(tmp_path).as_posix(),)


def test_every_input_glob_is_required_recursive_and_root_is_escaped(tmp_path):
    config = _config(tmp_path / "warehouse[fixture]")
    nested = (
        config.data_dir
        / "bars"
        / "intraday"
        / "1hour"
        / "year=2024"
        / "bucket=aa"
        / "bars.parquet"
    )
    nested.parent.mkdir(parents=True)
    pl.DataFrame({"ts": [date(2024, 1, 2)]}).write_parquet(nested)
    seen = {}

    published = run_research_publication(
        config,
        study_name="recursive-inputs",
        study_schema_version=1,
        parameters={},
        input_globs=[
            "bars/eod/bucket=*/bars.parquet",
            "bars/intraday/1hour/**/*.parquet",
        ],
        evaluate=lambda context: (
            seen.setdefault("files", context.input_files) and _output()
        ),
    )

    assert published.observation_count == 1
    assert len(seen["files"]) == 2
    assert verify_research_input_fingerprint(config, published.run_id).matches
    with pytest.raises(ValueError, match="missing/\\*\\*/\\*.parquet"):
        run_research_publication(
            config,
            study_name="required-inputs",
            study_schema_version=1,
            parameters={},
            input_globs=[
                "bars/eod/bucket=*/bars.parquet",
                "bars/intraday/missing/**/*.parquet",
            ],
            evaluate=lambda context: _output(),
        )


def test_successful_runs_are_immutable_in_sqlite(tmp_path):
    config = _config(tmp_path)
    published = _publish(config)

    with MetaStore(config.meta_path) as meta:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            meta._con.execute(
                "UPDATE research_runs SET source_revision = 'changed' WHERE run_id = ?",
                (published.run_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            meta._con.execute(
                """INSERT INTO research_metrics
                       (run_id, metric_name, dimensions_json, value, unit)
                   VALUES (?, 'late', '{}', 1.0, NULL)""",
                (published.run_id,),
            )
        assert meta.research_run(published.run_id)["status"] == "succeeded"


def test_publication_failure_records_bounded_error_and_removes_artifacts(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    run_ids = []
    original = MetaStore.succeed_research_run

    def fail_catalog_publish(self, **kwargs):
        run_ids.append(kwargs["run_id"])
        raise RuntimeError("catalog boundary failure " + "x" * 5000)

    monkeypatch.setattr(MetaStore, "succeed_research_run", fail_catalog_publish)
    with pytest.raises(RuntimeError, match="catalog boundary failure"):
        _publish(config)
    monkeypatch.setattr(MetaStore, "succeed_research_run", original)

    run_id = run_ids[0]
    assert not (tmp_path / "results" / "fixture-study" / run_id).exists()
    with MetaStore(config.meta_path) as meta:
        run = meta.research_run(run_id)
        assert run["status"] == "failed"
        assert len(run["error_summary"].encode("utf-8")) <= 4096
        assert run["observation_path"] is None
        assert meta.research_metrics(run_id) == []


def test_evaluator_failure_is_cataloged_and_no_input_fails_closed(tmp_path):
    config = _config(tmp_path)

    def fail(context):
        raise ValueError("study rejected its inputs")

    with pytest.raises(ValueError, match="study rejected"):
        run_research_publication(
            config,
            study_name="fixture-study",
            study_schema_version=1,
            parameters={},
            input_globs=["bars/eod/bucket=*/bars.parquet"],
            evaluate=fail,
        )
    with pytest.raises(ValueError, match="matched no files"):
        run_research_publication(
            config,
            study_name="empty-input",
            study_schema_version=1,
            parameters={},
            input_globs=["bars/intraday/1hour/**/*.parquet"],
            evaluate=lambda context: _output(),
        )

    with MetaStore(config.meta_path) as meta:
        runs = meta.research_runs()
        assert [row["status"] for row in runs] == ["failed", "failed"]
        assert all(row["observation_path"] is None for row in runs)


def test_catalog_filtered_loading_uses_only_compatible_succeeded_paths(tmp_path):
    config = _config(tmp_path)
    first = _publish(config, value=0.05)
    second = _publish(config, value=0.08)
    incompatible = _publish(config, version=2, value=0.10)
    orphan_path = (
        tmp_path / "results" / "fixture-study" / "orphan" / "observations.parquet"
    )
    orphan_path.parent.mkdir()
    pl.DataFrame(
        {"run_id": ["orphan"], "instrument_id": ["apple-id"], "bad": ["schema"]}
    ).write_parquet(orphan_path)

    loaded = load_research_observations(config, run_ids=[second.run_id, first.run_id])
    assert loaded.height == 2
    assert set(loaded["run_id"].to_list()) == {first.run_id, second.run_id}
    assert sorted(loaded["value"].to_list()) == [0.05, 0.08]

    con = connect_research(config, run_ids=[first.run_id])
    joined = con.execute(
        """SELECT observations.value, parameters.value_json
           FROM research_observations AS observations
           JOIN meta.research_parameters AS parameters USING (run_id)
           WHERE parameters.name = 'gap_threshold'"""
    ).fetchone()
    assert joined == (0.05, "-0.05")
    con.close()

    with pytest.raises(ValueError, match="one study and schema version"):
        load_research_observations(config, run_ids=[first.run_id, incompatible.run_id])


def test_empty_prestamped_observations_publish_and_load(tmp_path):
    config = _config(tmp_path)

    published = run_research_publication(
        config,
        study_name="empty-study",
        study_schema_version=1,
        parameters={},
        input_globs=["bars/eod/bucket=*/bars.parquet"],
        evaluate=lambda context: ResearchOutput(
            pl.DataFrame(
                schema={
                    "run_id": pl.Utf8,
                    "instrument_id": pl.Utf8,
                    "value": pl.Float64,
                }
            )
        ),
    )

    assert published.observation_count == 0
    loaded = load_research_observations(config, run_ids=[published.run_id])
    assert loaded.is_empty()
    assert loaded.schema["run_id"] == pl.Utf8


def test_same_version_schema_drift_is_rejected_before_common_query_setup(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    first = _publish(config)
    second = _publish(config, value=0.08)
    changed = pl.read_parquet(second.observation_path).with_columns(extra=pl.lit(1))
    changed.write_parquet(second.observation_path)
    monkeypatch.setattr(
        query_mod,
        "connect",
        lambda config: (_ for _ in ()).throw(
            AssertionError("common query surface must not be built")
        ),
    )

    with pytest.raises(RuntimeError, match="schemas differ"):
        connect_research(config, run_ids=[first.run_id, second.run_id])


def test_query_connections_close_on_load_and_setup_failure(tmp_path, monkeypatch):
    config = _config(tmp_path)
    published = _publish(config)

    class LoadedConnection:
        closed = False

        def execute(self, query):
            return self

        def pl(self):
            return pl.DataFrame({"run_id": [], "instrument_id": []})

        def close(self):
            self.closed = True

    loaded_connection = LoadedConnection()
    monkeypatch.setattr(
        query_mod, "connect_research", lambda config, run_ids: loaded_connection
    )
    query_mod.load_research_observations(config, run_ids=[published.run_id])
    assert loaded_connection.closed

    class FailingConnection:
        closed = False

        def from_parquet(self, *args, **kwargs):
            raise RuntimeError("view setup failed")

        def close(self):
            self.closed = True

    failing_connection = FailingConnection()
    monkeypatch.setattr(query_mod, "connect_research", connect_research)
    monkeypatch.setattr(query_mod, "connect", lambda config: failing_connection)
    with pytest.raises(RuntimeError, match="view setup failed"):
        connect_research(config, run_ids=[published.run_id])
    assert failing_connection.closed


def test_research_reconciliation_requires_explicit_apply(tmp_path):
    config = _config(tmp_path)
    run_id = "abandoned-run"
    with MetaStore(config.meta_path) as meta:
        meta.create_research_run(
            run_id=run_id,
            study_name="fixture-study",
            study_schema_version=1,
            parameters={},
        )
    layout = research_run_layout(config.data_dir, "fixture-study", run_id)
    layout.directory.mkdir(parents=True)
    (layout.directory / "partial.tmp").write_text("partial")
    orphan = config.data_dir / "results" / "fixture-study" / "orphan"
    orphan.mkdir()
    (orphan / "observations.parquet.tmp").write_text("partial")

    dry_run = reconcile_research_state(config)

    assert dry_run.stale_running_run_ids == (run_id,)
    assert dry_run.orphan_directories == (
        orphan.relative_to(config.data_dir).as_posix(),
    )
    assert layout.directory.exists() and orphan.exists()
    with MetaStore(config.meta_path) as meta:
        assert meta.research_run(run_id)["status"] == "running"

    applied = reconcile_research_state(config, apply=True)

    assert applied.failed_run_ids == (run_id,)
    assert not layout.directory.exists()
    assert not orphan.exists()
    with MetaStore(config.meta_path) as meta:
        assert meta.research_run(run_id)["status"] == "failed"


def test_research_validation_rejects_unsafe_or_unowned_output(tmp_path):
    config = _config(tmp_path)
    with pytest.raises(ValueError, match="filesystem-safe slug"):
        run_research_publication(
            config,
            study_name="../escape",
            study_schema_version=1,
            parameters={},
            input_globs=["bars/eod/bucket=*/bars.parquet"],
            evaluate=lambda context: _output(),
        )
    with pytest.raises(ValueError, match="unknown instrument_ids"):
        run_research_publication(
            config,
            study_name="fixture-study",
            study_schema_version=1,
            parameters={},
            input_globs=["bars/eod/bucket=*/bars.parquet"],
            evaluate=lambda context: ResearchOutput(
                pl.DataFrame({"instrument_id": ["AAPL"], "value": [1.0]})
            ),
        )
    with pytest.raises(ValueError, match="finite"):
        run_research_publication(
            config,
            study_name="fixture-study",
            study_schema_version=1,
            parameters={"bad": float("nan")},
            input_globs=["bars/eod/bucket=*/bars.parquet"],
            evaluate=lambda context: _output(),
        )


def test_research_publication_contends_on_the_shared_data_lock(tmp_path):
    config = _config(tmp_path)

    with DataDirectoryLock(tmp_path, operation="test holder"):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_publish, config)
            with pytest.raises(DataDirectoryBusyError):
                future.result()

    with MetaStore(config.meta_path) as meta:
        assert meta.research_runs() == []
