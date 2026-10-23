"""Verify guarded deletion of generated pipeline data contents."""

from pathlib import Path

from scripts.maintenance import reset_data


def test_clear_data_contents_preserves_root_and_removes_children(tmp_path: Path) -> None:
    data = tmp_path / "data"
    nested = data / "nested"
    nested.mkdir(parents=True)
    (nested / "file.nc").write_text("x")
    (data / "file.parquet").write_text("y")
    target = tmp_path / "outside"
    target.write_text("outside")
    (data / "link").symlink_to(target)

    removed = reset_data.clear_data_contents(data)

    assert removed == 3
    assert data.is_dir()
    assert list(data.iterdir()) == []
    assert target.read_text() == "outside"


def test_data_reset_prod_is_hard_denied(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.maintenance.reset_data.resolve_app_environment",
        lambda: "prod",
    )
    monkeypatch.setattr(
        "scripts.maintenance.reset_data.clear_data_contents",
        lambda: (_ for _ in ()).throw(AssertionError("must not delete")),
    )

    assert reset_data.main(["--execute"]) == 3


def test_data_reset_inspect_never_deletes(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.maintenance.reset_data.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr(
        "scripts.maintenance.reset_data.clear_data_contents",
        lambda: (_ for _ in ()).throw(AssertionError("inspect must not delete")),
    )

    assert reset_data.main([]) == 0


def test_data_reset_no_confirmation_never_deletes(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.maintenance.reset_data.resolve_app_environment",
        lambda: "dev",
    )
    monkeypatch.setattr("scripts.maintenance.reset_data._confirmation", lambda: False)
    monkeypatch.setattr(
        "scripts.maintenance.reset_data.clear_data_contents",
        lambda: (_ for _ in ()).throw(AssertionError("no must not delete")),
    )

    assert reset_data.main(["--execute"]) == 0
