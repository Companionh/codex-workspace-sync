from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "export_github_tree.py"


def load_module():
    spec = spec_from_file_location("export_github_tree", MODULE_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_export_tree_copies_curated_files_and_skips_runtime_state(tmp_path):
    module = load_module()
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    source.mkdir()

    (source / "README.md").write_text("hello\n", encoding="utf-8")
    (source / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
    (source / ".gitignore").write_text("tmp/\n", encoding="utf-8")

    (source / "src").mkdir()
    (source / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")

    (source / "scripts" / "windows").mkdir(parents=True)
    (source / "scripts" / "windows" / "push-repo.bat").write_text("@echo off\n", encoding="utf-8")

    (source / "docs").mkdir()
    (source / "docs" / "guide.md").write_text("# guide\n", encoding="utf-8")

    (source / "state").mkdir()
    (source / "state" / "server.db").write_text("secret\n", encoding="utf-8")
    (source / "backups").mkdir()
    (source / "backups" / "old.txt").write_text("old\n", encoding="utf-8")

    module.ROOT = source
    copied = module.export_tree(dest)

    assert copied >= 5
    assert (dest / "README.md").read_text(encoding="utf-8") == "hello\n"
    assert (dest / "src" / "app.py").exists()
    assert (dest / "docs" / "guide.md").exists()
    assert not (dest / "state").exists()
    assert not (dest / "backups").exists()


def test_export_tree_removes_stale_missing_files(tmp_path):
    module = load_module()
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    source.mkdir()
    dest.mkdir()

    (dest / "README.md").write_text("stale\n", encoding="utf-8")

    module.ROOT = source
    module.export_tree(dest)

    assert not (dest / "README.md").exists()
