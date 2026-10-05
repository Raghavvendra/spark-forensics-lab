from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "experiments" / "02_join_mitigations.py"


def test_join_mitigation_script_is_python_310_compatible():
    compile(SCRIPT.read_text(encoding="utf-8"), str(SCRIPT), "exec")
