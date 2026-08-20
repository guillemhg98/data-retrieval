"""Validation script: check configuration and environment."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))


def validate_environment() -> bool:
    """Validate project configuration and environment."""
    print("\n" + "=" * 80)
    print("PROJECT VALIDATION")
    print("=" * 80 + "\n")

    checks: list[bool] = []

    print("1. Checking Python version...")
    if sys.version_info >= (3, 9):
        print(f"   OK Python {sys.version.split()[0]}")
        checks.append(True)
    else:
        print(f"   FAIL Python {sys.version.split()[0]} (requires 3.9+)")
        checks.append(False)

    print("\n2. Checking directory structure...")
    required_dirs = [
        "config",
        "pipelines",
        "pipelines/shared",
        "pipelines/demand",
        "pipelines/diagnosis",
        "data",
        "data/sample/input",
        "selections",
    ]

    dir_ok = True
    for dir_path in required_dirs:
        full_path = PROJECT_ROOT / dir_path
        if full_path.exists():
            print(f"   OK {dir_path}")
        else:
            print(f"   FAIL {dir_path} (missing)")
            dir_ok = False
    checks.append(dir_ok)

    print("\n3. Checking key files...")
    required_files = [
        "config/config.py",
        "pipelines/shared/__init__.py",
        "pipelines/shared/db.py",
        "pipelines/shared/utils.py",
        "requirements.txt",
        ".env.example",
        "run_pipeline.py",
        "data/sample/input/demand_visits.csv",
        "data/sample/input/diagnosis_visits.csv",
        "data/sample/input/up_rs.csv",
    ]

    files_ok = True
    for file_path in required_files:
        full_path = PROJECT_ROOT / file_path
        if full_path.exists():
            print(f"   OK {file_path}")
        else:
            print(f"   FAIL {file_path} (missing)")
            files_ok = False
    checks.append(files_ok)

    print("\n4. Checking environment configuration...")
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        print("   OK .env file found")
    else:
        print("   WARN .env file not found")
        print("        This is OK for --sample. Copy .env.example to .env for production.")
    checks.append(True)

    print("\n5. Checking Python dependencies...")
    deps_ok = True
    try:
        import pandas

        print(f"   OK pandas {pandas.__version__}")
    except ImportError:
        print("   FAIL pandas not installed")
        deps_ok = False

    try:
        import pyarrow

        print(f"   OK pyarrow {pyarrow.__version__}")
    except ImportError:
        print("   FAIL pyarrow not installed")
        deps_ok = False

    try:
        import pyodbc

        pyodbc_version = getattr(pyodbc, "version", None) or getattr(
            pyodbc, "__version__", "installed"
        )
        print(f"   OK pyodbc {pyodbc_version}")
    except ImportError:
        print("   WARN pyodbc not installed. Required for production, not --sample.")

    try:
        import sqlalchemy

        print(f"   OK sqlalchemy {sqlalchemy.__version__}")
    except ImportError:
        print("   WARN sqlalchemy not installed")

    checks.append(deps_ok)

    print("\n6. Testing core imports...")
    try:
        from config.config import DemandConfig, DiagnosisConfig, get_config  # noqa: F401
        from pipelines.shared import load_state, save_state  # noqa: F401

        print("   OK config module")
        print("   OK shared utilities")
        checks.append(True)
    except Exception as exc:
        print(f"   FAIL Import error: {exc}")
        checks.append(False)

    print("\n" + "=" * 80)
    if all(checks):
        print("ALL CHECKS PASSED - Project is ready.")
        print("\nYou can now run:")
        print("  python run_pipeline.py --sample --all")
        print("  python run_pipeline.py --all")
    else:
        print("SOME CHECKS FAILED - Please fix the issues above.")
        print("\nCommon fixes:")
        print("  1. Create data directories: python setup.py")
        print("  2. Install dependencies: pip install -r requirements.txt")
        print("  3. Configure production .env: copy .env.example .env")
    print("=" * 80 + "\n")

    return all(checks)


if __name__ == "__main__":
    success = validate_environment()
    sys.exit(0 if success else 1)
