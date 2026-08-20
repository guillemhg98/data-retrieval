"""Create the local directory structure used by the pipelines."""
from pathlib import Path


def setup_project() -> None:
    """Initialize project structure and directories."""
    project_root = Path(__file__).parent
    print(f"Setting up project at: {project_root}")

    data_dirs = [
        "data/finals",
        "data/sample/input",
        "data/sample/output",
        "data/demand_pipeline/state",
        "data/demand_pipeline/incremental",
        "data/demand_pipeline/finals",
        "data/diagnosis_pipeline/state",
        "data/diagnosis_pipeline/incremental",
        "data/diagnosis_pipeline/finals",
        "data/diagnosis_pipeline/selected_codes",
        "selections",
    ]

    for dir_path in data_dirs:
        full_path = project_root / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        print(f"OK Created: {dir_path}")

    env_file = project_root / ".env"
    if not env_file.exists():
        print("\n.env file not found.")
        print("  This is fine for --sample mode.")
        print("  For production, copy .env.example to .env and update it:")
        print(f"  copy {project_root / '.env.example'} {env_file}")
    else:
        print("OK .env file found")

    print("\nProject setup complete.")
    print("\nNext steps:")
    print("1. Activate virtual environment: .venv\\Scripts\\activate")
    print("2. Install requirements: pip install -r requirements.txt")
    print("3. Validate sample mode: python run_pipeline.py --sample --all")
    print("4. Configure .env with database settings for production")


if __name__ == "__main__":
    setup_project()
