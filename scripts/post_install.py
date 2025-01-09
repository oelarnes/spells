# scripts/post_install.py
import os
import sys
from pathlib import Path

from spells.cache import spells_print


def modify_activation_script():
    spells_print("install", "configuring environment for spells")
    venv_path = os.environ.get("VIRTUAL_ENV")
    if not venv_path:
        print("No virtual environment found")
        return

    if sys.platform == "win32":
        activate_script = Path(venv_path) / "Scripts" / "activate.bat"
    else:
        activate_script = Path(venv_path) / "bin" / "activate"

    if not activate_script.exists():
        print(f"Activation script not found at {activate_script}")
        return

    with open(activate_script, "a") as f:
        f.write("\n# Custom environment setup\n")
        f.write("export LOG=$HOME/.local/share/spells/.logs/spells.log\n")


if __name__ == "__main__":
    modify_activation_script()
