#!/usr/bin/env python3
"""
setup.py
Script d'installation one-shot — à lancer une seule fois après clonage du projet.
  1. Vérifie la version Python
  2. Crée l'environnement virtuel
  3. Installe les dépendances
  4. Initialise la base de données SQLite
"""

import sys
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).parent
VENV_DIR = BASE_DIR / ".venv"
DB_DIR   = BASE_DIR / "data"


def check_python():
    if sys.version_info < (3, 11):
        print("❌  Python 3.11+ requis. Version actuelle :", sys.version)
        sys.exit(1)
    print(f"✅  Python {sys.version.split()[0]}")


def create_venv():
    if VENV_DIR.exists():
        print("✅  Virtualenv déjà présent (.venv)")
        return
    print("📦  Création du virtualenv...")
    subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)
    print("✅  Virtualenv créé dans .venv/")


def install_deps():
    pip = VENV_DIR / "bin" / "pip"
    print("📦  Installation des dépendances...")
    subprocess.run([str(pip), "install", "-q", "-r", "requirements.txt"], check=True)
    print("✅  Dépendances installées")


def init_database():
    DB_DIR.mkdir(exist_ok=True)
    python = VENV_DIR / "bin" / "python"
    print("🗄   Initialisation de la base de données...")
    subprocess.run(
        [str(python), "-c", "from db.database import init_db; init_db()"],
        cwd=BASE_DIR,
        check=True,
    )


def main():
    print("\n🚗  Car Pricer — Setup\n" + "─" * 40)
    check_python()
    create_venv()
    install_deps()
    init_database()
    print("\n" + "─" * 40)
    print("🎉  Setup terminé !")
    print(f"📂  Base de données : {DB_DIR / 'cars.db'}")
    print("\nProchaine étape :")
    print("  source .venv/bin/activate")
    print("  python scripts/1_fetch_vehicles.py --csv data/autoscout24.csv\n")


if __name__ == "__main__":
    main()
