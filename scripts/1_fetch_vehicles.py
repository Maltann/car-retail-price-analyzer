#!/usr/bin/env python3
"""
scripts/1_fetch_vehicles.py

Import du CSV CarsData dans la base SQLite.
  - Nettoie et normalise les données
  - Convertit miles → km et £ → €
  - Peuple vehicles + listings en une seule passe
  - Calcule les market_stats par véhicule et tranche km

Usage :
    python scripts/1_fetch_vehicles.py --csv data/CarsData.csv
    python scripts/1_fetch_vehicles.py --csv data/CarsData.csv --dry-run
"""

import argparse
import sys
from pathlib import Path

# Permet d'importer les modules du projet depuis n'importe où
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from rich.console import Console
from rich.progress import track
from rich.table import Table

from db.database import db_session, upsert_vehicle, insert_listing, upsert_market_stats
from config.settings import GBP_TO_EUR, MILES_TO_KM, MILEAGE_BRACKETS

console = Console()

# ----------------------------------------------------------------
# Normalisation des marques (CSV UK → noms officiels)
# ----------------------------------------------------------------
MAKE_NORMALIZATION = {
    "hyundi":     "Hyundai",
    "merc":       "Mercedes-Benz",
    "ford":       "Ford",
    "volkswagen": "Volkswagen",
    "vauxhall":   "Vauxhall",
    "BMW":        "BMW",
    "Audi":       "Audi",
    "toyota":     "Toyota",
    "skoda":      "Skoda",
    "honda":      "Honda",
    "nissan":     "Nissan",
    "kia":        "Kia",
    "peugeot":    "Peugeot",
    "renault":    "Renault",
    "citroen":    "Citroën",
    "seat":       "SEAT",
    "volvo":      "Volvo",
    "mini":       "MINI",
    "land rover": "Land Rover",
    "jaguar":     "Jaguar",
    "lexus":      "Lexus",
    "mazda":      "Mazda",
    "mitsubishi": "Mitsubishi",
    "subaru":     "Subaru",
    "suzuki":     "Suzuki",
    "fiat":       "Fiat",
    "alfa romeo": "Alfa Romeo",
    "jeep":       "Jeep",
    "porsche":    "Porsche",
    "tesla":      "Tesla",
}

# Traduction des types de carburant
FUEL_TRANSLATION = {
    "Petrol":   "Essence",
    "Diesel":   "Diesel",
    "Hybrid":   "Hybride",
    "Electric": "Électrique",
    "Other":    "Autre",
}

# Traduction des transmissions
TRANSMISSION_TRANSLATION = {
    "Manual":    "Manuelle",
    "Automatic": "Automatique",
    "Semi-Auto": "Semi-automatique",
    "Other":     "Autre",
}


# ----------------------------------------------------------------
# Chargement et nettoyage du CSV
# ----------------------------------------------------------------

def load_csv(path: Path) -> pd.DataFrame:
    """Charge le CSV et effectue tous les nettoyages nécessaires."""
    console.print(f"\n📂  Chargement de [bold]{path.name}[/bold]...")
    df = pd.read_csv(path)
    initial_count = len(df)

    # ── Nettoyage des noms de colonnes ──────────────────────────
    df.columns = df.columns.str.strip()

    # ── Suppression des espaces parasites dans les valeurs ──────
    for col in ["model", "Manufacturer", "transmission", "fuelType"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # ── Normalisation des marques ────────────────────────────────
    def normalize_make(raw: str) -> str:
        key = raw.strip().lower()
        return MAKE_NORMALIZATION.get(key) or MAKE_NORMALIZATION.get(raw.strip()) or raw.strip().title()

    df["make"] = df["Manufacturer"].map(normalize_make)

    # ── Traductions ──────────────────────────────────────────────
    df["fuel_type"]    = df["fuelType"].map(FUEL_TRANSLATION).fillna("Autre")
    df["transmission"] = df["transmission"].map(TRANSMISSION_TRANSLATION).fillna("Autre")

    # ── Conversion miles → km ────────────────────────────────────
    df["mileage_km"] = (
        pd.to_numeric(df["mileage"], errors="coerce") * MILES_TO_KM
    ).round(0).astype("Int64")

    # ── Conversion £ → € ─────────────────────────────────────────
    df["price_eur"] = (
        pd.to_numeric(df["price"], errors="coerce") * GBP_TO_EUR
    ).round(0)

    # ── Conversion cylindrée litres → cc ─────────────────────────
    df["engine_cc"] = (
        pd.to_numeric(df["engineSize"], errors="coerce") * 1000
    ).round(0).astype("Int64")

    # ── Année ────────────────────────────────────────────────────
    df["production_year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    # ── Suppression des lignes invalides ─────────────────────────
    before = len(df)
    df = df.dropna(subset=["make", "model", "production_year", "price_eur"])
    df = df[df["price_eur"] > 500]           # prix aberrants
    df = df[df["production_year"] >= 1990]   # années aberrantes
    after = len(df)

    console.print(f"   Lignes initiales  : [cyan]{initial_count:,}[/cyan]")
    console.print(f"   Lignes supprimées : [yellow]{before - after:,}[/yellow] (invalides)")
    console.print(f"   Lignes conservées : [green]{after:,}[/green]")

    return df.reset_index(drop=True)


# ----------------------------------------------------------------
# Calcul des market_stats
# ----------------------------------------------------------------

def compute_stats(prices: list[float], vehicle_id: int,
                  mileage_min: int, mileage_max: int) -> dict:
    """Calcule les percentiles et stats pour un groupe de prix."""
    arr = np.array(prices)
    return {
        "vehicle_id":  vehicle_id,
        "mileage_min": mileage_min,
        "mileage_max": mileage_max,
        "sample_size": len(arr),
        "price_min":   float(np.min(arr)),
        "price_p10":   float(np.percentile(arr, 10)),
        "price_p25":   float(np.percentile(arr, 25)),
        "price_median":float(np.median(arr)),
        "price_p75":   float(np.percentile(arr, 75)),
        "price_p90":   float(np.percentile(arr, 90)),
        "price_max":   float(np.max(arr)),
        "price_mean":  float(np.mean(arr)),
        "price_stddev":float(np.std(arr)),
    }


# ----------------------------------------------------------------
# Import principal
# ----------------------------------------------------------------

def run_import(csv_path: Path, dry_run: bool = False) -> None:
    df = load_csv(csv_path)

    stats = {
        "vehicles_created": 0,
        "vehicles_existing": 0,
        "listings_created": 0,
        "listings_skipped": 0,
        "stats_computed": 0,
    }

    console.print("\n🗄   Import en base...\n")

    with db_session() as conn:
        # ── Étape 1 : vehicles + listings ────────────────────────
        vehicle_prices: dict[tuple, dict] = {}  # (vehicle_id, km_bracket) → [prices]

        for _, row in track(df.iterrows(), total=len(df), description="Insertion..."):
            vehicle_data = {
                "make":            row["make"],
                "model":           row["model"],
                "model_version":   None,          # pas dans ce dataset
                "production_year": int(row["production_year"]),
                "body_type":       None,
                "fuel_type":       row["fuel_type"],
                "transmission":    row["transmission"],
                "drive_train":     None,
                "power_hp":        None,
                "power_kw":        None,
                "engine_cc":       int(row["engine_cc"]) if pd.notna(row["engine_cc"]) else None,
                "doors":           None,
                "seats":           None,
            }

            if dry_run:
                continue

            # Upsert véhicule
            existing = conn.execute(
                """SELECT id FROM vehicles
                   WHERE make = :make AND model = :model
                   AND production_year = :production_year
                   AND COALESCE(fuel_type,'') = COALESCE(:fuel_type,'')
                   AND COALESCE(transmission,'') = COALESCE(:transmission,'')""",
                vehicle_data,
            ).fetchone()

            vehicle_id = upsert_vehicle(conn, vehicle_data)

            if existing:
                stats["vehicles_existing"] += 1
            else:
                stats["vehicles_created"] += 1

            # Insert listing
            listing_data = {
                "vehicle_id":        vehicle_id,
                "source":            "carsdata_csv",
                "listing_url":       None,
                "price":             float(row["price_eur"]),
                "mileage_km":        int(row["mileage_km"]) if pd.notna(row["mileage_km"]) else None,
                "registration_date": str(int(row["production_year"])),
                "country_code": "GB",
                "city":          None,
                "seller_type":       None,
                "color":             None,
            }

            result = insert_listing(conn, listing_data)
            if result:
                stats["listings_created"] += 1
            else:
                stats["listings_skipped"] += 1

            # Accumulation des prix par tranche km pour les stats
            km = int(row["mileage_km"]) if pd.notna(row["mileage_km"]) else None
            if km is not None:
                for km_min, km_max in MILEAGE_BRACKETS:
                    if km_min <= km < km_max:
                        key = (vehicle_id, km_min, km_max)
                        vehicle_prices.setdefault(key, []).append(float(row["price_eur"]))
                        break

        if dry_run:
            console.print("[yellow]⚠️   Dry-run : aucune donnée écrite en base[/yellow]")
            return

        conn.commit()

        # ── Étape 2 : calcul des market_stats ────────────────────
        console.print("\n📊  Calcul des statistiques marché...")

        for (vehicle_id, km_min, km_max), prices in track(
            vehicle_prices.items(),
            description="Stats...",
        ):
            if len(prices) < 3:   # pas assez de données pour être significatif
                continue
            stats_data = compute_stats(prices, vehicle_id, km_min, km_max)
            stats_data["country_code"] = "GB"
            upsert_market_stats(conn, stats_data)
            stats["stats_computed"] += 1

    # ── Résumé ────────────────────────────────────────────────────
    console.print("\n")
    table = Table(title="✅  Import terminé", show_header=False, box=None)
    table.add_column(style="bold cyan")
    table.add_column(style="green")
    table.add_row("Véhicules créés",    f"{stats['vehicles_created']:,}")
    table.add_row("Véhicules existants",f"{stats['vehicles_existing']:,}")
    table.add_row("Annonces créées",    f"{stats['listings_created']:,}")
    table.add_row("Annonces ignorées",  f"{stats['listings_skipped']:,}")
    table.add_row("Stats calculées",    f"{stats['stats_computed']:,}")
    console.print(table)
    console.print("\n💡  Lance maintenant : [bold]python scripts/3_query.py[/bold]\n")


# ----------------------------------------------------------------
# CLI
# ----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Import CSV CarsData → SQLite")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("data/CarsData.csv"),
        help="Chemin vers le fichier CSV (défaut: data/CarsData.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simule l'import sans écrire en base",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        console.print(f"[red]❌  Fichier introuvable : {args.csv}[/red]")
        console.print("    Dépose le CSV dans le dossier [bold]data/[/bold] et relance.")
        sys.exit(1)

    run_import(args.csv, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
