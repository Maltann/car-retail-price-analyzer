#!/usr/bin/env python3
"""
scripts/2_import_autoscout24.py

Import du dataset AutoScout24 (autoscout24_dataset_20251108.csv) dans la base SQLite.
  - Peuple vehicles + listings en une seule passe
  - Calcule les market_stats par véhicule / tranche km / pays

Usage :
    python scripts/2_import_autoscout24.py
    python scripts/2_import_autoscout24.py --csv data/autoscout24_dataset_20251108.csv
    python scripts/2_import_autoscout24.py --country FR        # importer un seul pays
    python scripts/2_import_autoscout24.py --dry-run           # simulation sans écriture
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from rich.console import Console
from rich.progress import track
from rich.table import Table

from db.database import db_session, upsert_vehicle, upsert_market_stats
from config.settings import MILEAGE_BRACKETS

console = Console()

DEFAULT_CSV = Path("data/autoscout24_dataset_20251108.csv")

# ----------------------------------------------------------------
# Mapping colonnes CSV → schéma DB
# ----------------------------------------------------------------
VEHICLE_COLS = {
    "make":             "make",
    "model":            "model",
    "model_version":    "model_version",
    "production_year":  "production_year",
    "body_type":        "body_type",
    "primary_fuel":     "fuel_type",
    "transmission":     "transmission",
    "drive_train":      "drive_train",
    "power_hp":         "power_hp",
    "power_kw":         "power_kw",
    "cylinders_volume_cc": "engine_cc",
    "nr_doors":         "doors",
    "nr_seats":         "seats",
}

LISTING_COLS = {
    "id":                       "external_id",
    "price":                    "price",
    "price_currency":           "price_currency",
    "mileage_km":               "mileage_km",
    "registration_date":        "registration_date",
    "country_code":             "country_code",
    "city":                     "city",
    "seller_type":              "seller_type",
    "seller_company_name":      "seller_company_name",
    "body_color":               "color",
    "had_accident":             "had_accident",
    "has_full_service_history": "has_full_service_history",
    "non_smoking":              "non_smoking",
    "nr_prev_owners":           "nr_prev_owners",
    "envir_standard":           "envir_standard",
    "co2_emission_grper_km":    "co2_emission",
}


# ----------------------------------------------------------------
# Nettoyage
# ----------------------------------------------------------------

def bool_to_int(val) -> int:
    """Convertit True/False/NaN → 1/0."""
    if pd.isna(val):
        return 0
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, str):
        return 1 if val.lower() in ("true", "1", "yes") else 0
    return int(val)


def clean_mileage(val) -> int | None:
    """Nettoie le kilométrage : '10,500 km' → 10500."""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return int(val) if val > 0 else None
    cleaned = str(val).replace(",", "").replace(" km", "").strip()
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def load_csv(path: Path, country_filter: str | None = None) -> pd.DataFrame:
    console.print(f"\n📂  Chargement de [bold]{path.name}[/bold]...")

    # Colonnes à charger (évite de charger description, equipment... inutiles)
    usecols = list(VEHICLE_COLS.keys()) + list(LISTING_COLS.keys())
    # Certaines colonnes peuvent manquer selon la version du CSV
    all_cols = pd.read_csv(path, nrows=0).columns.tolist()
    usecols = [c for c in usecols if c in all_cols]

    df = pd.read_csv(path, usecols=usecols, low_memory=False)
    initial = len(df)

    # Filtre pays
    if country_filter and "country_code" in df.columns:
        df = df[df["country_code"] == country_filter.upper()]
        console.print(f"   Filtre pays [cyan]{country_filter.upper()}[/cyan] : {len(df):,} annonces")

    # Nettoyage kilométrage
    if "mileage_km" in df.columns:
        df["mileage_km"] = df["mileage_km"].apply(clean_mileage)

    # Booléens → int
    for col in ["had_accident", "has_full_service_history", "non_smoking"]:
        if col in df.columns:
            df[col] = df[col].apply(bool_to_int)

    # Numériques
    for col in ["price", "power_hp", "power_kw", "cylinders_volume_cc",
                "nr_doors", "nr_seats", "production_year", "nr_prev_owners",
                "co2_emission_grper_km"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Extraire l'année depuis registration_date si production_year est absent
    if "registration_date" in df.columns:
        mask = df["production_year"].isna() & df["registration_date"].notna()
        df.loc[mask, "production_year"] = (
            pd.to_datetime(df.loc[mask, "registration_date"], errors="coerce").dt.year
        )

    fuel_map = {
        "Regular/Benzine 91":         "Essence",
        "Super 95":                   "Essence",
        "Super E10 95":               "Essence",
        "Super Plus 98":              "Essence",
        "Regular/Benzine E10 91":     "Essence",
        "Super Plus E10 98":          "Essence",
        "Gasoline":                   "Essence",
        "Diesel":                     "Diesel",
        "Biodiesel":                  "Diesel",
        "Electricity":                "Électrique",
        "Hybrid":                     "Hybride",
        "Liquid petroleum gas (LPG)": "Autre",
        "Biogas":                     "Autre",
        "Ethanol":                    "Autre",
        "Domestic gas H":             "Autre",
        "Vegetable oil":              "Autre",
    }
    if "primary_fuel" in df.columns:
        df["primary_fuel"] = df["primary_fuel"].replace(fuel_map)

    # Suppression lignes invalides
    df = df.dropna(subset=["make", "model", "price", "production_year"])
    df = df[df["price"] > 500]
    df = df[df["production_year"] >= 1980]

    # Assurer que price_currency vaut EUR si absent
    if "price_currency" not in df.columns:
        df["price_currency"] = "EUR"
    df["price_currency"] = df["price_currency"].fillna("EUR")

    after = len(df)
    console.print(f"   Lignes initiales  : [cyan]{initial:,}[/cyan]")
    console.print(f"   Lignes supprimées : [yellow]{initial - after:,}[/yellow]")
    console.print(f"   Lignes conservées : [green]{after:,}[/green]")

    if "country_code" in df.columns:
        console.print(f"   Répartition pays  : {df['country_code'].value_counts().to_dict()}")

    return df.reset_index(drop=True)


# ----------------------------------------------------------------
# Calcul stats
# ----------------------------------------------------------------

def compute_stats(prices: list[float], vehicle_id: int,
                  country_code: str, mileage_min: int, mileage_max: int) -> dict:
    arr = np.array(prices)
    return {
        "vehicle_id":   vehicle_id,
        "country_code": country_code,
        "mileage_min":  mileage_min,
        "mileage_max":  mileage_max,
        "sample_size":  len(arr),
        "price_min":    float(np.min(arr)),
        "price_p10":    float(np.percentile(arr, 10)),
        "price_p25":    float(np.percentile(arr, 25)),
        "price_median": float(np.median(arr)),
        "price_p75":    float(np.percentile(arr, 75)),
        "price_p90":    float(np.percentile(arr, 90)),
        "price_max":    float(np.max(arr)),
        "price_mean":   float(np.mean(arr)),
        "price_stddev": float(np.std(arr)),
    }


# ----------------------------------------------------------------
# Import principal
# ----------------------------------------------------------------

def run_import(csv_path: Path, country_filter: str | None, dry_run: bool) -> None:
    df = load_csv(csv_path, country_filter)

    stats = {
        "vehicles_created":  0,
        "vehicles_existing": 0,
        "listings_created":  0,
        "listings_skipped":  0,
        "stats_computed":    0,
    }

    console.print("\n🗄   Import en base...\n")

    # Accumulateur pour les stats : (vehicle_id, country, km_bracket) → [prices]
    price_buckets: dict[tuple, list[float]] = {}

    INSERT_LISTING = """
        INSERT OR IGNORE INTO listings
            (vehicle_id, source, external_id, price, price_currency,
             mileage_km, registration_date, country_code, city,
             seller_type, seller_company_name, color,
             had_accident, has_full_service_history, non_smoking,
             nr_prev_owners, envir_standard, co2_emission)
        VALUES
            (:vehicle_id, :source, :external_id, :price, :price_currency,
             :mileage_km, :registration_date, :country_code, :city,
             :seller_type, :seller_company_name, :color,
             :had_accident, :has_full_service_history, :non_smoking,
             :nr_prev_owners, :envir_standard, :co2_emission)
    """

    def safe(row, col, default=None):
        val = row.get(col, default)
        return default if pd.isna(val) else val

    with db_session() as conn:
        for _, row in track(df.iterrows(), total=len(df), description="Insertion..."):
            row = row.to_dict()

            # ── Vehicle ───────────────────────────────────────────
            vehicle_data = {
                "make":            safe(row, "make", ""),
                "model":           safe(row, "model", ""),
                "model_version":   safe(row, "model_version"),
                "production_year": int(safe(row, "production_year", 0)),
                "body_type":       safe(row, "body_type"),
                "fuel_type":       safe(row, "primary_fuel"),
                "transmission":    safe(row, "transmission"),
                "drive_train":     safe(row, "drive_train"),
                "power_hp":        int(v) if (v := safe(row, "power_hp")) is not None else None,
                "power_kw":        int(v) if (v := safe(row, "power_kw")) is not None else None,
                "engine_cc":       int(v) if (v := safe(row, "cylinders_volume_cc")) is not None else None,
                "doors":           int(v) if (v := safe(row, "nr_doors")) is not None else None,
                "seats":           int(v) if (v := safe(row, "nr_seats")) is not None else None,
            }

            if dry_run:
                continue

            existing = conn.execute(
                """SELECT id FROM vehicles
                   WHERE make=:make AND model=:model
                   AND COALESCE(model_version,'')=COALESCE(:model_version,'')
                   AND production_year=:production_year
                   AND COALESCE(fuel_type,'')=COALESCE(:fuel_type,'')
                   AND COALESCE(transmission,'')=COALESCE(:transmission,'')""",
                vehicle_data,
            ).fetchone()

            vehicle_id = upsert_vehicle(conn, vehicle_data)
            if existing:
                stats["vehicles_existing"] += 1
            else:
                stats["vehicles_created"] += 1

            # ── Listing ───────────────────────────────────────────
            listing_data = {
                "vehicle_id":               vehicle_id,
                "source":                   "autoscout24",
                "external_id":              safe(row, "id"),
                "price":                    float(safe(row, "price", 0)),
                "price_currency":           safe(row, "price_currency", "EUR"),
                "mileage_km":               safe(row, "mileage_km"),
                "registration_date":        safe(row, "registration_date"),
                "country_code":             safe(row, "country_code"),
                "city":                     safe(row, "city"),
                "seller_type":              safe(row, "seller_type"),
                "seller_company_name":      safe(row, "seller_company_name"),
                "color":                    safe(row, "body_color"),
                "had_accident":             int(safe(row, "had_accident", 0)),
                "has_full_service_history": int(safe(row, "has_full_service_history", 0)),
                "non_smoking":              int(safe(row, "non_smoking", 0)),
                "nr_prev_owners":           safe(row, "nr_prev_owners"),
                "envir_standard":           safe(row, "envir_standard"),
                "co2_emission":             safe(row, "co2_emission_grper_km"),
            }

            cursor = conn.execute(INSERT_LISTING, listing_data)
            if cursor.rowcount:
                stats["listings_created"] += 1
            else:
                stats["listings_skipped"] += 1

            # ── Accumulation prix pour stats ──────────────────────
            km = safe(row, "mileage_km")
            price = float(safe(row, "price", 0))
            country = safe(row, "country_code", "XX")

            if km is not None and price > 0:
                for km_min, km_max in MILEAGE_BRACKETS:
                    if km_min <= km < km_max:
                        # Stats ALL pays
                        key_all = (vehicle_id, "ALL", km_min, km_max)
                        price_buckets.setdefault(key_all, []).append(price)
                        # Stats par pays
                        key_country = (vehicle_id, country, km_min, km_max)
                        price_buckets.setdefault(key_country, []).append(price)
                        break

        if dry_run:
            console.print("[yellow]⚠️   Dry-run : aucune donnée écrite[/yellow]")
            return

        conn.commit()

        # ── Calcul market_stats ───────────────────────────────────
        console.print("\n📊  Calcul des statistiques marché...")

        for (vehicle_id, country, km_min, km_max), prices in track(
            price_buckets.items(), description="Stats..."
        ):
            if len(prices) < 3:
                continue
            data = compute_stats(prices, vehicle_id, country, km_min, km_max)
            upsert_market_stats(conn, data)
            stats["stats_computed"] += 1

    # ── Résumé ────────────────────────────────────────────────────
    console.print()
    t = Table(title="✅  Import terminé", show_header=False, box=None)
    t.add_column(style="bold cyan")
    t.add_column(style="green")
    t.add_row("Véhicules créés",    f"{stats['vehicles_created']:,}")
    t.add_row("Véhicules existants",f"{stats['vehicles_existing']:,}")
    t.add_row("Annonces créées",    f"{stats['listings_created']:,}")
    t.add_row("Annonces ignorées",  f"{stats['listings_skipped']:,}")
    t.add_row("Stats calculées",    f"{stats['stats_computed']:,}")
    console.print(t)
    console.print("\n💡  Lance maintenant : [bold]python scripts/3_query.py[/bold]\n")


# ----------------------------------------------------------------
# CLI
# ----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Import AutoScout24 CSV → SQLite")
    parser.add_argument("--csv",     type=Path, default=DEFAULT_CSV,
                        help=f"Chemin vers le CSV (défaut: {DEFAULT_CSV})")
    parser.add_argument("--country", type=str, default=None,
                        help="Filtrer sur un pays (ex: FR, DE, IT)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulation sans écriture en base")
    args = parser.parse_args()

    if not args.csv.exists():
        console.print(f"[red]❌  Fichier introuvable : {args.csv}[/red]")
        console.print(f"    Dépose le CSV dans [bold]data/[/bold] et relance.")
        sys.exit(1)

    run_import(args.csv, args.country, args.dry_run)


if __name__ == "__main__":
    main()
