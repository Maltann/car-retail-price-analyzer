#!/usr/bin/env python3
"""
scripts/3_query.py

CLI d'interrogation de la base de données car_pricer.
Permet de rechercher un véhicule et d'obtenir ses stats marché.

Usage interactif :
    python scripts/3_query.py

Usage direct :
    python scripts/3_query.py --make Volkswagen --model Golf --year 2019
    python scripts/3_query.py --make BMW --model "Série 3" --year 2020 --km-min 50000 --km-max 100000
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich import box
from rapidfuzz import process, fuzz

from db.database import get_connection

console = Console()


# ----------------------------------------------------------------
# Recherche avec fuzzy matching
# ----------------------------------------------------------------

def get_all_makes(conn) -> list[str]:
    rows = conn.execute("SELECT DISTINCT make FROM vehicles ORDER BY make").fetchall()
    return [r["make"] for r in rows]


def get_models_for_make(conn, make: str) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT model FROM vehicles WHERE make = ? ORDER BY model",
        (make,)
    ).fetchall()
    return [r["model"] for r in rows]


def fuzzy_find_make(conn, query: str) -> str | None:
    """Retourne la marque la plus proche du texte saisi."""
    makes = get_all_makes(conn)
    result = process.extractOne(query, makes, scorer=fuzz.WRatio, score_cutoff=60)
    return result[0] if result else None


def fuzzy_find_model(conn, make: str, query: str) -> str | None:
    """Retourne le modèle le plus proche pour une marque donnée."""
    models = get_models_for_make(conn, make)
    result = process.extractOne(query, models, scorer=fuzz.WRatio, score_cutoff=60)
    return result[0] if result else None


def suggest_makes(conn, query: str, limit: int = 5) -> list[str]:
    makes = get_all_makes(conn)
    results = process.extract(query, makes, scorer=fuzz.WRatio, limit=limit)
    return [r[0] for r in results if r[1] >= 40]


def suggest_models(conn, make: str, query: str, limit: int = 5) -> list[str]:
    models = get_models_for_make(conn, make)
    results = process.extract(query, models, scorer=fuzz.WRatio, limit=limit)
    return [r[0] for r in results if r[1] >= 40]


# ----------------------------------------------------------------
# Récupération des données
# ----------------------------------------------------------------

def find_vehicles(conn, make: str, model: str, year: int | None) -> list:
    query = """
        SELECT DISTINCT id, make, model, model_version,
                        production_year, fuel_type, transmission
        FROM vehicles
        WHERE make = ? AND model = ?
    """
    params = [make, model]
    if year:
        query += " AND production_year = ?"
        params.append(year)
    query += " ORDER BY production_year DESC, fuel_type"
    return conn.execute(query, params).fetchall()


def get_stats(conn, vehicle_id: int, km_min: int | None, km_max: int | None) -> dict | None:
    """
    Cherche les stats dans market_stats en priorité,
    sinon les calcule à la volée depuis les listings.
    """
    # 1. Cherche dans le cache market_stats
    query = """
        SELECT * FROM market_stats
        WHERE vehicle_id = ?
    """
    params = [vehicle_id]

    if km_min is not None:
        query += " AND mileage_min <= ? AND mileage_max >= ?"
        params += [km_min, km_max or 999999]

    query += " ORDER BY computed_at DESC LIMIT 1"
    row = conn.execute(query, params).fetchone()
    if row:
        return dict(row)

    # 2. Calcul à la volée si pas de cache
    query2 = """
        SELECT price FROM listings
        WHERE vehicle_id = ? AND is_active = 1
    """
    params2 = [vehicle_id]
    if km_min is not None:
        query2 += " AND mileage_km >= ?"
        params2.append(km_min)
    if km_max is not None:
        query2 += " AND mileage_km <= ?"
        params2.append(km_max)

    prices = [r["price"] for r in conn.execute(query2, params2).fetchall()]
    if len(prices) < 3:
        return None

    prices.sort()
    n = len(prices)

    def percentile(p):
        idx = int(round(p / 100 * (n - 1)))
        return prices[idx]

    return {
        "sample_size":  n,
        "price_min":    prices[0],
        "price_p10":    percentile(10),
        "price_p25":    percentile(25),
        "price_median": percentile(50),
        "price_p75":    percentile(75),
        "price_p90":    percentile(90),
        "price_max":    prices[-1],
        "price_mean":   sum(prices) / n,
    }


def get_listings_sample(conn, vehicle_id: int, km_min: int | None,
                         km_max: int | None, limit: int = 5) -> list:
    query = """
        SELECT price, mileage_km, registration_date, source, country_code, city, seller_type
        FROM listings
        WHERE vehicle_id = ? AND is_active = 1
    """
    params = [vehicle_id]
    if km_min is not None:
        query += " AND mileage_km >= ?"
        params.append(km_min)
    if km_max is not None:
        query += " AND mileage_km <= ?"
        params.append(km_max)
    query += f" ORDER BY price ASC LIMIT {limit}"
    return conn.execute(query, params).fetchall()


# ----------------------------------------------------------------
# Affichage
# ----------------------------------------------------------------

def fmt_price(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:,.0f} €".replace(",", " ")


def fmt_km(value: int | None) -> str:
    if value is None:
        return "—"
    return f"{value:,} km".replace(",", " ")


def display_results(vehicle: dict, stats: dict, listings: list,
                    km_min: int | None, km_max: int | None) -> None:

    # ── Titre ────────────────────────────────────────────────────
    v = vehicle
    title_parts = [v["make"], v["model"]]
    if v["model_version"]:
        title_parts.append(v["model_version"])
    title_parts.append(str(v["production_year"]))
    if v["fuel_type"]:
        title_parts.append(v["fuel_type"])
    if v["transmission"]:
        title_parts.append(v["transmission"])
    title = "  ".join(title_parts)

    km_label = ""
    if km_min is not None or km_max is not None:
        km_label = f"  |  {fmt_km(km_min)} – {fmt_km(km_max)}"

    console.print()
    console.rule(f"[bold cyan]{title}{km_label}[/bold cyan]")

    # ── Stats marché ─────────────────────────────────────────────
    p25     = stats.get("price_p25")
    median  = stats.get("price_median")
    p75     = stats.get("price_p75")
    sample  = stats.get("sample_size", 0)

    stats_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    stats_table.add_column(style="dim")
    stats_table.add_column(style="bold white")

    stats_table.add_row("Annonces analysées",  str(sample))
    stats_table.add_row("Prix minimum",        fmt_price(stats.get("price_min")))
    stats_table.add_row("P10",                 fmt_price(stats.get("price_p10")))
    stats_table.add_row("[green]P25 — Bonne affaire[/green]",
                        f"[green]{fmt_price(p25)}[/green]")
    stats_table.add_row("[bold]Médiane marché[/bold]",
                        f"[bold yellow]{fmt_price(median)}[/bold yellow]")
    stats_table.add_row("P75",                 fmt_price(p75))
    stats_table.add_row("P90",                 fmt_price(stats.get("price_p90")))
    stats_table.add_row("Prix maximum",        fmt_price(stats.get("price_max")))
    stats_table.add_row("Moyenne",             fmt_price(stats.get("price_mean")))

    console.print(stats_table)

    # ── Indicateur d'opportunité ──────────────────────────────────
    if p25 and median:
        console.print(
            f"  [bold green]⚡ Acheter sous {fmt_price(p25)}[/bold green]"
            f"  |  "
            f"[bold red]🚫 Éviter au-dessus de {fmt_price(p75)}[/bold red]"
        )

    # ── Annonces les moins chères ─────────────────────────────────
    if listings:
        console.print()
        console.print("  [dim]Annonces les moins chères :[/dim]")
        listing_table = Table(box=box.SIMPLE, show_header=True, padding=(0, 2))
        listing_table.add_column("Prix",     style="bold green")
        listing_table.add_column("Km",       style="cyan")
        listing_table.add_column("Année",    style="white")
        listing_table.add_column("Source",   style="dim")
        listing_table.add_column("Vendeur",  style="dim")

        for l in listings:
            listing_table.add_row(
                fmt_price(l["price"]),
                fmt_km(l["mileage_km"]),
                str(l["registration_date"] or "—"),
                str(l["source"] or "—"),
                str(l["seller_type"] or "—"),
            )
        console.print(listing_table)

    console.rule(style="dim")
    console.print()


# ----------------------------------------------------------------
# Mode interactif
# ----------------------------------------------------------------

def prompt_search(conn) -> tuple:
    """Pose les questions à l'utilisateur et retourne les paramètres de recherche."""
    console.print("\n[bold cyan]🚗  Car Pricer — Recherche[/bold cyan]")
    console.rule(style="dim")

    # Marque
    while True:
        make_input = console.input("\n  [bold]Marque[/bold]      : ").strip()
        if not make_input:
            continue
        make = fuzzy_find_make(conn, make_input)
        if make:
            if make.lower() != make_input.lower():
                console.print(f"  [dim]→ Correction : [cyan]{make}[/cyan][/dim]")
            break
        suggestions = suggest_makes(conn, make_input)
        console.print(f"  [red]❌ Marque introuvable.[/red]")
        if suggestions:
            console.print(f"  [dim]Suggestions : {', '.join(suggestions)}[/dim]")

    # Modèle
    while True:
        model_input = console.input("  [bold]Modèle[/bold]      : ").strip()
        if not model_input:
            continue
        model = fuzzy_find_model(conn, make, model_input)
        if model:
            if model.lower() != model_input.lower():
                console.print(f"  [dim]→ Correction : [cyan]{model}[/cyan][/dim]")
            break
        suggestions = suggest_models(conn, make, model_input)
        console.print(f"  [red]❌ Modèle introuvable pour {make}.[/red]")
        if suggestions:
            console.print(f"  [dim]Suggestions : {', '.join(suggestions)}[/dim]")

    # Année (optionnel)
    year = None
    year_input = console.input("  [bold]Année[/bold]       : [dim](optionnel)[/dim] ").strip()
    if year_input.isdigit():
        year = int(year_input)

    # Kilométrage (optionnel)
    km = None
    km_margin = 10000 # 10k km -> default margin

    km_min = km_max = None
    km_input = console.input("  [bold]Kilometrage[/bold]     : [dim](optionnel)[/dim] ").strip()
    if km_input.isdigit():
        km = int(km_input)
    km_input = console.input("  [bold]Marge kilométrage[/bold]     : [dim](optionnel)[/dim] ").strip()
    if km_input.isdigit():
        km_margin = int(km_input)

    if km != None:
        km_min = km - km_margin
        km_max = km + km_margin

    return make, model, year, km_min, km_max


# ----------------------------------------------------------------
# Logique principale
# ----------------------------------------------------------------

def run_query(make: str, model: str, year: int | None,
              km_min: int | None, km_max: int | None) -> None:

    conn = get_connection()

    # Fuzzy matching si appelé en CLI
    resolved_make  = fuzzy_find_make(conn, make)
    if not resolved_make:
        console.print(f"[red]❌ Marque introuvable : {make}[/red]")
        suggestions = suggest_makes(conn, make)
        if suggestions:
            console.print(f"[dim]Suggestions : {', '.join(suggestions)}[/dim]")
        return

    resolved_model = fuzzy_find_model(conn, resolved_make, model)
    if not resolved_model:
        console.print(f"[red]❌ Modèle introuvable : {model} (pour {resolved_make})[/red]")
        suggestions = suggest_models(conn, resolved_make, model)
        if suggestions:
            console.print(f"[dim]Suggestions : {', '.join(suggestions)}[/dim]")
        return

    vehicles = find_vehicles(conn, resolved_make, resolved_model, year)

    if not vehicles:
        console.print(f"[red]❌ Aucun véhicule trouvé pour {resolved_make} {resolved_model}"
                      f"{' ' + str(year) if year else ''}[/red]")
        return

    found_any = False
    for v in vehicles:
        stats = get_stats(conn, v["id"], km_min, km_max)
        if not stats or stats["sample_size"] < 3:
            continue
        listings = get_listings_sample(conn, v["id"], km_min, km_max)
        display_results(dict(v), stats, listings, km_min, km_max)
        found_any = True

    if not found_any:
        console.print(
            f"[yellow]⚠️  Pas assez d'annonces pour calculer des stats fiables "
            f"({'avec ces filtres km' if km_min or km_max else 'sur ce véhicule'}).[/yellow]"
        )

    conn.close()


# ----------------------------------------------------------------
# CLI + boucle interactive
# ----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Interroger la base car_pricer")
    parser.add_argument("--make",   type=str, help="Marque du véhicule")
    parser.add_argument("--model",  type=str, help="Modèle du véhicule")
    parser.add_argument("--year",   type=int, help="Année de production")
    parser.add_argument("--km-min", type=int, help="Kilométrage minimum")
    parser.add_argument("--km-max", type=int, help="Kilométrage maximum")
    args = parser.parse_args()

    # Mode CLI direct
    if args.make and args.model:
        run_query(args.make, args.model, args.year, args.km_min, args.km_max)
        return

    # Mode interactif avec boucle
    conn = get_connection()
    try:
        while True:
            make, model, year, km_min, km_max = prompt_search(conn)
            conn.close()
            run_query(make, model, year, km_min, km_max)

            again = console.input("  Nouvelle recherche ? [dim](o/n)[/dim] : ").strip().lower()
            if again not in ("o", "oui", "y", "yes"):
                break
            conn = get_connection()
    except KeyboardInterrupt:
        console.print("\n\n[dim]Au revoir ![/dim]\n")


if __name__ == "__main__":
    main()
