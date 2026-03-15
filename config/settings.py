"""
config/settings.py
Paramètres globaux du projet
"""

from pathlib import Path

# ── Chemins ────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "data"
DB_PATH     = DATA_DIR / "cars.db"

# ── Scraping ───────────────────────────────────────────────────
SCRAPING = {
    "delay_min":        2.0,    # secondes entre requêtes (min)
    "delay_max":        5.0,    # secondes entre requêtes (max)
    "max_retries":      3,
    "timeout":          15,     # secondes
    "max_listings_per_vehicle": 50,
}

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ── Sources de données ─────────────────────────────────────────
SOURCES = {
    "autoscout24": {
        "base_url":    "https://www.autoscout24.fr",
        "search_url":  "https://www.autoscout24.fr/lst/{make}/{model}",
        "enabled":     True,
    },
    "lacentrale": {
        "base_url":    "https://www.lacentrale.fr",
        "search_url":  "https://www.lacentrale.fr/listing?makesModelsCommercialNames={make}%3A{model}",
        "enabled":     True,
    },
    "leboncoin": {
        "base_url":    "https://www.leboncoin.fr",
        "search_url":  "https://www.leboncoin.fr/voitures/offres/?q={make}+{model}",
        "enabled":     False,   # activer quand le scraper est prêt
    },
}

# ── Tranches kilométriques pour les stats ──────────────────────
MILEAGE_BRACKETS = [
    (0,      50_000),
    (50_000, 100_000),
    (100_000, 150_000),
    (150_000, 200_000),
    (200_000, 999_999),
]

# ── Conversion ────────────────────────────────────────────────
GBP_TO_EUR = 1.17       # taux £ → € à mettre à jour manuellement si besoin
MILES_TO_KM = 1.60934   # facteur de conversion miles → km

# ── Import CSV ─────────────────────────────────────────────────
CSV_IMPORT = {
    # Colonnes attendues dans le CSV AutoScout24 → colonnes internes
    "autoscout24_mapping": {
        "make":              "make",
        "model":             "model",
        "model_version":     "model_version",
        "price":             "price",
        "mileage":           "mileage_km",
        "first_registration":"registration_date",
        "year":              "production_year",
        "fuel":              "fuel_type",
        "gear":              "transmission",
        "hp":                "power_hp",
        "body_type":         "body_type",
        "color":             "color",
        "seller":            "seller_type",
    }
}
