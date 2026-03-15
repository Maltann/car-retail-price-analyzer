"""
db/database.py
Couche d'accès SQLite — connexion, initialisation, helpers CRUD
"""

import sqlite3
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Optional

# Chemin par défaut vers la base (modifiable via env var)
DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "cars.db"
DB_PATH = Path(os.getenv("CAR_PRICER_DB", DEFAULT_DB_PATH))
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Retourne une connexion SQLite avec row_factory et foreign keys activés."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row          # accès aux colonnes par nom
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


@contextmanager
def db_session(db_path: Path = DB_PATH):
    """Context manager : commit auto, rollback sur erreur."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH) -> None:
    """Crée toutes les tables en exécutant schema.sql."""
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    with db_session(db_path) as conn:
        conn.executescript(schema)
    print(f"✅  Base initialisée : {db_path}")


# ----------------------------------------------------------------
# Helpers — vehicles
# ----------------------------------------------------------------

def upsert_vehicle(conn: sqlite3.Connection, data: dict) -> int:
    """
    Insère ou ignore un véhicule (contrainte UNIQUE).
    Retourne l'id du véhicule (existant ou nouvellement créé).
    """
    sql_insert = """
        INSERT OR IGNORE INTO vehicles
            (make, model, model_version, production_year, body_type,
             fuel_type, transmission, drive_train, power_hp, power_kw,
             engine_cc, doors, seats)
        VALUES
            (:make, :model, :model_version, :production_year, :body_type,
             :fuel_type, :transmission, :drive_train, :power_hp, :power_kw,
             :engine_cc, :doors, :seats)
    """
    sql_select = """
        SELECT id FROM vehicles
        WHERE make = :make
          AND model = :model
          AND COALESCE(model_version, '') = COALESCE(:model_version, '')
          AND production_year = :production_year
          AND COALESCE(fuel_type, '') = COALESCE(:fuel_type, '')
          AND COALESCE(transmission, '') = COALESCE(:transmission, '')
    """
    conn.execute(sql_insert, data)
    row = conn.execute(sql_select, data).fetchone()
    return row["id"]


def get_vehicle(conn: sqlite3.Connection, vehicle_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM vehicles WHERE id = ?", (vehicle_id,)
    ).fetchone()


def search_vehicles(
    conn: sqlite3.Connection,
    make: str,
    model: str,
    year: Optional[int] = None,
    trim: Optional[str] = None,
) -> list[sqlite3.Row]:
    query = "SELECT * FROM vehicles WHERE make LIKE ? AND model LIKE ?"
    params: list = [f"%{make}%", f"%{model}%"]
    if year:
        query += " AND production_year = ?"
        params.append(year)
    if trim:
        query += " AND model_version LIKE ?"
        params.append(f"%{trim}%")
    query += " ORDER BY production_year DESC"
    return conn.execute(query, params).fetchall()


# ----------------------------------------------------------------
# Helpers — listings
# ----------------------------------------------------------------

def insert_listing(conn: sqlite3.Connection, data: dict) -> Optional[int]:
    """Insère une annonce. Ignore les doublons (listing_url UNIQUE)."""
    sql = """
        INSERT OR IGNORE INTO listings
            (vehicle_id, source, listing_url, price, mileage_km,
             registration_date, country_code, city, seller_type, color)
        VALUES
            (:vehicle_id, :source, :listing_url, :price, :mileage_km,
             :registration_date, :country_code, :city, :seller_type, :color)
    """
    cursor = conn.execute(sql, data)
    return cursor.lastrowid if cursor.rowcount else None


def get_listings_for_vehicle(
    conn: sqlite3.Connection,
    vehicle_id: int,
    mileage_min: Optional[int] = None,
    mileage_max: Optional[int] = None,
) -> list[sqlite3.Row]:
    query = "SELECT * FROM listings WHERE vehicle_id = ? AND is_active = 1"
    params: list = [vehicle_id]
    if mileage_min is not None:
        query += " AND mileage_km >= ?"
        params.append(mileage_min)
    if mileage_max is not None:
        query += " AND mileage_km <= ?"
        params.append(mileage_max)
    query += " ORDER BY price ASC"
    return conn.execute(query, params).fetchall()


# ----------------------------------------------------------------
# Helpers — market_stats
# ----------------------------------------------------------------

def upsert_market_stats(conn: sqlite3.Connection, data: dict) -> None:
    sql = """
        INSERT INTO market_stats
            (vehicle_id, country_code, mileage_min, mileage_max, sample_size,
             price_min, price_p10, price_p25, price_median,
             price_p75, price_p90, price_max, price_mean, price_stddev)
        VALUES
            (:vehicle_id, :country_code, :mileage_min, :mileage_max, :sample_size,
             :price_min, :price_p10, :price_p25, :price_median,
             :price_p75, :price_p90, :price_max, :price_mean, :price_stddev)
        ON CONFLICT(vehicle_id, country_code, mileage_min, mileage_max)
        DO UPDATE SET
            sample_size  = excluded.sample_size,
            price_min    = excluded.price_min,
            price_p10    = excluded.price_p10,
            price_p25    = excluded.price_p25,
            price_median = excluded.price_median,
            price_p75    = excluded.price_p75,
            price_p90    = excluded.price_p90,
            price_max    = excluded.price_max,
            price_mean   = excluded.price_mean,
            price_stddev = excluded.price_stddev,
            computed_at  = CURRENT_TIMESTAMP
    """
    conn.execute(sql, data)


# ----------------------------------------------------------------
# Helpers — portfolio
# ----------------------------------------------------------------

def add_to_portfolio(conn: sqlite3.Connection, data: dict) -> int:
    sql = """
        INSERT INTO portfolio
            (vehicle_id, purchase_price, purchase_date, purchase_mileage, notes)
        VALUES
            (:vehicle_id, :purchase_price, :purchase_date, :purchase_mileage, :notes)
    """
    cursor = conn.execute(sql, data)
    return cursor.lastrowid


if __name__ == "__main__":
    init_db()
