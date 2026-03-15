-- ============================================================
--  CAR PRICER — Schéma SQLite
--  Version : 2.0 (AutoScout24 dataset)
--  Exécuter dans DBeaver : clic droit sur la DB > Execute SQL Script
-- ============================================================

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ------------------------------------------------------------
-- 1. RÉFÉRENTIEL VÉHICULES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vehicles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    make            TEXT NOT NULL,
    model           TEXT NOT NULL,
    model_version   TEXT,
    production_year INTEGER NOT NULL,
    body_type       TEXT,
    fuel_type       TEXT,
    transmission    TEXT,
    drive_train     TEXT,
    power_hp        INTEGER,
    power_kw        INTEGER,
    engine_cc       INTEGER,
    doors           INTEGER,
    seats           INTEGER,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(make, model, model_version, production_year, fuel_type, transmission)
);

-- ------------------------------------------------------------
-- 2. ANNONCES MARCHÉ
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS listings (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id               INTEGER NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
    source                   TEXT NOT NULL,
    external_id              TEXT UNIQUE,
    listing_url              TEXT UNIQUE,
    price                    REAL NOT NULL,
    price_currency           TEXT DEFAULT 'EUR',
    mileage_km               INTEGER,
    registration_date        TEXT,
    country_code             TEXT,
    city                     TEXT,
    seller_type              TEXT,
    seller_company_name      TEXT,
    color                    TEXT,
    had_accident             INTEGER DEFAULT 0,
    has_full_service_history INTEGER DEFAULT 0,
    non_smoking              INTEGER DEFAULT 0,
    nr_prev_owners           INTEGER,
    envir_standard           TEXT,
    co2_emission             REAL,
    is_active                INTEGER DEFAULT 1,
    scraped_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 3. COTES DE RÉFÉRENCE
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id      INTEGER NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
    source          TEXT NOT NULL,
    mileage_min     INTEGER,
    mileage_max     INTEGER,
    cote            REAL NOT NULL,
    currency        TEXT DEFAULT 'EUR',
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 4. STATS MARCHÉ CALCULÉES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id      INTEGER NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
    country_code    TEXT DEFAULT 'ALL',
    mileage_min     INTEGER,
    mileage_max     INTEGER,
    sample_size     INTEGER NOT NULL,
    price_min       REAL,
    price_p10       REAL,
    price_p25       REAL,
    price_median    REAL,
    price_p75       REAL,
    price_p90       REAL,
    price_max       REAL,
    price_mean      REAL,
    price_stddev    REAL,
    computed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(vehicle_id, country_code, mileage_min, mileage_max)
);

-- ------------------------------------------------------------
-- 5. PORTEFEUILLE PERSONNEL
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id       INTEGER NOT NULL REFERENCES vehicles(id),
    purchase_price   REAL NOT NULL,
    purchase_date    TEXT NOT NULL,
    purchase_mileage INTEGER,
    sale_price       REAL,
    sale_date        TEXT,
    status           TEXT DEFAULT 'en_stock',
    notes            TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
--  INDEX
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_vehicles_make_model  ON vehicles(make, model);
CREATE INDEX IF NOT EXISTS idx_vehicles_year        ON vehicles(production_year);
CREATE INDEX IF NOT EXISTS idx_vehicles_fuel        ON vehicles(fuel_type);
CREATE INDEX IF NOT EXISTS idx_listings_vehicle_id  ON listings(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_listings_source      ON listings(source);
CREATE INDEX IF NOT EXISTS idx_listings_price       ON listings(price);
CREATE INDEX IF NOT EXISTS idx_listings_mileage     ON listings(mileage_km);
CREATE INDEX IF NOT EXISTS idx_listings_country     ON listings(country_code);
CREATE INDEX IF NOT EXISTS idx_listings_accident    ON listings(had_accident);
CREATE INDEX IF NOT EXISTS idx_market_stats_vehicle ON market_stats(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_market_stats_country ON market_stats(country_code);
CREATE INDEX IF NOT EXISTS idx_portfolio_status     ON portfolio(status);

-- ============================================================
--  VUES
-- ============================================================
CREATE VIEW IF NOT EXISTS v_vehicle_market AS
SELECT
    v.id, v.make, v.model, v.model_version, v.production_year,
    v.fuel_type, v.transmission, v.power_hp,
    ms.country_code, ms.mileage_min, ms.mileage_max,
    ms.sample_size, ms.price_p25, ms.price_median,
    ms.price_p75, ms.price_mean, ms.computed_at
FROM vehicles v
LEFT JOIN market_stats ms ON ms.vehicle_id = v.id
ORDER BY v.make, v.model, v.production_year;

CREATE VIEW IF NOT EXISTS v_portfolio_valuation AS
SELECT
    p.id AS portfolio_id, v.make, v.model, v.model_version,
    v.production_year, p.purchase_price, p.purchase_date,
    p.purchase_mileage, p.status,
    ms.price_median AS current_market_median,
    ms.price_p25    AS current_market_p25,
    ROUND(ms.price_median - p.purchase_price, 0) AS potential_gain,
    p.notes
FROM portfolio p
JOIN vehicles v ON v.id = p.vehicle_id
LEFT JOIN market_stats ms ON ms.vehicle_id = p.vehicle_id
    AND ms.country_code = 'ALL'
    AND p.purchase_mileage BETWEEN COALESCE(ms.mileage_min, 0)
                               AND COALESCE(ms.mileage_max, 999999)
WHERE p.status = 'en_stock';
