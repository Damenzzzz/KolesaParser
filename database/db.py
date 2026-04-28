import csv
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from scraper.config import DB_PATH, EXPORTS_DIR


CAR_COLUMNS = [
    "listing_id",
    "url",
    "source",
    "title",
    "brand",
    "model",
    "generation",
    "year",
    "price",
    "currency",
    "city",
    "region",
    "mileage_km",
    "body_type",
    "engine_volume_l",
    "fuel_type",
    "transmission",
    "drive_type",
    "steering_wheel",
    "color",
    "condition",
    "customs_cleared",
    "description",
    "generated_description",
    "seller_type",
    "published_at",
    "scraped_at",
    "photo_count",
    "is_active",
    "raw_characteristics",
    "created_at",
    "updated_at",
]


ML_COLUMNS = [
    "brand",
    "model",
    "year",
    "price",
    "city",
    "mileage_km",
    "body_type",
    "engine_volume_l",
    "fuel_type",
    "transmission",
    "drive_type",
    "steering_wheel",
    "color",
    "condition",
    "generation",
]


IMPORTANT_COLUMNS = [
    "brand",
    "model",
    "year",
    "price",
    "city",
    "mileage_km",
    "engine_volume_l",
    "fuel_type",
    "transmission",
]


class CarDatabase:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.init_db()

    def close(self) -> None:
        self.conn.close()

    def init_db(self) -> None:
        """Create tables if needed. Existing data is never deleted here."""
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id TEXT UNIQUE,
                url TEXT UNIQUE,
                source TEXT,

                title TEXT,
                brand TEXT,
                model TEXT,
                generation TEXT,
                year INTEGER,

                price INTEGER,
                currency TEXT,

                city TEXT,
                region TEXT,

                mileage_km INTEGER,
                body_type TEXT,
                engine_volume_l REAL,
                fuel_type TEXT,
                transmission TEXT,
                drive_type TEXT,
                steering_wheel TEXT,
                color TEXT,
                condition TEXT,
                customs_cleared TEXT,

                description TEXT,
                generated_description TEXT,
                seller_type TEXT,

                published_at TEXT,
                scraped_at TEXT,

                photo_count INTEGER,
                is_active INTEGER DEFAULT 1,

                raw_characteristics TEXT,

                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_brand ON cars(brand)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_brand_model ON cars(brand, model)")
        self.conn.commit()

    def insert_car(self, car: dict[str, Any]) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        row = {column: car.get(column) for column in CAR_COLUMNS}
        row["is_active"] = 1 if row.get("is_active") is None else row["is_active"]
        row["created_at"] = row.get("created_at") or now
        row["updated_at"] = now
        listing_id = row.get("listing_id")
        url = row.get("url")

        if self.car_exists(listing_id, url):
            return False

        placeholders = ", ".join("?" for _ in CAR_COLUMNS)
        columns_sql = ", ".join(CAR_COLUMNS)
        values = [row[column] for column in CAR_COLUMNS]

        cursor = self.conn.execute(
            f"INSERT OR IGNORE INTO cars ({columns_sql}) VALUES ({placeholders})",
            values,
        )
        if cursor.rowcount != 1:
            self.conn.commit()
            return False

        self.conn.commit()
        logging.getLogger("car_database").info("committed listing to SQLite: %s", listing_id or url)
        return True

    def car_exists(self, listing_id: Optional[str], url: Optional[str]) -> bool:
        row = self.conn.execute(
            """
            SELECT id FROM cars
            WHERE (? IS NOT NULL AND listing_id = ?)
            OR (? IS NOT NULL AND url = ?)
            LIMIT 1
            """,
            (listing_id, listing_id, url, url),
        ).fetchone()
        return row is not None

    def count_all_cars(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS count FROM cars").fetchone()
        return int(row["count"])

    def count_total(self) -> int:
        return self.count_all_cars()

    def count_by_brand(self, brand: Optional[str]) -> int:
        if not brand:
            return 0
        return len(self._matching_brand_rows(brand))

    def count_by_brand_model(self, brand: Optional[str], model: Optional[str]) -> int:
        if not brand or not model:
            return 0
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS count FROM cars
            WHERE lower(brand) = lower(?) AND lower(model) = lower(?)
            """,
            (brand, model),
        ).fetchone()
        return int(row["count"])

    def get_target_current_count(self, brand: str, model: str) -> int:
        target = self._target_for(brand, model)
        return len(self._matching_target_rows(target))

    def get_top_brands(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT COALESCE(brand, 'Unknown') AS brand, COUNT(*) AS count
            FROM cars
            GROUP BY COALESCE(brand, 'Unknown')
            ORDER BY count DESC, brand ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def get_top_models(self, limit: int = 30) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT
                COALESCE(brand, 'Unknown') AS brand,
                COALESCE(model, 'Unknown') AS model,
                COUNT(*) AS count
            FROM cars
            GROUP BY COALESCE(brand, 'Unknown'), COALESCE(model, 'Unknown')
            ORDER BY count DESC, brand ASC, model ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def top_brands(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.get_top_brands(limit)

    def top_models(self, limit: int = 30) -> list[sqlite3.Row]:
        return self.get_top_models(limit)

    def missing_value_stats(self, columns: list[str] = IMPORTANT_COLUMNS) -> list[dict[str, int]]:
        safe_columns = [column for column in columns if column in CAR_COLUMNS]
        total = self.count_all_cars()
        rows = []
        for column in safe_columns:
            missing = self.conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM cars
                WHERE {column} IS NULL OR trim(CAST({column} AS TEXT)) = ''
                """
            ).fetchone()
            rows.append({"column": column, "missing": int(missing["count"]), "total": total})
        return rows

    def model_report_dataframe(self) -> "pd.DataFrame":
        import pandas as pd

        return pd.read_sql_query(
            """
            SELECT
                brand,
                model,
                COUNT(*) AS count,
                AVG(price) AS avg_price,
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                MIN(year) AS min_year,
                MAX(year) AS max_year,
                AVG(mileage_km) AS avg_mileage
            FROM cars
            GROUP BY brand, model
            ORDER BY count DESC, brand ASC, model ASC
            """,
            self.conn,
        )

    def export_model_report(self) -> Path:
        path = EXPORTS_DIR / "model_report.csv"
        rows = self.conn.execute(
            """
            SELECT
                brand,
                model,
                COUNT(*) AS count,
                AVG(price) AS avg_price,
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                MIN(year) AS min_year,
                MAX(year) AS max_year,
                AVG(mileage_km) AS avg_mileage
            FROM cars
            GROUP BY brand, model
            ORDER BY count DESC, brand ASC, model ASC
            """
        ).fetchall()
        fieldnames = [
            "brand",
            "model",
            "count",
            "avg_price",
            "min_price",
            "max_price",
            "min_year",
            "max_year",
            "avg_mileage",
        ]
        self._write_csv(path, fieldnames, [dict(row) for row in rows])
        return path

    def target_model_report_rows(self) -> list[dict[str, Any]]:
        from scraper.config import TARGET_MODELS

        rows = []
        for target in TARGET_MODELS:
            matching_rows = self._matching_target_rows(target)
            prices = [row["price"] for row in matching_rows if row["price"] is not None]
            years = [row["year"] for row in matching_rows if row["year"] is not None]
            mileages = [row["mileage_km"] for row in matching_rows if row["mileage_km"] is not None]
            target_limit = int(target["limit"])
            current_count = len(matching_rows)
            rows.append(
                {
                    "brand": target["brand"],
                    "model": target["model"],
                    "target_limit": target_limit,
                    "current_count": current_count,
                    "remaining": max(0, target_limit - current_count),
                    "avg_price": self._average(prices),
                    "min_price": min(prices) if prices else None,
                    "max_price": max(prices) if prices else None,
                    "min_year": min(years) if years else None,
                    "max_year": max(years) if years else None,
                    "avg_mileage": self._average(mileages),
                }
            )
        return rows

    def export_target_model_report(self) -> Path:
        path = EXPORTS_DIR / "target_model_report.csv"
        rows = self.target_model_report_rows()
        fieldnames = [
            "brand",
            "model",
            "target_limit",
            "current_count",
            "remaining",
            "avg_price",
            "min_price",
            "max_price",
            "min_year",
            "max_year",
            "avg_mileage",
        ]
        with path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def completed_target_count(self) -> tuple[int, int]:
        rows = self.target_model_report_rows()
        completed = sum(1 for row in rows if row["remaining"] == 0)
        return completed, len(rows)

    def get_brand_progress(self) -> list[dict[str, Any]]:
        return self.brand_report_rows()

    def brand_report_rows(self) -> list[dict[str, Any]]:
        from scraper.config import BRAND_TARGETS

        rows = []
        for target in BRAND_TARGETS:
            matching_rows = self._matching_brand_rows(target["brand"])
            prices = [row["price"] for row in matching_rows if row["price"] is not None]
            years = [row["year"] for row in matching_rows if row["year"] is not None]
            mileages = [row["mileage_km"] for row in matching_rows if row["mileage_km"] is not None]
            models = {
                str(row["model"]).strip()
                for row in matching_rows
                if row.get("model") and str(row["model"]).strip()
            }
            target_limit = int(target["limit"])
            current_count = len(matching_rows)
            rows.append(
                {
                    "brand": target["brand"],
                    "target_limit": target_limit,
                    "current_count": current_count,
                    "remaining": max(0, target_limit - current_count),
                    "avg_price": self._average(prices),
                    "min_price": min(prices) if prices else None,
                    "max_price": max(prices) if prices else None,
                    "min_year": min(years) if years else None,
                    "max_year": max(years) if years else None,
                    "avg_mileage": self._average(mileages),
                    "unique_models": len(models),
                }
            )
        return rows

    def export_brand_report(self) -> Path:
        path = EXPORTS_DIR / "brand_report.csv"
        rows = self.brand_report_rows()
        fieldnames = [
            "brand",
            "target_limit",
            "current_count",
            "remaining",
            "avg_price",
            "min_price",
            "max_price",
            "min_year",
            "max_year",
            "avg_mileage",
            "unique_models",
        ]
        self._write_csv(path, fieldnames, rows)
        return path

    def export_brand_checkpoint_csv(self, brand: str, path: Optional[Path] = None) -> Path:
        path = path or self._brand_checkpoint_path(brand)
        rows = self._brand_car_rows(brand)
        tmp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._write_csv(tmp_path, CAR_COLUMNS, rows)
        tmp_path.replace(path)
        return path

    def completed_brand_count(self) -> tuple[int, int]:
        rows = self.brand_report_rows()
        completed = sum(1 for row in rows if row["remaining"] == 0)
        return completed, len(rows)

    def export_full_csv(self) -> Path:
        path = EXPORTS_DIR / "cars_full.csv"
        cursor = self.conn.execute("SELECT * FROM cars ORDER BY id ASC")
        fieldnames = [description[0] for description in cursor.description]
        self._write_csv(path, fieldnames, [dict(row) for row in cursor.fetchall()])
        return path

    def export_ml_csv(self) -> Path:
        path = EXPORTS_DIR / "cars_ml.csv"
        columns_sql = ", ".join(ML_COLUMNS)
        cursor = self.conn.execute(f"SELECT {columns_sql} FROM cars ORDER BY id ASC")
        self._write_csv(path, ML_COLUMNS, [dict(row) for row in cursor.fetchall()])
        return path

    def export_full(self) -> Path:
        return self.export_full_csv()

    def export_ml(self) -> Path:
        return self.export_ml_csv()

    def _brand_checkpoint_path(self, brand: str) -> Path:
        from scraper.brand_targets import normalize_brand_name

        normalized = normalize_brand_name(brand).lower()
        slug = re.sub(r"[^0-9a-z]+", "_", normalized).strip("_") or "brand"
        return EXPORTS_DIR / "checkpoints" / f"{slug}_checkpoint.csv"

    def _target_for(self, brand: str, model: str) -> dict[str, Any]:
        from scraper.target_models import find_target

        target = find_target(brand, model)
        if target:
            return target
        return {"brand": brand, "model": model, "limit": 0, "aliases": [model]}

    def _matching_target_rows(self, target: dict[str, Any]) -> list[dict[str, Any]]:
        from scraper.target_models import matches_target_model

        candidates = self.conn.execute(
            """
            SELECT brand, model, generation, title, price, year, mileage_km
            FROM cars
            WHERE lower(brand) = lower(?)
            """,
            (target["brand"],),
        ).fetchall()
        return [dict(row) for row in candidates if matches_target_model(dict(row), target)]

    def _matching_brand_rows(self, brand: str) -> list[dict[str, Any]]:
        from scraper.brand_targets import normalize_brand_name

        target_brand = normalize_brand_name(brand)
        if not target_brand:
            return []

        candidates = self.conn.execute(
            """
            SELECT brand, model, price, year, mileage_km
            FROM cars
            WHERE brand IS NOT NULL AND trim(CAST(brand AS TEXT)) != ''
            """
        ).fetchall()
        return [
            dict(row)
            for row in candidates
            if normalize_brand_name(row["brand"]) == target_brand
        ]

    def _brand_car_rows(self, brand: str) -> list[dict[str, Any]]:
        from scraper.brand_targets import normalize_brand_name

        target_brand = normalize_brand_name(brand)
        if not target_brand:
            return []

        columns_sql = ", ".join(CAR_COLUMNS)
        cursor = self.conn.execute(f"SELECT {columns_sql} FROM cars ORDER BY id ASC")
        return [
            dict(row)
            for row in cursor.fetchall()
            if normalize_brand_name(row["brand"]) == target_brand
        ]

    def _average(self, values: list[int | float]) -> Optional[float]:
        if not values:
            return None
        return round(sum(values) / len(values), 2)

    def _write_csv(self, path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
