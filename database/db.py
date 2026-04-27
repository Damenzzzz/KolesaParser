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

        placeholders = ", ".join("?" for _ in CAR_COLUMNS)
        columns_sql = ", ".join(CAR_COLUMNS)
        values = [row[column] for column in CAR_COLUMNS]

        cursor = self.conn.execute(
            f"INSERT OR IGNORE INTO cars ({columns_sql}) VALUES ({placeholders})",
            values,
        )
        self.conn.commit()
        return cursor.rowcount == 1

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
        row = self.conn.execute(
            "SELECT COUNT(*) AS count FROM cars WHERE lower(brand) = lower(?)",
            (brand,),
        ).fetchone()
        return int(row["count"])

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
        self.model_report_dataframe().to_csv(path, index=False)
        return path

    def export_full_csv(self) -> Path:
        import pandas as pd

        path = EXPORTS_DIR / "cars_full.csv"
        pd.read_sql_query("SELECT * FROM cars ORDER BY id ASC", self.conn).to_csv(path, index=False)
        return path

    def export_ml_csv(self) -> Path:
        import pandas as pd

        path = EXPORTS_DIR / "cars_ml.csv"
        columns_sql = ", ".join(ML_COLUMNS)
        pd.read_sql_query(f"SELECT {columns_sql} FROM cars ORDER BY id ASC", self.conn).to_csv(
            path,
            index=False,
        )
        return path

    def export_full(self) -> Path:
        return self.export_full_csv()

    def export_ml(self) -> Path:
        return self.export_ml_csv()
