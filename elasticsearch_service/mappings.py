CARS_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "listing_id": {"type": "keyword"},
            "url": {"type": "keyword"},
            "brand": {"type": "keyword"},
            "model": {"type": "keyword"},
            "city": {"type": "keyword"},
            "year": {"type": "integer"},
            "price": {"type": "integer"},
            "mileage_km": {"type": "integer"},
            "body_type": {"type": "keyword"},
            "engine_volume_l": {"type": "float"},
            "fuel_type": {"type": "keyword"},
            "transmission": {"type": "keyword"},
            "drive_type": {"type": "keyword"},
            "steering_wheel": {"type": "keyword"},
            "color": {"type": "keyword"},
            "condition": {"type": "keyword"},
            "description": {"type": "text"},
            "generated_description": {"type": "text"},
            "scraped_at": {"type": "date", "ignore_malformed": True},
            "brand_normalized": {"type": "keyword"},
            "model_normalized": {"type": "keyword"},
            "city_normalized": {"type": "keyword"},
            "transmission_normalized": {"type": "keyword"},
            "fuel_type_normalized": {"type": "keyword"},
            "body_type_normalized": {"type": "keyword"},
        }
    }
}

