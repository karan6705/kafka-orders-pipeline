"""
generate_data.py

Generates a larger synthetic orders CSV using Faker.
Usage:
    python scripts/generate_data.py --rows 500 --output data/input/orders_sample.csv
"""

import argparse
import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()

CATEGORIES = {
    "Electronics": ["Wireless Headphones", "USB-C Hub", "Mechanical Keyboard", "Bluetooth Speaker", "Monitor Light Bar", "Wireless Mouse", "Noise-Cancelling Earbuds"],
    "Footwear":    ["Running Shoes", "Trail Boots", "Casual Sneakers", "Sandals"],
    "Sports":      ["Yoga Mat", "Resistance Bands", "Foam Roller", "Jump Rope", "Water Bottle"],
    "Kitchen":     ["Coffee Maker", "Ceramic Mug", "Insulated Tumbler", "Blender"],
    "Home":        ["Desk Lamp", "Throw Blanket", "Wall Clock", "Scented Candle", "Air Purifier"],
    "Office":      ["Standing Desk Mat", "Laptop Stand", "Sticky Notes Pack", "Notebook Set"],
}

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "NL", "SE", "JP", "KR", "SG", "BR"]
STATUSES  = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
STATUS_WEIGHTS = [0.20, 0.30, 0.25, 0.20, 0.05]

PRICE_RANGE = {
    "Electronics": (25.00, 199.00),
    "Footwear":    (40.00, 120.00),
    "Sports":      (10.00, 80.00),
    "Kitchen":     (8.00, 150.00),
    "Home":        (12.00, 160.00),
    "Office":      (5.00, 60.00),
}


def generate_row(index: int, base_time: datetime) -> dict:
    category = random.choice(list(CATEGORIES.keys()))
    product_name = random.choice(CATEGORIES[category])
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(*PRICE_RANGE[category]), 2)
    total_price = round(unit_price * quantity, 2)
    created_at = base_time + timedelta(seconds=index * random.randint(90, 300))

    return {
        "order_id":    f"ORD-{index:04d}",
        "customer_id": f"CUST-{random.randint(1, 9999):04d}",
        "product_id":  f"PROD-{random.randint(1, 500):03d}",
        "product_name": product_name,
        "category":    category,
        "quantity":    quantity,
        "unit_price":  unit_price,
        "total_price": total_price,
        "status":      random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "country":     random.choice(COUNTRIES),
        "created_at":  created_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic orders CSV")
    parser.add_argument("--rows",   type=int, default=100,                          help="Number of rows to generate (default: 100)")
    parser.add_argument("--output", type=str, default="data/input/orders_sample.csv", help="Output CSV file path")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    base_time = datetime(2024, 1, 15, 8, 0, 0)
    fieldnames = ["order_id", "customer_id", "product_id", "product_name",
                  "category", "quantity", "unit_price", "total_price",
                  "status", "country", "created_at"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, args.rows + 1):
            writer.writerow(generate_row(i, base_time))

    print(f"Generated {args.rows} rows → {output_path}")


if __name__ == "__main__":
    main()
