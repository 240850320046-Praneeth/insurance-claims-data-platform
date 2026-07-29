import csv
import os
import random
from datetime import date, timedelta

from config import (
    HOSPITAL_COUNT,
    RANDOM_SEED,
    OUTPUT_DIR,
    HOSPITAL_FILE
)

# Make random generation reproducible
random.seed(RANDOM_SEED)

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)


# Possible source values

cities = [
    "Hyderabad",
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Chennai",
    "Pune",
    "Kolkata"
]

hospital_types = [
    "GENERAL",
    "MULTI_SPECIALITY",
    "SPECIALITY",
    "CLINIC"
]

network_statuses = [
    "NETWORK",
    "NON_NETWORK"
]


# Helper function for random dates

def random_date(start_date, end_date):
    days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, days))


# Output file path

output_path = os.path.join(
    OUTPUT_DIR,
    HOSPITAL_FILE
)


# Create CSV file

with open(output_path, "w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    # Header
    writer.writerow([
        "hospital_id",
        "hospital_name",
        "city",
        "hospital_type",
        "network_status",
        "bed_capacity",
        "created_at"
    ])


    # Generate hospitals one at a time

    for i in range(1, HOSPITAL_COUNT + 1):

        hospital_id = f"H{i:06d}"

        hospital_name = f"HealthCare Hospital {i:06d}"

        city = random.choice(cities)

        hospital_type = random.choice(hospital_types)

        network_status = random.choice(network_statuses)

        bed_capacity = random.randint(20, 1000)

        created_at = random_date(
            date(2000, 1, 1),
            date(2026, 7, 1)
        )


        # Write hospital record

        writer.writerow([
            hospital_id,
            hospital_name,
            city,
            hospital_type,
            network_status,
            bed_capacity,
            created_at
        ])


print(f"Generated {HOSPITAL_COUNT:,} hospitals")
print(f"Output: {output_path}")