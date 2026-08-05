# 1. IMPORTS

import csv
import os
import random
from datetime import datetime, timedelta, date

from config import (
    CUSTOMER_COUNT,
    RANDOM_SEED,
    OUTPUT_DIR,
    CUSTOMER_FILE
)


# 2. INITIAL SETUP

random.seed(RANDOM_SEED)

os.makedirs(OUTPUT_DIR, exist_ok=True)


# 3. SOURCE VALUES

first_names = [
    "Rahul", "Priya", "Arjun", "Sneha", "Vikram",
    "Ananya", "Kiran", "Neha", "Rohit", "Meera"
]

last_names = [
    "Sharma", "Reddy", "Kumar", "Patel", "Singh",
    "Rao", "Gupta", "Nair", "Verma", "Iyer"
]

cities = [
    "Hyderabad",
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Chennai",
    "Pune",
    "Kolkata"
]


# 4. HELPER FUNCTIONS

def random_date(start_date, end_date):
    days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, days))


def random_datetime(start_datetime, end_datetime):
    seconds = int((end_datetime - start_datetime).total_seconds())
    return start_datetime + timedelta(
        seconds=random.randint(0, seconds)
    )


# 5. OUTPUT FILE LOCATION

output_path = os.path.join(
    OUTPUT_DIR,
    CUSTOMER_FILE
)


# 6. CREATE CSV AND GENERATE CUSTOMERS

with open(output_path, "w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    # CSV Header
    writer.writerow([
        "customer_id",
        "first_name",
        "last_name",
        "date_of_birth",
        "gender",
        "email",
        "phone",
        "city",
        "created_at",
        "modified_at"
    ])

    # Generate customers one at a time
    for i in range(1, CUSTOMER_COUNT + 1):

        customer_id = f"C{i:07d}"

        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        dob = random_date(
            date(1950, 1, 1),
            date(2005, 12, 31)
        )

        gender = random.choice(["M", "F"])

        email = (
            f"{first_name.lower()}.{last_name.lower()}"
            f"{i}@example.com"
        )

        phone = f"9{random.randint(100000000, 999999999)}"

        city = random.choice(cities)

        # Generate Created Timestamp
        created_at = random_datetime(
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2026, 7, 1, 23, 59, 59)
        )

        # Modified Timestamp is same initially
        modified_at = created_at

        # 7. INTENTIONAL DATA QUALITY PROBLEMS

        error_roll = random.random()

        if error_roll < 0.01:
            email = ""

        elif error_roll < 0.015:
            city = ""

        elif error_roll < 0.017:
            dob = "2035-01-01"

        elif error_roll < 0.019:
            gender = "UNKNOWN"

        # 8. WRITE CUSTOMER TO CSV

        writer.writerow([
            customer_id,
            first_name,
            last_name,
            dob,
            gender,
            email,
            phone,
            city,
            created_at.strftime("%Y-%m-%d %H:%M:%S"),
            modified_at.strftime("%Y-%m-%d %H:%M:%S")
        ])


# 9. FINISHED

print(f"Generated {CUSTOMER_COUNT:,} customers")
print(f"Output: {output_path}")