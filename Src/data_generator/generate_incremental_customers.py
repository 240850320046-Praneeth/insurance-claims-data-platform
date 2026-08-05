import csv
import random
from datetime import datetime, timedelta

OUTPUT_FILE = "data/source/incremental_customers.csv"

START_ID = 500001
END_ID = 501000

cities = [
    "Hyderabad",
    "Bangalore",
    "Chennai",
    "Mumbai",
    "Delhi"
]

states = [
    "Telangana",
    "Karnataka",
    "Tamil Nadu",
    "Maharashtra",
    "Delhi"
]

genders = [
    "Male",
    "Female"
]

first_names = [
    "Rahul",
    "Anil",
    "Kiran",
    "Sneha",
    "Priya",
    "Amit",
    "Neha",
    "Ravi"
]

last_names = [
    "Sharma",
    "Reddy",
    "Patel",
    "Verma",
    "Singh",
    "Kumar"
]


def random_datetime(start, end):
    delta = end - start
    return start + timedelta(
        seconds=random.randint(0, int(delta.total_seconds()))
    )


with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    writer.writerow([
        "customer_id",
        "first_name",
        "last_name",
        "gender",
        "date_of_birth",
        "city",
        "state",
        "country",
        "phone_number",
        "email",
        "created_date",
        "modified_date"
    ])

    for customer_id in range(START_ID, END_ID + 1):

        first = random.choice(first_names)
        last = random.choice(last_names)

        created = random_datetime(
            datetime(2026, 8, 6),
            datetime(2026, 8, 6, 23, 59)
        )

        writer.writerow([
            customer_id,
            first,
            last,
            random.choice(genders),
            "1995-01-01",
            random.choice(cities),
            random.choice(states),
            "India",
            f"9{random.randint(100000000,999999999)}",
            f"{first.lower()}.{last.lower()}{customer_id}@gmail.com",
            created.strftime("%Y-%m-%d %H:%M:%S"),
            created.strftime("%Y-%m-%d %H:%M:%S")
        ])

print("Generated incremental_customers.csv")