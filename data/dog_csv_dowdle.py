import csv
import itertools
from datetime import datetime

# Define possible values
dog_names = ["Nimeria", "Duke", "Rex", "Louie", "Bubba", "Daisy", "Harley", "Bailey"]
breeds = ["Golden Retriever", "Labrador", "German Shepherd", "Poodle", "Bulldog", "Beagle"]
toys = ["Tennis ball", "Rope", "Squeaky toy", "Frisbee", "Stick", "Stuffie"]

# Output CSV filename
output_file = "dog_messages.csv"

# Generate all combinations
combinations = itertools.product(dog_names, breeds, toys)

# Write to CSV
with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=["timestamp", "dog_name", "breed", "toy"])
    writer.writeheader()

    for combo in combinations:
        dog_names, breeds, toys = combo
        row = {
            "timestamp": datetime.utcnow().isoformat(),
            "dog_name": dog_names,
            "breed": breeds,
            "toy": toys,
        }
        writer.writerow(row)

print(f"CSV file '{output_file}' created with all message combinations.")
