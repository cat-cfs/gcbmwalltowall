from pathlib import Path

class Rollback:

    def __init__(self, age_distribution_path, inventory_year):
        self.age_distribution_path = Path(age_distribution_path)
        self.inventory_year = inventory_year
