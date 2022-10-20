from pathlib import Path
from spatial_inventory_rollback.application.app import run as spatial_rollback

class Rollback:

    def __init__(
        self, age_distribution, inventory_year, rollback_year=1990,
        prioritize_disturbances=False, single_draw=False,
        establishment_disturbance_type="Wildfire"
    ):
        self.age_distribution = Path(age_distribution)
        self.inventory_year = inventory_year
        self.rollback_year = rollback_year
        self.prioritize_disturbances = prioritize_disturbances
        self.single_draw = single_draw
        self.establishment_disturbance_type = establishment_disturbance_type
    
    def run(self, tiled_layers_path, input_db_path):
        tiled_layers_path = Path(tiled_layers_path).absolute()
        input_db_path = Path(input_db_path).absolute()

        inventory_year = self.inventory_year
        if isinstance(inventory_year, str):
            inventory_year = str(next(tiled_layers_path.glob(f"{inventory_year}_moja.tif*")))

        spatial_rollback(
            input_layers=str(tiled_layers_path),
            input_db=str(input_db_path),
            inventory_year=inventory_year,
            rollback_year=self.rollback_year,
            rollback_age_distribution=str(self.age_distribution),
            prioritize_disturbances=self.prioritize_disturbances,
            establishment_disturbance_type=self.establishment_disturbance_type,
            single_draw=self.single_draw,
            output_path=str(tiled_layers_path.joinpath("..", "rollback")),
            stand_replacing_lookup=None)
