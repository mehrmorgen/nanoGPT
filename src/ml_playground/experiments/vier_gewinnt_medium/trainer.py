"""
This script runs the trainer for the vier_gewinnt experiment.
"""

import sys
from pathlib import Path

# This is a bit of a hack to make the script runnable from the command line.
# It adds the project root to the python path.
# Assumes the script is in src/ml_playground/experiments/vier_gewinnt/
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root))


def main():
    """
    Runs the training for the vier_gewinnt experiment.
    """
    from ml_playground.configuration.loading import load_full_experiment_config
    from ml_playground.training.loop.runner import Trainer as CoreTrainer
    from ml_playground.cli import _global_device_setup

    experiment_name = "vier_gewinnt_medium"
    # Assume config.toml is in the same directory as this script
    config_path = Path(__file__).parent / "config.toml"

    # The project home is 4 levels up from this script
    project_home = Path(__file__).resolve().parents[4]

    print(
        f"Loading configuration for experiment '{experiment_name}' from '{config_path}'"
    )
    exp_config = load_full_experiment_config(
        config_path=config_path,
        project_home=project_home,
        experiment_name=experiment_name,
    )

    train_cfg = exp_config.train
    shared_cfg = exp_config.shared

    if not train_cfg.runtime:
        print("Runtime configuration is missing for training.")
        sys.exit(1)

    _global_device_setup(
        train_cfg.runtime.device,
        train_cfg.runtime.dtype,
        train_cfg.runtime.seed,
    )

    print(f"Running trainer for experiment: {experiment_name}")

    trainer = CoreTrainer(train_cfg, shared_cfg)
    trainer.run()

    print(f"Trainer for {experiment_name} finished.")


if __name__ == "__main__":
    main()
