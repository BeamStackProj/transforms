import os
import subprocess
from typing import List
from utils import LogHandler

logger = LogHandler.logger

class VirtualEnvManager:
    """Manage the virtual environment and package installations."""

    def __init__(self, venv_dir: str):
        self.venv_dir = venv_dir

    def create_venv(self) -> None:
        logger.info(f"Creating virtual environment in {self.venv_dir}")
        subprocess.run(['python', '-m', 'venv', self.venv_dir], check=True)

    def install_packages(self, packages: List[str]) -> None:
        logger.info(f"Installing packages: {', '.join(packages)}")
        
        # Get the list of currently installed packages
        installed_packages = subprocess.check_output(
            [os.path.join(self.venv_dir, 'bin', 'pip'), 'list', '--format=freeze']
        ).decode('utf-8').splitlines()
        
        installed_packages_set = {pkg.split('==')[0] for pkg in installed_packages}

        for package in packages:
            if package not in installed_packages_set:
                logger.info(f"Installing package: {package}")
                subprocess.run([os.path.join(self.venv_dir, 'bin', 'pip'), 'install', package], check=True)
            else:
                logger.info(f"Package '{package}' is already installed; skipping installation.")