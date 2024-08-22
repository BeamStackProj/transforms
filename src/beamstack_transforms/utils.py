import sys
import subprocess
import importlib
from dataclasses import dataclass, field


@dataclass
class ImportParams:
    module: str
    objects: list = field(default_factory=list)


def install_package(package: list[str] = None):
    for p in package:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", str(p)])


def import_package(modules: list[ImportParams]) -> list:
    imported = []
    for module in modules:
        if not module.objects:
            imported.append(importlib.import_module(module.module))
        else:
            for object in module.objects:
                imported.append(
                    getattr(importlib.import_module(module.module), object))

    return imported
