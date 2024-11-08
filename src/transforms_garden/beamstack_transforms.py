import os
import json
import hashlib
import base64
import subprocess
import sys
import yaml
import apache_beam as beam
from apache_beam.transforms import external
from typing import Any, Iterable, Mapping, Optional, Callable
from apache_beam.yaml.yaml_provider import ExternalProvider
from apache_beam.utils import subprocess_server
import logging
import importlib.util

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ExternalProvider.register_provider_type('BeamstackTransform')
def BeamstackTransform(urns, path):
    with open(path, 'r') as f:
        transform_yaml = yaml.safe_load(f)
    
    config = {
        'urns': urns,
        'yaml_path': path,
        'dependencies': transform_yaml.get('dependencies', []),
        'runner': transform_yaml.get('runner', [])
    }
    
    return BeamstackTransformProvider(urns, config)


class BeamstackTransformProvider(ExternalProvider):
    def __init__(self, urns, config):
        super().__init__(urns, BeamstackExpansionService(config))
        self.config = config
        self.transforms = config.get('urns', {})

        logger.info(f"Transforms: {self.transforms}")

    def available(self) -> bool:
        return True

    def cache_artifacts(self) -> Optional[Iterable[str]]:
        return [self._service._venv()]

    def create_transform(self, 
                         typ: str, 
                         args: Mapping[str, Any], 
                         yaml_create_transform: Callable[[Mapping[str, Any], Iterable[beam.PCollection]], beam.PTransform]) -> Optional[beam.PTransform]:
        """Create a PTransform based on decoded source code and configurations."""
        if callable(self._service):
            self._service = self._service()

        logger.info(f"Creating transform of type: {typ} with args: {args}")

        if typ in self.transforms:
            transform_class = self._load_transform_class(typ)
            if callable(transform_class):
                config_args = args.get('config', {})
                try:
                    return transform_class(**config_args)
                except TypeError as e:
                    logger.error(f"Error initializing transform '{typ}': {e}")
                    raise
            else:
                logger.error(f"{typ} is not a callable transform class.")
        else:
            logger.error(f"Transform type '{typ}' is not recognized in BeamstackTransform.")
        return None

    
    def _module_class_map(self) -> dict:
        """Transform module and class dictionary map"""
        self.yaml_path = self.config.get('yaml_path')
        
        with open(self.yaml_path, 'r') as file:
            data = yaml.safe_load(file)
            self.transforms = data['transforms']

            transform_map = {}
            for item in self.transforms:
                for _, value in item.items():
                    file, transform_class = value.split(':')
                    transform_map[transform_class] = file

        return transform_map

    def _load_transform_class(self, transform_name):
        """Dynamically loads and returns a transform class by name."""
        transform_map = self._module_class_map()

        try:
            logger.info(f"Loading transform class for: {transform_name}")
            spec = importlib.util.spec_from_file_location(transform_map[transform_name], os.path.join(self._service._venv_path(), transform_map[transform_name]))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            transform_class = getattr(module, transform_name)
            logger.info(f"Loaded transform class: {transform_class}")
            return transform_class
        except Exception as e:
            logger.error(f"Failed to load transform {transform_name}: {e}")
            raise e


    @classmethod
    def provider_from_spec(cls, spec):
        urns = spec['transforms']
        config = spec['config']
        return cls(urns, config)


class BeamstackExpansionService:
    VENV_CACHE = os.path.expanduser("~/.apache_beam/cache/beamstack_venvs")

    def __init__(self, config):
        self.config = config
        self.runner = config.get('runner')
        self.yaml_path = config.get('yaml_path')
        self.base_python = sys.executable
        self._packages = config.get('dependencies', [])
        self._service = None

        self._load_yaml()

    def _load_yaml(self):
        """Loads and decodes the transforms.yaml file."""
        with open(self.yaml_path, 'r') as file:
            data = yaml.safe_load(file)
            self._packages = data.get('dependencies', [])
            self.transforms = data['transforms']
            self.source_code = data['source_code']
            self.encoding = data['encoding']

            for src_name, encoded_code in self.source_code.items():
                decoded_code = base64.b64decode(encoded_code).decode('utf-8')
                self._write_source_file(src_name, decoded_code)
                self._source_module = src_name

    def _write_source_file(self, src_name, code):
        """Writes decoded code to file for each source."""
        file_path = os.path.join(self._venv_path(), src_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.write(code)

    def _venv_path(self):
        """Returns the path for the virtual environment directory based on the packages and runner."""
        key = json.dumps({'binary': self.base_python, 'packages': sorted(self._packages), 'runner': self.runner})
        venv_hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
        return os.path.join(self.VENV_CACHE, venv_hash)

    def _venv(self):
        """Creates and returns the virtual environment path if not exists."""
        venv = self._venv_path()
        print(f"Virtual Environment: {venv}")
        if not os.path.exists(venv):
            subprocess.run([self.base_python, '-m', 'venv', venv], check=True)
            venv_pip = os.path.join(venv, 'bin', 'pip')
            subprocess.run([venv_pip, 'install'] + self._packages, check=True)
        return venv

    def __enter__(self):
        venv = self._venv()
        self._service_provider = subprocess_server.SubprocessServer(
            external.ExpansionAndArtifactRetrievalStub,
            [
                os.path.join(venv, 'bin', 'python'),
                '-m',
                'apache_beam.runners.portability.expansion_service_main',
                '--port',
                '{{PORT}}',
                '--fully_qualified_name_glob=*',
                '--pickle_library=cloudpickle',
            ]
        )
        self._service = self._service_provider.__enter__()
        return self._service

    def __exit__(self, *args):
        self._service_provider.__exit__(*args)
        self._service = None
        
        if os.path.exists(self._venv_path()):
            subprocess.run(['rm', '-rf', self._venv_path()])
            logger.info("Cleaned up virtual environment after pipeline run.")