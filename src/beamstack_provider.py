import os
import json
import hashlib
import base64
import subprocess
import sys
import yaml
import apache_beam as beam
from typing import Any, Iterable, Mapping, Optional, Callable
from apache_beam.yaml.yaml_provider import ExternalProvider
import logging
import importlib.util
import urllib.request
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BeamstackProviderPathHandler:
    def is_local_file(self, path: str) -> bool:
        """Check if the path is a local file."""
        return os.path.exists(path)

    def is_github_file(self, path: str) -> bool:
        """Check if the path is a GitHub file URL."""
        parsed_url = urlparse(path)
            
        if parsed_url.netloc == "raw.githubusercontent.com":
            return True
        if parsed_url.netloc == "github.com":
            return True
        
        return False

    def is_gcs_file(self, path: str) -> bool:
        """Check if the path is a Google Cloud Storage URL."""
        return path.startswith('gs://')

    def handle_local_file(self, path: str):
        """Handle local file or directory path."""
        logger.info(f"Pulling transforms yaml from: {path}")
        if not os.path.exists(path):
            raise FileNotFoundError(f"The local path '{path}' does not exist.")
        return path

    def handle_github_file(self, file_url: str, target_dir: str):
        """Download file from a public GitHub repository to the target path."""
        logger.info(f"Pulling transforms yaml from GitHub url: {file_url}")

        if "github.com" in file_url and "/blob/" in file_url:
            file_url = file_url.replace("github.com", "raw.githubusercontent.com").replace("blob/", "")
        
        os.makedirs(target_dir, exist_ok=True)

        file_name = os.path.basename(file_url)
        local_file_path = os.path.join(target_dir, file_name)
        
        try:
            logger.info(f"Downloading {file_name} to {local_file_path}")
            urllib.request.urlretrieve(file_url, local_file_path)
        except Exception as e:
            logger.info(f"Error occured during file download: {e}")

        return local_file_path

    def handle_gcs_file(self, gcs_path: str, target_dir: str):
        """Download files from a public GCS bucket to a target path."""
        logger.info(f"Pulling transforms yaml from GCS path: {gcs_path}")

        gcs_path = gcs_path[len("gs://"):]
        bucket_name, _, object_name = gcs_path.partition('/')
        public_url = f"https://storage.googleapis.com/{bucket_name}/{object_name}"
        
        os.makedirs(target_dir, exist_ok=True)
        local_file_path = os.path.join(target_dir, os.path.basename(object_name))
        
        try:
            logger.info(f"Downloading {os.path.basename(object_name)} to {target_dir}")
            urllib.request.urlretrieve(public_url, local_file_path)
        except Exception as e:
            logger.info(f"Error downloading file from {public_url}: {e}")
        
        return local_file_path

@ExternalProvider.register_provider_type('BeamstackTransform')
def BeamstackTransform(urns, path):
    target_dir = '/tmp/beamstack_transforms'
    
    path_handler = BeamstackProviderPathHandler()

    if path_handler.is_local_file(path):
        transform_yaml_path = path_handler.handle_local_file(path)
    elif path_handler.is_github_file(path):
        transform_yaml_path = path_handler.handle_github_file(path, target_dir)
    elif path_handler.is_gcs_file(path):
        transform_yaml_path = path_handler.handle_gcs_file(path, target_dir)
    else:
        raise ValueError(f"Unsupported path type: {path}")
    
    with open(transform_yaml_path, 'r') as f:
        transform_yaml = yaml.safe_load(f)
    
    config = {
        'urns': urns,
        'yaml_path': transform_yaml_path,
        'dependencies': transform_yaml.get('dependencies', [])
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

    
    def _module_class_map(self) -> dict:
        """Transform module and class dictionary map"""
        self.yaml_path = self.config.get('yaml_path')
        
        with open(self.yaml_path, 'r') as file:
            data = yaml.safe_load(file)
            self.transforms = data['transforms']

            transform_map = {}
            for item in self.transforms:
                for _, value in item.items():
                    module_name, transform_class = value.split(':')
                    transform_map[transform_class] = module_name

        return transform_map

    def _load_transform_class(self, transform_name):
        """Dynamically loads and returns a transform class by name."""
        transform_map = self._module_class_map()

        try:
            logger.info(f"Loading transform class for: {transform_name}")
            
            spec = importlib.util.spec_from_file_location(
                f"{transform_map[transform_name]}.py",
                os.path.join(self._service._venv_path(), f"{transform_map[transform_name]}.py")
            )
            if spec is None:
                logger.error(f"Specification for module '{transform_map[transform_name]}' could not be found.")
                return None

            module = importlib.util.module_from_spec(spec)
            sys.path.insert(0, os.path.dirname(spec.origin))
            spec.loader.exec_module(module)
            transform_class = getattr(module, transform_name)
            logger.info(f"Loaded transform class: {transform_class}")
            return transform_class
        except Exception as e:
            logger.error(f"Failed to load transform {transform_name}: {e}")
            raise e


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
            self.source_code = data['source_code']
            self.encoding = data['encoding']

            for module_name, encoded_code in self.source_code.items():
                decoded_code = base64.b64decode(encoded_code).decode('utf-8')
                self._write_source_file(f"{module_name}.py", decoded_code)
                self._source_module = f"{module_name}.py"

    def _write_source_file(self, src_name, code):
        """Writes decoded code to file for each source."""
        venv = self._venv_path()
        file_path = os.path.join(venv, src_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.write(code)

    def _venv_path(self):
        """Returns the path for the virtual environment directory based on the packages and runner."""
        key = json.dumps({'binary': self.base_python, 'packages': sorted(self._packages), 'runner': self.runner})
        venv_hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
        venv = os.path.join(self.VENV_CACHE, venv_hash)
        
        if not os.path.exists(venv):
            subprocess.run([self.base_python, '-m', 'venv', venv], check=True)
        
        site_packages_path = os.path.join(venv, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages')
        if site_packages_path not in sys.path:
            sys.path.insert(0, site_packages_path)

        venv_pip = os.path.join(venv, 'bin', 'pip')
        
        if os.path.exists(venv_pip):
            installed_packages = subprocess.check_output(
                [venv_pip, 'list', '--format=freeze']
            ).decode('utf-8').splitlines()
            
            installed_packages_set = {pkg.split('==')[0] for pkg in installed_packages}
            
            for package in self._packages:
                if package not in installed_packages_set:
                    logger.info(f"Installing package: {package}")
                    subprocess.run([venv_pip, 'install', package], check=True)
                else:
                    logger.info(f"Package '{package}' is already installed; skipping installation.")
        else:
            raise FileNotFoundError(f"Could not find pip at expected location: {venv_pip}")
        
        return venv