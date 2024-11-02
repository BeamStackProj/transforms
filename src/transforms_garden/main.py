import os
import sys
import base64
import subprocess
import yaml
import logging
from typing import Any, Dict, List
import importlib.util
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YAMLValidator:
    """Validate the YAML file structure."""
    
    @staticmethod
    def validate(yaml_data: Dict[str, Any]) -> None:
        required_fields = ['metadata', 'source', 'dependencies', 'transforms', 'source_code', 'encoding']
        
        for field in required_fields:
            if field not in yaml_data:
                logger.error(f"Missing required field: {field}")
                raise ValueError(f"Missing required field: {field}")
        
        if yaml_data['encoding'] != 'base64':
            logger.error("Invalid encoding type; must be 'base64'.")
            raise ValueError("Invalid encoding type; must be 'base64'.")


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


class PipelineDataHandler:
    """Handles reading from source and writing to sink in the Beam pipeline."""
    
    def __init__(self, source_path: str, sink_path: str):
        self.source_path = source_path
        self.sink_path = sink_path

    def read_source(self, p: beam.Pipeline) -> beam.PCollection:
        """Read data from the source path into a Beam PCollection."""
        logger.info(f"Reading data from source path: {self.source_path}")
        return p | "ReadFromSource" >> beam.io.ReadFromText(self.source_path)

    def write_sink(self, pcollection: beam.PCollection) -> None:
        """Write the resulting PCollection to the sink path."""
        logger.info(f"Writing data to sink path: {self.sink_path}")
        pcollection | "WriteToSink" >> beam.io.WriteToText(self.sink_path)


class TransformManager:
    """Manage decoding and handling of transform source codes."""
    
    def __init__(self, transforms: Dict[str, str], source_code: Dict[str, str]):
        self.transforms = transforms
        self.source_code = source_code
        self.decoded_code = {}

    def decode_source_code(self) -> None:
        logger.info("Decoding source code from base64.")
        for source, code in self.source_code.items():
            decoded = base64.b64decode(code).decode('utf-8')
            self.decoded_code[source] = decoded
            logger.debug(f"Decoded source for {source}: {decoded}...")

class PipelineBuilder:
    """Build and run the Apache Beam pipeline."""

    def __init__(self, 
                 transforms: Dict[str, str], 
                 decoded_code: Dict[str, str], 
                 venv_dir: str, 
                 source_path: str, 
                 sink_path: str,
                 pipeline_options: Dict[str, Any]):
        
        self.transforms = transforms
        self.decoded_code = decoded_code
        self.venv_dir = venv_dir
        self.pipeline_data = PipelineDataHandler(source_path, sink_path)
        self.pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

    def load_transform_class(self, module_name: str, class_name: str, source_code: str):
        """Dynamically load a class from a decoded source code string."""
        # Create a temporary module name
        temp_module_name = f"temp_{module_name.replace('.', '_')}"
        spec = importlib.util.spec_from_loader(temp_module_name, loader=None)
        module = importlib.util.module_from_spec(spec)

        # Execute the source code in the module's dictionary
        exec(source_code, module.__dict__)

        # Register the module in sys.modules
        sys.modules[temp_module_name] = module

        # Access the class from the module
        transform_class = getattr(module, class_name)

        # Check if it's a valid Beam PTransform
        if not issubclass(transform_class, beam.PTransform):
            raise TypeError(f"{class_name} is not a subclass of beam.PTransform")

        return transform_class

    def build_pipeline(self) -> None:
        logger.info("Building the Apache Beam pipeline.")

        # Building the pipeline
        with beam.Pipeline(options=self.pipeline_options) as pipeline:
            
            # Read data from the source
            input_data = self.pipeline_data.read_source(pipeline)
            
            # Apply each transform in transforms list
            for transform in self.transforms:
                try:
                    transform_name, transform_info = list(transform.items())[0]
                    source_file, class_name = transform_info.split(":")
                    source_code = self.decoded_code.get(source_file)

                    if not source_code:
                        logger.error(f"Source code for {source_file} not found.")
                        continue

                    # Dynamically load the transform class
                    transform_class = self.load_transform_class(source_file, class_name, source_code)
                    logger.info(f"Successfully loaded transform: {transform_name} ({class_name})")

                    # Apply the transform
                    logger.info(f"Applying {transform_name} to the pipeline.")
                    input_data = input_data | f"Apply_{transform_name}" >> transform_class()
                
                except Exception as e:
                    logger.error(f"Error applying transform {transform_name}: {e}")

            # Write the transformed data to the sink
            self.pipeline_data.write_sink(input_data)


class BeamstackSDK:
    """Main SDK class to orchestrate the process."""
    
    def __init__(self, yaml_file: str, source_path: str, sink_path: str, pipeline_options: Dict[str, Any]):
        self.source_path = source_path
        self.sink_path = sink_path
        self.pipeline_options = pipeline_options
        self.yaml_file = yaml_file
        self.yaml_data = self.load_yaml()
        self.venv_dir = 'venv' ### virtual env path

    def load_yaml(self) -> Dict[str, Any]:
        with open(self.yaml_file, 'r') as file:
            return yaml.safe_load(file)

    def run(self) -> None:
        # Validate YAML
        YAMLValidator.validate(self.yaml_data)

        # Create virtual environment and install dependencies
        venv_manager = VirtualEnvManager(self.venv_dir)
        venv_manager.create_venv()
        venv_manager.install_packages(self.yaml_data['dependencies'])

        # Decode source code
        transform_manager = TransformManager(
            transforms=self.yaml_data['transforms'],
            source_code=self.yaml_data['source_code']
        )
        transform_manager.decode_source_code()
        
        # Build and run the pipeline
        pipeline_builder = PipelineBuilder(
            transforms=self.yaml_data['transforms'],
            decoded_code=transform_manager.decoded_code,
            venv_dir=self.venv_dir,
            source_path=self.source_path,
            sink_path=self.sink_path,
            pipeline_options=self.pipeline_options
        )
        pipeline_builder.build_pipeline()


if __name__ == "__main__":
    pipeline_options = {
        'runner': 'DirectRunner'
    }

    yaml_path = 'pipeline.yaml'
    source_path = 'input_data.txt'
    sink_path = 'output_data.txt'

    transformsdk = BeamstackSDK(
        yaml_file=yaml_path, 
        source_path=source_path, 
        sink_path=sink_path, 
        pipeline_options=pipeline_options
    )
    
    transformsdk.run()