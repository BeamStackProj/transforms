import yaml
import logging
from typing import Any, Dict
from yaml_handler import YAMLValidator
from packages_handler import VirtualEnvManager
from pipeline_handler import TransformManager, PipelineBuilder
from utils import LogHandler


logger = LogHandler.logger


class BeamstackTransforms:
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

    transformsdk = BeamstackTransforms(
        yaml_file=yaml_path, 
        source_path=source_path, 
        sink_path=sink_path, 
        pipeline_options=pipeline_options
    )
    
    transformsdk.run()