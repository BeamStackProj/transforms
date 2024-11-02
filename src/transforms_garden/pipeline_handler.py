import sys
import base64
from typing import Any, Dict
import importlib.util
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils import LogHandler

logger = LogHandler.logger

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