from typing import Any, Dict
from utils import LogHandler

logger = LogHandler.logger

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