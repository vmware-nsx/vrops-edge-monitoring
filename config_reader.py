# config_reader.py
import yaml
from typing import Dict, Any, Tuple

def load_config(config_file: str = 'config.yaml', 
               credentials_file: str = 'credentials.yaml') -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Load both configuration and credentials files
    Returns:
        Tuple containing configuration dict and credentials dict
    """
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        with open(credentials_file, 'r') as f:
            credentials = yaml.safe_load(f)
        
        return config, credentials
    except Exception as e:
        raise Exception(f"Failed to load configuration: {e}")
