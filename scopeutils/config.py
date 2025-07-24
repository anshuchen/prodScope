# will update with packages and functions as we go
'''
# dependencies

import pandas as pd
import numpy as np
import os
import sys
import re
import time
from itertools import islice
import datetime
import ast
import csv

'''

import os
import json
import yaml
from pathlib import Path

class ConfigManager:
    """Manages configuration files in user-accessible locations"""
    
    def __init__(self, config_name="scope_config"):
        self.config_name = config_name
        
    def get_config_path(self, create_if_missing=True):
        config_path = Path.cwd() / f"{self.config_name}.yaml"
        
        if create_if_missing and not config_path.exists():
            self.create_default_config(config_path)
            
        return config_path
    
    def create_default_config(self, config_path):
        default_config = {
            'data_paths': {
                'raw_data_dir': '.scopeProject/data/raw',
                'processed_data': '.scopeProject/data/processed',
                'model_dir':'.scopeProject/UAE-Large-V1'
            }
        }
        
        # Create directory if it doesn't exist
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write default config
        with open(config_path, 'w') as f:
            yaml.dump(default_config, f, default_flow_style=False, indent=2)
            
        print(f"Created default config file at: {config_path}")
        print("Please edit this file to customize your paths and settings.")
    
    def load_config(self):
        config_path = self.get_config_path()
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"Error loading config: {e}")
            return None
    
    def get_data_path(self, key):
        config = self.load_config()
        return Path(config['data_paths'][key]) if config else None

# Convenience functions for the package
config_manager = ConfigManager()

def get_config_path():
    return config_manager.get_config_path()

def load_config():
    """Load the configuration dictionary"""
    return config_manager.load_config()

def get_data_path(key):
    return config_manager.get_data_path(key)


# Example usage:
# my_package/core.py
'''
def main_function():
    """Example function that uses config paths"""
    from ..scopeUtils/config import get_data_path
    
    # Get paths from config
    data_dir = get_data_path('raw_data_dir')
    
    # Your actual function logic here
    pass
'''