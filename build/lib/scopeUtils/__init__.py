
# scopeUtils/__init__.py
"""
My Package - Auto-imports dependencies and exposes main functions
"""

# Import external dependencies
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# Import your own modules
#from .core import main_function, data_processor
#from .utils import helper_function
from .config import get_config_path, load_config
from .example import add

# Make everything available at package level
__all__ = [
    'pd', 'np', 'plt', 'Path',  # External dependencies
    'test_add',
    #'main_function', 'data_processor', 'helper_function',  # Your functions
    'get_config_path', 'load_config'  # Config functions
]

# Optional: Set up any package-level initialization
print("scopeUtils loaded with all dependencies!")