
# scopeutils/__init__.py
"""
scopeUtils - Auto-imports dependencies and exposes main functions
"""

# Import external dependencies
from pathlib import Path # for importing file paths

# Import your own modules
from .config import get_config_path, load_config, get_data_path
from .embed import *

# print("scopeUtils loaded with all dependencies!")