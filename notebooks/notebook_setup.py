# notebooks/notebook_setup.py
"""Setup script for Jupyter notebooks in the project."""

# Setup project path
import sys
import os
from pathlib import Path
project_root = Path.cwd().parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# External imports
from datetime import datetime, timedelta
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
from scipy import stats
import seaborn as sns
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.stattools import acf, pacf

warnings.filterwarnings('ignore')

# Plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")