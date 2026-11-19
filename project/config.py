"""
Global configuration file for MindMap project
Provides filesystem paths, channel mapping and other settings.
"""

import os
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ===================================================
# 1. Filesystem paths and directory setup
# ===================================================

PROJECT_DIR: Path = Path(__file__).parent
DATA_DIR: Path = PROJECT_DIR / "data"
PROCESSED_DIR: Path = DATA_DIR / "processed"
FEATURES_DIR: Path = DATA_DIR / "features"
NOTEBOOKS_DIR: Path = PROJECT_DIR / "notebooks"
ASSETS_DIR: Path = DATA_DIR / "assets"
REPORTS_DIR: Path = PROJECT_DIR / "reports"
FIGURES_DIR: Path = REPORTS_DIR / "figures"
METRICS_DIR: Path = REPORTS_DIR / "metrics"
LOGS_DIR: Path = REPORTS_DIR / "logs"
MANIFEST_PATH: Path = DATA_DIR / "manifest.parquet"


def ensure_directories() -> None:
    """Create standard project data and reporting directories if missing."""
    for directory in (
        DATA_DIR,
        PROCESSED_DIR,
        FEATURES_DIR,
        ASSETS_DIR,
        REPORTS_DIR,
        FIGURES_DIR,
        METRICS_DIR,
        LOGS_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


ensure_directories()

# ===================================================
# 2. General reproducibility & ingestion settings
# ===================================================

RANDOM_SEED: int = 42
ALL_MOBILITIES: bool = True
DEFAULT_LIMIT_SUBJECTS: int = 3
INGESTION_SCHEMA_VERSION: str = "1.0.0"

# ===================================================
# 3. MOABB dataset and paradigm settings
# ===================================================

FMIN: float = 8.0
FMAX: float = 32.0
EXPECTED_SFREQ_HZ: float = 1000.0
EXPECTED_TRIAL_INTERVAL: Tuple[float, float] = (1.5, 5.5)

# ===================================================
# 4. Channel definition and mappings
# ===================================================

MED_COLUMNS: List[str] = [
    # 59 EEG Channels                                                              
    'Fpz', 'Fp1', 'Fp2', 'AF3', 'AF4', 'AF7', 'AF8',                                                
    'Fz', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8',                                           
    'FCz', 'FC1', 'FC2', 'FC3', 'FC4', 'FC5', 'FC6', 'FT7', 'FT8',                                  
    'Cz', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'T7', 'T8',                                           
    'CP1', 'CP2', 'CP3', 'CP4', 'CP5', 'CP6', 'TP7', 'TP8',                                         
    'Pz', 'P3', 'P4', 'P5', 'P6', 'P7', 'P8',                                                       
    'POz', 'PO3', 'PO4', 'PO5', 'PO6', 'PO7', 'PO8',                                                
    'Oz', 'O1', 'O2',             
    
    # 1 ECG Channel
    'ECG',
    
    # 4 EOG Channels                            
    'EOG1', 'EOG2', 'EOG3', 'EOG4' 
]

EEG_CHANNELS: List[str] = MED_COLUMNS[:59]
ECG_CHANNELS: List[str] = MED_COLUMNS[59:60]
EOG_CHANNELS: List[str] = MED_COLUMNS[60:64]

CHANNEL_TYPES: Dict[str, str] = {
    **{ch: "eeg" for ch in EEG_CHANNELS},
    **{ch: "ecg" for ch in ECG_CHANNELS},
    **{ch: "eog" for ch in EOG_CHANNELS},
}

NUM_CHANNELS: int = len(MED_COLUMNS)
RAW_CHANNEL_NAMES: List[str] = [f'ch_{i}' for i in range(NUM_CHANNELS)]

CHANNEL_MAP: Dict[str, str] = dict(zip(RAW_CHANNEL_NAMES, MED_COLUMNS))

# ===================================================
# 5. Storage settings
# ===================================================

PARQUET_ENGINE: str = "pyarrow"
PARQUET_COMPRESSION: str = "snappy"

# ===================================================
# 6. Helper functions
# ===================================================

def get_subject_parquet_path(subject_id: int, limit_subjects: Optional[int] = None) -> Path:
    return DATA_DIR / f"data_sub_{subject_id:02d}.parquet"

def get_subject_npz_path(subject_id: int) -> Path:
    return DATA_DIR / f"data_sub_{subject_id:02d}.npz"

def get_all_parquet_files() -> List[Path]:
    return sorted(list(DATA_DIR.glob("data_sub_*.parquet")))

def get_all_npz_files() -> List[Path]:
    return sorted(list(DATA_DIR.glob("data_sub_*.npz")))

def get_eeg_channel_names() -> List[str]:
    return list(EEG_CHANNELS)

def get_channel_type_counts() -> Dict[str, int]:
    """Return channel count summary by signal modality."""
    return {
        "eeg": len(EEG_CHANNELS),
        "ecg": len(ECG_CHANNELS),
        "eog": len(EOG_CHANNELS),
        "total": NUM_CHANNELS,
    }

def to_relative_path(path: Path) -> str:
    """Return path relative to PROJECT_DIR using POSIX separators."""
    resolved = Path(path).resolve()
    project_root = PROJECT_DIR.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return resolved.as_posix()

def atomic_write_text(target_path: Path, content: str, encoding: str = "utf-8") -> Path:
    """Write text atomically via a temporary file in the same directory."""
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding=encoding) as handle:
            handle.write(content)
        os.replace(tmp_name, target)
    except Exception:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise
    return target

def atomic_write_parquet(df, target_path: Path, **kwargs) -> Path:
    """Write a pandas DataFrame to Parquet atomically."""
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    os.close(fd)
    try:
        df.to_parquet(
            tmp_name,
            engine=kwargs.pop("engine", PARQUET_ENGINE),
            compression=kwargs.pop("compression", PARQUET_COMPRESSION),
            index=kwargs.pop("index", False),
            **kwargs,
        )
        os.replace(tmp_name, target)
    except Exception:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise
    return target
