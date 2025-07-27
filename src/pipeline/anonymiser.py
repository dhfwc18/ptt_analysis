# pipeline/anonymiser.py
"""Anonymiser module for usernames in PTT data."""

__all__ = ["UserAnonymiser", "anonymise_data"]

# Internal imports
from config.config import MAPPING_DIR

# External imports
import hashlib
import os
import random
import string
import uuid
from typing import Dict, Optional

import pandas as pd

# Get logger
from config.logging_config import get_logger

logger = get_logger(__name__)

class UserAnonymiser:
    """
    Simple framework for user name anonymisation. Usernames are
    converted to anonymous IDs with various methods, including
    hashing, random strings, UUIDs, sequential numbering, and random
    numeric IDs.
    """
    def __init__(self, method='hash', seed=42, id_length=8):
        """
        Initialise the anonymiser with the specified method and
        parameters.

        Parameters
        ----------
        method : str, optional
            The anonymisation method to use. The default is 'hash' and
            the options are:
            - `hash`: Use a hash function to create a deterministic ID.
            - `random_string`: Generate a random string ID.
            - `uuid`: Use UUIDs to create unique IDs.
            - `sequential`: Use sequential numbering for IDs.
            - `numeric`: Create a numeric ID based on a hash.
        seed : int, optional
            The seed for random number generation to ensure
            reproducibility. The default is 42.
        id_length : int, optional
            The length of the radomly generated anonymisation IDs. The
            default is 8.
        """
        self.method = method
        self.seed = seed
        self.id_length = id_length
        self.mapping = {}
        self.reverse_mapping = {}
        random.seed(seed)
        self.counter = 1

    def _hash_id(self, name: str) -> str:
        """Create deterministic hash-based ID."""
        hash_object = hashlib.md5(f"{name}{self.seed}".encode())
        hash_hex = hash_object.hexdigest()[:self.id_length]
        return f"user_{hash_hex}"

    def _random_string_id(self, name: str) -> str:
        """Create random string ID (deterministic per name)."""
        # Use name as additional seed for consistency
        temp_random = random.Random(f"{name}{self.seed}")
        chars = string.ascii_lowercase + string.digits
        random_str = ''.join(temp_random.choices(chars, k=self.id_length))
        return f"user_{random_str}"

    def _uuid_id(self, name: str) -> str:
        """Create UUID-based ID."""
        namespace = uuid.UUID('12345678-1234-5678-1234-123456789012')
        deterministic_uuid = uuid.uuid5(namespace, f"{name}{self.seed}")
        uuid_str = str(deterministic_uuid).replace('-', '')
        return f"user_{uuid_str[:self.id_length]}"

    def _sequential_id(self, name: str) -> str:
        """Create IDs via sequential numbering."""
        user_id = f"user_{self.counter:0{self.id_length}d}"
        self.counter += 1
        return user_id

    def _numeric_id(self, name: str) -> str:
        """Create random numeric ID."""
        hash_int = int(
            hashlib.md5(f"{name}{self.seed}".encode()).hexdigest(), 16
        )
        numeric_id = str(hash_int)[:self.id_length]
        return numeric_id.zfill(self.id_length)

    def anonymise_name(self, original_name: Optional[str]) -> str:
        """
        Convert original name to an anonymous ID.

        Parameters
        ----------
        original_name : str
            The original user name to anonymise.

        Returns
        -------
        str
            Anonymised user ID.
        """
        if pd.isna(original_name) or original_name == '':
            return 'UNKNOWN'

        # Check if already anonymised
        if original_name in self.mapping:
            return self.mapping[original_name]

        if self.method == 'hash':
            anon_id = self._hash_id(original_name)
        elif self.method == 'random_string':
            anon_id = self._random_string_id(original_name)
        elif self.method == 'uuid':
            anon_id = self._uuid_id(original_name)
        elif self.method == 'sequential':
            anon_id = self._sequential_id(original_name)
        elif self.method == 'numeric':
            anon_id = self._numeric_id(original_name)
        else:
            raise ValueError(f"Unknown method: {self.method}")

        self.mapping[original_name] = anon_id
        self.reverse_mapping[anon_id] = original_name

        return anon_id

    def deanonymise_id(self, anon_id: str) -> Optional[str]:
        """
        Convert anonymous ID back to original name.

        Parameters
        ----------
        anon_id : str
            The anonymised user ID to convert back.

        Returns
        -------
        Optional[str]
            The original user name if found, otherwise None.
        """
        return self.reverse_mapping.get(anon_id)

    def anonymise_dataframe(
            self, df: pd.DataFrame, column_name: str
    ) -> pd.DataFrame:
        """
        Convert names in a DataFrame column to anonymous IDs.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the user names.
        column_name : str
            The name of the column to anonymise.

        Returns
        -------
        pd.DataFrame
            A copy of the DataFrame with the specified column
            anonymised.
        """
        df_copy = df.copy()
        df_copy[column_name] = df_copy[column_name].apply(self.anonymise_name)
        return df_copy

    def deanonymise_dataframe(
            self, df: pd.DataFrame, column_name: str
    ) -> pd.DataFrame:
        """
        Convert anonymous IDs in a DataFrame column back to original
        names.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the anonymised user IDs.
        column_name : str
            The name of the column to deanonymise.

        Returns
        -------
        pd.DataFrame
            A copy of the DataFrame with the specified column
            deanonymised.
        """
        df_copy = df.copy()
        df_copy[column_name] = (
            df_copy[column_name].apply(self.deanonymise_id)
        )
        return df_copy

    def get_mapping(self) -> Dict[str, str]:
        """Get original to anonymous mapping."""
        return self.mapping.copy()

    def get_reverse_mapping(self) -> Dict[str, str]:
        """Get anonymous to original mapping."""
        return self.reverse_mapping.copy()

    def save_mapping(self, filepath: str):
        """Save mapping to CSV."""
        mapping_df = pd.DataFrame([
            {'original': orig, 'anonymous': anon}
            for orig, anon in self.mapping.items()
        ])
        mapping_df.to_csv(filepath, index=False)
        logger.info(f"Mapping saved to {filepath}")

    def load_mapping(self, filepath: str):
        """Load mapping from CSV."""
        mapping_df = pd.read_csv(filepath)
        self.mapping = dict(
            zip(mapping_df['original'], mapping_df['anonymous'])
        )
        self.reverse_mapping = dict(
            zip(mapping_df['anonymous'], mapping_df['original'])
        )
        # Update counter for sequential method
        if self.method == 'sequential':
            max_num = max(
                [int(aid.split('_')[1]) for aid in self.mapping.values()
                 if aid.startswith('user_') and aid.split('_')[1].isdigit()],
                default=0
            )
            self.counter = max_num + 1
        logger.info(f"Mapping loaded from {filepath}")

def anonymise_data(
        df: pd.DataFrame, column_name: str, save_to_file: Optional[str] = None
) -> pd.DataFrame:
    """
    Utility function to anonymise a DataFrame column with a
    UserAnonymiser object (with all settings at the default).

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the usernames.
    column_name : str
        The name of the column to anonymise.
    save_to_file : Optional[str], optional
        If provided, the mapping will be saved into the mappings
        directory in output based on the name specified.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with the specified column anonymised.
    """
    anonymiser = UserAnonymiser()
    anonymised_df = anonymiser.anonymise_dataframe(df, column_name)
    if save_to_file:
        mapping_path = MAPPING_DIR / save_to_file
        os.makedirs(MAPPING_DIR, exist_ok=True)
        anonymiser.save_mapping(mapping_path)
    return anonymised_df

