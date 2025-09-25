#import Library
import configparser
import secrets
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import re
import json

#load env
load_dotenv()

#Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

#Stream Handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

# --- File Handler ---
file_handler = logging.FileHandler("app.log")  # log file name
file_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


#Setup config
config = configparser.ConfigParser()
config.read("./kafka/kafka_config.ini")


def parse_env_variable(value):
    """Parse environment variable with default value fallback."""
    match = re.match(r'\$\{([^:}]+):?([^}]*)\}', value)
    if match:
        env_var, default_value = match.groups()
        resolved_value = os.getenv(env_var, default_value)
        logger.debug(f"Resolved env var {env_var} -> {resolved_value}")
        return resolved_value
    return value

def load_config(section_name):
    """Load and return config section with environment variable substitutions."""
    if section_name not in config:
        logger.error(f"Section {section_name} not found in config file")
        raise ValueError(f"Section {section_name} not found in config file")

    # Replace environment variables in the section
    section_config = {
            key:parse_env_variable(value)
            for key, value in config.items(section_name)
            }

    logger.info(f"Loaded config for section [{section_name}]: {section_config}")
    return section_config


