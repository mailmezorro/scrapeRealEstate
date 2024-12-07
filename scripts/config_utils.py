import json
import os

def load_config_file(config_file):
    with open(config_file, "r") as file:
        config = json.load(file)
    return config