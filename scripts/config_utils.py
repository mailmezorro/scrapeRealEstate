import json

def load_config_file(config_file="config.json"):
    with open(config_file, "r") as file:
        config = json.load(file)
    return config