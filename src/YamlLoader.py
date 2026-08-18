import yaml

def load(confPath):
    with open(confPath, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)
