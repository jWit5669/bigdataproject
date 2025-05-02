from pymongo import MongoClient
import yaml
import certifi

def load_config():
    """Load configuration from the YAML file.

    Returns:
        dict: Configuration data.
    """
    with open("config.yaml", "r") as file:
        return yaml.safe_load(file)


config = load_config()


def get_mongodb_connection():
    """Create a MongoDB connection using the configuration.

    Returns:
        Connection: MongoDB connection object.
    """
    client = MongoClient( config["mongo"]["ATLAS_URI"], tls=True, tlsCAFile=certifi.where() )[ config["mongo"]["DB_NAME"] ]
    return client
