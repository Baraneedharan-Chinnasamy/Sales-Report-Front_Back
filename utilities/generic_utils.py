import importlib
from database.database import get_db

# Dictionary mapping business name to the correct models file
MODEL_FILES = {
    "ZNG45F8J27LKMNQ": "models.zing_db",  
    "PRT9X2C6YBMLV0F": "models.pkm_db",
    "BEE7W5ND34XQZRM": "models.bee_db",
    "ADBXOUERJVK038L": "models.adb_db"
}

def get_dynamic_db(business: str):
    if business is None:
        raise ValueError("Business Name  is required")
    return next(get_db(business))


def get_models(business: str):
    """Dynamically import the correct models file."""
    if business not in MODEL_FILES:
        raise ValueError(f"Models for {business} not found")
    
    module_name = MODEL_FILES[business]
    
    try:
        models_module = importlib.import_module(module_name)
        return models_module
    except ModuleNotFoundError:
        raise ValueError(f"Models module {module_name} not found")