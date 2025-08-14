import os

REDIS_PREFIX:str="dispatch-automation"

MODEL_DIRS = {
    "EXPORT": os.path.join("load_scheduler", "models", "scheduler", "export_models"),
    "IMPORT": os.path.join("load_scheduler", "models", "scheduler", "import_models")
}
