import os, sys


def set_env_vars():
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Add the parent directory to sys.path
    sys.path.append(PROJECT_DIR)
    os.environ["PROJECT_DIR"] = PROJECT_DIR


print("Setting Env Vars")
set_env_vars()