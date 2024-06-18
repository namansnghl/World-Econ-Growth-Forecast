import logging
import logging.config
import json, os
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "log_config.json")

log_dir = os.path.join("/home/airflow/logs/")
os.makedirs(log_dir, exist_ok=True)

class setup_logging:
    global file_path
    
    def __init__(self, file: str = None) -> None:
        self.config_file = file or file_path
        self.logger = None
        self._load_config()

    def set_logger(self, config_logger:str = "main_logger") -> None:
        self.logger =  logging.getLogger(config_logger)
    
    def write(self, level:str, msg:str):
        if not self.logger:
            raise ValueError("No logger was set before logging")

        level_lower = level.lower()

        # Determine the logging level based on the provided string
        if level_lower in ['info', 'i']:
            self.logger.info(msg)
        elif level_lower in ['warning', 'warn', 'w']:
            self.logger.warning(msg)
        elif level_lower in ['error', 'e']:
            self.logger.error(msg)
        elif level_lower in ['critical', 'c']:
            self.logger.critical(msg)
        else:
            # Default to debug level if an invalid log level is provided
            self.logger.debug(msg)


    def _load_config(self):
        with open(self.config_file, 'r') as f:
            config = json.load(f)
            logging.config.dictConfig(config)




if __name__ == "__main__":
    my_logger = setup_logging(file_path)
    my_logger.set_logger("test_logger")
    my_logger.write('debug', 'Debug message')
    my_logger.write('info', 'Info message')
    my_logger.write('warning', 'Warning message')
    my_logger.write('error', 'Error message')
    my_logger.write('critical', 'Critical message')
    my_logger.write('invalid', 'Invalid message')