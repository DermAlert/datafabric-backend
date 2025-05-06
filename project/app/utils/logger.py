import logging
import sys
from pathlib import Path
import json
from datetime import datetime
import os

# Create logs directory if it doesn't exist
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Configure logger
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

# Log to file with rotation
log_file_path = log_dir / f"app_{datetime.now().strftime('%Y-%m-%d')}.log"
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)

# Log to console as well
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Create custom formatter for structured logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
            
        # Add extra fields if present
        if hasattr(record, "extra"):
            log_record.update(record.extra)
            
        return json.dumps(log_record)

# Set formatter for handlers
json_formatter = JsonFormatter()
file_handler.setFormatter(json_formatter)
console_handler.setFormatter(json_formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Remove propagation to avoid duplicate logs
logger.propagate = False