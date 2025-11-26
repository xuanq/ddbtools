# Optional logger setup
logger = None
try:
    from loguru import logger
    logger.disable("ddbtools")
except ImportError:
    # Create a no-op logger if loguru is not installed
    class NoOpLogger:
        def disable(self, *args, **kwargs):
            pass
        def enable(self, *args, **kwargs):
            pass
        def info(self, *args, **kwargs):
            pass
        def debug(self, *args, **kwargs):
            pass
        def warning(self, *args, **kwargs):
            pass
        def error(self, *args, **kwargs):
            pass
    logger = NoOpLogger()