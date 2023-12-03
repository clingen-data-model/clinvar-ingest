log_conf = {
    "version": 1,
    "disable_existing_loggers": False,
    "propagate": True,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(module)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        }
    },
    "loggers": {
        "api": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,  # Necessary for the pytest caplog fixture.
        }
    },
}
