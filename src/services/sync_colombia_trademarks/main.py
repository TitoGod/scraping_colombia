from src.utils.logging_config import setup_logging
from src.functions.sync_orchestrator import run_sync_process
from src.middlewares.rollbar_config import use_rollbar

@use_rollbar
def handler(event, context=None):
    logger = setup_logging()
    case_status = event.get("case_status")
    if not case_status:
        logger.critical("'case_status' must be provided in the event.")
        return
    run_sync_process(logger, case_status)
    return event