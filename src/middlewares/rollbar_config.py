import rollbar
import os
import sys

def use_rollbar(handler):
    rollbar.init(
        access_token=os.getenv("ROLLBAR_TOKEN"), 
        environment=os.getenv("ENV_STAGE")
    )

    def wrapper(*args, **kwargs):
        try:
            return handler(*args, **kwargs)
        except Exception as err:
            rollbar.report_exc_info(sys.exc_info())
            raise err

    wrapper.__name__ = handler.__name__
    return wrapper