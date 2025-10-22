import json
import os
from dotenv import load_dotenv
import argparse
from src.services.sync_colombia_trademarks.main import handler

def container_handler(event, context=None):
    return handler(event, context)


if __name__ == '__main__':
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run the Colombia trademarks synchronization process.")
    parser.add_argument(
        '--status',
        type=str,
        required=True,
        choices=['active', 'inactive'],
        help="The case status to scrape ('active' or 'inactive')."
    )
    args = parser.parse_args()
    event_params = {
        "case_status": args.status
    }
    print(f"Running handler with event: {json.dumps(event_params, indent=2)}")
    container_handler(event_params)