from typing import TypedDict

class PathNames(TypedDict):
    tmp_path: str

PATHS: PathNames = {
    "tmp_path": "tmp/"
}

class S3PathNames(TypedDict):
    bucket_name: str
    reports_folder: str

S3_PATHS: S3PathNames = {
    "bucket_name": "usrv-scraping",
    "reports_folder": "reports-scraping-colombia"
}