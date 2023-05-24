import hashlib
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional
from zipfile import ZipFile

import pandas as pd
from pydantic import BaseModel, SecretStr, ValidationError

from etl.models import Customer
from etl.utils import get_key_prefix_from_timestamp
from packages.rdbms import DBConfig, DBConnection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ZIP_FILE = "test_data.zip"


class Config(BaseModel):
    password: SecretStr


def retrieve_password():
    """Retrieve password from environment variable"""
    keyword = os.environ.get("iconic_keyword")  # Get from env variable rather than hardcode in code for security reason
    if not keyword:
        raise ValueError("Env variable `iconic_keyword` could not be found!")
    return SecretStr(hashlib.sha256(keyword.encode("utf-8")).hexdigest())


def log_invalidated_record(record: Dict, errors: List):
    """Log invalidated record to file"""
    item = {
        "record": record,
        "errors": errors,
    }

    file_path = f"invalidated/{get_key_prefix_from_timestamp()}"
    Path.mkdir(Path(file_path), parents=True, exist_ok=True)

    with open(f"{file_path}/customers.jsonl", "a+") as f:
        f.write(json.dumps(item) + "\n")


def process_record(record: str):
    """Process record and return valid record"""
    data = json.loads(record)
    try:
        customer = Customer(**data)
        return customer.dict()
    except ValidationError as ex:
        log_invalidated_record(data, ex.errors())


def process_input_zip_file() -> List:
    """Process input file and return valid records"""
    config = Config(password=retrieve_password())

    records = []
    with ZipFile(ZIP_FILE) as zip_file:
        with zip_file.open("data.json", pwd=bytes(config.password.get_secret_value(), "utf-8")) as f:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(process_record, line) for line in f]

            for future in as_completed(futures):
                record = future.result()
                if record:
                    records.append(record)
    return records


def load_records_to_database(records: Optional[List]):
    file_path = f"validated/{get_key_prefix_from_timestamp()}"
    Path.mkdir(Path(file_path), parents=True, exist_ok=True)

    csv_file_path = Path(f"{file_path}/customers.csv")
    df = pd.DataFrame(records)
    df.drop_duplicates(inplace=True)
    df.to_csv(csv_file_path, index=False)

    db_config = DBConfig()
    with open("etl/load_customers.sql") as f:
        upsert_query = f.read()

    with DBConnection(db_config) as conn:
        conn.upsert_csv_psql_table(csv_file_path, "dev.customers", upsert_query)
        conn.commit()


def main():
    records = process_input_zip_file()
    load_records_to_database(records)


if __name__ == "__main__":
    main()
    logger.info("Done!")
