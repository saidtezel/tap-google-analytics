import json
import hashlib

def load_json(path):
    with open(path) as f:
        return json.load(f)

def generate_sdc_record_hash(view_id, report_date, dimensions):
    """
    Generates a SHA 256 hash to be used as the primary key for records
    associated with a report. This consists of a UTF-8 encoded JSON list
    containing:
    - The account_id, web_property_id, profile_id of the associated report
    - Pairs of ("ga:dimension_name", "dimension_value")
    - Report start_date value in YYYY-mm-dd format
    - Report end_date value in YYYY-mm-dd format
    Start and end date are included to maintain flexibility in the event the
    tap is extended to support wider date ranges.
    WARNING: Any change in the hashing mechanism, data, or sorting will
    REQUIRE a major version bump! As it will invalidate all previous
    primary keys and cause new data to be appended.
    """

    # NB: Do not change the ordering of this list, it is the source of the PK hash
    hash_source_data = [view_id, report_date]
    hash_source_data.extend(dimensions)

    hash_source_bytes = json.dumps(hash_source_data).encode('utf-8')
    return hashlib.sha256(hash_source_bytes).hexdigest()