from datetime import date

from pipelines.ingestion.fetch import archive_key


def test_archive_key_matches_openaq_layout():
    key = archive_key(2178, date(2023, 1, 5))
    assert key == (
        "records/csv.gz/locationid=2178/year=2023/month=01/"
        "location-2178-20230105.csv.gz"
    )
