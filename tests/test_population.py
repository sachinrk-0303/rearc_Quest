"""The population source's reporting helper.

summarise exists so the ACS 2020 gap is visible in the run log at ingest time,
rather than discovered much later as a mysteriously short join.
"""

from ingest.population import summarise

# The shape of the real gap: ACS 1-year estimates were not published for 2020.
WITH_GAP = b'{"data":[{"Year":2018},{"Year":2019},{"Year":2021},{"Year":2022}]}'


def test_summarise_counts_rows_and_finds_the_gap():
    assert summarise(WITH_GAP) == (4, [2020])


def test_summarise_reports_no_gap_for_a_contiguous_series():
    assert summarise(b'{"data":[{"Year":2013},{"Year":2014}]}') == (2, [])


def test_summarise_does_not_depend_on_row_order():
    """The API does not promise an ordering, and a gap is a property of the
    set of years, not of the sequence they arrived in."""
    shuffled = b'{"data":[{"Year":2021},{"Year":2018},{"Year":2022},{"Year":2019}]}'
    assert summarise(shuffled) == summarise(WITH_GAP)
