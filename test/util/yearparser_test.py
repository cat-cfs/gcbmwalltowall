from gcbmwalltowall.util.yearparser import YearParser


def test_default_pattern():
    parser = YearParser()
    for input_string, expected_result in (
        ("", None),
        ("123", None),
        ("foo_1997", 1997),
        ("foo_1998_bar", 1998),
        ("1973", 1973),
        ("12345", 1234),
    ):
        assert parser.try_parse_year(input_string) == expected_result


def test_slice_pattern():
    for input_string, pattern, expected_result in (
        ("foo_1997", [1, 5], None),
        ("foo_1998_bar", [4, 9], 1998),
        ("derp_2086_herp_2100_", [5, 10], 2086),
        ("12345", [0, 5], 1234),
    ):
        parser = YearParser(pattern)
        assert parser.try_parse_year(input_string) == expected_result


def test_substring_pattern():
    for input_string, pattern, expected_result in (
        ("foo_1997", "bar_yyyy", None),
        ("foo_1998_bar", "yyyy_", 1998),
        ("derp_2086_herp_2100_", "_yyyy_he", 2086),
    ):
        parser = YearParser(pattern)
        assert parser.try_parse_year(input_string) == expected_result


def test_multiple_candidates():
    for input_string, pattern, expected_result in (
        ("foo_1997_bar_2023", None, None),
        ("foo_1997_bar_2023", [4, 18], None),
        ("foo_1997_bar_2023", "yyyy_bar_yyyy", None),
    ):
        parser = YearParser(pattern)
        assert parser.try_parse_year(input_string) == expected_result
