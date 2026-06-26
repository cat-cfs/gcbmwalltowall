import re
import logging


class YearParser:

    def __init__(self, pattern: str | list[int] | None = None):
        """Parse a yyyy-format year out of a string, optionally using a pattern
        hint.
        
        Params:
            pattern: If unspecified, find a run of 4 digits closest to the end
                of the string. Also accepts a string like "foo_yyyy_", where
                yyyy indicates the position of the year within a substring to
                match, or a start and end index denoting a substring slice to
                scan for a year.
        """
        self._yyyy_pattern = r"(\d{4})"
        self._pattern = pattern
        
    def try_parse_year(self, text):
        matches = None
        if self._pattern is None:
            matches = re.findall(self._yyyy_pattern, text)
        elif isinstance(self._pattern, list):
            substr = text[self._pattern[0]:self._pattern[1]]
            matches = re.findall(self._yyyy_pattern, substr)
        elif "yyyy" in self._pattern:
            pattern = self._pattern.replace("yyyy", self._yyyy_pattern)
            matches = re.findall(pattern, text)

        if not matches:
            return None
        
        if len(matches) > 1:
            pattern = self._pattern or self._yyyy_pattern
            logging.fatal(
                "Multiple yyyy-format candidates found for pattern {pattern} "
                f"in {text}"
            )

            return None

        try:
            year = int(matches[-1])
            return year
        except:
            return None
