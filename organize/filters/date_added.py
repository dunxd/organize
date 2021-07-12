from typing import Dict, Optional

import pendulum  # type: ignore
import subprocess
from pathlib import Path
from organize.utils import DotDict

from .filter import Filter


class DateAdded(Filter):

    """
    Matches files by date added

    :param int years:
        specify number of years

    :param int months:
        specify number of months

    :param float weeks:
        specify number of weeks

    :param float days:
        specify number of days

    :param float hours:
        specify number of hours

    :param float minutes:
        specify number of minutes

    :param float seconds:
        specify number of seconds

    :param str mode:
        either 'older' or 'newer'. 'older' matches all files added to the filesystem
        before the given time, 'newer' matches all files added within
        the given time. (default = 'older')

    :param str timezone:
        specify timezone

    :returns:
        - ``{dateadded.year}`` -- the year the file was added to the filesystem
        - ``{dateadded.month}`` -- the month the file was added to the filesystem
        - ``{dateadded.day}`` -- the day the file was added to the filesystem
        - ``{dateadded.hour}`` -- the hour the file was added to the filesystem
        - ``{dateadded.minute}`` -- the minute the file was added to the filesystem
        - ``{dateadded.second}`` -- the second the file was added to the filesystem

    Examples:
        - Show all files on your desktop added at least 10 days ago:

          .. code-block:: yaml
            :caption: config.yaml

            rules:
              - folders: '~/Desktop'
                filters:
                  - dateadded:
                      days: 10
                actions:
                  - echo: 'Was added at least 10 days ago'

        - Show all files on your desktop which were added within the last
          5 hours:

          .. code-block:: yaml
            :caption: config.yaml

            rules:
              - folders: '~/Desktop'
                filters:
                  - dateadded:
                      hours: 5
                      mode: newer
                actions:
                  - echo: 'Was added within the last 5 hours'

        - Sort pdfs by year added to filesystem

          .. code-block:: yaml
            :caption: config.yaml

            rules:
              - folders: '~/Documents'
                filters:
                  - extension: pdf
                  - DateAdded
                actions:
                  - move: '~/Documents/PDF/{lastmodified.year}/'

        - Use specific timezone when processing files

          .. code-block:: yaml
            :caption: config.yaml

            rules:
              - folders: '~/Documents'
                filters:
                  - extension: pdf
                  - dateadded:
                      timezone: "Europe/Moscow"
                actions:
                  - move: '~/Documents/PDF/{dateadded.day}/{dateadded.hour}/'
    """

    def __init__(
        self,
        years=0,
        months=0,
        weeks=0,
        days=0,
        hours=0,
        minutes=0,
        seconds=0,
        mode="older",
        timezone=pendulum.tz.local_timezone(),
    ) -> None:
        self._mode = mode.strip().lower()
        if self._mode not in ("older", "newer"):
            raise ValueError("Unknown option for 'mode': must be 'older' or 'newer'.")
        self.is_older = self._mode == "older"
        self.timezone = timezone
        self.timedelta = pendulum.duration(
            years=years,
            months=months,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
        )

    def pipeline(self, args: DotDict) -> Optional[Dict[str, pendulum.DateTime]]:
        file_added = self._date_added(args.path)
        # Pendulum bug: https://github.com/sdispater/pendulum/issues/387
        # in_words() is a workaround: total_seconds() returns 0 if years are given
        if self.timedelta.in_words():
            is_past = (file_added + self.timedelta).is_past()
            match = self.is_older == is_past
        else:
            match = True
        if match:
            return {"dateadded": file_added}
        return None

    def _date_added(self, path: Path) -> pendulum.DateTime:
        stat = path.stat()
        try:
            time = subprocess.check_output(["mdls", "-name", "kMDItemDateAdded", "-raw", path])
            except AttributeError:
                # This will only work on Mac, so otherwise return last modified date
                time = stat.st_mtime
        return pendulum.from_timestamp(float(path.stat().st_mtime), tz=self.timezone)

    def __str__(self):
        return "[DateAdded] All files added to filesystem %s than %s" % (
            self._mode,
            self.timedelta.in_words(),
        )
