#!/usr/bin/env python3

from bisect import bisect_left
from datetime import datetime
import re
import requests

URL = "https://s3.amazonaws.com/tcmg476/http_access_log"
CACHED_LOG_FILENAME = "parsing_log_1"
APACHE_LOG_DATE_FORMAT = "%d/%b/%Y"
OUTPUT_DATE_FORMAT = "%Y-%m-%d"

# log format:
# hostname( identity)? userid [DD/Mon/YYYY:HH:MM:SS -####] "request" status response_size
# Note that this skips malformed lines like the one on line 604735 in the
# example file. Unfortunately, this causes the regex to be much longer than
# it needs to be. I'd love to write a proper parser for this, but the
# prospect of doing so in a dynamically typed language scares me a little.
APACHE_LOG_REGEX = r"^[_0-9.A-Za-z-]+(?: [_0-9.A-Za-z-]+)? [_0-9.A-Za-z-]+ \[(\d{2}\/\w{3}\/\d{4}):\d{2}:\d{2}:\d{2} -\d{4}\] \"[^\"]*\" \d{3} [0-9-]+$"

file_contents = ""

try:
    # Counterintuitively, brazenly trying to open the file and handling any
    # resultant errors is better than checking if the file exists. Checking for
    # existence before opening the file could lead to a time-of-check to
    # time-of-use bug.
    with open(CACHED_LOG_FILENAME, "r") as log:
        file_contents = log.read()
except FileNotFoundError:
    with open(CACHED_LOG_FILENAME, "w") as log:
        r = requests.get(URL, stream=True)
        # When reading a file, Python automatically decodes the data from
        # UTF-8 and normalizes all Windows-style newlines to Unix-style
        # newlines. Here, we have to do it ourselves.
        file_contents = r.content.decode().replace("\r\n", "\n")
        log.write(file_contents)

# Extract the dates from every valid log entry
date_strs = re.findall(APACHE_LOG_REGEX, file_contents, re.MULTILINE)
dates = [datetime.strptime(date, APACHE_LOG_DATE_FORMAT) for date in date_strs]

# This will allow us to binary search for the first date from six months ago
dates.sort()
last_date = dates[-1]
# This isn't exactly 6 months, but it's probably close enough
six_months_before_last = last_date.replace(month=last_date.month - 6)
# Find where `six_months_before_last` would be if it was in the list.
# If it's already in the list, this finds its first occurence.
# This is equivalent to searching for the earliest date in the last six months.
six_months_index = bisect_left(dates, six_months_before_last)
last_six_months = dates[six_months_index:]

first_date_str = dates[0].strftime(OUTPUT_DATE_FORMAT)
last_six_months_str = six_months_before_last.strftime(OUTPUT_DATE_FORMAT)
last_date_str = last_date.strftime(OUTPUT_DATE_FORMAT)

print(f"Between {first_date_str} and {last_date_str}, there were {len(dates)} requests made to our website")
print(f"In the last six months ({last_six_months_str} - {last_date_str}), there were {len(last_six_months)} requests made to our website")
