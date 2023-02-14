#!/usr/bin/env python3

from bisect import bisect_left
from datetime import datetime
import re
from urllib.request import urlopen

URL = "https://s3.amazonaws.com/tcmg476/http_access_log"
CACHED_LOG_FILENAME = "parsing_log_1"
APACHE_LOG_DATE_FORMAT = "%d/%b/%Y:%H:%M:%S %z"
OUTPUT_DATE_FORMAT = "%Y-%m-%d"

# log format:
# hostname( identity)? userid [DD/Mon/YYYY:HH:MM:SS timezone_offset] "request" status response_size
# Note that this skips malformed lines like the one on line 604735 in the
# example file. Unfortunately, this causes the regex to be much longer than
# it needs to be. I'd love to write a proper parser for this, but the
# prospect of doing so in a dynamically typed language scares me a little.
APACHE_LOG_REGEX = r"^([_0-9.A-Za-z-]+)(?: ([_0-9.A-Za-z-]+))? ([_0-9.A-Za-z-]+) \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\] \"(.*)\" (\d{3}) ([0-9-]+)$"

class LogLine:
    def __init__(self, match):
        # matches are effectively 1-indexed; 0 refers to the entire match
        self.hostname = match.group(1)
        self.identity = match.group(2)
        self.user_id = match.group(3)
        self.date = datetime.strptime(match.group(4), APACHE_LOG_DATE_FORMAT)
        self.request = match.group(5)
        self.status_code = match.group(6)
        self.response_size = match.group(7)

    @staticmethod
    def read_many_from(log_contents):
        """Parses each valid line in `log_contents` into a list of `LogLine`s."""
        log_matches = re.finditer(APACHE_LOG_REGEX, log_contents, re.MULTILINE)
        return [LogLine(log_match) for log_match in log_matches]

def main():
    file_contents = ""

    try:
        # Counterintuitively, brazenly trying to open the file and handling any
        # resultant errors is better than checking if the file exists. Checking
        # for existence before opening the file could lead to a time-of-check
        # to time-of-use bug.
        with open(CACHED_LOG_FILENAME, "r") as log:
            file_contents = log.read()
    except FileNotFoundError:
        with open(CACHED_LOG_FILENAME, "w") as log:
            r = urlopen(URL)
            # When reading a file, Python automatically decodes the data from
            # UTF-8 and normalizes all Windows-style newlines to Unix-style
            # newlines. Here, we have to do it ourselves.
            file_contents = r.read().decode().replace("\r\n", "\n")
            log.write(file_contents)

    log_lines = LogLine.read_many_from(file_contents)
    # This will allow us to search for the first date from six months ago
    log_lines.sort(key=lambda line: line.date)
    last_date = log_lines[-1].date
    # This isn't exactly 6 months, but it's probably close enough
    six_months_before_last = last_date.replace(month=last_date.month - 6)

    # Find where `six_months_before_last` would be if it was in the list.
    # If it's already in the list, this finds its first occurence.
    # This is equivalent to searching for the earliest date in the last six
    # months.
    six_months_index = bisect_left(
        log_lines,
        six_months_before_last,
        key=lambda line: line.date,
    )
    last_six_months = log_lines[six_months_index:]

    first_date_str = log_lines[0].date.strftime(OUTPUT_DATE_FORMAT)
    last_six_months_str = six_months_before_last.strftime(OUTPUT_DATE_FORMAT)
    last_date_str = last_date.strftime(OUTPUT_DATE_FORMAT)

   #Iterating for redirect request and Error Request
    Redirect_count = 0
    Error_count = 0

    for line in log_lines:
        if line.status_code[0] == "3":
            Redirect_count += 1
        elif line.status_code[0] == "4":
            Error_count += 1


    #Dictionary to store requests and how often they appear
    requests_dict = {}

    #basically copied from the example in general
    #iterates through every request, if it exists it adds one, if not the request is added
    #to the dictionary
    for line in log_lines:
        if line.request in requests_dict:
            requests_dict[line.request] +=1
        else:
            requests_dict[line.request] = 1

    #goes through the dictionary. If the number of requests is higher than max_requests,
    #that number is stored in max_requests and the name in max_requests_name
    max_requests = 0
    max_requests_name = 'start'
    for request in requests_dict:
        if requests_dict[request] > max_requests:
            max_requests = requests_dict[request]
            max_requests_name = request

    min_requests = 2000
    min_requests_name = 'start'
    for request in requests_dict:
        if requests_dict[request] < min_requests:
            min_requests = requests_dict[request]
            min_requests_name = request


    total_responses = len(log_lines)
    print(f"Between {first_date_str} and {last_date_str}, there were {len(log_lines)} requests made to our website")
    print(f"In the last six months ({last_six_months_str} - {last_date_str}), there were {len(last_six_months)} requests made to our website")
    print("Total number of redirects:", Redirect_count)
    print("Percentage of redirect request: {0:.1%}".format(Redirect_count/total_responses))
    print("Total number of Errors:", Error_count)
    print("Percentage of error request: {0:.1%}".format(Error_count/total_responses))
    print(f"The most requested file: {max_requests_name}")
    print(f"The least requested file: {min_requests_name}")

if __name__ == "__main__":
    main()
