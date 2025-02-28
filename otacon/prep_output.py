import csv
from datetime import datetime
import argparse
from typing import TextIO
from pathvalidate import sanitize_filename


def assemble_outfile_name(args: argparse.Namespace, month) -> str:
    """
    Assemble the outfile name out of the search parameters in human-readable and sanitized form.
    Full path is returned.
    """
    outfile_name = "comment_extraction" if args.searchmode == 'comms' else 'submissions_extraction'

    # add user/subreddit if provided
    if args.name is not None:
        if len(args.name) < 5:
            outfile_name += "_from_" + args.src + "_" + ';'.join(args.name)
        else:
            outfile_name += "_from_" + args.src + "_" + ';'.join(list(args.name)[:5])
    # add timeframe info
    # this allows for the name to make sense with any or both of the timeframe bounds absent or present
    if month is not None:
        outfile_name += "_from_" + month
    else:
        if args.time_from is not None:
            outfile_name += "_from_" + str(args.time_from[0]) + '-' + str(args.time_from[1])
        if args.time_to is not None:
            outfile_name += "_up_to_" + str(args.time_to[0]) + '-' + str(args.time_to[1])
    # add optional filters
    if args.popularity is not None:
        outfile_name += "_score_over_" + str(args.popularity)
    if args.toplevel:
        outfile_name += "_toplevel-only_"
    if args.sample:
        outfile_name += "_sample-" + str(args.sample) + "_"
    # add time of search
    outfile_name += "_executed-at_" + datetime.now().strftime('%Y-%m-%d_at_%Hh-%Mm-%Ss')
    # sanitize to avoid illegal filename characters
    outfile_name = sanitize_filename(outfile_name)
    # specify the month of the reddit data
    outfile_name = outfile_name + "_" + month if month is not None else outfile_name
    # add file ending
    outfile_name += ".csv" if not args.return_all else ".jsonl"

    return outfile_name


def write_csv_headers(outfile_path: str, reviewfile_path: TextIO):
    """Write the headers for both the results file and the file for filtered-out hits."""
    with open(outfile_path, 'a', encoding='utf-8') as outf, open(reviewfile_path, "a", encoding='utf-8') as reviewf:
        headers = ['text', 'span', 'subreddit', 'score', 'user', 'flairtext', 'date', 'permalink']
        csvwriter = csv.writer(outf, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(headers)

        headers.append('filter reason')
        csvwriter = csv.writer(reviewf, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(headers)