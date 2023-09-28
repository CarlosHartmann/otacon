'''
otacon: Extracts Reddit comments from the offline version of the Pushshift Corpus dumps (see README for further info)

Usage:
Basic command:
poetry run python path/to/otacon/otacon/main.py

Required args:
--input or -I: path to directory containing the Pushshift data dumps
--output or -O: desired output path

Optional args: (supplying at least one of them is however required)
--time-from or -F: earliest month to extract from in YYYY-MM format
--time-to or -T: latest month to extract form in YYYY-MM format
--src or -S: source to extract from, either "subreddit" or "user"
--name or -N: name of the source to extract from
--regex or -R: regular expression to use for matching
--popularity or -P: minimum voting score threshold for extracted comments
--toplever or -TL: only extract top-level comments

Soft filters for:
profanity
bot-generated comments

Hard filters for:
Regex match inside a quoted line
Duplicates

Output:
CSV file with search parameters and time of execution in the filename.
Includes span (for regex matches), subreddit, score, user, flairtext, date, and permalink as metadata.
Soft-filtered comments are included in a separate file with their respective filtering reason.

'''

import os
import re
import csv
import json
import logging
import calendar
import argparse
from datetime import datetime
from typing import TextIO

import spacy
from zstandard import ZstdDecompressor
#from profanity_filter import ProfanityFilter
from pathvalidate import sanitize_filename

# load SpaCy and add the profanity-filter module as a SpaCy pipeline
#nlp = spacy.load('en_core_web_sm')
#profanity_filter = ProfanityFilter(nlps={'en': nlp})
#nlp.add_pipe(profanity_filter.spacy_component, last=True)

# keep track of already-processed comments throughout function calls
hash_list = []


def find_all_matches(text, regex):
    """Iterate through all regex matches in a text, yielding the span of each as tuple."""
    r = re.compile(regex)
    for match in r.finditer(text):
        yield (match.start(), match.end())


def inside_quote(text: str, span: tuple) -> bool:
    """
    Test if a span-marked match is inside a quoted line.
    Such lines in Reddit data begin with "&gt;".
    """
    end = span[1]
    relevant_text = text[:end]
    return True if re.search('&gt;[^\n]+$', relevant_text) else False # tests if there is no linebreak between a quote symbol and the match


def extract(comment: dict, regex: str, outfile: TextIO, filter_reason: str):
    """
    Extract a comment text and all relevant metadata.
    If no regex is supplied, extract the whole comment leaving the span field blank.
    If a regex is supplied, extract each match separately with its span info.
    Discard regex matches found inside of a quoted line.
    """
    text = comment['body']
    user = comment['author']
    flairtext = comment['author_flair_text']
    subreddit = comment['subreddit']
    score = comment['score']
    date = comment['created_utc']
    
    # assemble a standard Reddit URL for older data
    url_base = "https://www.reddit.com/r/"+subreddit+"/comments/"
    oldschool_link = url_base + comment['link_id'].split("_")[1] + "//" + comment['id']

    # choose the newer "permalink" metadata instead if available
    permalink = "https://www.reddit.com" + comment['permalink'] if 'permalink' in comment.keys() else oldschool_link

    csvwriter = csv.writer(outfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

    if regex is None:
        span = None
        row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
        csvwriter.writerow(row)
    else:
        for span in find_all_matches(text, regex):
            if not inside_quote(text, span):
                span = str(span)
                row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
                csvwriter.writerow(row)


def filter(comment: dict, popularity_threshold: int) -> tuple:
    """
    Test if a Reddit comment breaks any of the filtering rules.
    This is for nuanced criteria so positives are kept for manual review.
    """
    if popularity_threshold is not None:
        if comment['score'] < popularity_threshold:
            return True, "score below defined threshold"
    
    text = comment['body']
    #if nlp(text)._.is_profane:
    #    return True, "offensive language"

    if "i'm a bot" in text.lower():
        return True, "non-human generated"
    
    return False, None


def relevant(comment: dict, args: argparse.Namespace) -> bool:
    """
    Test if a Reddit comment is at all relevant to the search.
    This is for broad criteria so negatives are discarded.
    The filters are ordered by how unlikely they are to pass for efficiency.
    """
    if args.name is not None: # if a subreddit or user was specified as argument, the comment's metadata are checked accordingly
        src = 'author' if args.src == 'user' else 'subreddit' # the username is called 'author' in the data
        if comment[src] != args.name: 
            return False
    
    if args.toplevel and not comment['parent_id'].startswith('t3'):
        return False

    if args.regex is None: # if no regex is specified, every comment is relevant at this point
        pass
    elif re.search(args.regex, comment['body']): # checks if regex matches at least once, matches are extracted later
        pass
    else:
        return False
    
    h = hash(json.dumps(comment, sort_keys=True)) # dicts are unhashable, their original json form is preferrable
    if h in hash_list: # hash check with all previous comments in case the data contain redundancies
        return False
    else:
        hash_list.append(h)
        return True



def write_csv_headers(outfile_path: str, reviewfile_path: TextIO):
    """Write the headers for both the results file and the file for filtered-out hits."""
    with open(outfile_path, 'a', encoding='utf-8') as outf, open(reviewfile_path, "a", encoding='utf-8') as reviewf:
        headers = ['text', 'span', 'subreddit', 'score', 'user', 'flairtext', 'date', 'permalink']
        csvwriter = csv.writer(outf, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(headers)

        headers.append('filter reason')
        csvwriter = csv.writer(reviewf, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(headers)


def read_redditfile(file: str) -> dict:
    """
    Iterate over the pushshift JSON lines, yielding them as Python dicts.
    Decompress iteratively if necessary.
    """
    # older files in the dataset are uncompressed while newer ones use zstd compression and have .xz, .bz2, or .zst endings
    if not file.endswith('.bz2') and not file.endswith('.xz') and not file.endswith('.zst'):
        with open(file, 'r', encoding='utf-8') as infile:
            for line in infile:
                l = json.loads(line)
                yield(l)
    else:
        for comment, some_int in read_lines_zst(file):
            yield json.loads(comment)

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		logging.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close() 



def within_timeframe(month: str, time_from: tuple, time_to: tuple) -> bool:
    """Test if a given month from the Pushshift Corpus is within the user's provided timeframe."""
    # a month's directory name has the format "RC YYYY-MM"
    y = int(month.split(" ")[1].split("-")[0])
    m = int(month.split("-")[1])

    if time_from is not None:
        from_year, from_month = time_from[0], time_from[1]

        if y < from_year:
            return False
        if y == from_year and m < from_month:
            return False
    
    if time_to is not None:
        to_year, to_month= time_to[0], time_to[1]

        if y > to_year:
            return False
        if y == to_year and m > to_month:
            return False
    
    return True


def fetch_data_timeframe(input_dir: str) -> tuple:
    """
    Establish a timeframe based on all directories found in the input directory.
    Used when no timeframe was given by user.
    """
    months = [elem.replace("RC ", "") for elem in os.listdir(input_dir) if elem.startswith('RC')]
    months = sorted(months)
    months = [(int(elem.split("-")[0]), int(elem.split("-")[1])) for elem in months]
    return months[0], months[-1]


def establish_timeframe(time_from: tuple, time_to: tuple, input_dir: str) -> list:
    """Return all months of the data within a timeframe as list of directories."""
    months = [elem for elem in os.listdir(input_dir) if elem.startswith("RC")] # all available months in the input directory

    return sorted([month for month in months if within_timeframe(month, time_from, time_to)])


def valid_date(string) -> tuple:
    """
    Check if a given date follows the required formatting and is valid.
    Returns a (year, month) tuple.
    Used as ArgParser type.
    """
    if re.search('^20[012]\d\-0?\d[012]?$', string):
        year, month = int(string.split("-")[0]), int(string.split("-")[1])
        if month > 12 or month < 1:
            msg = f"not a valid month: {month}"
            raise argparse.ArgumentTypeError(msg)
        else:
            return (year, month)
    else:
        msg = f"not a valid date: {string}"
        raise argparse.ArgumentTypeError(msg)


def dir_path(string) -> str:
    """
    Test if a given path exists on the machine.
    Used as ArgParser type.
    """
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)


def assemble_outfile_name(args: argparse.Namespace) -> str:
    """
    Assemble the outfile name out of the search parameters in human-readable and sanitized form.
    Full path is returned.
    """
    outfile_name = "comment_extraction"
    # add regex input
    if args.regex is not None:
        outfile_name += "_matching_'" + args.regex
    else:
        outfile_name += "_"
    # add user/subreddit if provided
    if args.name is not None:
        outfile_name += "_from_" + args.src + "_" + args.name
    # add timeframe info
    # this allows for the name to make sense with any or both of the timeframe bounds absent or present
    if args.time_from is not None:
        outfile_name += "_from_" + str(args.time_from[0]) + '-' + str(args.time_from[1])
    if args.time_to is not None:
        outfile_name += "_up_to_" + str(args.time_to[0]) + '-' + str(args.time_to[1])
    # add optional filters
    if args.popularity is not None:
        outfile_name += "_score_over_" + str(args.popularity)
    if args.toplevel:
        outfile_name += "_toplevel-only_"
    # add time of search
    outfile_name += "_" + datetime.now().strftime('%Y-%m-%d_at_%Hh-%Mm-%Ss')
    # sanitize to avoid illegal filename characters
    outfile_name = sanitize_filename(outfile_name)
    # add path and file ending
    outfile = args.output + "/" + outfile_name + ".csv"

    return outfile



def define_parser() -> argparse.ArgumentParser:
    """Define console argument parser."""
    parser = argparse.ArgumentParser(description="Keyword search comments from the Pushshift data dumps")

    # directories
    parser.add_argument('--input', '-I', type=dir_path, required=True,
                        help="The directory containing the input data, ie. the Pushshift data dumps.")
    parser.add_argument('--output', '-O', type=dir_path, required=True,
                        help="The directory where search results will be saved to.")
    
    # timeframe
    parser.add_argument('--time_from', '-F', type=valid_date, required=False,
                        help="The beginning of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no lower bound.")
    parser.add_argument('--time_to', '-T', type=valid_date, required=False,
                        help="The end of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no upper bound.")
    
    # search parameters
    parser.add_argument('--src', '-S', choices=['user', 'subreddit'], required=False,
                        help="The source of the comments, can either be 'user' or 'subreddit'.")
    parser.add_argument('--name', '-N', required=False,
                        help="The name of the user or subreddit to be searched. If absent, every comment will be searched.")
    parser.add_argument('--regex', '-R', required=False,
                        help="The regex to search the comments with. If absent, all comments matching the other parameters will be extracted.")
    parser.add_argument('--popularity', '-P', type=int, required=False,
                        help="Popularity threshold: Filters out comments with a score lower than the given value.")
    parser.add_argument('--toplevel', '-TL', action='store_true', required=False,
                        help="Only consider top-level comments, ie. comments not posted as a reply to another comment, but directly to a post.")
    
    # special
    parser.add_argument('--count', '-C', action='store_true',
                        help="Only counts the relevant comments per month and prints the statistic to console.")

    return parser


def handle_args() -> argparse.Namespace:
    """Handle argument-related edge cases by throwing meaningful errors."""
    parser = define_parser()
    args = parser.parse_args()

    # all search parameters are optional to allow for different types of searches
    # should they all be missing, it would lead to data overflow as every comment would be extracted
    # this ignores the popularity and toplevel arguments because they alone would still lead to overflow
    if args.time_from is None and args.time_to is None and args.src is None and args.regex is None:
        parser.error("Not enough parameters supplied. Search would return too many comments.")

    # the 'src' argument is required if 'name' is given, however both are optional
    if args.name is not None and args.src is None:
        parser.error("argument --name requires argument --src also be given.")
    # the 'name' argument is required if 'src' is given, however both are optional
    elif args.src is not None and args.name is None:
        parser.error("argument --src requires argument --name also be given.")

    # ensure that the timeframe makes sense (either the from-year is later than to-year, or the from-month is later than to-month in the same year)
    # only necessary if both endpoints are given
    if args.time_from is not None and args.time_to is not None:
        if args.time_from[0] > args.time_to[0] or (args.time_from[0] == args.time_to[0] and args.time_from[1] > args.time_to[1]):
            parser.error("argument --time_from is later than --time_to")
    # if no timeframe is given, all available months are searched
    elif args.time_from is None and args.time_to is None:
        logging.info("No timeframe supplied. Searching all months found in the input directory.")
        args.time_from, args.time_to = fetch_data_timeframe(args.input)
    
    return args



def log_month(month: str):
    """Send a message to the log with a month's real name for better clarity."""
    year = month.replace("RC ", "").split("-")[0] # get year string from the format 'RC YYYY-MM'
    m_num = int(month.split("-")[1]) # get month integer
    m_name = calendar.month_name[m_num]

    logging.info("Processing " + m_name + " " + year)
    

def get_data_file(path: str) -> str:
    """
    Find the correct file type of each month directory.
    Files can be plain, zst, xz, or bz2.
    Throw error if no usable file is present in directory.
    """
    for ending in ['', '.zst', '.xz', '.bz2']:
        if os.path.isfile(path+ending):
            return path+ending
    logging.warning("Month directory " + dir + " does not contain a valid data dump file.")
    exit()



def main():
    logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')

    args = handle_args()
    timeframe = establish_timeframe(args.time_from, args.time_to, args.input)
    logging.info(str(len(timeframe)) + " month(s) in given timeframe. Check if any month is missing in the data if the number does not make sense.")
    
    # prepare the output and review files (for results and filtered-out comments)
    outfile = assemble_outfile_name(args)
    reviewfile = outfile[:-4] + "_filtered-out_matches.csv"
    if not args.count:
        write_csv_headers(outfile, reviewfile)

    # instantiate counter
    if args.count:
        counter = 0

    for month in timeframe:
        # reset counter for each month
        if args.count and month is not timeframe[0]:
            print(counter, "relevant comments found.")
            counter = 0

        log_month(month)
        infile_dir = args.input + "/" + month + "/"
        infile_name = month.replace(" ", "_") # the files have an underscore where the directories have a space char
        infile = get_data_file(infile_dir+infile_name)
        for comment in read_redditfile(infile):
            if relevant(comment, args):
                if not args.count:
                    # files are repeatedly opened and closed to enable the user to live-monitor the results
                    with open(outfile, "a", encoding="utf-8") as outf, \
                        open(reviewfile, "a", encoding="utf-8") as reviewf:

                        filtered, reason = filter(comment, args.popularity)
                        if not filtered:
                            extract(comment, args.regex, outf, filter_reason=None)
                        else:
                            extract(comment, args.regex, reviewf, filter_reason=reason)
                elif args.count:
                    counter += 1

    # print the count for the last month
    if args.count:
        print(counter, "relevant comments found.")


if __name__ == "__main__":
    main()