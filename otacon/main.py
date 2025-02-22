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
import pprint
import random
import logging
import calendar
import argparse
from datetime import datetime
from typing import TextIO

from zstandard import ZstdDecompressor
#from profanity_filter import ProfanityFilter
from pathvalidate import sanitize_filename

from otacon.finalize import *

# keep track of already-processed comments throughout function calls
hash_list = []

# return stats from which subreddits the relevant comments were and how many per subreddits
stats_dict = {}


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


def extract(args, comment_or_post: dict, regex: str, include_quoted: bool, outfile: TextIO, filter_reason: str):
    """
    Extract a comment or post text and all relevant metadata.
    If no regex is supplied, extract the whole comment leaving the span field blank.
    If a regex is supplied, extract each match separately with its span info.
    Discard regex matches found inside of a quoted line.
    """
    
    if args.return_all:
        comment_or_post = json.dumps(comment_or_post)
        _=outfile.write(comment_or_post+'\n')
    
    else:
        text = comment_or_post['body'] if args.searchmode == 'comms' else comment_or_post['selftext']
        user = comment_or_post['author']
        flairtext = comment_or_post['author_flair_text']
        subreddit = comment_or_post['subreddit']
        score = comment_or_post['score']
        date = comment_or_post['created_utc']
        
        # assemble a standard Reddit URL for older data
        url_base = "https://www.reddit.com/r/"+subreddit+"/comments/"
        
        oldschool_link = url_base + comment_or_post['link_id'].split("_")[1] + "//" + comment_or_post['id'] if 'link_id' in comment_or_post.keys() else None

        # choose the newer "permalink" metadata instead if available
        permalink = "https://www.reddit.com" + comment_or_post['permalink'] if 'permalink' in comment_or_post.keys() else oldschool_link

        csvwriter = csv.writer(outfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if regex is None:
            span = None
            row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
            csvwriter.writerow(row)
        else:
            for span in find_all_matches(text, regex):
                if not include_quoted and not inside_quote(text, span):
                    span = str(span)
                    row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
                    csvwriter.writerow(row)


def filter(comment_or_post: dict, popularity_threshold: int) -> tuple:
    """
    Test if a Reddit comment_or_post breaks any of the filtering rules.
    This is for nuanced criteria so positives are kept for manual review.
    """
    if popularity_threshold is not None:
        if comment_or_post['score'] < popularity_threshold:
            return True, "score below defined threshold"
    if 'body' in list(comment_or_post.keys()):
        text = comment_or_post['body']
    else:
        text = comment_or_post['selftext']

    if "i'm a bot" in text.lower():
        return True, "non-human generated"
    
    return False, None


def relevant(comment_or_post: dict, args: argparse.Namespace) -> bool:
    """
    Test if a Reddit comment or post is at all relevant to the search.
    This is for broad criteria so negatives are discarded.
    The filters are ordered by how unlikely they are to pass for efficiency.
    """
    if args.name is not None: # if a subreddit or user was specified as argument, the comment's metadata are checked accordingly
        src = 'author' if args.src == 'user' else 'subreddit' # the username is called 'author' in the data
        
        if src not in comment_or_post.keys() or comment_or_post[src].lower() not in args.name: 
            return False
    
    if args.toplevel and not comment_or_post['parent_id'].startswith('t3'):
        return False
    
    regex = args.commentregex if args.searchmode == 'comms' else args.postregex
    body = 'body' if args.searchmode == 'comms' else 'selftext'

    if regex is not None:
        search = re.search(regex, comment_or_post[body]) if args.case_sensitive else re.search(regex, comment_or_post[body], re.IGNORECASE)
        if search: # checks if comment regex matches at least once, matches are extracted later
            pass
        else:
            return False
    
    if args.flairregex is not None:
        if comment_or_post['author_flair_text'] is None:
            return False
        else:
            search = re.search(args.flairregex, comment_or_post['author_flair_text']) if args.case_sensitive else re.search(args.flairregex, comment_or_post['author_flair_text'], re.IGNORECASE)
            return True if search else False
    
    if args.userregex is not None:
        search = re.search(args.userregex, comment_or_post['author']) if args.case_sensitive else re.search(args.userregex, comment_or_post['author'], re.IGNORECASE)
        return True if search else False


    if args.spacy_search:
        token = args.spacy_search[0]
        pos = args.spacy_search[1]
        text = comment_or_post[body]
        if token in text:
            doc = args.nlp(text)
            tk_list = [(elem.text.lower(), elem.pos_) for elem in doc]
            if (token.lower(), pos) in tk_list:
                pass
            else:
                return False
        else:
            return False

    
    h = hash(json.dumps(comment_or_post, sort_keys=True)) # dicts are unhashable, their original json form is preferrable
    if h in hash_list: # hash check with all previous comments/posts in case the data contain redundancies
        return False
    else:
        hash_list.append(h)
        stats_dict.setdefault(comment_or_post['subreddit'], 0)
        stats_dict[comment_or_post['subreddit']] += 1  
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
        for comment_or_post, some_int in read_lines_zst(file):
            try:
                yield json.loads(comment_or_post)
            except json.decoder.JSONDecodeError:
                print(f"Encountered a ZST parsing error in {file}")
                pass

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
    month = re.sub('\.\w+$', '', month) # remove file ending
    y = int(month.split("_")[1].split("-")[0])
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
    months = [elem.replace("RC_", "") for elem in os.listdir(input_dir) if not elem.endswith(".txt")]
    months = [elem.replace("RS_", "") for elem in months]
    months = [elem.replace(".zst", "") for elem in months if elem.endswith('.zst')]

    months = sorted(months)
    months = [(int(elem.split("-")[0]), int(elem.split("-")[1])) for elem in months]
    return months[0], months[-1]


def establish_timeframe(time_from: tuple, time_to: tuple, input_dir: str) -> list:
    """Return all months of the data within a timeframe as list of directories."""
    months = [elem for elem in os.listdir(input_dir) if elem.startswith("RC") or elem.startswith("RS")] # all available months in the input directory

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


def sample_float(num) -> float:
    try:
        num = float(num)
    except:
        raise TypeError(f"{num} is not a recognized number format.")
    
    if num > 1.0 or num < 0:
        raise TypeError("Sample size must be given as number between 0.0 and 1.0")
    
    return num


def comment_regex(string) -> str:
    """
    Some modifications for supplied regexes.
    Currently just to allow for quoted blocks to come at the beginning if the supplied regex asks for regex matches at the beginning of comments via ^
    Also allows file paths.
    """
    
    if os.path.isfile(string):
        regex = open(string, "r", encoding="utf-8").read()
    else:
        regex = string

    initial_regex_tester = "^((?:\(\?<[=!].*?\)))?(\^)" # to check if expression has ^ at beginning, while also allow for look-behind statements that can contain ^

    if re.search(initial_regex_tester, regex):
        flag = re.search(f'{initial_regex_tester}(.+$)', regex).group(1) # in case there is a flag of the type (?i) at the start
        flag = '' if flag is None else flag

        expr = re.search(f'{initial_regex_tester}(.+$)', regex).group(3)
        
        regex = flag+ '^' + r'(>.+\n\n)*' + expr
        logging.info(f"Regex changed to {regex}")
    elif regex == '':
        logging.info("Regex is empty. Either argument value was forgotten or supplied filepath does not exist.")
        exit()
    else:
        logging.info(f"Supplied regex: {regex}")
        logging.info(f"If this regex is a filepath but you intended to use the contents of that file, check the path and that the file exists.")
    

    return regex


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


def pos_tuple(text):
    tk, pos = text.split(',')[0], text.split(',')[1]
    return (tk, pos)


def define_parser() -> argparse.ArgumentParser:
    """Define console argument parser."""
    parser = argparse.ArgumentParser(description="Keyword search comments from the Pushshift data dumps")

    # directories
    parser.add_argument('--input', '-I', type=dir_path, required=True,
                        help="The directory containing the input data, ie. the Pushshift data dumps.")
    parser.add_argument('--output', '-O', type=dir_path, required=False,
                        help="The directory where search results will be saved to.")
    
    # timeframe
    parser.add_argument('--time_from', '-F', type=valid_date, required=False,
                        help="The beginning of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no lower bound.")
    parser.add_argument('--time_to', '-T', type=valid_date, required=False,
                        help="The end of the timeframe to be searched, in the format YYYY-MM. If absent, a timeframe is assumed with no upper bound.")
    
    # search parameters
    parser.add_argument('--src','--source' '-S', choices=['user', 'subreddit'], required=False,
                        help="The source of the comments, can either be 'user' or 'subreddit'.")
    parser.add_argument('--name', '-N', action='append', required=False,
                        help="The name of the user(s) or subreddit(s) to be searched. If absent, every comment will be searched.")
    parser.add_argument('--commentregex', '-CR', type=comment_regex, required=False,
                        help="The regex to search the comments with. If absent, all comments matching the other parameters will be extracted. Can be a filepath of a file that contains the regex.")
    parser.add_argument('--flairregex', '-FR', type=comment_regex, required=False,
                        help="The regex to search the comment flairs with. If absent, all comments matching the other parameters will be extracted. Can be a filepath of a file that contains the regex.")
    parser.add_argument('--postregex', '-PR', type=comment_regex, required=False, 
                        help="The regex to search the post text with. Will only be used if the source is identified to contain the word 'submissions.'")
    parser.add_argument('--userregex', '-UR', type=comment_regex, required=False,
                        help="The regex to search the user names with. If absent, all comments matching the other parameters will be extracted. Can be a filepath of a file that contains the regex.")
    parser.add_argument('--case-sensitive', '-CS', action='store_true',
                        help="Makes search case-sensitive if any regex (comment or flair) was supplied.")
    parser.add_argument('--popularity', '-P', type=int, required=False,
                        help="Popularity threshold: Filters out comments with a score lower than the given value.")
    parser.add_argument('--toplevel', '-TL', action='store_true', required=False,
                        help="Only consider top-level comments, ie. comments not posted as a reply to another comment, but directly to a post.")
    parser.add_argument('--spacy-search', '-SS', type=pos_tuple,
                        help="Supply a token with expected POS tag to search how often this token is found with that POS-tag. Requires language specification")
    parser.add_argument('--language', '-L', required=False,
                        help="Language to be used for spacy search.")
    
    # special
    parser.add_argument('--count', '-C', action='store_true',
                        help="Only counts the relevant comments per month and prints the statistic to console.")
    parser.add_argument('--include_quoted', action='store_true',
                        help="Include regex matches that are inside Reddit quotes (lines starting with >, often but not exclusively used to quote other Reddit users)")
    parser.add_argument('--sample', '-SMP', type=sample_float, required=False,
                        help="Retrieve a sample of results fitting the other parameters. Sample size is given as float between 0.0 and 1.0 where 1.0 returns 100% of results")
    parser.add_argument('--return_all', action='store_true', required=False,
                        help="Will return every search hit in its original and complete JSON form.")
    parser.add_argument('--dont_filter', action='store_true', required=False,
                        help="Skip any filtering.")
    parser.add_argument('--reverse_order', action='store_true', required=False,
                        help="Iterate through the relevant months in reverse order, i.e. from most recent to oldest.")
    parser.add_argument('--no_cleanup', action='store_true', required=False,
                        help="Will skip the cleanup (amassing results in a single file) at the end.")

    return parser


def handle_args() -> argparse.Namespace:
    """Handle argument-related edge cases by throwing meaningful errors."""
    parser = define_parser()
    args = parser.parse_args()

    if args.output is None and not args.count:
        parser.error("Since you're not just counting, you need to supply an output directory.")

    # all search parameters are optional to allow for different types of searches
    # should they all be missing, it would lead to data overflow as every comment would be extracted
    # this ignores the popularity and toplevel arguments because they alone would still lead to overflow
    if args.time_from is None and args.time_to is None and args.src is None and args.commentregex is None and args.flairregex is None and args.userregex is None:
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
    
    if args.spacy_search and not args.language:
        parser.error("You did not supply a language for the SpaCy search.")

    # makes checking slightly more efficient
    if args.name is not None:
        args.name = [elem.lower() for elem in args.name]
        args.name = set(args.name)
    
    if 'submissions' in args.input:
        args.searchmode = 'subs'
    elif 'comments' in args.input:
        args.searchmode = 'comms'
    else:
        args.searchmode = 'comms' # at least for now, no third search mode implemented, defaults to comments

    return args


def log_month(month: str):
    """Send a message to the log with a month's real name for better clarity."""
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = month.split("-")[0] # get year string from the format 'RC_YYYY-MM.zst'
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


def get_samplesize(month: str, sample_proportion: float, input_dir) -> int:
    "Get the number of data points that shall be considered based on the provided %"
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = month.split("-")[0] # get year string from the format 'RC_YYYY-MM.zst'
    m_num = month.split("-")[1] # get month number as string

    countfile = os.path.join(input_dir, "monthly-counts.txt")
    with open(countfile, "r") as infile:
        reader = csv.reader(infile, delimiter='\t')
        for row in reader:
            y, m, count = row[0], row[1], int(row[2])
            if y == year and m == m_num:
                samplesize = round(sample_proportion * count)
                logging.info(f"With a sample proportion of {sample_proportion}, {samplesize} comments will be looked at.")
                return samplesize, count


def get_samplepoints(month, sample_proportion, input_dir):
    "Find the needed samplesize and then generate a list of indexes to consider"
    samplesize, count = get_samplesize(month, sample_proportion, input_dir)
    all_data_points = list(range(0, count))
    sample = random.sample(all_data_points, samplesize)
    return sorted(sample)


def process_month(month, args, outfile, reviewfile):
    log_month(month)
    relevant_count = 0
    total_count = -1
    infile = args.input + "/" + month

    if args.sample:
        sample_points = get_samplepoints(month, args.sample, args.input)

    for comment_or_post in read_redditfile(infile):
        total_count += 1
        if not args.sample or (args.sample and total_count == sample_points[0]):

            if args.sample:
                del sample_points[0]
                if len(sample_points) == 0:
                    break
            
            if relevant(comment_or_post, args):
                relevant_count += 1
                if not args.count:
                    with open(outfile, "a", encoding="utf-8") as outf, \
                            open(reviewfile, "a", encoding="utf-8") as reviewf:
                        
                        filtered, reason = filter(comment_or_post, args.popularity) if not args.dont_filter else False, None
                        if not filtered:
                            extract(args, comment_or_post, args.commentregex, args.include_quoted, outf, filter_reason=None)
                        else:
                            extract(args, comment_or_post, args.commentregex, args.include_quoted, reviewf, filter_reason=reason)
        
    
    if args.count:
        return relevant_count


def fetch_model(lang):
    if lang.lower() == "german" or lang.lower() == "deutsch":
        return 'de_dep_news_trf'
    else:
        logging.info("Only German spacy models are currently installed.")
        exit()


def main():
    logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')
    args = handle_args()
    timeframe = establish_timeframe(args.time_from, args.time_to, args.input)
    if args.reverse_order:
        timeframe.reverse()
    logging.info(f"Searching from {timeframe[0]} to {timeframe[-1]}")

    if args.spacy_search:
        logging.info("Importing spacy…")
        import spacy
        model = fetch_model(args.language)
        logging.info(f"Importing {model}…")
        nlp = spacy.load(model)
        args.nlp = nlp

    if not args.count:
        args.output = os.path.abspath(args.output)

    # Writing the CSV headers
    if not args.count:
        for month in timeframe:
            outfile = assemble_outfile_name(args, month)
            outfile = os.path.join(args.output, outfile)
            reviewfile = outfile[:-4] + "_filtered-out_matches.csv" if not args.return_all else outfile[:-4] + "_filtered-out_matches.jsonl"
            reviewfile = os.path.join(args.output, reviewfile)
            if not args.return_all:
                write_csv_headers(outfile, reviewfile)
            process_month(month, args, outfile, reviewfile)
    elif args.count:
        total_count = 0
        for month in timeframe:
            count = process_month(month, args, outfile=None, reviewfile=None)
            logging.info(f"{count} instances for {month}")
            total_count += count
            if args.output:
                stats_file = os.path.join(args.output, f'otacon_search_stats_{month}.txt')
                with open(stats_file, "w") as outfile:
                    _=outfile.write(json.dumps(stats_dict))
        logging.info(f"{total_count} total instances")

    if not args.count and not args.no_cleanup:
        cleanup(args.output, extraction_name=assemble_outfile_name(args, month=None))

    if args.output:
        stats_file = os.path.join(args.output, 'otacon_search_stats.txt')
        with open(stats_file, "w") as outfile:
            _=outfile.write(json.dumps(stats_dict))
    


if __name__ == "__main__":
    main()