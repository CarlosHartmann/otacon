'''
otacon: Extracts Reddit comments from the offline version of the Pushshift Corpus dumps (see README for further info)

For Usage and Flags see README doc.
'''

import os
import re
import csv
import json
import random
import datetime
import hashlib
import logging
import calendar
import argparse
from typing import TextIO

from otacon.finalize import cleanup, gather_output_files, extract_time_info
from otacon.argument_handling import define_parser, handle_args
from otacon.pushshift_handling import read_redditfile
from otacon.prep_input import establish_timeframe
from otacon.prep_output import assemble_outfile_name, write_csv_headers
from otacon.sampling import get_samplepoints

# set seeds for reproducibility
random.seed(42)

# keep track of already-processed comments throughout function calls
hash_set = set()

# return stats from which subreddits the relevant comments were and how many per subreddits
stats_dict = {}

# for reservoir sampling
reservoir = []
relevant_count = 0


def find_all_matches(text, regex):
    """Iterate through all regex matches in a text, yielding the span of each as tuple."""
    
    for match in regex.finditer(text):
        yield (match.start(), match.end())


def inside_quote(text: str, span: tuple) -> bool:
    """
    Test if a span-marked match is inside a quoted line.
    Such lines in Reddit data begin with "&gt;".
    """
    end = span[1]
    relevant_text = text[:end]
    return True if re.search('^&gt;[^\n]+$', relevant_text) else False # tests if there is no linebreak between a line-initial quote symbol and the match


def extract(args, comment_or_post: dict, compiled_comment_regex: str, include_quoted: bool, outfile: TextIO, filter_reason: str):
    '''
    Extract and write comment or post data to a CSV file with optional regex filtering.
    This function processes Reddit comment or post data and writes it to the specified output file.
    It handles three modes of operation:
    1. Return all data: If args.return_all is True, writes the entire JSON object as a single line.
    2. Single match mode: If args.firstmatch is True, extracts the first (non-quoted) regex match.
    3. Multiple matches mode: Extracts all matches at the specified index, respecting the include_quoted flag. If sampling is active, only the match at the given index is extracted.
        
    Args:
        args: Arguments object containing:
            - return_all (bool): If True, return the entire comment/post as JSON.
            - searchmode (str): Either 'comms' for comments or 'posts' for posts.
            - firstmatch (bool): If True, extract only the first regex match found.
        comment_or_post (dict): A dictionary containing:
            - 'index' (int): The match index to extract in multiple matches mode.
            - 'entry' (dict): The Reddit comment or post data as a dictionary.
        compiled_comment_regex (str): Compiled regex pattern for matching text. If None, extracts entire text.
        include_quoted (bool): If True, includes matches found inside quoted lines. If False, excludes them.
        outfile (TextIO): Output file object where CSV data will be written.
        filter_reason (str): Reason for filtering, included in output.
    Returns:
        None
    Writes:
        Either a JSON line (if args.return_all) or CSV rows with the following fields:
        type, year, month, id, text, span, subreddit, score, user, flairtext, date, permalink, filter_reason
    Raises:
        SystemExit: If firstmatch mode is enabled and no non-quoted match is found.
    '''
    
    index = comment_or_post['index']
    comment_or_post = comment_or_post['entry']

    if args.return_all:
        comment_or_post = json.dumps(comment_or_post)
        _=outfile.write(comment_or_post+'\n')
    
    else:
        type = 'comment' if args.searchmode == 'comms' else 'post'
        time = datetime.datetime.fromtimestamp(int(comment_or_post['created_utc']), tz=datetime.timezone.utc)
        year, month = time.year, time.month
        month = datetime.datetime.fromtimestamp(int(comment_or_post['created_utc']), tz=datetime.timezone.utc).month
        id = comment_or_post['id']
        text = comment_or_post['body'] if args.searchmode == 'comms' else comment_or_post['selftext']
        user = comment_or_post['author']
        flairtext = comment_or_post['author_flair_text']
        subreddit = comment_or_post['subreddit']
        score = comment_or_post['score']
        date = comment_or_post['created_utc']
        
        # assemble a standard Reddit URL for older data
        url_base = f"https://www.reddit.com/r/{subreddit}/comments/"
        
        oldschool_link = f"{url_base}{comment_or_post['link_id'].split('_')[1]}//{comment_or_post['id']}" if 'link_id' in comment_or_post.keys() else None

        # choose the newer "permalink" metadata instead if available
        permalink = f"https://www.reddit.com{comment_or_post['permalink']}"  if 'permalink' in comment_or_post.keys() else oldschool_link

        csvwriter = csv.writer(outfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if compiled_comment_regex is None:
            span = None
            row = [type, year, month, id, text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
            csvwriter.writerow(row)
        elif args.firstmatch:
            # find first match that is not quoted if not include_quoted
            matches = list(find_all_matches(text, compiled_comment_regex))
            matches = [span for span in matches if not inside_quote(text, span)] if not include_quoted else matches
            span = matches[0] if matches else None
            
            row = [type, year, month, id, text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
            csvwriter.writerow(row)

        else:
            matches = list(find_all_matches(text, compiled_comment_regex))
            if not include_quoted:
                matches = [span for span in matches if not inside_quote(text, span)]
            
            if index < len(matches):
                span = matches[index]
                row = [type, year, month, id, text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
                csvwriter.writerow(row)
            else:
                logging.warning(f"Index {index} out of range for {len(matches)} matches")


def filter(comment_or_post: dict, popularity_threshold: int) -> tuple:
    """
    Test if a Reddit comment or post breaks any of the filtering rules.
    This is for nuanced criteria so positives are kept for manual review.
    """
    if popularity_threshold is not None:
        if comment_or_post['score'] < popularity_threshold:
            return True, "score below defined threshold"
    text = comment_or_post['body'] if 'body' in comment_or_post else comment_or_post['selftext']

    if "i'm a bot" in text.lower():
        return True, "non-human generated"
    
    return False, None


def filter_then_extract(comment_or_post: dict, compiled_comment_regex, args, include_quoted: bool, outfile: TextIO, reviewfile: TextIO):
    """First filter a comment or post, then extract it to the appropriate file."""
    filtered, reason = filter(comment_or_post['entry'], args.popularity) if args.dont_filter is None else False, None # apply filtering unless --dont_filter is set
    if not filtered:
        extract(args, comment_or_post, compiled_comment_regex, include_quoted, outfile, filter_reason=None)
    else:
        extract(args, comment_or_post, compiled_comment_regex, include_quoted, reviewfile, filter_reason=reason)


def relevant(comment_or_post: dict, args: argparse.Namespace) -> bool:
    '''
    Test if a Reddit comment or post is relevant to the search criteria.
    Performs broad filtering to discard irrelevant content based on multiple criteria.
    Filters are ordered by likelihood of failure for computational efficiency.
    Args:
        comment_or_post (dict): A Reddit comment or post object containing metadata
            such as 'author', 'subreddit', 'body'/'selftext', 'parent_id', 'title',
            'author_flair_text', etc.
        args (argparse.Namespace): Parsed command-line arguments containing:
            - name (str or None): Target subreddit or user to filter by
            - src (str): Source type ('user' or 'subreddit')
            - case_sensitive (bool): Whether to perform case-sensitive matching
            - toplevel (bool): If True, only match top-level posts (parent_id starts with 't3')
            - searchmode (str): Either 'comms' (comments) or 'posts'
            - commentregex (re.Pattern or None): Regex pattern for comment body
            - postregex (re.Pattern or None): Regex pattern for post selftext
            - include_quoted (bool): Whether to include quoted text in regex search
            - titleregex (re.Pattern or None): Regex pattern for post title
            - flairregex (re.Pattern or None): Regex pattern for author flair
            - userregex (re.Pattern or None): Regex pattern for author username
            - spacy_search (tuple or None): Tuple of (token, POS_tag) for NLP-based search
            - nlp (spacy.Language): Spacy language model for NLP processing
            - no_stats (bool): If False, updates statistics dictionary
    Returns:
        bool: True if the comment/post passes all relevance filters and is not a duplicate,
              False otherwise.
    Side Effects:
        - Adds MD5 hash of the comment/post to a module-level hash_set to prevent duplicates
        - Updates module-level stats_dict with subreddit counts if no_stats is False
    Note:
        Requires module-level variables: hash_set (set), stats_dict (dict)
        Requires helper functions: find_all_matches(), inside_quote()
    '''

    if args.name is not None: # if a subreddit or user was specified as argument, the comment's metadata are checked accordingly
        src = 'author' if args.src == 'user' else 'subreddit' # the username is called 'author' in the data
        
        nm = comment_or_post[src] if args.case_sensitive else comment_or_post[src].lower()
        
        if src not in comment_or_post.keys() or nm not in args.name: 
            return False
    
    if args.toplevel and not comment_or_post['parent_id'].startswith('t3'):
        return False
    
    regex = args.commentregex if args.searchmode == 'comms' else args.postregex
    body = 'body' if args.searchmode == 'comms' else 'selftext'

    if regex is not None and args.include_quoted:
        search = re.search(regex, comment_or_post[body])
        if search: # checks if comment regex matches at least once, matches are extracted later
            pass
        else:
            return False
    elif regex is not None and not args.include_quoted:
        matches_with_spans = list(find_all_matches(comment_or_post[body], regex))
        matches = [span for span in matches_with_spans if not inside_quote(comment_or_post[body], span)]
        if len(matches) == 0:
            return False
    
    # Check if title, flair, or user regexes match; return False if they don't match or value is missing
    for regex, key in zip([args.titleregex, args.flairregex, args.userregex], ['title', 'author_flair_text', 'author']):
        if regex is not None:
            # Get the value, using .get() for optional flair field
            value = comment_or_post[key] if key != 'author_flair_text' else comment_or_post.get('author_flair_text')
            # Discard if value is missing (useful only for flair) or regex doesn't match
            if value is None or not re.search(regex, value):
                return False

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

    
    h = hashlib.md5(json.dumps(comment_or_post, sort_keys=True).encode()).hexdigest() # dicts are unhashable, their original json form is preferrable; hexdigest is used to reduce memory consumption
    if h in hash_set: # hash check with all previous comments/posts in case the data contain redundancies
        return False
    else:
        hash_set.add(h)
        if not args.no_stats:
            stats_dict.setdefault(comment_or_post['subreddit'], 0)
            stats_dict[comment_or_post['subreddit']] += 1  
        return True


def assess_number_of_matches(comment_or_post: dict, compiled_comment_regex, args) -> int:
    """Count the number of matches in a comment or post if a regex is supplied and the firstmatch flag is not set."""
    text = comment_or_post['body'] if args.searchmode == 'comms' else comment_or_post['selftext']
    matches = list(find_all_matches(text, compiled_comment_regex))
    if not args.include_quoted:
        matches = [span for span in matches if not inside_quote(text, span)]
    return len(matches)


def log_month(month: str):
    """Send a message to the log with a month's real name for better clarity."""
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = month.split("-")[0] # get year string from the format 'RC_YYYY-MM.zst'
    m_num = int(month.split("-")[1]) # get month integer
    m_name = calendar.month_name[m_num]

    logging.info("Processing " + m_name + " " + year)


def handle_review_stub(reviewfile: TextIO):
    """If no entries were filtered out, remove the review file and log this info."""
    lines_in_review = sum(1 for line in open(reviewfile.name, "r", encoding="utf-8")) - 1
    if lines_in_review > 0:
        logging.info(f"{lines_in_review} entries were filtered out into {reviewfile.name}.")
    else:
        os.remove(reviewfile.name)
        logging.info("No entries were filtered out.")


def process_month(month, args, outfile, reviewfile):
    """
    Process Reddit data for a specific month and extract relevant comments/posts.
    This function reads Reddit data from a monthly file, filters entries based on
    relevance criteria, and either writes matches to output files, performs reservoir
    sampling, or counts matches depending on the provided arguments.
    Args:
        month (str): The month identifier used to locate the input file.
        args (Namespace): Command-line arguments containing:
            - input (str): Path to input directory containing monthly data files.
            - sample (int, optional): Sample size for sampling strategy.
            - count (bool): If True, only count relevant entries without writing output.
            - reservoir_size (int, optional): Size of reservoir for sampling algorithm.
            - commentregex (str, optional): Regular expression pattern to match comments.
            - firstmatch (bool): If True, count only first match per entry.
            - include_quoted (bool): Whether to include quoted content in extraction.
        outfile (str): Path to the output file for matched entries.
        reviewfile (str): Path to the review file for filtered entries.
    Returns:
        int or None: If args.count is True, returns the monthly count of relevant matches.
                     Otherwise, returns None and writes results to files or reservoir.
    Side Effects:
        - Modifies global 'reservoir' list with sampled entries if reservoir_size is set.
        - Modifies global 'relevant_count' counter.
        - Creates/appends to outfile and reviewfile if not counting and not using reservoir.
        - Calls handle_review_stub() to finalize review file.
    Raises:
        Implicitly may raise exceptions from read_redditfile(), relevant(), and file I/O operations.
    """
    log_month(month)
    monthly_relevant_count = 0
    total_count = -1
    infile = os.path.join(args.input, month)

    # for reservoir sampling
    global reservoir
    global relevant_count

    if args.sample:
        sample_points = get_samplepoints(month, args.sample, args.input)

    if not args.count and not args.reservoir_size:
        outf, reviewf = open(outfile, "a", encoding="utf-8"), open(reviewfile, "a", encoding="utf-8")

    for comment_or_post in read_redditfile(infile):
        total_count += 1
        if not args.sample or (args.sample and total_count == sample_points[0]):

            # if sampling, remove the first sample point from the ordered list
            if args.sample:
                del sample_points[0]
                if len(sample_points) == 0: # no more sample points left
                    break
            
            if relevant(comment_or_post, args):
                weight = assess_number_of_matches(comment_or_post, args.commentregex, args) if args.commentregex and not args.firstmatch else 1
                if weight == 0:
                    continue
                monthly_relevant_count += weight
                
                # Reservoir sampling logic
                for i in range(weight):
                    relevant_count += 1
                    entrydict = {'entry': comment_or_post, 'index': i}
                    if args.reservoir_size:
                        if len(reservoir) < args.reservoir_size:
                            reservoir.append(entrydict)
                        else:
                            # calculate j for reservoir sampling accounting for the current item's position in the stream
                            j = random.randint(0, relevant_count - 1)
                            if j < args.reservoir_size:
                                reservoir[j] = entrydict
                    else:
                        if not args.count:
                            filter_then_extract(entrydict, args.commentregex, args, args.include_quoted, outf, reviewf)

    if not args.count and not args.reservoir_size:
        outf.close()
        reviewf.close()
        handle_review_stub(reviewf)
    elif args.count:
        return monthly_relevant_count


def open_files(args, month) -> tuple:
    """Open the output and review files for writing and write their headers."""
    outfile = assemble_outfile_name(args, month)
    outfile = os.path.join(args.output, outfile)
    reviewfile = outfile[:-4] + "_filtered-out_matches.csv" if not args.return_all else outfile[:-4] + "_filtered-out_matches.jsonl"
    reviewfile = os.path.join(args.output, reviewfile)
    if not args.return_all and not args.reservoir_size:
        write_csv_headers(outfile, reviewfile)
    return outfile, reviewfile


def setup_logging_and_args():
    """Initialize logging and parse command-line arguments."""
    logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')
    return handle_args()


def setup_spacy(args):
    """Load spacy model if NLP search is enabled."""
    if args.spacy_search:
        logging.info("Importing spacy…")
        import spacy
        logging.info(f"Importing {args.language} model…")
        args.nlp = spacy.load(args.language)


def process_timeframe(args, timeframe, outfile, reviewfile):
    """Process all months in the timeframe with normal extraction."""
    for month in timeframe:
        process_month(month, args, outfile, reviewfile)


def process_count_mode(args, timeframe):
    """Process timeframe in count mode and output statistics."""
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


def process_reservoir_sampling(args, timeframe, outfile, reviewfile):
    """Process timeframe using reservoir sampling and write results."""
    for month in timeframe:
        process_month(month, args, outfile=None, reviewfile=None)
    
    logging.info(f"Writing reservoir of size {args.reservoir_size} to output.")
    
    if not args.return_all:
        write_csv_headers(outfile, reviewfile)
    
    outf, reviewf = open(outfile, "a", encoding="utf-8"), open(reviewfile, "a", encoding="utf-8")
    
    logging.info(f"Size of reservoir: {len(reservoir)}")
    
    for comment_or_post in reservoir:
        filter_then_extract(comment_or_post, args.commentregex, args, args.include_quoted, outf, reviewf)
    
    outf.close()
    reviewf.close()
    handle_review_stub(reviewf)


def write_final_stats(args):
    """Write final statistics to file if enabled."""
    if args.output and not args.no_stats:
        stats_file = os.path.join(args.output, 'otacon_search_stats.txt')
        with open(stats_file, "w") as outfile:
            _=outfile.write(json.dumps(stats_dict))


def main():
    args = setup_logging_and_args()
    timeframe = establish_timeframe(args.time_from, args.time_to, args.input)
    if args.reverse_order:
        timeframe.reverse()
    logging.info(f"Searching from {timeframe[0]} to {timeframe[-1]}")

    if args.reservoir_size is not None:
        logging.info(f"Using reservoir sampling with size {args.reservoir_size}.")

    setup_spacy(args)

    if not args.count:
        args.output = os.path.abspath(args.output)
        outfile, reviewfile = open_files(args, timeframe[0])

    if args.count:
        process_count_mode(args, timeframe)
    elif args.reservoir_size is not None:
        outfile, reviewfile = open_files(args, timeframe[0])
        process_reservoir_sampling(args, timeframe, outfile, reviewfile)
    else:
        process_timeframe(args, timeframe, outfile, reviewfile)

    if not args.count and args.no_cleanup is None and args.reservoir_size is None:
        cleanup(args.output, extraction_name=assemble_outfile_name(args, month=None))

    write_final_stats(args)
    

if __name__ == "__main__":
    main()