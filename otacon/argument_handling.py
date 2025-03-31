import argparse

from otacon.data_types import comment_regex, sample_float, valid_date, dir_path, pos_tuple
from otacon.prep_input import fetch_data_timeframe

import logging
logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')

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
    parser.add_argument('--titleregex', '-TR', type=comment_regex, required=False,
                        help="The regex to search the post titles with.")
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
    parser.add_argument('--no_stats', action="store_true", required=False,
                        help="Removes per-subreddit statistics. Might improve efficiency.")

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
        if not args.case_sensitive:
            args.name = [elem.lower() for elem in args.name]
        args.name = set(args.name)
    
    if 'submissions' in args.input:
        args.searchmode = 'subs'
    elif 'comments' in args.input:
        args.searchmode = 'comms'
    else:
        args.searchmode = 'comms' # at least for now, no third search mode implemented, defaults to comments

    return args