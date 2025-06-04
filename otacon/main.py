'''
otacon: Extracts Reddit comments from the offline version of the Pushshift Corpus dumps (see README for further info)

For Usage and Flags see README doc.
'''

import os
import re
import csv
import json
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

# keep track of already-processed comments throughout function calls
hash_set = set()

# return stats from which subreddits the relevant comments were and how many per subreddits
stats_dict = {}


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
    return True if re.search('&gt;[^\n]+$', relevant_text) else False # tests if there is no linebreak between a quote symbol and the match


def extract(args, comment_or_post: dict, compiled_comment_regex: str, include_quoted: bool, outfile: TextIO, filter_reason: str):
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

        if compiled_comment_regex is None:
            span = None
            row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
            csvwriter.writerow(row)
        else:
            for span in find_all_matches(text, compiled_comment_regex):
                if not include_quoted and not inside_quote(text, span):
                    span = str(span)
                    row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
                    csvwriter.writerow(row)


def filter(comment_or_post: dict, popularity_threshold: int) -> tuple:
    """
    Test if a Reddit comment or post breaks any of the filtering rules.
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
        
        nm = comment_or_post[src] if args.case_sensitive else comment_or_post[src].lower()
        
        if src not in comment_or_post.keys() or nm not in args.name: 
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
    
    if args.titleregex is not None:
        title = comment_or_post['title']
        search = re.search(args.titleregex, title) if args.case_sensitive else re.search(args.titleregex, title, re.IGNORECASE)
        return True if search else False
    
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

    
    h = hashlib.md5(json.dumps(comment_or_post, sort_keys=True).encode()) # dicts are unhashable, their original json form is preferrable
    if h in hash_set: # hash check with all previous comments/posts in case the data contain redundancies
        return False
    else:
        hash_set.add(h)
        if not args.no_stats:
            stats_dict.setdefault(comment_or_post['subreddit'], 0)
            stats_dict[comment_or_post['subreddit']] += 1  
        return True


def log_month(month: str):
    """Send a message to the log with a month's real name for better clarity."""
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = month.split("-")[0] # get year string from the format 'RC_YYYY-MM.zst'
    m_num = int(month.split("-")[1]) # get month integer
    m_name = calendar.month_name[m_num]

    logging.info("Processing " + m_name + " " + year)


def process_month(month, args, outfile, reviewfile):
    log_month(month)
    relevant_count = 0
    total_count = -1
    infile = args.input + "/" + month
    compiled_comment_regex = re.compile(args.commentregex) if args.commentregex else None

    if args.sample:
        sample_points = get_samplepoints(month, args.sample, args.input)

    if not args.count:
        outf, reviewf = open(outfile, "a", encoding="utf-8"), open(reviewfile, "a", encoding="utf-8")

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
                    
                        
                        filtered, reason = filter(comment_or_post, args.popularity) if not args.dont_filter else False, None
                        if not filtered:
                            extract(args, comment_or_post, compiled_comment_regex, args.include_quoted, outf, filter_reason=None)
                        else:
                            extract(args, comment_or_post, compiled_comment_regex, args.include_quoted, reviewf, filter_reason=reason)
    if not args.count:
        outf.close()
        reviewf.close()
    elif args.count:
        return relevant_count


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
        logging.info(f"Importing {args.language} model…")
        nlp = spacy.load(args.language)
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

    if args.output and not args.no_stats:
        stats_file = os.path.join(args.output, 'otacon_search_stats.txt')
        with open(stats_file, "w") as outfile:
            _=outfile.write(json.dumps(stats_dict))
    

if __name__ == "__main__":
    main()