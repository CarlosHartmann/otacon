import re
import os
import logging
import argparse

logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')

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
    """
    A float for pulling samples of N% of all matched posts or comments.
    Since it is a proportion, it must be strictly between 0 and 1.0.
    """
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


def pos_tuple(text):
    tk, pos = text.split(',')[0], text.split(',')[1]
    return (tk, pos)