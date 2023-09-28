import pytest
import argparse

import os
import re
import datetime

from otacon.main import valid_date
from otacon.main import dir_path
from otacon.main import within_timeframe
from otacon.main import filter
from otacon.main import fetch_data_timeframe
from otacon.main import inside_quote

def test_valid_date():
    assert valid_date('2010-01') == (2010, 1)
    assert valid_date('2019-02') == (2019, 2)
    assert valid_date('2019-2') == (2019, 2)
    
    with pytest.raises(argparse.ArgumentTypeError):
        assert valid_date('2010-12-01') != (2010, 12)
    with pytest.raises(argparse.ArgumentTypeError):    
        assert valid_date('2010-13') != (2010, 13)
    with pytest.raises(argparse.ArgumentTypeError):    
        assert valid_date('2010-00') != (2010, 0)
    with pytest.raises(argparse.ArgumentTypeError):    
        assert valid_date('201-10') != (201, 10)


def test_dir_path():
    assert dir_path('/Users/Carlitos') == '/Users/Carlitos'

    with pytest.raises(NotADirectoryError):
        assert dir_path('/Some/Kind/Of/Clown/Path') != '/Some/Kind/Of/Clown/Path'


def test_within_timeframe():
    assert within_timeframe('RC 2010-03', (2010, 1), (2010, 6)) == True
    assert within_timeframe('RC 2010-03', (2010, 1), (2010, 3)) == True
    assert within_timeframe('RC 2010-03', (2010, 3), (2010, 6)) == True
    assert within_timeframe('RC 2010-03', (2010, 3), (2010, 3)) == True
    assert within_timeframe('RC 2011-03', (2010, 5), (2012, 1)) == True
    assert within_timeframe('RC 2011-03', (2010, 1), (2012, 5)) == True
    assert within_timeframe('RC 2011-03', (2001, 12), (2011, 3)) == True


    assert within_timeframe('RC 2009-03', (2010, 1), (2010, 6)) == False
    assert within_timeframe('RC 2020-03', (2010, 1), (2010, 6)) == False

def test_filter():
    assert filter({'body':'Gosh darn it, anyway!'}, popularity_threshold=None) == (False, None)
    assert filter({'body': 'Fuck fuck fuck fuck fuck fuck fuck!'}, popularity_threshold=None) == (True, 'offensive language')
    assert filter({'body':"Bleep bloop, I'm a bot!"}, popularity_threshold=None) == (True, 'non-human generated')

def test_fetch_data_timeframe():
    assert fetch_data_timeframe("data") == ((2010, 1), (2012, 8))

def test_inside_quote():
    text = '''Lookie here:
    
    &gt; It was me who ate the pop tart.
    
    They said it themself!'''
    r = re.search('pop tart', text)
    span1 = (r.start(), r.end())

    r = re.search('themself', text)
    span2 = (r.start(), r.end())

    assert inside_quote(text, span1) == True
    assert inside_quote(text, span2) == False