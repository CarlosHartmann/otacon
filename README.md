# Otacon – Reddit comment extractor for the Pushshift Data Dumps

[Pushshift](https://pushshift.io) was the exhaustive corpus of publicly available Reddit comments. Up until Reddit's changes to its API on May 1<sup>st</sup> 2023 this corpus was accessible through a variety of ways. Ever since, however, the offline data dumps provided by the Pushshift maintainers have become the only viable way to access the data. The data dumps end at April 2023 and it is yet unclear if the data will ever be continued as this is up to the Reddit administration, which is implementing major policy changes at the moment.


## Why Reddit comments?

From a linguistics perspective, Reddit can be a highly valuable resource for socio-linguistic data. Users engage in discussions in different Subreddits, which can tell us something about their identity, such as geographical area, age, gender,  class, race, socio-political opinions, and many more. Also, some users comment regularly throughout the years, giving us a glimpse of an individual's changing use of language.

The data are far from perfect, but in my personal opinion, they make up what they lack in confirmability by their rich meta-data and sheer numbers. Uncompressed, the Reddit corpus comprises around 20TB of text.

## How to source the data

This project contains only a few of the older months for testing purposes. The rest of the data could be downloaded from files.pushshift.io, although they have been unsteadily online since early 2023. A [torrent version](https://academictorrents.com/details/7c0645c94321311bb05bd879ddee4d0eba08aaee) with data up until December 2022 exists as well, which is online as of yet. This might change with possible future ToS updates by Reddit.

## How to access the data

This project is one easy-to-use tool for accessing the data. If you find that the features are lacking for your own use-case and you are comfortable with writing Python code, the basic strategy is as follows:
Do not uncompress the data as it will most likely overflow your hard drive. Instead, use [code from here](https://github.com/Watchful1/PushshiftDumps) to iteratively uncompress and yield the comments for processing.

## Installation

Otacon is a [poetry](https://python-poetry.org) project. The installation is the same as for any other poetry project:
* make sure to have poetry installed and functional
* download or git clone the repository
* `cd` into it
* `poetry install`

If poetry does not raise an error, the installation worked.

## Usage

While in the otacon directory, use `poetry run python otacon/main.py` as the basic execution command.

### The following flags are required:

`--input` or `-I` – a path to the folder that contains the Pushshift data. The data are expected to be comprised of directories that each contain the data of one month, eg. `".../redditcomments/RC 2014-06/RC_2014-06.zst"`.

`--output` or `-O` – a path to the directory where you want the output CSV to be stored in.

### Optional flags

While none of these are required to run, supplying none of them would essentially mean that you want to copy all of the data uncompressed. Since this is not the intended way to use the script and it would most likely overflow your storage, it is required to use at least one of the following flags:

#### Timeframe

To set the timeframe from when you want to get Reddit comments, use these two flags.

`--time-from` or `-F` – Earliest month to retrieve data from. Use the format YYYY-MM, e.g. `2015-09`

`--time-to` or `-T` – Latest month to retrieve data from. Use the format YYYY-MM, e.g. `2017-01`.

To set a more precise bound, you will need to use the filter using the timestamp of the results.

#### Source

To retrieve comments from a single user or subreddit, use these two flags:

`--src`  or `-S`– specify if you want comments from a user or subreddit, only two possible values are `user` and `subreddit`.

`--name` or `-N` specify the name of the user/subreddit you want data from. If the user/subreddit did not exist in the specified timeframe (or has never existed at all), the script will still iterate through all of the data and simply return empty output files.

#### Regex

`--regex`or `-R` – This flag allows you to only return comments that contain at least one match to your specified expression. Will return separate results for each match, specifying the index values (=span) of each one.

Example:
Command contains `--regex "g\w+"`
For the comment "Never gonna give you up", the script will return:

`"Never gonna give you up" … (6, 11) …`
`"Never gonna give you up" … (12, 16) …`

For further regex specifications, use the ?-markers in the expression itself. For example, if you want to perform a case-insensitive search, add `(?i)` at the beginning of the expression.

#### Other comment filters

`--popularity`  or `-P`– Allows you to set any score threshold. Comments with lower voting scores, i.e. popularity, will be returned in a separate file. Accepts any integer as value, for example: --popularity 12

`--toplevel` or `-TL` – Only considers comments written as replies to a post, not to another comment. Does not need a value.

### Example

`poetry run python otacon/main.py -I ./data -O ./output --time_from 2010-7 --time_to 2010-9 -R "South Africa"`


## Soft filters

To help with linguistic processing of the results, some filtering rules are applied in every search. As this can yield false positives, the comments caught by these filters are not discarded, but instead returned in a separate file with for-review in the filename. This inclusion in a separate file is what makes these filters 'soft' as opposed to hard filters that would discard the data entirely. The filters are:

Profanity – some language might cause problems if they are published or processed using publicly accessible infrastructure such as an LLM. The script employs a fairly strict profanity filter to make sure every profanity is reviewed by the user.

Non-human generated content – ideally, this filter would catch any type of spam. As it is, it only discards comments made by bots as they are required by Reddit to contain the string `i'm a bot`.


## Hard filters

These filtering rules make it so a comment is discarded without the option to review the comments. This is only for 100% certain rules. The two implemented filters are:

* Duplicate comments: Some duplicates have been observed in the data so each processed comment is hashed to avoid duplicates in the results.
* (Only when regex is specified) Regex inside quoted line: If a regex match is inside of a quoted line, it does not technically belong to that comment. Rather, it is either from a different comment and is therefore likely already included elsewhere in the results, or it is from a Reddit-external source. In any case, it would not be a meaningful data point for the language use of a user or subreddit and is therefore discarded without further review.

## Output

The script outputs two files, one for the main results and the other for the filtered, for-review ones. They are in CSV format, using ; as a separator and with the `QUOTE_MINIMAL` setting of the standard Python `csv` library.

The comments are returned with the following metadata:

* `text` – The entirety of the comment itself.
* `span` – When a regex was specified; the indexes of the matched string
* `subreddit` – The subreddit where the comment was written
* `score` – The final amount of upvotes minus downvotes (the amount of upvotes and downvotes is only accessible in older Reddit data and was not reliable even then)
* `user` – The username of the comment's author
* `flairtext` – The flair they had set at the time; not applicable to every subreddit or user.
* `date` – The exact time in UTC when the comment was first published
* `permalink` – A URL to the comment.
* `filter reason` – Only in the for-review file; the filter that caught a comment