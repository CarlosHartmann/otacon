# Otacon – Reddit comment extractor for the Pushshift Data Dumps

[Pushshift](https://pushshift.io) was the exhaustive corpus of publicly available Reddit comments. Up until Reddit's changes to its API on May 1<sup>st</sup> 2023 this corpus was accessible through a variety of ways. See the project's whitepaper on [ArXiv](arXiv:2001.08435) Ever since, however, the offline data dumps provided by the Pushshift maintainers have become the only viable way to access the data. It depends on the specific project if using the data dumps from months after April 2023 is problematic or not.

## Why Reddit comments?

From a linguistics perspective, Reddit can be a highly valuable resource for sociolinguistic research. Users engage in discussions in different subreddits, which can tell us something about their identity, such as geographical area, age, gender,  class, race, socio-political opinions, amongst other things. Also, some users comment regularly throughout the years, giving us a glimpse of an individual's changing use of language.

Social media is furthermore an interesting type of language as it shares some characteristics of oral language while being written and born digital. Research on social media language is still in its infancy due to the recency, the fast-paced (and indeed, seemingly increasing speed of change of) evolution, and the many intricately confounding influences on social media language.

The authenticity of Reddit data is not easily verifiable. People are mostly anonymous and lots of engagement is either unorganic or in some way provoked. For linguistics, alternative data collection techniques such as interviews and lab settings are inauthentic for other reasons. Social media data are at the least produced in an unsupervised and  spontaneous manner. As for the sociolinguistic variables, traditional data recording techniques allow for perfectly accurate metadata while social media data force us to guess based on user-given information or metadata. However, this is counteracted by the main strength of such data: its sheer numbers. Uncompressed, the Reddit corpus comprises around 20TB of text.

## How to source the data

This project contains only a few of the older months for testing purposes. The rest of the data could be downloaded from files.pushshift.io, although access to the site is restricted now. A [torrent version](https://academictorrents.com/details/7c0645c94321311bb05bd879ddee4d0eba08aaee) with data up until December 2022 exists as well, which is online as of yet. This might change with possible future ToS updates by Reddit.

## How to access the data

This project is one easy-to-use tool for accessing the data. If you find that the features are lacking for your own use-case and you are comfortable with writing Python code, the basic strategy is as follows:
Do not uncompress the data as it will most likely overflow your hard drive. Instead, use [code from here](https://github.com/Watchful1/PushshiftDumps) to iteratively uncompress and yield the comments for processing.

## Installation

Otacon is a [poetry](https://python-poetry.org) project. The installation is the same as for any other poetry project:
* make sure to have poetry installed and functional. It might require Python 3 to be pre-installed.
* download or `git clone` the repository
* `cd` into it
* `poetry install`
* If there are errors, update poetry to its newest version.
* If errors persist, it might have to be necessary to downgrade poetry to a previous version.

If poetry does not raise an error, the installation worked.

## Usage

While in the otacon directory, use `poetry run python otacon/main.py` as the basic execution command.

### Data-related flags

`--input` or `-I` – a path to the folder that contains the Pushshift data. The data are expected to be located in a directory containing only Pushshift dump files all next to each other (i.e. not within their own directories as they were sometimes uploaded). This is required.
The data is furthermore expected to be located either in a directory called `submissions` for posts or `comments` for comments. The script might not behave as intended if the input data are in a differently-named directory.

`--output` or `-O` – a path to the directory where you want the output file to be stored in. This is required unless you use the `count` flag (see below).

### Optional flags

While none of these are required to run, supplying none of them would essentially mean that you want to copy all of the data uncompressed. Since this is not the intended way to use the script and it would most likely overflow your storage, it is strongly recommended to use at least one of the following flags:

#### Timeframe

To set the timeframe from when you want to get Reddit comments, use these two flags.

`--time-from` or `-F` – Earliest month to retrieve data from. Use the format YYYY-MM, e.g. `2015-09`

`--time-to` or `-T` – Latest month to retrieve data from. Use the format YYYY-MM, e.g. `2017-01`.

To set a more precise bound, you could post-process the output however you like. The data come with UNIX timestamps.

#### Source

To retrieve comments from a single user or subreddit, use these two flags:

`--src`  or `-S`– specify if you want comments from a user or subreddit (or several), only two possible values are `user` and `subreddit`.

`--name` or `-N` – specify the name of the user/subreddit you want data from. If the user/subreddit did not exist in the specified timeframe (or has never existed at all), the script will still iterate through all of the data and simply return empty output files. This flag can be set any number of times and the values will be appended to a list of source subreddits or users, respectively, to be searched. e.g. `--name Tom --name Dick --name Harry`

#### Regex

`--commentregex`or `-CR` – This flag allows you to only return comments that contain at least one match to your specified expression in their text body. Will return separate results for each match, specifying the index values (=span) of each one.

Example:
Command contains `--regex "g\w+"`
For the comment `Never gonna give you up`, the script will return:

* `"Never gonna give you up" … (6, 11) …` where the tuple is the span and points to `gonna`
* `"Never gonna give you up" … (12, 16) …` where the span points to `give`

For further regex specifications, use the ?-markers in the expression itself. For example, if you want to perform a case-insensitive search, add `(?i)` at the beginning of the expression.

To match posts using regex, use the flag `postregex` or `PR`.
To match the titles of posts using regex, use the flag `titleregex` or `TR`.
To retrieve comments with a flair matching your regex, use the flag `flairregex` or `FR`.
To retrieve comments from users whose name matches your regex, use the flag `userregex` or `UR`.

#### Disambiguated search

Sometimes you may want to extract comments that contain an ambiguous word that has different meanings depending on syntactic position, e.g. German `halt` as an adverb and not as an imperative of the verb `halten`.

`--spacy-search` allows you to specify a token and its expected SpaCy POS tag in tuple form.
`--language` is required for this so the correct model is loaded. The value of this will be given to the `spacy.load()` function of spacy.

Currently, only a German model has been added to the poetry project. You must add the required models to your local poetry project by running `poetry add [URL to the model's wheel]`

#### Other comment filters

`--popularity`  or `-P`– Allows you to set any score threshold. Comments with lower voting scores, i.e. popularity, will be returned in a separate file. Accepts any integer as value, for example: --popularity 12

`--toplevel` or `-TL` – Only considers comments written as replies to a post, not to another comment. Does not need a value.


#### Special Search Operations

`--case-sensitive` or `-CS` makes the search, whether by direct matching or by any of the regex flags, case sensitive. If omitted (i.e. the default option), the search is case insensitive. This applies to all regex flags (see above).

`--count` or `-C` will not retrieve any comments but only count every comment that fit the search parameters and output the results per month.

`--include_quoted` will include every regex match in comments or posts, regardless of position. The default behavior extends your regex matches so that quoted lines (i.e. starting with `>`) will be disregarded.

`--sample` or `-SMP` will pull a sample of all relevant comments. Sample size is given as float between 0.0 and 1.0 where 1.0 returns 100% of results

`--return_all` will return all metadata from the Pushshift dumps and not just the pre-selected ones (see section "Output" below). The format will thus not be CSV but instead JSONL, which is raw text with one JSON object corresponding to a comment on each new line.

`--dont_filter` will skip all filtering steps (see below).

`--reverse_order` will process the months in the input directory from last (i.e. most recent) to first (i.e. oldest / furthest in the past). This is useful for especially long data extractions that can be halved in time by running otacon on two different machines from its two different endpoints. Note that clean-up must then be performed manually.

`--no_cleanup` will skip the clean-up step that collects all returned results into a single file and deletes the by-month files.

### Example

`poetry run python otacon/main.py -I ./data -O ./output --time_from 2010-7 --time_to 2010-9 -R "South Africa"`


## Soft filters

To help with linguistic processing of the results, some filtering rules are applied in every search. As this can yield false positives, the comments caught by these filters are not discarded, but instead collected with a note in the `filter_reason` column (see below). This inclusion in the results is what makes these filters 'soft' as opposed to hard filters that would discard the data entirely. The only soft filter at the moment is for:

Non-human generated content – ideally, this filter would catch any type of spam. As it is, it only discards comments made by bots as they are required by Reddit to contain the string `i'm a bot`.

## Hard filters

These filtering rules make it so a comment is discarded without the option to review it. This is only for 100% certain rules. The two implemented filters are:

* Duplicate comments: Some duplicates have been observed in the data so each processed comment is hashed to avoid duplicates in the results.
* (Only when regex is specified) Regex inside quoted line: If a regex match is inside of a quoted line, it does not technically belong to that comment. Rather, it is either from a different comment and is therefore likely already included elsewhere in the results, or it is from a Reddit-external source. In any case, it would not be a meaningful data point for the language use of a user or subreddit and is therefore discarded without further review. This behavior can be deactivated using an argument flag (see above).

## Output

The script outputs a file for each month while running, which are then concatenated into a single file. They are normally in CSV format (unless all metadata are pulled, see `return_all` flag above), using ; as a separator and with the `QUOTE_MINIMAL` setting of the standard Python `csv` library.

The comments are returned with the following metadata:

* (NOT YET IMPLEMENTED) `id` – the comment or post ID given by Reddit see [here](https://www.reddit.com/r/redditdev/comments/dy6bca/on_reddit_how_can_i_find_a_comments_id/) for clarification. 
* `text` – The entirety of the comment itself.
* `span` – When a regex was specified; the indexes of the matched string
* `subreddit` – The subreddit where the comment was written
* `score` – The final amount of upvotes minus downvotes (the amount of upvotes and downvotes is only accessible in older Reddit data and was not reliable even then)
* `user` – The username of the comment's author
* `flairtext` – The flair they had set at the time; not applicable to every subreddit or user.
* `date` – The exact time in UTC when the comment was first published (UNIX timespamt)
* `permalink` – A URL to the comment.
* `filter reason` – Only in the for-review file; the filter that caught a comment

For further processing, it is advisable to add an ID column to keep track of your data, a `year`, `month`, and/or `day` column for more user-friendly time info, and/or a `type` column in case you want to combine comment and post entires in your data. If no `commentregex` was used, the `span` column will be empty. After reviewing the flagged entries using the `filter reason` column, you can probably delete it.

#### Privacy & Ethics

If you intend on publishing the data in some way, you may consider deleting/pseudonymizing at least the `user` column and removing the `permalink` column. The `flairtext` column can, depending on the source subreddit, also contain identifying information. If your data is especially sensitive, a manual review of the `text` contents may be advisable.
However, Pushshift contains only comments that were publicly available at the time of collection (which, as I understand, was monthly starting sometime in 2015). Users agreed to the Terms of Service and therefore also to the public availability of their posted comments. However, they were in that moment in all likelihood not fully aware of people using their data in other ways than standard interactions on Reddit. It is your responsibility to ensure that the use of the data for your project is in line with any ethical and privacy guidelines stipulated by your institution, governing bodies, and/or project shareholders.
