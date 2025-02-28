import os
import csv
import random

import logging
logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')


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