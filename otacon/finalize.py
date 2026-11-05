'''
finalize: Functions necessary to post-process the retrieved data. Mainly to collect all output and put it into a single file.
Warning: This assumes that all files in a given directory belong to the same data extraction and that no other process is currently accessing them.
'''


import os
import re
import csv
import logging
from subprocess import call


def extract_time_info(filename):
    '''Reads the year and month info from each filename.'''
    year = re.search('\d{4}', filename).group()
    month = re.search('\-(\d{2})', filename).group(1)
    return year, month


def gather_output_files(directory):
    '''Returns list of files that are assumed to belong to the output.'''
    return [elem for elem in os.listdir(directory) if elem.endswith(".csv") or elem.endswith(".jsonl") and not elem.startswith(".") and re.search('\d{4}\-\d{2}', elem)]


def finalize_run(outfile: str, reviewfile: str, final_name: str):
    """
    Rename this run's result file (and its filtered-out file, if one was kept) after the whole
    search instead of only its first month. A run already writes all of its months into a single
    file, so nothing needs merging. Only the two files this run wrote are touched: nothing is
    merged, deleted or overwritten, so other files in the output directory are always safe.
    """
    directory = os.path.dirname(outfile)
    stem, ext = os.path.splitext(final_name)
    for src, dst in [(outfile, final_name), (reviewfile, f"{stem}_filtered-out_matches{ext}")]:
        dst = os.path.join(directory, dst)
        if not os.path.exists(src) or os.path.abspath(src) == os.path.abspath(dst):
            continue
        if os.path.exists(dst):
            logging.warning(f"{dst} already exists; keeping the result under its original name {src}")
            continue
        os.rename(src, dst)
        logging.info(f"Saved as {dst}")


def cleanup(directory, extraction_name):
    """
    Consolidates CSV/JSONL files from directory into a single output file.
    Legacy: meant for the old one-file-per-month output and for manual use via this module's main().
    It merges and deletes EVERY matching file in the directory, so never point it at a directory
    that holds other results. otacon.main uses finalize_run() instead.
    """
    directory = os.path.abspath(directory)
    f_list = gather_output_files(directory)
    os.chdir(directory)
    
    output_path = os.path.join(directory, extraction_name)
    
    # Handle JSONL files: concatenate directly
    if f_list[0].endswith(".jsonl"):
        call(f'cat {" ".join(f_list)} > {output_path}', shell=True)
        for elem in f_list:
            os.remove(elem)
        return
    
    # Handle CSV files: merge with metadata
    with open(output_path, "w", encoding="utf-8") as outfile:
        csvwriter = csv.writer(outfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(['type', 'year', 'month', 'id', 'text', 'span', 'subreddit', 'score', 'user', 'flairtext', 'date', 'permalink', 'filter reason'])
        
        for file in f_list:
            # Determine type from filename prefix
            data_type = "comment" if file.startswith("comment") else "submission"
            year, month = extract_time_info(file)
            
            # Read and merge each file, skipping header and empty rows
            with open(file, "r", encoding="utf-8") as infile:
                csvreader = csv.reader(infile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
                next(csvreader)  # Skip header
                for row in csvreader:
                    if row[0] != 'text':
                        csvwriter.writerow([data_type, year, month] + row)
            
            os.remove(file)
    
    


def main():
    directory = input("Give the directory of the output: ")
    extraction_name = input("Give a name for the extraction that produced the data: ")
    cleanup(directory, extraction_name)


if __name__ == "__main__":
    main()