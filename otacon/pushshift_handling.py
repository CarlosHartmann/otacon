'''These are taken from Pushshift collaborator Watchful1. See README for GitHub link.'''

import json
from zstandard import ZstdDecompressor

import logging
logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')

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