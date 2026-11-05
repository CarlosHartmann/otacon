'''These are taken from Pushshift collaborator Watchful1. See README for GitHub link.'''

import json
from zstandard import ZstdDecompressor

import logging
logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')

def read_redditfile(file: str, prefilter_names=None, case_sensitive: bool = False) -> dict:
    """
    Iterate over the pushshift JSON lines, yielding them as Python dicts.
    Decompress iteratively if necessary.

    If prefilter_names is given, raw lines that do not contain any of the names as a substring
    are skipped before JSON parsing, which is by far the most expensive step. This is only a
    cheap pre-selection: relevant() still performs the exact author/subreddit check afterwards.
    """
    if prefilter_names:
        yield from _read_redditfile_prefiltered(file, prefilter_names, case_sensitive)
        return

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


# ---- prefiltered reading (see --prefilter) ----
# Works on raw bytes: splitting on b"\n" is always safe in UTF-8, and only the few lines that
# contain a searched name ever get decoded and parsed. Usernames and subreddit names are ASCII,
# so bytes.lower() (which only lowercases ASCII) is sufficient for case-insensitive matching.

def _encode_needles(names, case_sensitive: bool) -> list:
    return [n.encode('utf-8') if case_sensitive else n.lower().encode('utf-8') for n in names]


def _parse_raw_line(line: bytes, file: str):
    try:
        return json.loads(line)
    except UnicodeDecodeError:
        logging.warning(f"Invalid UTF-8 in a line of {file}; decoding with replacement characters")
        try:
            return json.loads(line.decode('utf-8', errors='replace'))
        except json.decoder.JSONDecodeError:
            pass
    except json.decoder.JSONDecodeError:
        pass
    print(f"Encountered a ZST parsing error in {file}")
    return None


def _read_redditfile_prefiltered(file: str, names, case_sensitive: bool):
    needles = _encode_needles(names, case_sensitive)
    if not file.endswith('.bz2') and not file.endswith('.xz') and not file.endswith('.zst'):
        lines = read_lines_plain_prefiltered(file, needles, case_sensitive)
    else:
        lines = read_lines_zst_prefiltered(file, needles, case_sensitive)
    for line in lines:
        parsed = _parse_raw_line(line, file)
        if parsed is not None:
            yield parsed


def read_lines_plain_prefiltered(file_name: str, needles: list, case_sensitive: bool):
    """Yield the raw lines of an uncompressed dump that contain at least one needle."""
    with open(file_name, 'rb') as infile:
        for line in infile:
            haystack = line if case_sensitive else line.lower()
            if any(n in haystack for n in needles):
                yield line


def _matching_lines(data: bytes, haystack: bytes, end: int, needles: list) -> list:
    """Return the (start, end) offsets of every line in data[:end] that contains a needle, in file order."""
    spans = set()
    for n in needles:
        pos = haystack.find(n, 0, end)
        while pos != -1:
            line_start = data.rfind(b'\n', 0, pos) + 1
            line_end = data.find(b'\n', pos, end)
            line_end = end if line_end == -1 else line_end
            spans.add((line_start, line_end))
            pos = haystack.find(n, line_end, end)
    return sorted(spans)


def read_lines_zst_prefiltered(file_name: str, needles: list, case_sensitive: bool, chunk_size: int = 2**27):
    """
    Yield the raw lines of a zst dump that contain at least one needle.
    Decompressed chunks without any needle are skipped as a whole, without being split into lines.
    Unlike read_lines_zst, a final line without a trailing newline is not dropped.
    """
    with open(file_name, 'rb') as file_handle:
        reader = ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
        buffer = b''
        while True:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            data = buffer + chunk
            cut = data.rfind(b'\n')  # everything before cut consists of complete lines
            if cut == -1:
                buffer = data
                continue
            haystack = data if case_sensitive else data.lower()
            for start, end in _matching_lines(data, haystack, cut, needles):
                yield data[start:end]
            buffer = data[cut + 1:]

        if buffer.strip():
            haystack = buffer if case_sensitive else buffer.lower()
            if any(n in haystack for n in needles):
                yield buffer

        reader.close()