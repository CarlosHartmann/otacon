'''
Memory-compact set of MD5 digests for deduplication.

A Python set of hexdigest strings costs ~106 bytes per entry, which reaches several GB for
months with tens of millions of matches. This open-addressing table stores the first 8 bytes
of each digest in a flat array of unsigned 64-bit integers (16-32 bytes per entry).
'''

from array import array

_INITIAL_SLOTS = 1 << 16
_EMPTY = 0


class DigestSet:
    def __init__(self):
        self.clear()

    def clear(self):
        '''Remove all entries and release the table's memory.'''
        self._table = array('Q', [_EMPTY]) * _INITIAL_SLOTS
        self._mask = _INITIAL_SLOTS - 1
        self._len = 0

    def __len__(self):
        return self._len

    @staticmethod
    def _key(digest: bytes) -> int:
        # 0 marks an empty slot; a 64-bit key of 0 is remapped (collision odds are negligible)
        return int.from_bytes(digest[:8], 'little') or 1

    def _slot(self, key: int) -> int:
        '''Linear probing: index of the slot holding key, or of the empty slot where it belongs.'''
        table, mask = self._table, self._mask
        index = key & mask
        while True:
            current = table[index]
            if current == key or current == _EMPTY:
                return index
            index = (index + 1) & mask

    def __contains__(self, digest: bytes) -> bool:
        return self._table[self._slot(self._key(digest))] != _EMPTY

    def add(self, digest: bytes) -> None:
        key = self._key(digest)
        index = self._slot(key)
        if self._table[index] != _EMPTY:
            return
        self._table[index] = key
        self._len += 1
        if self._len * 2 > len(self._table): # keep the load factor at or below 0.5
            self._grow()

    def _grow(self):
        old_table = self._table
        self._table = array('Q', [_EMPTY]) * (2 * len(old_table)) # no temporary bytes copy
        self._mask = len(self._table) - 1
        for key in old_table:
            if key != _EMPTY:
                self._table[self._slot(key)] = key
