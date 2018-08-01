# hashbitmap
A hash-bitmap index


## Details

The algorithm uses two known components:

1. A compressed bitmap ([Roaring-Bitmap](http://roaringbitmap.org/) is used)
2. A hash algorithm that yields 256-bit hashes with a hamming-weight of between 1 and 16. (It uses a hashing-technique, commonly used in bloom-filters).
3. A simple key-value-database that uses 32-bit integer as key.

### Details on the hash function

The hash function is a function that takes an average byte-sequence and yields
a hash value with a low hamming weight. The binary representation of the hash
looks like this: `0000000000000001000000000000000100000000000000000000000010010000`
This is a bit-string with 64 bit and a hamming-weight of 4.

The hash function uses another, regular, hash function as base, which returns a
dense 128 bit hash. This particular implementation uses the 128-bit FNV1a hash algorithm as basis.

```py
def sparse_hash(message):
	bits = [0]*256 # 256 bit array
	for b in bytes(fnv1a_128bit(message)):
		bits[b] = 1
	return bits
```

Most hashes will have a hamming-weight of 16. If the result of the inner hash function has two or more identical bytes,
the hamming-weight of the output will be lower than 16. The ideal hash function would have a constant
hamming-weight.

### How the hash is used

The index is looks like a giant bit table. Every hash represents a row, and every column is a single Bitmap.

If we search for a certain hash, which has a hamming weight of not more than 16, we only need to look at 16
of the 256 columns. From these 16 columns, an intersection is calculated. The intersection represents the set
of rows, where the wanted hash can be found.

## Limitations

Because the entire set of bitmaps must be loaded into memory, there is a certain constraint of how big the index can be.

## Godocs

- [table](https://godoc.org/github.com/maxymania/hashbitmap/table)

