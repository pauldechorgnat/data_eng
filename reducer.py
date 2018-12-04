#! /usr/bin/env python3

import sys


word2count = {}

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    word2count[word] = word2count.get(word, 0) + count

for word in word2count.keys():
    print(word, '\t', word2count[word], sep='')

