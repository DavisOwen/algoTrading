#!/usr/bin/env python

import os
import csv
import heapq

csv_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                       'csv_files')
with open(os.path.join(csv_dir, 'quandl_wiki.csv')) as file:
    csv_reader = csv.reader(file, delimiter=',')
    dates = []
    s = set()
    for row in csv_reader:
        if row[0] in s:
            continue
        s.add(row[0])
        heapq.heappush(dates, (row[1], row[0]))
        print("Pushing {symbol} {date}".format(symbol=row[0], date=row[1]))
    print(heapq.nsmallest(12, dates))
