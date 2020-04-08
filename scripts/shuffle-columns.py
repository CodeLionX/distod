#!/usr/bin/env python3

import csv
import json
import argparse
import random
from collections import OrderedDict
from collections.abc import Iterator


class Reader(Iterator):

    def __init__(self, filename, delimiter, has_header):
        self.offset = 0
        self.reader = None
        self.current = ()

        f = open(filename, "r", newline="", encoding="utf-8")  # if error try using iso-8859-1
        if has_header:
            self.reader = csv.DictReader(f, delimiter=delimiter)
            self.current = (self.offset, next(self.reader))
        else:
            first_line = f.readline()
            elems = [value.strip() for value in first_line.split(delimiter)]
            header = self._generate_synthetic_column_names(len(elems))

            # yield first line
            first_row = {}
            for name, value in zip(header, elems):
                first_row[name] = value
            self.current = (self.offset, first_row)

            # use reader from now on
            self.reader = csv.DictReader(f, fieldnames=header, delimiter=delimiter)

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)
        self.offset += 1
        return self.offset, row

    def headers(self):
        return self.reader.fieldnames

    def _generate_synthetic_column_names(self, length):
        ordinal_a = ord('A')

        def index_to_name(i):
            name = ""
            number = i
            while number > 0:
                remainder = number % 26
                if remainder == 0:
                    name += "Z"
                    number = (number // 26) - 1
                else:
                    name = name + str(chr(ordinal_a + remainder - 1))
                    number = number // 26
            # reverse string
            return name[::-1]
        return [index_to_name(i) for i in range(1, length + 1)]


def main(filename, delimiter, has_header, target, with_json):
    reader = Reader(filename + ".csv", delimiter, has_header)
    header = [name for name in reader.headers()]
    random.shuffle(header)

    json_data = []
    # write CSV
    with open(target + ".csv", 'w', encoding="utf-8") as csv_out:
        for i, row in reader:
            if has_header and i == 0:
                csv_out.write(delimiter.join(header) + "\n")
            values = [str(row[key]) for key in header]
            csv_out.write(delimiter.join(values) + "\n")
            if with_json:
                shuffled_row = OrderedDict([(key, row[key]) for key in header])
                shuffled_row["index"] = i
                json_data.append(json.dumps(shuffled_row))

    # write JSON
    if with_json:
        with open(target + ".json", 'w', encoding="utf-8") as out:
            for s in json_data:
                out.write(s + ",\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Shuffle the order of columns in a CSV file and optionally in a JSON file.')
    parser.add_argument('--delimiter', type=str, default=",",
                        help='CVS delimiter')
    parser.add_argument('--has-header', type=bool, default=False,
                        help='set to true if the csv file has a header line')
    parser.add_argument('-j', '--json', action='store_true',
                        help='shuffle the order of columns of the JSON file as well (JSON file must be in the same ' +
                             'directory as the CSV file')
    parser.add_argument('source',
                        help='source file name without suffix (.csv and .json are automatically appended)')
    parser.add_argument('target',
                        help='target file path without suffix')

    args = parser.parse_args()
    main(args.source, args.delimiter, args.has_header, args.target, args.json)
    # main("../tmp/plista-sub", ";", False, "../tmp/plista-sub-tmp", True)
