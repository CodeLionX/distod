#!/usr/bin/env python3

import csv
import json
import argparse


def generate_synthetic_column_names(length):
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


def read_source(filename, delimiter, has_header):
    with open(filename, "r", newline="") as f:
        if has_header:
            reader = csv.DictReader(f, delimiter=delimiter)
            offset = 0
        else:
            first_line = f.readline()
            elems = [value.strip() for value in first_line.split(delimiter)]
            header = generate_synthetic_column_names(len(elems))

            # yield first line
            first_row = {}
            for name, value in zip(header, elems):
                first_row[name] = value
            yield 0, first_row

            # use reader from now on
            offset = 1
            reader = csv.DictReader(f, fieldnames=header, delimiter=delimiter)

        for i, row in enumerate(reader):
            yield i + offset, row


def perform_substitution(reader):
    for i, row in reader:
        yield i, {k: hash(v) for k, v in row.items()}


def main(filename, delimiter, has_header, target, substitution_enabled, duplicate):
    with open(target + ".csv", 'w', encoding='utf-8') as csv_out:
        with open(target + ".json", 'w', encoding='utf-8') as out:
            reader = read_source(filename, delimiter, has_header)
            if substitution_enabled:
                reader = perform_substitution(reader)

            for i, row in reader:
                if duplicate and has_header and i == 0:
                    headers = row.keys()
                    csv_out.write(delimiter.join(headers) + "\n")
                if duplicate:
                    values = [str(v) for v in row.values()]
                    csv_out.write(delimiter.join(values) + "\n")
                row["index"] = i
                obj = json.dumps(row)
                out.write(obj + ",\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert a csv file to JSON and substitute all values with integers')
    parser.add_argument('--delimiter', type=str, default=",",
                        help='CVS delimiter')
    parser.add_argument('--has-header', type=bool, default=False,
                        help='set to true of the csv file has a header line')
    parser.add_argument('-s', '--substitute', action='store_true',
                        help='use integer value substitution')
    parser.add_argument('-d', '--duplicate', action='store_true',
                        help='also write all data to a separate csv file')
    parser.add_argument('source',
                        help='source file path (csv format)')
    parser.add_argument('target',
                        help='target file path (where json is written to (without suffix)')

    args = parser.parse_args()
    # main("../data/adult-48842-14.csv", ";", False, "../data/adult-sub", True, True)
    main(args.source, args.delimiter, args.has_header, args.target, args.substitute, args.duplicate)
