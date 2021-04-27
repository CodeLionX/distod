#!/usr/bin/env python3

import sys

import pandas as pd
import numpy as np
from tqdm import tqdm
from lxml import etree


skip_list = ["cdrom", "school", "chapter", "publnr", "note", "editor", "url", "journal", "crossref", "address", "cite", "isbn"]

def get_tags(root):
    tag_names = set()
    for element in root:
        for child_el in element:
            tag_names.add(child_el.tag)
    for ignore_tag in skip_list:
        tag_names.remove(ignore_tag)
    return tag_names


def parse(path):
    print(f"Reading XML from {path} using DTD")
    parser = etree.XMLParser(load_dtd=True, encoding="iso8859-1")
    tree = etree.parse(path, parser)
    root = tree.getroot()
    length = len(root)
    print(f"XML loaded and {length} publications found!")
    print("Searching publication attributes = tags = column names ...")
    tag_names = get_tags(root)
    column_names = ["entry_type", "publication_type"] + list(tag_names)
    print(f"... finished: Columns: {column_names}")

    df = pd.DataFrame(index=range(length), columns=column_names)
    print("Created target dataframe.")

    for i, el in tqdm(enumerate(root), desc="Parsing", total=length, unit_scale=True):
        entry_type = el.tag
        publication_type = el.get("publtype")
        # cite_key = el.get("key")
        # dblp_date = el.get("mdate")
        authors = []
        attribs = {}

        # parse contents
        for child in el:
            if child.tag == "author":
                authors.append(child.text)
            elif child.tag in skip_list:
                continue
            else:
                attribs[child.tag] = child.text
        
        attribs["author"] = ", ".join(authors)
        attribs["entry_type"] = entry_type
        attribs["publication_type"] = publication_type
        # attribs["dblp_date"] = dblp_date
        # attribs["cite_key"] = cite_key
        if "journal" in attribs:
            attribs["booktitle"] = attribs["journal"]
            del attribs["journal"]

        df.loc[i] = attribs
    return df


if __name__ == "__main__":
    in_filename = sys.argv[1]
    if len(sys.argv) == 3 and sys.argv[2]:
        out_filename = sys.argv[2]
    else:
        out_filename = f"{in_filename}.csv"

    df = parse(in_filename)
    print(f"Storing resulting CSV to {out_filename}")
    df.to_csv(out_filename, index=False)
