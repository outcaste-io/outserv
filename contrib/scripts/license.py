#!/usr/bin/python

import sys

notice = """
// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.
"""

def addCopyright(file):
    print("Add copyright to", file)
    f = open(file, "r+")
    lines = f.readlines()
    lines.insert(0, notice)
    f.seek(0)
    for l in lines:
        f.write(l)
    f.close()

def update(file):
    f = open(file, "r")
    lines = f.readlines()
    f.close()

    found = False
    end = 0
    for idx, l in enumerate(lines):
        if "Copyright" in l and "Dgraph" in l:
            start = idx - 1
            found = True
            break

    if not found:
        addCopyright(file)
        return

    for idx, l in enumerate(lines[start:]):
        if "*/" in l:
            end = start + idx
            break

    if end == 0:
        print("ERROR: Couldn't find copyright:", file)
        return

    updated = lines[:start]
    updated.extend(lines[end+1:])
    updated.insert(start, notice)
    f = open(file, "w")
    for l in updated:
        f.write(l)
    f.close()

if len(sys.argv) == 0:
    sys.exit(0)

update(sys.argv[1])
