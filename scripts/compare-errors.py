#!/usr/bin/env python3


def compare_candidates():
    lines = {}
    with open("deployment/tested-candidates.txt", 'r') as file1:
        lines = {line for line in file1.readlines()}

    print("Dict size " + str(len(lines)))

    thor_lines = set()
    unmatched = []
    with open("deployment/tested-candidates-thor01.txt", 'r') as file2:
        for line in file2:
            thor_lines.add(line)
            if line not in lines:
                unmatched.append(line)
    
    print(str(len(unmatched)) + " tested candidates by thor01 that are too much:")
    for line in unmatched:
        print(line.strip())
    
    """
    unmatched = []
    for line in lines:
        if line not in thor_lines:
            unmatched.append(line)
    
    print(str(len(unmatched)) + " not tested candidates by thor01: ")
    for line in unmatched:
        print(line.strip())
    """


def compare_empty_partitions():
    lines = {}
    with open("deployment/empty-partitions.log", 'r') as file1:
        lines = {line.split("|")[1] for line in file1.readlines()}

    print("Dict size " + str(len(lines)))

    for node in ["2", "thor01", "thor01-2", "thor02", "thor02-2", "thor03", "thor03-2", "thor04", "thor04-2"]:
        unmatched = []
        with open("deployment/empty-partitions-" + node  + ".log", 'r') as file2:
            for line in file2:
                stripped = line.split("|")[1]
                if stripped not in lines:
                    unmatched.append(stripped)
        
        print(str(len(unmatched)) + " unmatched candidates for " + node + ": " + str(unmatched))

if __name__ == "__main__":
    compare_candidates()
