#!/usr/bin/env python3

import os
import re
from datetime import timedelta
import statistics
# import time


# r'^\S*(\s+)\S+([\s.]+)\S+([\s]+)\S+([\s:]+).*$'
logline_pattern = r'^\s*([\d.]+)\s+([\d.]+)\s+([\d]+)\s+([\d:]+).*$'
runfolder_pattern = r'^.*/\d$'
time_pattern = "%H:%M:%S"


def parse_timedelta(s):
    components = s.split(":")
    return timedelta(
        hours=int(components[0]),
        minutes=int(components[1]),
        seconds=int(components[2])
    )
    # return time.strptime(s, time_pattern)


def format_timedelta(td):
    # "%d:%d:%d" % (td)
    return str(td).split(".")[0]
    # return time.strftime(time_pattern, td)


def timedelta_mean(xs):
    in_seconds = map(lambda x: x.total_seconds(), xs)
    mean_seconds = statistics.mean(in_seconds)
    return timedelta(seconds=mean_seconds)


def print_as_csv(metrics, root):
    with open(os.path.join(root, "aggregate-sys-metrics.csv"), 'w', encoding="UTF-8") as f:
        f.write("Configuration,Node,median_pcpu,max_pmem,median_vsz,cum_time,cum_time_seconds\n")
        for exp in metrics:
            for node in metrics[exp]:
                f.write("{:s},{:s},{:02.2f},{:02.2f},{:.0f},{:s},{:.0f}\n".format(
                    exp,
                    node,
                    metrics[exp][node]["pcpu"],
                    metrics[exp][node]["max_pmem"],
                    metrics[exp][node]["vsz"],
                    format_timedelta(metrics[exp][node]["cum_time"]),
                    metrics[exp][node]["cum_time"].total_seconds()
                ))


def collect_metrics(root):
    exp_metrics = {}
    run_metrics = {}
    cum_times = []
    for folder, _, filelist in os.walk(root, topdown=False):
        for f in filelist:
            if f.startswith("sys-metrics"):
                pcpus = []
                max_pmem = .0
                vszs = []
                max_cum_time = parse_timedelta("00:00:00")
                with open(os.path.join(folder, f), 'r', encoding="UTF-8") as fh:
                    for line in fh:
                        m = re.match(logline_pattern, line)
                        if m:
                            groups = m.groups()
                            pcpus.append(float(groups[0]))
                            max_pmem = max(max_pmem, float(groups[1]))
                            vszs.append(int(groups[2]))
                            max_cum_time = max(max_cum_time, parse_timedelta(groups[3]))
                        else:
                            print("No match! For line: %s" % line)
                pcpu = statistics.median(pcpus)
                vsz = statistics.median(vszs)
                if "odin01" in f:
                    node_type = "leader"
                elif "thor" in f:
                    node_type = "follower-thor"
                else:
                    node_type = "follower-odin"

                if node_type not in run_metrics:
                    run_metrics[node_type] = {
                        "pcpu": [],
                        "max_pmem": [],
                        "vsz": [],
                        "cum_time": []
                    }
                run_metrics[node_type]["pcpu"].append(pcpu)
                run_metrics[node_type]["max_pmem"].append(max_pmem)
                run_metrics[node_type]["vsz"].append(vsz)
                cum_times.append(max_cum_time)

        if re.match(runfolder_pattern, folder):
            print("Processed run folder %s" % folder)
            for key in run_metrics:
                run_metrics[key]["cum_time"].append(sum(cum_times, start=parse_timedelta("00:00:00")))
            cum_times = []
        if folder != root and not re.match(runfolder_pattern, folder):
            print("Experiment processed: %s" % folder)
            exp = folder.split("/")[-1]
            for key in run_metrics:
                if exp not in exp_metrics:
                    exp_metrics[exp] = {}
                metrics = run_metrics[key]
                # print(metrics)
                exp_metrics[exp][key] = {
                    "pcpu": statistics.mean(metrics["pcpu"]),
                    "max_pmem": statistics.mean(metrics["max_pmem"]),
                    "vsz": statistics.mean(metrics["vsz"]),
                    "cum_time": timedelta_mean(metrics["cum_time"])
                }
    return exp_metrics


if __name__ == "__main__":
    root = "experiments/results/distod-exp8-jvms.bak"
    metrics = collect_metrics(root)
    print_as_csv(metrics, root)
