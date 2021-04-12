# DISTOD experiments

This folder contains the configuration, artefacts, and scripts used to perform the experiments in the paper.

## Structure & Ansible

We use a combination of [Ansible](https://www.ansible.com/) and bash scripts to automate the configuration and execution of the experiments on a twelve node cluster.

The folder [`ansible`](./ansible) contains the Ansible configuration files and playbooks.
Each algorithm's experiment scripts are located in their corresponding folder.
DISTOD and FASTOD-BID read the same data format, the datasets should be located in the `experiments/data` folder.
Since DIST-FASTOD-BID reads JSON, it reads the transformed datasets from `experiments/fastod-spark/data`.
The original datasets can be downloaded from [the HPI repeatability website](https://hpi.de/naumann/projects/repeatability/data-profiling/fds.html)
and should be preprocessed with the [`to-json.py`-script](../scripts/to-json.py) to substitute all values with an integer representation and transform them to header-less CSV and JSON files.

## How to run

Executing an experiment from the `experiments`-folder is done using [Ansible](https://www.ansible.com/) playbooks, for example:

```sh
cd experiments
ansible-playbook -i ansible/inventory.ini ansible/fastod.yml -e 'experiment=exp1-datasets'
```

If the experiment is in the _Wait until experiment finished_ step, one can safely stop the Ansible _driver_ process (on the local machine) using `Ctrl-C`.
After the experiment finished, you can then obtain the results by **changing** the `load-results.yml` playbook to the executed experiment and running:

```sh
ansible-playbook -i ansible/inventory.ini ansible/load-results.yml
```

## Experiments

| Experiment | DISTOD | FASTOD-BID | DIST-FASTOD-BID | Description |
| :--- | :---: | :---: | :---: | :--- |
| exp1-datasets | [:heavy_check_mark:](./distod/exp1-datasets.sh) | [:heavy_check_mark:](./fastod/exp1-datasets.sh) | [:heavy_check_mark:](./fastod-spark/exp1-datasets.sh) | Tests each algorithm in its most powerfull configuration on all datasets. |
| exp2-nodes | [:heavy_check_mark:](./distod/exp2-nodes.sh) | (n/a) | [:heavy_check_mark:](./fastod-spark/exp2-nodes.sh) | Scales the number of nodes on the adult dataset. |
| exp3-cost | [:heavy_check_mark:](./distod-cost/exp3-cost.sh) | :x: | :x: | Scales the number of cores on the hepatitis and adult datasets. |
| exp4-rows | [:heavy_check_mark:](./distod/exp4-rows.sh) | :x: | :x: | Scales the number of rows on the adult, flight, and ncvoter datasets. |
| exp5-columns | [:heavy_check_mark:](./distod/exp5-columns.sh) | :x: | :x: | Scales the number of columns on the plista dataset. |
| exp6-memory | **Performed manually!** | [:heavy_check_mark:](./fastod/exp6-memory.sh) | **Performed manually!** | Compares the runtime of all algorithms with different heap memory limits. |
| exp7-caching | [:heavy_check_mark:](./distod/exp7-caching.sh) | (n/a) | (n/a) | Compares the runtimes of DISTOD with partition caching turned off or on. |
| exp8-jvms | [:heavy_check_mark:](./distod/exp8-jvms.sh) | (n/a) | (n/a) | Runs DISTOD on different JVMs and using different GCs and settings. |
| exp9-dispatchers | [:heavy_check_mark:](./distod/exp9-dispatchers.sh) | (n/a) | (n/a) | Runs the DISTOD master and the workers on different dispatcher implementations to compare their impact. |
| exp10-network | [:heavy_check_mark:](./distod/exp10-network.sh) | (n/a) | :x: | Measures the network utilization while DISTOD is running on the full cluster. **Requires password-less `sudo` and [iptraf](http://iptraf.seul.org/) installed (`iptraf-ng` in `PATH`).** |
