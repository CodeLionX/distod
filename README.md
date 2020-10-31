# DISTOD [![Build Status](https://travis-ci.com/CodeLionX/distod.svg?branch=master)](https://travis-ci.com/CodeLionX/distod) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/eab2894478bf40bda1a1067f826e94cb)](https://www.codacy.com/manual/CodeLionX/distod?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CodeLionX/distod&amp;utm_campaign=Badge_Grade) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/eab2894478bf40bda1a1067f826e94cb)](https://www.codacy.com/manual/CodeLionX/distod?utm_source=github.com&utm_medium=referral&utm_content=CodeLionX/distod&utm_campaign=Badge_Coverage)

The DISTOD data profiling algorithm is a distributed algorithm to discover bidirectional order dependencies (in set-based form) from relational data.
DISTOD is based on the single-threaded FASTOD-BID algorithm [1], but DISTOD scales elastically to many machines outperforming FASTOD-BID by up to orders of magnitude.

## Abstract

Bidirectional order dependencies (bODs) capture order relationships between lists of attributes in a relational table.
They can express that, for example, sorting books by _publication date_ in ascending order also sorts them by _age_ in descending order.
The knowledge about order relationships is useful for many data management tasks, such as query optimization, data cleaning, or consistency checking.
Because the bODs of a specific dataset are usually not explicitly given, they need to be discovered.
The discovery of all minimal bODs (in set-based canonical form) is a task with exponential complexity in the number of attributes, though, which is why existing bOD discovery algorithms cannot process datasets of practically relevant size in a reasonable time.
In this paper, we propose the _distributed_ bOD discovery algorithm DISTOD, whose execution time scales with the available hardware.
DISTOD is a scalable, robust, and elastic bOD discovery approach that combines efficient pruning techniques for bOD candidates in set-based canonical form with a novel, reactive, and distributed search strategy.
Our evaluation on various datasets shows that DISTOD outperforms both single-threaded [1] and distributed [2] state-of-the-art bOD discovery algorithms by up to orders of magnitude;
it can, in particular, process much larger datasets.

## Citation

> tbd

<!--
If you use this code and method, please considering using the following reference:

```biblatex
@article{SchmidlAndPapenbrock2020Efficient,
    author={Sebastian {Schmidl} and Thorsten {Papenbrock}},
    title={Efficient Distributed Discovery of Bidirectional Order Dependencies},
    journal={},
    year={2020},
    volume={},
    number={},
    pages={},
    doi={},
    keywords={
        bidirectional order dependencies;
        distributed computing;
        actor programming;
        parallelization;
        data profiling;
        dependency discovery
    },
}
```

You can find the paper about this algorithm [here]().
-->

## Installation & Usage

### Prerequisites

The following software is required:

- Java JRE > 1.8.0

### Procedure

- Download the DISTOD jar-file (`distod-vx.x.x.jar`) from the [latest release](https://github.com/CodeLionX/distod/releases/latest).
  In the following examples, we always refer to the DISTOD jar-file with `distod.jar`.
- Execute the algorithm with:
  ```sh
  java -Xms2g -Xmx2g -XX:+UseG1GC \
    -Ddistod.input.path="data/iris.csv" -Ddistod.input.has-header="yes" \
    -Dfile.encoding=UTF-8 \
    -jar distod.jar
  ```

> **Attention!**
>
> Please make sure to always run DISTOD with the G1 GC (`-XX:+UseG1GC`) and both heap memory limits (`-Xms` and `-Xmx`) set.
> If you do not explicitly disable system monitoring, it is required to set both heap memory limits during application start to the same value.
> This allows the system monitoring component to make more accurate decision regarding the memory usage. 


### Algorithm configuration

DISTOD exposes various parameters to control its execution and features as configuration options.
Configuration values can be set on the command line or using a configuration file:

- Using the CLI: `java -Xms2g -Xmx2g -XX:+UseG1GC -Ddistod.input.path="data/iris.csv" -Ddistod.input.has-header="yes" -Dfile.encoding=UTF-8 -jar distod.jar`
- To use a configuration file `distod.conf` with the content
  ```hocon
  distod.input {
    path = data/iris.csv
    has-header = yes
  }
  ```
  run DISTOD using `java -Xms2g -Xmx2g -XX:+UseG1GC -Dconfig.file=distod.conf -Dfile.encoding=UTF-8 -jar distod.jar`.

For a full description of all ways one can use and set configuration options, we refer to the [`lightbend/config` documentation](https://github.com/lightbend/config) which we make use of here.
You can find all parameters of DISTOD, their default value, and their description in the [`application.conf` file](./distod/src/main/resources/application.conf) bundled with DISTOD.

You can also change the logging behavior of DISTOD by altering the configuration of DISTOD's [logback logger](http://logback.qos.ch/manual/introduction.html). 
Follow this procedure:
- Create a [logback configuration file](http://logback.qos.ch/manual/configuration.html) (e.g. `logback.xml`) with your logging configuration, such as:
  ```xml
  <?xml version="1.0" encoding="UTF-8"?>

  <configuration>
      <appender name="FILE" class="ch.qos.logback.core.FileAppender">
          <file>distod.log</file>
          <append>false</append>
          <encoder>
              <pattern>[%d{HH:mm:ss.SSS} %-5level] %30.30X{akkaSource:-local}| %msg%n</pattern>
          </encoder>
      </appender>

      <root level="INFO">
          <appender-ref ref="FILE"/>
      </root>
  </configuration>
  ```
- Supply DISTOD with the path to this configuration file via the option `-Dlogback.configurationFile`:
  ```sh
  java -Xms2g -Xmx2g -XX:+UseG1GC -Dlogback.configurationFile=logback.xml \
    -Ddistod.input.path="data/iris.csv" -Ddistod.input.has-header="yes" \
    -Dfile.encoding=UTF-8 \
    -jar distod.jar
  ```

### GC tuning

After some careful, manual evaluation on the OpenJDK 1.8.0_265 (64-Bit) Server VM,
we set the following additional java options to tune the Java GC to clean up more older generation objects that get freed up by DISTOD's `PartitionMgr`.

- `-XX:G1ReservePercent=10`: matches DISTOD's default parameter value for `distod.monitoring.heap-eviction-threshold` (see [configuration file](./distod/src/main/resources/application.conf))
- `-XX:G1HeapWastePercent=1`: reduce waste and try to reclaim as much OldGen as possible; increases mixed GC cycles (reduced from 5)
- `-XX:MaxGCPauseMillis=400`: allow longer GC pauses (increased from 200)
- `-XX:G1MixedGCLiveThresholdPercent=60`: already mark regions that only contain 60% garbage (reduced from 85)
- `-XX:G1MixedGCCountTarget=10`: have up to 10 consecutive mixed runs to clean up more OldGen garbage (increased from 8)
- `-XX:G1OldCSetRegionThresholdPercent=20`: reclaim up to 20% (of heap size) OldGen garbage in one run (increased from 10)

This results in the following run-command:

```sh
java -Xms2g -Xmx2g -XX:+UseG1GC \
  -XX:+UnlockExperimentalVMOptions \
  -XX:G1ReservePercent=10 -XX:MaxGCPauseMillis=400 -XX:G1HeapWastePercent=1 \
  -XX:G1MixedGCLiveThresholdPercent=60 -XX:G1MixedGCCountTarget=10 -XX:G1OldCSetRegionThresholdPercent=20 \
  -Dconfig.file="$distod.conf" -Dlogback.configurationFile=logback.xml -Dfile.encoding=UTF-8 \
  -jar distod.jar
```

### Experiments

The configuration of DISTOD and its competitors used for the experiments in the paper can be found in the [`experiments`-folder](./experiments).
Note that we do not publish the results of the experiments in this Github-Repository due to their size.
Please contact [Sebastian Schmidl](https://github.com/codelionx) directly for the experiments' result backups.

## Building

### Prerequisites

- Git (v2.25.x)
- Java (v1.8.0 or greater)
- SBT (v1.3.x)
- (Scala version is managed by SBT, currently targets Scala 2.13.1)

## Procedure

1. Clone this repository
   ```shell script
   git clone git@github.com:CodeLionX/distod.git
   ```
2. Build the whole project and create runtime artefacts
   ```sh
   sbt assembly
   ```
   You can find the fat-jar for DISTOD in the target folder of the `distod`-module.

- If you just want to build the `distod`-module run `sbt distod/assembly`.
- If you want to run `distod` from within SBT, use `sbt "; project distod; set javaOptions += "-Dconfig.file=distod.conf"; run"`

## Contributing

If you want to contribute to this project, you are more then welcome to do so.
Please read the [contribution guidlines](./CONTRIBUTING.md) before submitting new issues or pull requests.

### Profiling with JMC

[JMC](https://www.oracle.com/technetwork/java/javaseproducts/mission-control/index.html) is a tool to analyze metric recordings with the Java Flight Recorder (JFR).
JFR is a very lightweight way to collect low level and detailed runtime information built into the Oracle JDK.
The application (JAR) must therefore be run with an Oracle JVM.
You can download it (Java SE JDK 11) from [their website](https://www.oracle.com/technetwork/java/javase/downloads/jdk11-downloads-5066655.html).
A Oracle account is required (free).

To profile the application, build the DISTOD assembly with `sbt assembly` and run it with an Oracle JVM using the following parameters:

```bash
oracle-java -XX:+FlightRecorder -XX:StartFlightRecording=maxage=5m,filename=distod-1.jfr,dumponexit=true -Dcom.sun.management.jmxremote.autodiscovery=true -jar distod.jar
```

If you use a JDK older then version 11, you also have to enable the commercial features with the flag `-XX:+UnlockCommercialFeatures` before the other options are available.

You can adjust the filename of the results in the parameters.
Afterwards the profiling results can be examined using the JDK Mission Control (JMC).
You can download it from [this site](https://www.oracle.com/technetwork/java/javase/downloads/jmc7-downloads-5868868.html).

### Profiling with VisualVM

Start a longer running job with DISTOD, then open [VisualVM](https://visualvm.github.io/) and connect to the running VM.
You can profile the process using the sample tab ("CPU").
Stop it at any time to inspect the results without them changing constantly.

If you want to monitor a remote process, you can start the following daemon for VisualVM to connect to:

```bash
jstatd -J-Djava.security.policy=jstatd.all.policy
```

An [example `jstatd.all.policy`-file](./deployment/jstatd.all.policy) can be found in the `deployment` folder.

If you want to use the sampler, you have to enable JMX as well.
To do this, use the following options when starting the JVM and then connect to `hostname:9010` using VisualVM:

- `-Dcom.sun.management.jmxremote`: enables JMX
- `-Dcom.sun.management.jmxremote.authenticate=false`, `-Dcom.sun.management.jmxremote.ssl=false`: disables authentication (**dangerous!**)
- `-Dcom.sun.management.jmxremote.port=9010`, `-Dcom.sun.management.jmxremote.rmi.port=9010`: sets remote port
- `-Djava.rmi.server.hostname="$(hostname)"`: sets remote host to bind to

Example call:

```sh
java -Xms2g -Xmx2g -XX:+UseG1GC  \
  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Djava.rmi.server.hostname="$(hostname)" \
  -Dconfig.file="distod.conf" \
  -Dlogback.configurationFile=logback.xml \
  -Dfile.encoding=UTF-8 \
  -jar distod.jar

```

### Useful commands

- Supply program arguments to `run`-command in SBT shell:

  ```sbtshell
  ; set javaOptions += "-Dconfig.file=path/to/config.conf"; run
  ```

- Pin a process to a CPU core (or multiple).
  The CPU number starts with 1.
  This command only works on linux:

  ```bash
  taskset --cpu-list 1,2 <cmd>
  ```

- Running the distributed version of FASTOD with Spark:
  - Admin must start spark on all nodes
  - Copy over dependencies (`lib`-folder), application (`jar`-file) on head node of cluster (`odin01`)
  - Convert dataset to json and make sure that only numerical values are used.
    You can substitute the hash of the original values with the `scripts/to-json.py -s` script.
  - Copy dataset to all nodes
  - Use `spark-submit` on head node to start algorithm

  ```bash
  spark-submit --jars libs/fastutil-6.1.0.jar,libs/lucene-core-4.5.1.jar --class FastODMain --master spark://odin01:7077 --executor-memory 10G --num-executors 2 --executor-cores 10 --total-executor-cores 20 distributed-fastod.jar file:${DATASET}" "${BATCHSIZE}"
  ```

  The `total-executor-cores` values is calculated based on the number of executors (nodes) and the number of processors (cores) that should be used by the executor.


## References

- [1] Szlichta, Jaroslaw, Parke Godfrey, Lukasz Golab, Mehdi Kargar, and Divesh Srivastava. “Effective and Complete Discovery of Bidirectional Order Dependencies via Set-Based Axioms.” The VLDB Journal 27, no. 4 (2018): 573–91. https://doi.org/10.1007/s00778-018-0510-0.
- [2] Saxena, Hemant, Lukasz Golab, and Ihab F. Ilyas. “Distributed Implementations of Dependency Discovery Algorithms.” Proceedings of the VLDB Endowment 12, no. 11 (2019): 1624–36. https://doi.org/10.14778/3342263.3342638.
