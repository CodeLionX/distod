# DISTOD [![Build Status](https://travis-ci.com/CodeLionX/distod.svg?branch=master)](https://travis-ci.com/CodeLionX/distod) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/eab2894478bf40bda1a1067f826e94cb)](https://www.codacy.com/manual/CodeLionX/distod?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CodeLionX/distod&amp;utm_campaign=Badge_Grade) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/eab2894478bf40bda1a1067f826e94cb)](https://www.codacy.com/manual/CodeLionX/distod?utm_source=github.com&utm_medium=referral&utm_content=CodeLionX/distod&utm_campaign=Badge_Coverage)

DISTOD algorithm: Distributed discovery of bidirectional order dependencies

## Usage

Running DISTOD with a custom configuration:

```bash
java -Dconfig.file=path/to/config.conf -jar distod.jar
```

Or inside the SBT shell (`sbt`-command):

```sbtshell
; set javaOptions += "-Dconfig.file=path/to/config.conf"; run
```

Running DISTOD with a custom logging configuration:

```bash
java -Dlogback.configurationFile=deployment/logback.xml -jar distod.jar
```

## Profiling with JMC

[JMC](https://www.oracle.com/technetwork/java/javaseproducts/mission-control/index.html) is a tool to analyze metric recordings with the Java Flight Recorder (JFR).
JFR is a very lightweight way to collect low level and detailed runtime information built into the Oracle JDK.
The application (JAR) must therefore be run with an Oracle JVM.
You can download it (Java SE JDK 11) from [their wesite](https://www.oracle.com/technetwork/java/javase/downloads/jdk11-downloads-5066655.html).
A Oracle account is required (free).

To profile the application, build the DISTOD assembly with `sbt assembly` and run it with an Oracle JVM using the following parameters:

```bash
oracle-java -XX:+FlightRecorder -XX:StartFlightRecording=maxage=5m,filename=distod-1.jfr,dumponexit=true -Dcom.sun.management.jmxremote.autodiscovery=true -jar distod.jar
```

If you use a JDK older then version 11, you also have to enable the commercial features with the flag `-XX:+UnlockCommercialFeatures` before the other options are available.

You can adjust the filename of the results in the parameters.
Afterwards the profiling results can be examined using the JDK Mission Control (JMC).
You can download it from [this site](https://www.oracle.com/technetwork/java/javase/downloads/jmc7-downloads-5868868.html).

## Profiling with VisualVM

Start a longer running job with DISTOD, then open [VisualVM](https://visualvm.github.io/) and connect to the running VM.
You can profile the process using the sample tab ("CPU").
Stop it at any time to inspect the results without them changing constantly.

If you want to monitor a remote process, you can start the following deamon for VisualVM to connect to:

```bash
jstatd -J-Djava.security.policy=jstatd.all.policy
```

If you want to use the sampler, you have to enable JMX as well.

## Useful commands

Pin a process to a CPU core (or multiple).
The CPU number starts with 1.
This command only works on linux:

```bash
taskset --cpu-list 1,2 <cmd>
```

