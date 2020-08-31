# Metanome integration for DISTOD

> **Metanome and DISTOD are currently not compatible!**

DISTOD requires substantial changes to the Metanome tool
because it not only discovers a new type of dependencies (bidirectional set-based order dependencies) that is not part of the current Metanome API
but also uses a host of libraries and dynamic class loading that is not compatible to the current way of algorithm loading implemented in Metanome.

## Integration status matrix

| Issue Number | Issue | Affected project | Identified | Solution | Implemented |
| :--- | :--- | :--- | :---: | :--- | :---: |
| 1 | DISTOD requires various parameters to be set | DISTOD | :heavy_check_mark: | Expose parameters using Metanome API | :heavy_check_mark: |
| 2 | DISTOD reads CSV files directly | DISTOD | :heavy_check_mark: | Use Metanome API to read input (new `DataReader` actor) | :x: |
| 3 | DISTOD writes outputs to a file directly | DISTOD | :heavy_check_mark: | Use Metanome API to write output (new `ResultCollector` actor) | :x: |
| 4 | DISTOD (akka) loads configuration files from the class path | Metanome | :heavy_check_mark: | Add algorithm's jar-file to the classpath in Metanome | :heavy_check_mark: |
| 5 | DISTOD (akka) dynamically loads classes (reflection) | Metanome | :heavy_check_mark: | Add algorithm's jar-file to the classpath in Metanome | :heavy_check_mark: |
| 6 | DISTOD and Metanome use and bring their own [Jackson databind](https://github.com/FasterXML/jackson-databind) leading to a class being on the classpath multiple times (with different versions) | Metanome | :x: |  | :x: |
| 7 | DISTOD produces bidirectional set-based ODs not supported by `OrderDependencyAlgorithm` | Metanome | :heavy_check_mark: | Extend Metanome API with new algorithm type and result type | :x: | 

## Integration blocker: Issue 6

Metanome includes all its libraries in the classpath of the JVM the benchmarked algorithm is executed in.
This includes a lot of transitive libraries from Hibernate, Tomcat, and other runtime tools.
When an algorithm comes with its own set of libraries, there is a high chance that there are version clashes.
This is the case for DISTOD (first observed one is with Jackson databind).

We can not exclude the algorithm's dependencies because they are required by the algorithm.
Excluding the Metanome dependencies from the algorithm's process requires various changes to the Metanome tool itself,
because Metanome performs database queries from the algorithm's process to load configuration parameters and to store results.

Options to resolve this issue:

- Try to use different class loaders for the Metanome tooling and the algorithm.
  Are libraries, such as Akka, picking up the correct class loader for their dynamic class loading?
- Remove most of the Metanome dependencies from the algorithm's process by using inter-process communication.
  All libraries used in the algorithm's process must be included in the Metanome integration API,
  so that the build tool can resolve the dependencies.
  This also means that the Metanome algorithm execution code becomes part of the API.
- ?


## Reproduce current integration state and error

To reproduce the current DISTOD-Metanome integration state follow the steps below:

1. Build the patched Metanome tool and deploy it locally
   - Clone the forked Metanome repository `CodeLionX/Metanome` and checkout the `fix/jarLoading`-branch:
     ```sh
     git clone -b fix/jarLoading git@github.com:CodeLionX/Metanome.git
     ```
     This fork changes the algorithm execution component of Metanome to include the Algorithm's jar-file into the classpath,
     allowing the use of packaged configuration files and dynamic class loading (by Java reflection).
     Both mechanisms are required by DISTOD because it uses Akka which heavily relies on dynamic class loading for their extension system. 
   - Load the frontend code:
     ```sh
     git submodule init && git submodule update
     ```
   - Build Metanome:
     ```sh
     mvn clean install -DskipTests
     ```
   - Assemble the deployment artifact and extract it into the `deployment`-folder:
     ```sh
     mvn -f deployment/pom.xml package
     unzip -d deployment/ deployment/target/deployment-*-package_with_tomcat.zip
     ```
2. Build DISTOD metanome integration module
   - In this repository (`CodeLionX/distod`) checkout the `feature/metanomeIntegration`-branch.
     ```sh
     git clone -b feature/metanomeIntegration git@github.com:CodeLionX/distod.git
     ```
   - Build DISTOD metanome code:
     ```sh
     sbt metanomeIntegration/assembly
     ```
   - Copy DISTOD algorithm into Metanome's algorithm folder:
     ```sh
     cp distod-metanome-integration/target/scala-2.13/distod-metanome-integration-assembly-*.jar \
       <metanome>/deployment/backend/WEB-INF/classes/algorithms/distod.jar
     ```
3. Load DISTOD in Metanome and execute algorithm
   - Start Metanome
     ```sh
     cd <metanome>/deployment && ./run.sh
     ```
   - Open [`http://localhost:8080/`](http://localhost:8080/) in a Browser
   - Add `distod.jar` as an algorithm
   - Select `distod` and any dataset in Metanome
   - Scroll down and execute the algorithm
   - You can observe all output in the shell you started Metanome from
   - The following error is thrown by DISTOD:
     ```
     Uncaught error from thread [distod-akka.actor.internal-dispatcher-2]: tried to access field com.fasterxml.jackson.core.JsonFactory.DEFAULT_ROOT_VALUE_SEPARATOR from class com.fasterxml.jackson.core.JsonFactoryBuilder, shutting down JVM since 'akka.jvm-exit-on-fatal-error' is enabled for ActorSystem[distod]
     java.lang.IllegalAccessError: tried to access field com.fasterxml.jackson.core.JsonFactory.DEFAULT_ROOT_VALUE_SEPARATOR from class com.fasterxml.jackson.core.JsonFactoryBuilder
             at com.fasterxml.jackson.core.JsonFactoryBuilder.<init>(JsonFactoryBuilder.java:36)
             at akka.serialization.jackson.JacksonObjectMapperProvider$.createJsonFactory(JacksonObjectMapperProvider.scala:75)
             at akka.serialization.jackson.JacksonObjectMapperProvider$.createObjectMapper(JacksonObjectMapperProvider.scala:234)
             at akka.serialization.jackson.JacksonObjectMapperProvider.create(JacksonObjectMapperProvider.scala:325)
             at akka.serialization.jackson.JacksonObjectMapperProvider.$anonfun$getOrCreate$1(JacksonObjectMapperProvider.scala:283)
             at java.util.concurrent.ConcurrentHashMap.computeIfAbsent(ConcurrentHashMap.java:1660)
             at akka.serialization.jackson.JacksonObjectMapperProvider.getOrCreate(JacksonObjectMapperProvider.scala:283)
             at akka.serialization.jackson.JacksonJsonSerializer.<init>(JacksonSerializer.scala:98)
             at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
             at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
             at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
             at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
             at akka.actor.ReflectiveDynamicAccess.$anonfun$createInstanceFor$1(ReflectiveDynamicAccess.scala:40)
             at scala.util.Try$.apply(Try.scala:210)
             at akka.actor.ReflectiveDynamicAccess.createInstanceFor(ReflectiveDynamicAccess.scala:35)
             at akka.actor.ReflectiveDynamicAccess.$anonfun$createInstanceFor$5(ReflectiveDynamicAccess.scala:48)
             at scala.util.Success.flatMap(Try.scala:258)
             at akka.actor.ReflectiveDynamicAccess.createInstanceFor(ReflectiveDynamicAccess.scala:47)
             at akka.serialization.Serialization$$anonfun$serializerOf$1$$anonfun$applyOrElse$1.applyOrElse(Serialization.scala:396)
             at akka.serialization.Serialization$$anonfun$serializerOf$1$$anonfun$applyOrElse$1.applyOrElse(Serialization.scala:392)
             at scala.util.Failure.recoverWith(Try.scala:240)
             at akka.serialization.Serialization$$anonfun$serializerOf$1.applyOrElse(Serialization.scala:392)
             at akka.serialization.Serialization$$anonfun$serializerOf$1.applyOrElse(Serialization.scala:390)
             at scala.util.Failure.recoverWith(Try.scala:240)
             at akka.serialization.Serialization.serializerOf(Serialization.scala:390)
             at akka.serialization.Serialization.$anonfun$serializers$2(Serialization.scala:424)
             at scala.collection.Iterator$$anon$9.next(Iterator.scala:573)
             at scala.collection.immutable.HashMapBuilder.addAll(HashMap.scala:2338)
             at scala.collection.immutable.HashMap$.from(HashMap.scala:2162)
             at scala.collection.immutable.HashMap$.from(HashMap.scala:2138)
             at scala.collection.MapOps$WithFilter.map(Map.scala:353)
             at akka.serialization.Serialization.<init>(Serialization.scala:424)
             at akka.serialization.SerializationExtension$.createExtension(SerializationExtension.scala:18)
             at akka.serialization.SerializationExtension$.createExtension(SerializationExtension.scala:14)
             at akka.actor.ActorSystemImpl.registerExtension(ActorSystem.scala:1159)
             at akka.actor.ActorSystemImpl.$anonfun$loadExtensions$1(ActorSystem.scala:1200)
             at scala.collection.IterableOnceOps.foreach(IterableOnce.scala:553)
             at scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:551)
             at scala.collection.AbstractIterable.foreach(Iterable.scala:921)
             at akka.actor.ActorSystemImpl.loadExtensions$1(ActorSystem.scala:1195)
             at akka.actor.ActorSystemImpl.loadExtensions(ActorSystem.scala:1213)
             at akka.actor.ActorSystemImpl.liftedTree2$1(ActorSystem.scala:1044)
             at akka.actor.ActorSystemImpl._start$lzycompute(ActorSystem.scala:1031)
             at akka.actor.ActorSystemImpl._start(ActorSystem.scala:1031)
             at akka.actor.ActorSystemImpl.start(ActorSystem.scala:1054)
             at akka.actor.typed.ActorSystem$.createInternal(ActorSystem.scala:278)
             at akka.actor.typed.ActorSystem$.apply(ActorSystem.scala:192)
             at com.github.codelionx.distod.ActorSystem$.create(ActorSystem.scala:41)
             at com.github.codelionx.distod.DistodAlgorithm.execute(DistodAlgorithm.scala:62)
             at de.metanome.backend.algorithm_execution.AlgorithmExecutor.executeAlgorithm(AlgorithmExecutor.java:186)
             at de.metanome.backend.algorithm_execution.AlgorithmExecution.main(AlgorithmExecution.java:222)
     ```