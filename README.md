# Collection of Flink Java Examples

- `PaymentsVolumeJob` - calculates the total amount and number of payments for each customer in a given time window.

## Getting Started
Generate a new project from a quick start template:

```
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.20.3 \
  -DgroupId=com.andreyfadeev \
  -DartifactId=flink-java-examples \
  -Dversion=0.1 \
  -DinteractiveMode=false
```

Although Flink 2.x was released and contains a lot of cool new features and APIs, I'm still using 1.20.3 for this project as most of the managed Flink providers still don't support 2.x (AWS Flink, Ververica Platform, etc.)

## Testing

Tests are written with `MiniClusterWithClientResource` (an in-memory Flink cluster) with pluggable source and sink implementations.

To run tests:
```shell 
mvn clean test
```

Read more about testing here: https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/datastream/testing/

## Deployments

I've experimented with AWS Flink, the deployment is done by compling JAR file and uploading it to S3. AWS Flink job is configured with the S3 location of the JAR file.

To build JAR:
```shell 
mvn clean package
ls target/flink-java-examples-0.1.jar 
```

## Flink documentation links

- Windows: https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/datastream/operators/windows/
