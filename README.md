# Dynamic Rules Engine

## Project Description

The Dynamic Rules Engine is a serverless application that enables real-time evaluation of rules against incoming sensor data. It leverages AWS Kinesis Data Streams for ingesting sensor data and rule definitions, Amazon Managed Service for Apache Flink for processing the data and evaluating rules, and AWS Lambda functions for handling ingestion and processing operations.

The key components include:
- Amazon Managed Service for Apache Flink
- Amazon Kinesis Data Streams
- AWS Lambda (for testing)

## Creating Rules Engine Jar

Use Maven to build jar.

```bash
mvn package -f rules-engine/pom.xml
```

## Deploy CDK Stack

```bash
npm install ## Install required dependencies for CDK deployment
cdk deploy RulesEngineCdkStack ## Deploy stack
```

## Example Rule
```json
{
  "id": "cda160c0-c790-47da-bd65-4abae838af3a", // Some UUID
  "name": "RuleTest1",
  "status": "ACTIVE", // ACTIVE or INACTIVE
  "equipmentName": "THERMOSTAT_1",
  "ruleExpression": "(SENSOR_cebb1baf_2df0_4267_b489_28be562fccea.hasChanged(5))",
  "sensorWindowMap": {
    "SENSOR_cebb1baf_2df0_4267_b489_28be562fccea": 5 // Map of how long the sensor value should be persisted
  }
}
```

## Example Sensor Value
```json
{
  "equipment": {
    "id": "THERMOSTAT_1"
  },
  "id": "SENSOR_cebb1baf_2df0_4267_b489_28be562fccea", // UUID of sensor
  "measureValue": 10,
  "eventTimestamp": 1721666423000
}
```

## Contributing

Pull requests are welcome.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License
This library is licensed under the MIT-0 License. See the LICENSE file.
[MIT](https://choosealicense.com/licenses/mit/)


