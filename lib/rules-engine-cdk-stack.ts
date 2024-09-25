import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { KinesisAnalyticsFlinkConstruct } from "../constructs/kinesis-data-analytics-flink-construct";
import { NagSuppressions } from "cdk-nag";

export class RulesEngineCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const rulesStream = this.createKinesisDataStream("RulesStream");
    const sensorValueStream = this.createKinesisDataStream("SensorValueStream");
    const alertStream = this.createKinesisDataStream("AlertStream");
    const rulesOpStream = this.createKinesisDataStream("RulesOpStream");

    const rulesEngine = new KinesisAnalyticsFlinkConstruct(
      this,
      "RulesEngine",
      {
        filePath:
          "rules-engine/target/amazon-msf-java-dynamic-rules-engine-1.0.jar",
        propertyConfig: {
          propertyGroups: [
            {
              propertyGroupId: "kinesis",
              propertyMap: {
                dataTopicName: sensorValueStream.streamName,
                alertsTopicARN: alertStream.streamArn,
                rulesTopicName: rulesStream.streamName,
                ruleOperationsTopicARN: rulesOpStream.streamArn,
                region: this.region,
                streamPosition: "LATEST",
              },
            },
          ],
        },
      }
    );

    rulesStream.grantRead(rulesEngine.role);
    sensorValueStream.grantRead(rulesEngine.role);
    alertStream.grantWrite(rulesEngine.role);
    rulesOpStream.grantWrite(rulesEngine.role);

    this.createIngestionLambdaFunction(
      "SensorIngestFn",
      "event-ingestion",
      sensorValueStream
    );
    this.createIngestionLambdaFunction(
      "RuleIngestionFn",
      "rule-ingestion",
      rulesStream
    );
    this.createProcessingLambdaFunction(
      "RuleProcessingFn",
      "rule-ops-ingestion",
      rulesOpStream
    );
    this.createProcessingLambdaFunction(
      "AlertProcessingFn",
      "alert-ingestion",
      alertStream
    );
  }

  private createKinesisDataStream(name: string): cdk.aws_kinesis.Stream {
    return new cdk.aws_kinesis.Stream(this, name, {
      streamName: name,
      shardCount: 1,
    });
  }

  private createIngestionLambdaFunction(
    name: string,
    functionFolder: string,
    stream: cdk.aws_kinesis.Stream
  ): cdk.aws_lambda.Function {
    const func = new cdk.aws_lambda.Function(this, name, {
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_12,
      code: cdk.aws_lambda.Code.fromAsset(`lambda/${functionFolder}/`),
      handler: "index.lambda_handler",
      environment: {
        KINESIS_STREAM_ARN: stream.streamArn,
      },
    });
    stream.grantWrite(func.role!);
    this.supressLambdaExecutionRoleNags(func);
    return func;
  }
  private createProcessingLambdaFunction(
    name: string,
    functionFolder: string,
    stream: cdk.aws_kinesis.Stream
  ): cdk.aws_lambda.Function {
    const func = new cdk.aws_lambda.Function(this, name, {
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_12,
      code: cdk.aws_lambda.Code.fromAsset(`lambda/${functionFolder}/`),
      handler: "index.lambda_handler",
    });
    stream.grantRead(func.role!);
    // connect stream to function
    func.addEventSource(
      new cdk.aws_lambda_event_sources.KinesisEventSource(stream, {
        startingPosition: cdk.aws_lambda.StartingPosition.LATEST,
      })
    );
    this.supressLambdaExecutionRoleNags(func);    
    return func;
  }
  private supressLambdaExecutionRoleNags(functionRole: cdk.aws_lambda.Function): void {
    NagSuppressions.addResourceSuppressions(
      functionRole,
      [
        {
          id: "AwsSolutions-IAM4",
          reason: "Grantable roles added to Lambda Function role",
          appliesTo: [
            {
              regex: "/^Policy::arn:<AWS::Partition>:iam::aws:policy\/service-role\/AWSLambdaBasicExecutionRole$/g",
            },
          ],
        },
      ],
      true
    );
  }
}
