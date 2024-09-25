/**
 * Copyright 2023 Amazon.com, Inc. and its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *   http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import * as cdk from "aws-cdk-lib";
import { NagSuppressions } from "cdk-nag";
import { Construct } from "constructs";

export interface KinesisAnalyticsFlinkConstructProps extends cdk.StackProps {
  filePath: string;
  role?: cdk.aws_iam.Role;
  propertyConfig?: cdk.aws_kinesisanalyticsv2.CfnApplication.EnvironmentPropertiesProperty;
}

const defaultProps: Partial<KinesisAnalyticsFlinkConstructProps> = {};

/**
 * Deploys the Kinesis Flink Jar construct
 */
export class KinesisAnalyticsFlinkConstruct extends Construct {
  application: cdk.aws_kinesisanalyticsv2.CfnApplication;
  bucket: cdk.aws_s3.Bucket;
  role: cdk.aws_iam.Role;
  constructor(
    parent: Construct,
    name: string,
    props: KinesisAnalyticsFlinkConstructProps
  ) {
    super(parent, name);

    props = { ...defaultProps, ...props };

    // We'll use the role from props OR create one
    this.role = props.role || this.createKinesisDataAnalyticsRole();

    // Grant Read/Write access to the role for the uploaded bucket
    const updatedBucket = this.createJarUploadBucket(props.filePath);
    this.bucket = updatedBucket.jarFileBucket;
    this.bucket.grantReadWrite(this.role);

    // We get the bucket deployment so we can select the first element of the array
    // and use it as the Jar key of the application
    const bucketDeployment = updatedBucket.bucket_deployment;

    // Create the app
    this.application = this.createApplication(
      name,
      this.bucket,
      cdk.Fn.select(0, bucketDeployment.objectKeys),
      this.role,
      props.propertyConfig || {}
    );


    // CDK Nag supressions
    this.supressBucketDeploymentNags();
    this.supressBucketNags(this.bucket);
    this.supressKDARoleNags(this.role);
  }

  private createKinesisDataAnalyticsRole(): cdk.aws_iam.Role {
    const role = new cdk.aws_iam.Role(this, `KDARole`, {
      assumedBy: new cdk.aws_iam.ServicePrincipal(
        "kinesisanalytics.amazonaws.com"
      ),
      managedPolicies: [
        cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
          "AmazonKinesisAnalyticsFullAccess"
        ),
        cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
          "CloudWatchFullAccess"
        ),
      ],
    });
    return role;
  }

  private createJarUploadBucket(jarPath: string): {
    jarFileBucket: cdk.aws_s3.Bucket;
    bucket_deployment: cdk.aws_s3_deployment.BucketDeployment;
  } {
    const jarFileBucket = new cdk.aws_s3.Bucket(this, `KDAJarFileBucket`, {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: cdk.aws_s3.BlockPublicAccess.BLOCK_ALL,
      encryption: cdk.aws_s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
    });

    // default of 128MiB isn't large enough for larger deployments. More memory doesn't improve the performance.
    // You want just enough memory to guarantee deployment
    const memoryLimit = 512;
    const bucket_deployment = new cdk.aws_s3_deployment.BucketDeployment(
      this,
      `KDAJarDeployment`,
      {
        sources: [cdk.aws_s3_deployment.Source.asset(jarPath)],
        destinationBucket: jarFileBucket,
        extract: false,
        memoryLimit,
      }
    );
    this.supressBucketDeploymentNags();

    return { jarFileBucket, bucket_deployment };
  }
  private createApplication(
    name: string,
    bucket: cdk.aws_s3.Bucket,
    jarFile: string,
    executionRole: cdk.aws_iam.Role,
    properties: cdk.aws_kinesisanalyticsv2.CfnApplication.EnvironmentPropertiesProperty,
    runtimeEnvironment: string = "FLINK-1_19",
    applicationMode: string = "STREAMING",
    codeContentType: string = "ZIPFILE",
    snapshotsEnabled: boolean = true,
    loggingEnabled: boolean = true
  ): cdk.aws_kinesisanalyticsv2.CfnApplication {
    const application = new cdk.aws_kinesisanalyticsv2.CfnApplication(
      this,
      `KDA`,
      {
        applicationName: name,
        applicationMode: applicationMode,
        runtimeEnvironment: runtimeEnvironment,
        serviceExecutionRole: executionRole.roleArn,
        runConfiguration: {
          flinkRunConfiguration: {
            allowNonRestoredState: false,
          },
          applicationRestoreConfiguration: {
            applicationRestoreType: "RESTORE_FROM_LATEST_SNAPSHOT",
          },
        },
        applicationConfiguration: {
          environmentProperties: properties,
          applicationSnapshotConfiguration: {
            snapshotsEnabled: snapshotsEnabled,
          },
          flinkApplicationConfiguration: {
            checkpointConfiguration: {
              configurationType: "CUSTOM",
              checkpointingEnabled: true,
              checkpointInterval: 60000,
              minPauseBetweenCheckpoints: 5000,
            },
            monitoringConfiguration: {
              configurationType: "CUSTOM",
              logLevel: "WARN",
            },
          },
          applicationCodeConfiguration: {
            codeContentType: codeContentType,
            codeContent: {
              s3ContentLocation: {
                bucketArn: bucket.bucketArn,
                fileKey: jarFile,
              },
            },
          },
        },
      }
    );
    if (loggingEnabled && application.applicationName) {
      // Create log stream
      const logGroup = new cdk.aws_logs.LogGroup(
        this,
        "KDAApplicationLogGroup",
        {
          logGroupName: `/aws/kinesisanalyticsv2/application/${application.applicationName}`,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
          retention: cdk.aws_logs.RetentionDays.ONE_WEEK,
        }
      );
      const logStream = new cdk.aws_logs.LogStream(
        this,
        "KDAApplicationLogStream",
        {
          logGroup: logGroup,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }
      );
      const logging =
        new cdk.aws_kinesisanalyticsv2.CfnApplicationCloudWatchLoggingOption(
          this,
          "KDAApplicationCloudWatchLoggingOption",
          {
            applicationName: application.applicationName,
            cloudWatchLoggingOption: {
              logStreamArn: `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`,
            },
          }
        );
      logging.node.addDependency(application);
    }

    return application;
  }

  private supressBucketDeploymentNags(): void {
    const stack = cdk.Stack.of(this);
    stack.node.findAll().forEach(({ node }: { node: any }) => {
      const re = [
        new RegExp(
          `${stack.stackName}/Custom::CDKBucketDeployment.+/Resource`,
          "g"
        ),
        new RegExp(
          `${stack.stackName}/Custom::CDKBucketDeployment.+/ServiceRole/.+/Resource`,
          "g"
        ),
      ];
      if (re.some((r) => r.test(node.path))) {
        NagSuppressions.addResourceSuppressionsByPath(
          stack,
          node.path,
          [
            {
              id: "AwsSolutions-L1",
              reason: "Buckey Deployment Lambda uses older runtime version",
            },
            {
              id: "AwsSolutions-IAM5",
              reason:
                "Bucket Deployment uses several IAM wildcards that are necessary",
            },
            {
              id: "AwsSolutions-IAM4",
              reason:
                "Bucket Deployment uses several IAM wildcards that are necessary",
            },
          ],
          true
        );
      }
    });
  }

  private supressKDARoleNags(role: cdk.aws_iam.Role): void {
    NagSuppressions.addResourceSuppressions(
      role,
      [
        {
          id: "AwsSolutions-IAM4",
          reason: "Role needs full access to Cloudwatch & KDA",
          appliesTo: [
            {
              regex:
                "/^Policy::arn:<AWS::Partition>:iam::aws:policy/(CloudWatchFullAccess|AmazonKinesisAnalyticsFullAccess)$/g",
            },
          ],
        },
        {
          id: "AwsSolutions-IAM5",
          reason: "Allow access to S3 Bucket & S3 read/write via CDK grant method",
          appliesTo: [
            {
              regex:
                "/^Action::s3:.*$/g",
            },
            {
              regex:
                "/^Resource::.*\.Arn>\/.*$/g",
            },
          ]
        },
      ],
      true
    );
  }

  private supressBucketNags(bucket: cdk.aws_s3.Bucket): void {
    NagSuppressions.addResourceSuppressions(
      bucket,
      [
        {
          id: "AwsSolutions-S1",
          reason: "Server access logs not necessary for demo",
        },
      ],
      true
    );
  }
}
