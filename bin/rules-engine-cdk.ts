#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { RulesEngineCdkStack } from "../lib/rules-engine-cdk-stack";
import { AwsSolutionsChecks } from "cdk-nag";

const app = new cdk.App();
new RulesEngineCdkStack(app, "RulesEngineCdkStack", {});

cdk.Aspects.of(app).add(new AwsSolutionsChecks({ logIgnores: true }));
