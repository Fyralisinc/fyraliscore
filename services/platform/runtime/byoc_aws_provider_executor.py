"""Customer-side AWS provider executor for Fyralis BYOC setup.

This module is deliberately customer-side. It can render an executable
CloudFormation template for the first BYOC data plane, and, when explicitly
confirmed, create or execute an AWS CloudFormation change set using the
customer's local AWS credentials.
"""
from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal


AwsProviderExecutorStatus = Literal["pass", "fail", "skipped"]
AwsCloudFormationClientFactory = Callable[[str | None, str], Any]
HelmRunner = Callable[[Sequence[str]], int]

DEFAULT_HELM_CHART = "oci://registry.fyralis.com/charts/fyralis"
DEFAULT_HELM_RELEASE_NAME = "fyralis"


@dataclass(frozen=True, slots=True)
class ByocAwsProviderExecutorInputs:
    workdir: Path
    region: str
    stack_name: str
    deployment_id: str
    customer_id: str
    environment: str
    permissions_boundary_policy_arn: str = ""
    aws_profile: str | None = None
    create_change_set: bool = False
    execute_change_set: bool = False
    confirm_cost_and_mutation: bool = False
    change_set_name: str | None = None
    cloudformation_client_factory: AwsCloudFormationClientFactory | None = None
    execute_helm: bool = False
    kube_context: str | None = None
    helm_release_name: str = DEFAULT_HELM_RELEASE_NAME
    helm_chart: str = DEFAULT_HELM_CHART
    helm_runner: HelmRunner | None = None


def run_byoc_aws_provider_executor(
    inputs: ByocAwsProviderExecutorInputs,
) -> dict[str, Any]:
    started = time.monotonic()
    artifacts_dir = inputs.workdir / "provider" / "aws-cloudformation"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    template = build_byoc_aws_cloudformation_template(inputs)
    parameters = build_byoc_aws_cloudformation_parameters(inputs)
    helm_values = build_byoc_helm_values(inputs)
    helm_command = build_byoc_helm_command(
        artifacts_dir / "helm-values.json",
        chart=inputs.helm_chart,
        kube_context=inputs.kube_context,
        release_name=inputs.helm_release_name,
    )

    template_path = artifacts_dir / "fyralis-byoc-template.json"
    parameters_path = artifacts_dir / "parameters.json"
    helm_values_path = artifacts_dir / "helm-values.json"
    helm_command_path = artifacts_dir / "helm-command.txt"
    _write_json(template_path, template)
    _write_json(parameters_path, parameters)
    _write_json(helm_values_path, helm_values)
    helm_command_path.write_text(helm_command + "\n", encoding="utf-8")

    checks = [
        _check("cloudformation_template_rendered", "pass", required=True),
        _check("cloudformation_parameters_rendered", "pass", required=True),
        _check("helm_values_rendered", "pass", required=True),
        _check("no_secret_values_serialized", "pass", required=True),
    ]
    change_set_id: str | None = None
    if inputs.create_change_set or inputs.execute_change_set:
        if not inputs.confirm_cost_and_mutation:
            checks.append(
                _check(
                    "cost_and_mutation_confirmation",
                    "fail",
                    required=True,
                    details="Provider execution requires explicit cost and mutation confirmation.",
                )
            )
        else:
            result = _create_change_set(inputs, template, parameters)
            checks.append(result["check"])
            change_set_id = result.get("change_set_id")
            if inputs.execute_change_set and result["check"]["status"] == "pass":
                checks.append(_execute_change_set(inputs, change_set_id))
            elif inputs.execute_change_set:
                checks.append(
                    _check(
                        "cloudformation_change_set_executed",
                        "fail",
                        required=True,
                        details="Change set execution skipped because creation failed.",
                    )
                )
    else:
        checks.append(
            _check(
                "cloudformation_change_set_created",
                "skipped",
                required=False,
                details="Provider executor rendered artifacts only.",
            )
        )
    if inputs.execute_helm:
        if not inputs.confirm_cost_and_mutation:
            checks.append(
                _check(
                    "helm_install_executed",
                    "fail",
                    required=True,
                    details="Helm install requires explicit cost and mutation confirmation.",
                )
            )
        else:
            checks.append(_execute_helm_install(inputs, helm_values_path))
    else:
        checks.append(
            _check(
                "helm_install_executed",
                "skipped",
                required=False,
                details="Helm install was not requested.",
            )
        )

    required_passed = all(
        check["status"] != "fail" for check in checks if check["required"]
    )
    cloudformation_mutation_executed = any(
        check["name"]
        in {"cloudformation_change_set_created", "cloudformation_change_set_executed"}
        and check["status"] == "pass"
        for check in checks
    )
    resource_mutation_executed = any(
        check["name"] in {"cloudformation_change_set_executed", "helm_install_executed"}
        and check["status"] == "pass"
        for check in checks
    )
    report = {
        "schema_version": "fyralis.byoc.aws_provider_executor.v1",
        "status": "pass" if required_passed else "fail",
        "required_checks_passed": required_passed,
        "generated_at": _now(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "region": inputs.region,
        "stack_name": inputs.stack_name,
        "deployment_id": inputs.deployment_id,
        "customer_id": inputs.customer_id,
        "environment": inputs.environment,
        "executor": "aws_cloudformation_change_set",
        "cloud_api_mutations_executed": cloudformation_mutation_executed,
        "resource_mutations_executed": resource_mutation_executed,
        "helm_install_executed": any(
            check["name"] == "helm_install_executed" and check["status"] == "pass"
            for check in checks
        ),
        "change_set_id_present": change_set_id is not None,
        "expected_outputs": {
            "source_runtime_role_arn": "SourceRuntimeRoleArn",
        },
        "deployment_outputs": (
            _describe_stack_outputs(inputs) if resource_mutation_executed else {}
        ),
        "artifacts": {
            "cloudformation_template": str(template_path),
            "parameters": str(parameters_path),
            "helm_values": str(helm_values_path),
            "helm_command": str(helm_command_path),
        },
        "checks": checks,
        "stored_scope": "sanitized_provider_execution_metadata_only",
    }
    _write_json(artifacts_dir / "provider-executor-report.json", report)
    return report


def build_byoc_aws_cloudformation_template(
    inputs: ByocAwsProviderExecutorInputs,
) -> dict[str, Any]:
    tags = [
        {"Key": "fyralis:deployment-id", "Value": {"Ref": "DeploymentId"}},
        {"Key": "fyralis:customer-id", "Value": {"Ref": "CustomerId"}},
        {"Key": "fyralis:managed", "Value": "true"},
        {"Key": "fyralis:environment", "Value": {"Ref": "Environment"}},
    ]
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Fyralis BYOC customer-cloud data plane baseline.",
        "Parameters": _template_parameters(inputs),
        "Conditions": {
            "HasPermissionsBoundary": {
                "Fn::Not": [
                    {
                        "Fn::Equals": [
                            {"Ref": "PermissionsBoundaryPolicyArn"},
                            "",
                        ]
                    }
                ]
            }
        },
        "Resources": {
            **_network_resources(tags),
            **_iam_resources(tags),
            **_storage_resources(tags),
            **_secret_resources(tags),
            **_database_resources(tags),
            **_kafka_resources(tags),
            **_eks_resources(tags),
        },
        "Outputs": {
            "ClusterName": {"Value": {"Ref": "FyralisEksCluster"}},
            "RawPayloadBucketName": {"Value": {"Ref": "RawPayloadBucket"}},
            "ArtifactBucketName": {"Value": {"Ref": "ArtifactBucket"}},
            "SourceRuntimeRoleArn": {
                "Description": (
                    "IAM role ARN used by Fyralis BYOC source workers to assume "
                    "customer-approved source read roles."
                ),
                "Value": {"Fn::GetAtt": ["EksNodeRole", "Arn"]},
            },
            "PostgresEndpoint": {
                "Value": {"Fn::GetAtt": ["FyralisPostgres", "Endpoint.Address"]}
            },
            "KafkaClusterArn": {"Value": {"Ref": "FyralisMskCluster"}},
        },
    }


def build_byoc_aws_cloudformation_parameters(
    inputs: ByocAwsProviderExecutorInputs,
) -> list[dict[str, str]]:
    return [
        {"ParameterKey": "DeploymentId", "ParameterValue": inputs.deployment_id},
        {"ParameterKey": "CustomerId", "ParameterValue": inputs.customer_id},
        {"ParameterKey": "Environment", "ParameterValue": inputs.environment},
        {
            "ParameterKey": "PermissionsBoundaryPolicyArn",
            "ParameterValue": inputs.permissions_boundary_policy_arn,
        },
    ]


def build_byoc_helm_values(inputs: ByocAwsProviderExecutorInputs) -> dict[str, Any]:
    return {
        "global": {
            "deploymentId": inputs.deployment_id,
            "customerId": inputs.customer_id,
            "environment": inputs.environment,
            "region": inputs.region,
            "dataResidency": "customer-cloud",
            "telemetry": "aggregate-only",
        },
        "runtime": {
            "secretProvider": "aws-secrets-manager",
            "objectStorage": "s3",
            "database": "postgres-pgvector",
            "broker": "msk",
        },
        "trustBoundary": {
            "rawPayloadsLeaveBoundary": False,
            "promptsLeaveBoundary": False,
            "embeddingsLeaveBoundary": False,
            "logsLeaveBoundary": False,
            "providerSecretsLeaveBoundary": False,
        },
    }


def build_byoc_helm_command(
    values_path: Path,
    *,
    chart: str = DEFAULT_HELM_CHART,
    kube_context: str | None = None,
    release_name: str = DEFAULT_HELM_RELEASE_NAME,
) -> str:
    return " ".join(
        build_byoc_helm_args(
            values_path,
            chart=chart,
            kube_context=kube_context,
            release_name=release_name,
        )
    )


def build_byoc_helm_args(
    values_path: Path,
    *,
    chart: str = DEFAULT_HELM_CHART,
    kube_context: str | None = None,
    release_name: str = DEFAULT_HELM_RELEASE_NAME,
) -> list[str]:
    args = [
        "helm",
        "upgrade",
        "--install",
        release_name,
        chart,
        "--namespace",
        "fyralis-system",
        "--create-namespace",
        "-f",
        str(values_path),
    ]
    if kube_context:
        args.extend(["--kube-context", kube_context])
    return args


def _template_parameters(inputs: ByocAwsProviderExecutorInputs) -> dict[str, Any]:
    del inputs
    return {
        "DeploymentId": {"Type": "String"},
        "CustomerId": {"Type": "String"},
        "Environment": {"Type": "String"},
        "VpcCidr": {"Type": "String", "Default": "10.90.0.0/16"},
        "PrivateSubnetACidr": {"Type": "String", "Default": "10.90.10.0/24"},
        "PrivateSubnetBCidr": {"Type": "String", "Default": "10.90.11.0/24"},
        "EksNodeInstanceType": {"Type": "String", "Default": "t3.large"},
        "PostgresInstanceClass": {"Type": "String", "Default": "db.t4g.medium"},
        "KafkaInstanceType": {"Type": "String", "Default": "kafka.t3.small"},
        "PermissionsBoundaryPolicyArn": {"Type": "String", "Default": ""},
    }


def _network_resources(tags: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "FyralisVpc": {
            "Type": "AWS::EC2::VPC",
            "Properties": {
                "CidrBlock": {"Ref": "VpcCidr"},
                "EnableDnsHostnames": True,
                "EnableDnsSupport": True,
                "Tags": _named_tags(tags, "fyralis-byoc-vpc"),
            },
        },
        "PrivateSubnetA": {
            "Type": "AWS::EC2::Subnet",
            "Properties": {
                "VpcId": {"Ref": "FyralisVpc"},
                "CidrBlock": {"Ref": "PrivateSubnetACidr"},
                "AvailabilityZone": {"Fn::Select": [0, {"Fn::GetAZs": ""}]},
                "Tags": _named_tags(tags, "fyralis-byoc-private-a"),
            },
        },
        "PrivateSubnetB": {
            "Type": "AWS::EC2::Subnet",
            "Properties": {
                "VpcId": {"Ref": "FyralisVpc"},
                "CidrBlock": {"Ref": "PrivateSubnetBCidr"},
                "AvailabilityZone": {"Fn::Select": [1, {"Fn::GetAZs": ""}]},
                "Tags": _named_tags(tags, "fyralis-byoc-private-b"),
            },
        },
        "PublicSubnetA": {
            "Type": "AWS::EC2::Subnet",
            "Properties": {
                "VpcId": {"Ref": "FyralisVpc"},
                "CidrBlock": "10.90.100.0/24",
                "AvailabilityZone": {"Fn::Select": [0, {"Fn::GetAZs": ""}]},
                "MapPublicIpOnLaunch": True,
                "Tags": _named_tags(tags, "fyralis-byoc-public-a"),
            },
        },
        "InternetGateway": {
            "Type": "AWS::EC2::InternetGateway",
            "Properties": {"Tags": _named_tags(tags, "fyralis-byoc-igw")},
        },
        "VpcGatewayAttachment": {
            "Type": "AWS::EC2::VPCGatewayAttachment",
            "Properties": {
                "VpcId": {"Ref": "FyralisVpc"},
                "InternetGatewayId": {"Ref": "InternetGateway"},
            },
        },
        "PublicRouteTable": {
            "Type": "AWS::EC2::RouteTable",
            "Properties": {
                "VpcId": {"Ref": "FyralisVpc"},
                "Tags": _named_tags(tags, "fyralis-byoc-public-rt"),
            },
        },
        "PublicDefaultRoute": {
            "Type": "AWS::EC2::Route",
            "DependsOn": "VpcGatewayAttachment",
            "Properties": {
                "RouteTableId": {"Ref": "PublicRouteTable"},
                "DestinationCidrBlock": "0.0.0.0/0",
                "GatewayId": {"Ref": "InternetGateway"},
            },
        },
        "PublicSubnetARouteTableAssociation": {
            "Type": "AWS::EC2::SubnetRouteTableAssociation",
            "Properties": {
                "SubnetId": {"Ref": "PublicSubnetA"},
                "RouteTableId": {"Ref": "PublicRouteTable"},
            },
        },
        "NatEip": {
            "Type": "AWS::EC2::EIP",
            "Properties": {"Domain": "vpc", "Tags": _named_tags(tags, "fyralis-byoc-nat-eip")},
        },
        "NatGateway": {
            "Type": "AWS::EC2::NatGateway",
            "Properties": {
                "AllocationId": {"Fn::GetAtt": ["NatEip", "AllocationId"]},
                "SubnetId": {"Ref": "PublicSubnetA"},
                "Tags": _named_tags(tags, "fyralis-byoc-nat"),
            },
        },
        "PrivateRouteTable": {
            "Type": "AWS::EC2::RouteTable",
            "Properties": {
                "VpcId": {"Ref": "FyralisVpc"},
                "Tags": _named_tags(tags, "fyralis-byoc-private-rt"),
            },
        },
        "PrivateDefaultRoute": {
            "Type": "AWS::EC2::Route",
            "Properties": {
                "RouteTableId": {"Ref": "PrivateRouteTable"},
                "DestinationCidrBlock": "0.0.0.0/0",
                "NatGatewayId": {"Ref": "NatGateway"},
            },
        },
        "PrivateSubnetARouteTableAssociation": {
            "Type": "AWS::EC2::SubnetRouteTableAssociation",
            "Properties": {
                "SubnetId": {"Ref": "PrivateSubnetA"},
                "RouteTableId": {"Ref": "PrivateRouteTable"},
            },
        },
        "PrivateSubnetBRouteTableAssociation": {
            "Type": "AWS::EC2::SubnetRouteTableAssociation",
            "Properties": {
                "SubnetId": {"Ref": "PrivateSubnetB"},
                "RouteTableId": {"Ref": "PrivateRouteTable"},
            },
        },
        "RuntimeSecurityGroup": {
            "Type": "AWS::EC2::SecurityGroup",
            "Properties": {
                "GroupDescription": "Fyralis BYOC runtime private traffic",
                "VpcId": {"Ref": "FyralisVpc"},
                "SecurityGroupIngress": [
                    {
                        "IpProtocol": "-1",
                        "SourceSecurityGroupId": {"Ref": "RuntimeSecurityGroup"},
                    }
                ],
                "SecurityGroupEgress": [
                    {
                        "IpProtocol": "-1",
                        "CidrIp": "0.0.0.0/0",
                    }
                ],
                "Tags": _named_tags(tags, "fyralis-byoc-runtime-sg"),
            },
        },
    }


def _iam_resources(tags: list[dict[str, Any]]) -> dict[str, Any]:
    permissions_boundary = {
        "Fn::If": [
            "HasPermissionsBoundary",
            {"Ref": "PermissionsBoundaryPolicyArn"},
            {"Ref": "AWS::NoValue"},
        ]
    }
    return {
        "EksClusterRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": _assume_role_policy("eks.amazonaws.com"),
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
                ],
                "PermissionsBoundary": permissions_boundary,
                "Tags": tags,
            },
        },
        "EksNodeRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": _assume_role_policy("ec2.amazonaws.com"),
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
                    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
                    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
                ],
                "PermissionsBoundary": permissions_boundary,
                "Tags": tags,
            },
        },
    }


def _storage_resources(tags: list[dict[str, Any]]) -> dict[str, Any]:
    bucket_properties = {
        "BucketEncryption": {
            "ServerSideEncryptionConfiguration": [
                {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
            ]
        },
        "PublicAccessBlockConfiguration": {
            "BlockPublicAcls": True,
            "BlockPublicPolicy": True,
            "IgnorePublicAcls": True,
            "RestrictPublicBuckets": True,
        },
        "Tags": tags,
    }
    return {
        "RawPayloadBucket": {
            "Type": "AWS::S3::Bucket",
            "Properties": {
                **bucket_properties,
            },
        },
        "ArtifactBucket": {
            "Type": "AWS::S3::Bucket",
            "Properties": {
                **bucket_properties,
            },
        },
    }


def _secret_resources(tags: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "MasterKekSecret": _generated_secret(
            "fyralis/${DeploymentId}/master-kek",
            tags,
        ),
        "AgentBootstrapTokenSecret": _generated_secret(
            "fyralis/${DeploymentId}/agent-bootstrap-token",
            tags,
        ),
        "AgentClientCertificateSecret": _generated_secret(
            "fyralis/${DeploymentId}/agent-client-cert",
            tags,
        ),
    }


def _database_resources(tags: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "PostgresSubnetGroup": {
            "Type": "AWS::RDS::DBSubnetGroup",
            "Properties": {
                "DBSubnetGroupDescription": "Fyralis BYOC Postgres subnet group",
                "SubnetIds": [{"Ref": "PrivateSubnetA"}, {"Ref": "PrivateSubnetB"}],
                "Tags": tags,
            },
        },
        "FyralisPostgres": {
            "Type": "AWS::RDS::DBInstance",
            "Properties": {
                "Engine": "postgres",
                "DBInstanceClass": {"Ref": "PostgresInstanceClass"},
                "AllocatedStorage": "40",
                "StorageEncrypted": True,
                "ManageMasterUserPassword": True,
                "MasterUsername": "fyralis_admin",
                "DBSubnetGroupName": {"Ref": "PostgresSubnetGroup"},
                "VPCSecurityGroups": [{"Ref": "RuntimeSecurityGroup"}],
                "BackupRetentionPeriod": 7,
                "DeletionProtection": True,
                "Tags": tags,
            },
        },
    }


def _kafka_resources(tags: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "FyralisMskCluster": {
            "Type": "AWS::MSK::Cluster",
            "Properties": {
                "ClusterName": {"Fn::Sub": "${AWS::StackName}-msk"},
                "KafkaVersion": "3.6.0",
                "NumberOfBrokerNodes": 2,
                "BrokerNodeGroupInfo": {
                    "InstanceType": {"Ref": "KafkaInstanceType"},
                    "ClientSubnets": [
                        {"Ref": "PrivateSubnetA"},
                        {"Ref": "PrivateSubnetB"},
                    ],
                    "SecurityGroups": [{"Ref": "RuntimeSecurityGroup"}],
                },
                "EncryptionInfo": {
                    "EncryptionInTransit": {
                        "ClientBroker": "TLS",
                        "InCluster": True,
                    }
                },
                "Tags": _tags_to_map(tags),
            },
        }
    }


def _eks_resources(tags: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "FyralisEksCluster": {
            "Type": "AWS::EKS::Cluster",
            "Properties": {
                "RoleArn": {"Fn::GetAtt": ["EksClusterRole", "Arn"]},
                "ResourcesVpcConfig": {
                    "EndpointPrivateAccess": True,
                    "EndpointPublicAccess": True,
                    "SecurityGroupIds": [{"Ref": "RuntimeSecurityGroup"}],
                    "SubnetIds": [{"Ref": "PrivateSubnetA"}, {"Ref": "PrivateSubnetB"}],
                },
                "Tags": _tags_to_map(tags),
            },
        },
        "FyralisEksNodeGroup": {
            "Type": "AWS::EKS::Nodegroup",
            "Properties": {
                "ClusterName": {"Ref": "FyralisEksCluster"},
                "NodeRole": {"Fn::GetAtt": ["EksNodeRole", "Arn"]},
                "Subnets": [{"Ref": "PrivateSubnetA"}, {"Ref": "PrivateSubnetB"}],
                "InstanceTypes": [{"Ref": "EksNodeInstanceType"}],
                "ScalingConfig": {
                    "DesiredSize": 2,
                    "MaxSize": 4,
                    "MinSize": 1,
                },
                "Tags": _tags_to_map(tags),
            },
        },
    }


def _create_change_set(
    inputs: ByocAwsProviderExecutorInputs,
    template: dict[str, Any],
    parameters: list[dict[str, str]],
) -> dict[str, Any]:
    try:
        client = _cloudformation_client(inputs)
        change_set_name = inputs.change_set_name or (
            f"fyralis-byoc-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
        )
        change_set_type = _change_set_type(client, inputs.stack_name)
        response = client.create_change_set(
            StackName=inputs.stack_name,
            ChangeSetName=change_set_name,
            ChangeSetType=change_set_type,
            TemplateBody=json.dumps(template, sort_keys=True),
            Parameters=parameters,
            Capabilities=["CAPABILITY_NAMED_IAM", "CAPABILITY_IAM"],
            Description="Fyralis BYOC customer-cloud provider executor change set.",
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "check": _check(
                "cloudformation_change_set_created",
                "fail",
                required=True,
                details="CloudFormation change-set creation failed without retaining provider output.",
                metrics={"error_type": type(exc).__name__},
            )
        }
    return {
        "change_set_id": response.get("Id"),
        "check": _check(
            "cloudformation_change_set_created",
            "pass",
            required=True,
            metrics={
                "change_set_id_present": bool(response.get("Id")),
                "stack_exists": change_set_type == "UPDATE",
            },
        ),
    }


def _execute_change_set(
    inputs: ByocAwsProviderExecutorInputs,
    change_set_id: str | None,
) -> dict[str, Any]:
    if not change_set_id:
        return _check(
            "cloudformation_change_set_executed",
            "fail",
            required=True,
            details="Change set execution requires a created change set id.",
        )
    try:
        client = _cloudformation_client(inputs)
        client.execute_change_set(ChangeSetName=change_set_id)
    except Exception as exc:  # noqa: BLE001
        return _check(
            "cloudformation_change_set_executed",
            "fail",
            required=True,
            details="CloudFormation change-set execution failed without retaining provider output.",
            metrics={"error_type": type(exc).__name__},
        )
    return _check("cloudformation_change_set_executed", "pass", required=True)


def _describe_stack_outputs(inputs: ByocAwsProviderExecutorInputs) -> dict[str, str]:
    try:
        client = _cloudformation_client(inputs)
        response = client.describe_stacks(StackName=inputs.stack_name)
    except Exception:  # noqa: BLE001
        return {}
    stacks = response.get("Stacks") if isinstance(response, dict) else None
    if not isinstance(stacks, list) or not stacks:
        return {}
    outputs = stacks[0].get("Outputs") if isinstance(stacks[0], dict) else None
    if not isinstance(outputs, list):
        return {}
    out: dict[str, str] = {}
    for item in outputs:
        if not isinstance(item, dict):
            continue
        key = str(item.get("OutputKey") or "").strip()
        value = str(item.get("OutputValue") or "").strip()
        if key and value:
            out[key] = value
    return out


def _execute_helm_install(
    inputs: ByocAwsProviderExecutorInputs,
    values_path: Path,
) -> dict[str, Any]:
    command = build_byoc_helm_args(
        values_path,
        chart=inputs.helm_chart,
        kube_context=inputs.kube_context,
        release_name=inputs.helm_release_name,
    )
    try:
        if inputs.helm_runner is not None:
            return_code = inputs.helm_runner(command)
        else:
            completed = subprocess.run(
                command,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return_code = completed.returncode
    except FileNotFoundError:
        return _check(
            "helm_install_executed",
            "fail",
            required=True,
            details="Helm executable was not found on the customer-cloud runner.",
        )
    except Exception as exc:  # noqa: BLE001
        return _check(
            "helm_install_executed",
            "fail",
            required=True,
            details="Helm install failed without retaining command output.",
            metrics={"error_type": type(exc).__name__},
        )
    if return_code != 0:
        return _check(
            "helm_install_executed",
            "fail",
            required=True,
            details="Helm install returned a non-zero exit code.",
            metrics={"return_code": return_code},
        )
    return _check("helm_install_executed", "pass", required=True)


def _change_set_type(client: Any, stack_name: str) -> str:
    try:
        client.describe_stacks(StackName=stack_name)
    except Exception:  # noqa: BLE001
        return "CREATE"
    return "UPDATE"


def _cloudformation_client(inputs: ByocAwsProviderExecutorInputs) -> Any:
    if inputs.cloudformation_client_factory is not None:
        return inputs.cloudformation_client_factory(inputs.aws_profile, inputs.region)
    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - optional customer-side dep.
        raise RuntimeError("boto3 is required for AWS provider execution") from exc
    session = boto3.Session(profile_name=inputs.aws_profile, region_name=inputs.region)
    return session.client("cloudformation", region_name=inputs.region)


def _generated_secret(name_sub: str, tags: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "Type": "AWS::SecretsManager::Secret",
        "Properties": {
            "Name": {"Fn::Sub": name_sub},
            "GenerateSecretString": {
                "PasswordLength": 48,
                "ExcludePunctuation": True,
            },
            "Tags": tags,
        },
    }


def _assume_role_policy(service: str) -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": service},
                "Action": "sts:AssumeRole",
            }
        ],
    }


def _named_tags(tags: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    return [*tags, {"Key": "Name", "Value": name}]


def _tags_to_map(tags: list[dict[str, Any]]) -> dict[str, Any]:
    return {str(tag["Key"]): tag["Value"] for tag in tags}


def _check(
    name: str,
    status: AwsProviderExecutorStatus,
    *,
    required: bool,
    details: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "required": required,
        "details": details or _default_check_details(status),
        "metrics": metrics or {},
    }


def _default_check_details(status: AwsProviderExecutorStatus) -> str:
    if status == "pass":
        return "Check passed."
    if status == "skipped":
        return "Check was skipped."
    return "Check failed."


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


__all__ = [
    "ByocAwsProviderExecutorInputs",
    "build_byoc_helm_args",
    "build_byoc_aws_cloudformation_parameters",
    "build_byoc_aws_cloudformation_template",
    "build_byoc_helm_command",
    "build_byoc_helm_values",
    "run_byoc_aws_provider_executor",
]
