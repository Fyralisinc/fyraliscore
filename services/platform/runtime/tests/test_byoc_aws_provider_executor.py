from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from services.platform.runtime.byoc_aws_provider_executor import (
    ByocAwsProviderExecutorInputs,
    run_byoc_aws_provider_executor,
)


def test_provider_executor_renders_aws_and_helm_artifacts(tmp_path: Path) -> None:
    report = run_byoc_aws_provider_executor(_inputs(tmp_path))

    assert report["schema_version"] == "fyralis.byoc.aws_provider_executor.v1"
    assert report["status"] == "pass"
    assert report["cloud_api_mutations_executed"] is False
    assert report["resource_mutations_executed"] is False
    assert report["helm_install_executed"] is False

    artifacts = report["artifacts"]
    template = json.loads(Path(artifacts["cloudformation_template"]).read_text())
    resource_types = {
        resource["Type"] for resource in template["Resources"].values()
    }
    assert {
        "AWS::EKS::Cluster",
        "AWS::EKS::Nodegroup",
        "AWS::RDS::DBInstance",
        "AWS::S3::Bucket",
        "AWS::MSK::Cluster",
        "AWS::SecretsManager::Secret",
    }.issubset(resource_types)
    assert template["Outputs"]["SourceRuntimeRoleArn"]["Value"] == {
        "Fn::GetAtt": ["EksNodeRole", "Arn"]
    }
    assert report["expected_outputs"]["source_runtime_role_arn"] == (
        "SourceRuntimeRoleArn"
    )
    assert Path(artifacts["parameters"]).is_file()
    assert Path(artifacts["helm_values"]).is_file()
    assert "helm upgrade --install" in Path(artifacts["helm_command"]).read_text()


def test_provider_executor_blocks_mutation_without_confirmation(
    tmp_path: Path,
) -> None:
    report = run_byoc_aws_provider_executor(
        _inputs(
            tmp_path,
            create_change_set=True,
            execute_change_set=True,
            execute_helm=True,
        )
    )

    assert report["status"] == "fail"
    assert report["required_checks_passed"] is False
    assert report["cloud_api_mutations_executed"] is False
    assert report["resource_mutations_executed"] is False


def test_provider_executor_can_create_and_execute_change_set(
    tmp_path: Path,
) -> None:
    client = _FakeCloudFormationClient()

    report = run_byoc_aws_provider_executor(
        _inputs(
            tmp_path,
            create_change_set=True,
            execute_change_set=True,
            confirm_cost_and_mutation=True,
            cloudformation_client_factory=lambda _profile, _region: client,
        )
    )

    assert report["status"] == "pass"
    assert report["cloud_api_mutations_executed"] is True
    assert report["resource_mutations_executed"] is True
    assert report["change_set_id_present"] is True
    assert client.created_change_sets[0]["ChangeSetType"] == "CREATE"
    assert client.executed_change_sets == ["cs-123"]
    assert report["deployment_outputs"]["SourceRuntimeRoleArn"] == (
        "arn:aws:iam::587628268464:role/fyralis-runtime"
    )


def test_provider_executor_can_run_helm_install(
    tmp_path: Path,
) -> None:
    commands: list[list[str]] = []

    def helm_runner(command: Sequence[str]) -> int:
        commands.append(list(command))
        return 0

    report = run_byoc_aws_provider_executor(
        _inputs(
            tmp_path,
            execute_helm=True,
            confirm_cost_and_mutation=True,
            helm_runner=helm_runner,
        )
    )

    assert report["status"] == "pass"
    assert report["helm_install_executed"] is True
    assert report["resource_mutations_executed"] is True
    assert commands[0][:4] == ["helm", "upgrade", "--install", "fyralis"]


def _inputs(tmp_path: Path, **overrides: Any) -> ByocAwsProviderExecutorInputs:
    values: dict[str, Any] = {
        "workdir": tmp_path,
        "region": "us-east-1",
        "stack_name": "fyralis-byoc-test",
        "deployment_id": "dep_customer",
        "customer_id": "cus_customer",
        "environment": "pilot",
    }
    values.update(overrides)
    return ByocAwsProviderExecutorInputs(**values)


class _FakeCloudFormationClient:
    def __init__(self) -> None:
        self.created_change_sets: list[dict[str, Any]] = []
        self.executed_change_sets: list[str] = []
        self.stack_created = False

    def describe_stacks(self, *, StackName: str) -> dict[str, Any]:  # noqa: N803
        if not self.stack_created:
            raise RuntimeError(f"stack not found: {StackName}")
        return {
            "Stacks": [
                {
                    "Outputs": [
                        {
                            "OutputKey": "SourceRuntimeRoleArn",
                            "OutputValue": (
                                "arn:aws:iam::587628268464:role/fyralis-runtime"
                            ),
                        }
                    ]
                }
            ]
        }

    def create_change_set(self, **kwargs: Any) -> dict[str, str]:
        self.created_change_sets.append(kwargs)
        return {"Id": "cs-123"}

    def execute_change_set(self, *, ChangeSetName: str) -> None:  # noqa: N803
        self.executed_change_sets.append(ChangeSetName)
        self.stack_created = True
