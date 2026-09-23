from __future__ import annotations

import json
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Generic, TypeVar, cast
from warnings import warn

from attrs import define, field
from thicks.slims import Slims

from boblib.record import (
    RECORD,
    STATUS,
    ProtocolRunRecord,
    ResultRecord,
    TypedColumn,
)
from boblib.slims import (
    get_protocol_runs_for_test_in_workflow,
    get_results_by_protocol_run,
    get_status_by_pk,
    get_status_by_table_and_id,
    get_test_by_name,
    get_workflow_by_uid,
)

if TYPE_CHECKING:
    from boblib.s3 import S3Manager

T = TypeVar("T")


def _convert_remote_location(value: dict | None) -> RemoteFileLocation | None:
    if value is None:
        return None
    if isinstance(value, RemoteFileLocation):
        return value
    endpoint = value.pop("endpoint", None)
    bucket = value.pop("bucket", None)
    key = value.pop("key", None)
    for key in value:
        warn(f"Unexpected key '{key}' in remote location: {value[key]}")
    return RemoteFileLocation(
        endpoint=endpoint,
        bucket=bucket,
        key=key,
    )

class _PipelineMetadataRecord(ResultRecord):
    rslt_cf_fileLocations: TypedColumn[str | None]

@define(slots=False, kw_only=True)
class RemoteFileLocation:
    endpoint: str
    bucket: str
    key: str


@define(slots=False, kw_only=True)
class FileLocation:
    local: Path | None = field(default=None, converter=lambda v: Path(v) if v is not None else None)
    remote: RemoteFileLocation | None = field(default=None, converter=_convert_remote_location)

    def __attrs_post_init__(self):
        if self.local is None and self.remote is None:
            raise ValueError("Both local and remote file locations are missing.")

    def ensure_local(self, s3: S3Manager, dst: Path) -> None:
        dst = Path(dst)
        if self.local is not None and self.local.exists():
            return
        if self.remote is None:
            raise ValueError("No remote location available to download the file.")
        if not dst.exists():
            dst.mkdir(parents=True, exist_ok=True)
        elif not dst.is_dir():
            raise ValueError(f"Destination path '{dst}' exists and is not a directory.")
        local_path = dst / self.remote.key
        s3.fetch(self.remote, local_path)
        self.local = local_path


@define(slots=False)
class PipelineBase(Generic[RECORD]):
    record: RECORD

    @cached_property
    def _slims(self) -> Slims:
        return Slims(
            name=f"Bob_{self.record.pk()}",
            url=self.record.slims_api.raw_url,
            username=self.record.slims_api.username,
            password=self.record.slims_api.password,
            oauth=self.record.slims_api.oauth,
            client_id=self.record.slims_api.client_id,
            client_secret=self.record.slims_api.client_secret,
            repo_location=self.record.slims_api.repo_location,
            **self.record.slims_api.request_params,
        )


@define(slots=False)
class PipelineMetadata(PipelineBase[_PipelineMetadataRecord]):
    _files: dict[str, list[FileLocation]] = field(factory=dict, init=False)

    @property
    def status(self) -> STATUS:
        status = get_status_by_pk(self._slims, self.record.rslt_fk_status.value)
        return status.stts_id.value

    @status.setter
    def status(self, value: STATUS) -> None:
        new_status = get_status_by_table_and_id(self._slims, "Result", value)
        self.record.update({"rslt_fk_status": new_status.pk()})

    @cached_property
    def test_name(self) -> str | None:
        return self.record.test_name.value

    @cached_property
    def sample_name(self) -> str | None:
        return self.record.rslt_fk_content.displayValue

    @property
    def files(self) -> dict[str, list[FileLocation]]:
        if self._files:
            return self._files
        if self.record.rslt_cf_fileLocations.value is None:
            self._files = {}
            return self._files
        data = json.loads(self.record.rslt_cf_fileLocations.value)
        if not isinstance(data, dict):
            raise TypeError("Expected a dictionary for file locations.")

        results = {key: [FileLocation(**entry) for entry in entries] for key, entries in data.items()}
        self._files = results
        return self._files

    def ensure_local_files(self, s3: S3Manager, dst: Path) -> None:
        for locations in self.files.values():
            for location in locations:
                location.ensure_local(s3, dst)


@define(slots=False)
class PipelineProtocolRun(PipelineBase[ProtocolRunRecord]):
    test: str | None = None

    @property
    def name(self) -> str:
        return self.record.xprn_name.value

    @cached_property
    def samples(self) -> list[PipelineMetadata]:
        results = get_results_by_protocol_run(self._slims, self.record.pk(), test_name=self.test)
        return [PipelineMetadata(cast(_PipelineMetadataRecord, result)) for result in results]

    @property
    def cancelled(self) -> bool:
        return self.record.xprn_cancelled.value

    def cancel(self) -> None:
        self.cancelled = True

    @cancelled.setter
    def cancelled(self, value: bool) -> None:
        if not (self.cancelled or self.completed):
            self.record.update({"xprn_cancelled": value})

    @property
    def completed(self) -> bool:
        return self.record.xprn_completed.value

    @completed.setter
    def completed(self, value: bool) -> None:
        if not (self.cancelled or self.completed):
            self.record.update({"xprn_completed": value})

    def finish(self) -> None:
        self.completed = True


def get_pipeline_protocol_runs(
    slims: Slims,
    test_name: str,
    workflow_uid: str,
    max_age: timedelta = timedelta(days=30),
) -> list[PipelineProtocolRun]:
    test = get_test_by_name(slims, test_name)
    workflow = get_workflow_by_uid(slims, workflow_uid)
    results = get_protocol_runs_for_test_in_workflow(
        slims,
        workflow_pk=workflow.pk(),
        test_pk=test.pk(),
        max_age=max_age,
    )
    return [PipelineProtocolRun(result, test=test_name) for result in results]
