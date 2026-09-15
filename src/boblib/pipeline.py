from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from types import SimpleNamespace
from typing import Generic, TypeVar, cast
from warnings import warn

from attrs import define, field
from thicks.internal import Column, Record
from thicks.slims import Slims

from boblib.slims import (
    get_protocol_runs_for_test_in_workflow,
    get_results_by_protocol_run,
    get_test_by_name,
    get_workflow_by_uid,
)

T = TypeVar("T")
RECORD = TypeVar("RECORD", bound=Record)


class TypedColumn(Column, Generic[T]):
    value: T
    displayValue: str | None


class SubscriptableNamespace(ABC, SimpleNamespace, Generic[T]):
    @abstractmethod
    def __getattr__(self, key) -> T: ...
    def __getitem__(self, key) -> T:
        return getattr(self, key)


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


class _PipelineProtocolRunRecord(Record):
    xprn_name: TypedColumn[str]
    xprn_cancelled: TypedColumn[bool]
    xprn_completed: TypedColumn[bool]


class _PipelineMetadataRecord(Record):
    test_name: TypedColumn[str]
    rslt_fk_content: TypedColumn[int]
    rslt_value: TypedColumn[str]
    rslt_cf_fileLocations: TypedColumn[str | None]


@define(slots=False)
class PipelineBase(Generic[RECORD]):
    _record: RECORD

    @cached_property
    def record(self) -> RECORD:
        return self._record

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
    @cached_property
    def files(self) -> SubscriptableNamespace[FileLocation]:
        if self._record.rslt_cf_fileLocations.value is None:
            return SubscriptableNamespace[FileLocation]()

        data = json.loads(self._record.rslt_cf_fileLocations.value)
        if not isinstance(data, dict):
            raise TypeError("Expected a dictionary for file locations.")

        results = {key: FileLocation(**entry) for key, entries in data.items() for entry in entries}
        return SubscriptableNamespace(**results)


@define(slots=False)
class PipelineProtocolRun(PipelineBase[_PipelineProtocolRunRecord]):
    test: str | None = None

    @property
    def name(self) -> str:
        return self._record.xprn_name.value

    @cached_property
    def samples(self) -> list[PipelineMetadata]:
        results = get_results_by_protocol_run(self._slims, self._record.pk(), test_name=self.test)
        return [PipelineMetadata(cast(_PipelineMetadataRecord, result)) for result in results]

    @property
    def cancelled(self) -> bool:
        return self._record.xprn_cancelled.value

    @cancelled.setter
    def cancelled(self, value: bool) -> None:
        if not (self.cancelled or self.completed):
            self.record.update({"xprn_cancelled": value})

    @property
    def completed(self) -> bool:
        return self._record.xprn_completed.value

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
        slims, workflow_pk=workflow.pk(), test_pk=test.pk(), max_age=max_age
    )
    return [PipelineProtocolRun(cast(_PipelineProtocolRunRecord, result)) for result in results]
