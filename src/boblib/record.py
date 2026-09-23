from typing import Generic, Literal, TypeAlias, TypeVar

from thicks.internal import Column, Record

T = TypeVar("T")
RECORD = TypeVar("RECORD", bound=Record)
STATUS: TypeAlias = Literal[
    "PENDING",
    "AVAILABLE",
    "VERIFIED",
    "VALIDATED",
    "CANCELLED",
    "REJECTED",
]


class TypedColumn(Column, Generic[T]):
    value: T
    displayValue: str | None


class ProtocolRunRecord(Record):
    xprn_name: TypedColumn[str]
    xprn_cancelled: TypedColumn[bool]
    xprn_completed: TypedColumn[bool]


class ResultRecord(Record):
    test_name: TypedColumn[str]
    rslt_fk_content: TypedColumn[int]
    rslt_value: TypedColumn[str]
    rslt_fk_status: TypedColumn[int]


class StatusRecord(Record):
    stts_uniqueIdentifier: TypedColumn[str]
    stts_id: TypedColumn[STATUS]
    stts_name: TypedColumn[str]
    stts_pk: TypedColumn[int]
