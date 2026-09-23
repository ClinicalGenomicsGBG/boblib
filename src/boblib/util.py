from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from typing import Literal

T = TypeVar("T")


@contextmanager
def filter_warnings(
    action: Literal[
        "ignore",
        "default",
        "error",
        "always",
        "module",
        "once",
    ] = "ignore",
    category: type[Warning] = UserWarning,
):
    with warnings.catch_warnings():
        warnings.simplefilter(action, category)
        yield
