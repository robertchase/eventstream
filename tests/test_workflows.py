"""Micro-tests for workflow registry storage."""

from __future__ import annotations

import pytest

from eventstream.logic import workflows
from eventstream.logic.workflow_parser import ParseError

_SOURCE = """\
NAME    w
INITIAL s
ACTION noop
  SET k v
STATE s TERMINAL
"""


async def test_register_returns_version_one() -> None:
    result = await workflows.register(_SOURCE)
    assert result["name"] == "w"
    assert result["version"] == 1
    assert result["registered_at"] > 0


async def test_register_increments_version() -> None:
    await workflows.register(_SOURCE)
    again = await workflows.register(_SOURCE)
    assert again["version"] == 2


async def test_register_validates() -> None:
    with pytest.raises(ParseError):
        await workflows.register("INITIAL nope\n")


async def test_get_returns_latest_by_default() -> None:
    await workflows.register(_SOURCE)
    await workflows.register(_SOURCE)
    got = await workflows.get("w")
    assert got["version"] == 2
    assert got["source"] == _SOURCE
    assert got["ast"]["name"] == "w"


async def test_get_specific_version() -> None:
    await workflows.register(_SOURCE)
    await workflows.register(_SOURCE)
    v1 = await workflows.get("w", version=1)
    assert v1["version"] == 1


async def test_get_unknown_raises() -> None:
    with pytest.raises(workflows.WorkflowNotFound):
        await workflows.get("ghost")


async def test_get_unknown_version_raises() -> None:
    await workflows.register(_SOURCE)
    with pytest.raises(workflows.WorkflowNotFound):
        await workflows.get("w", version=99)


async def test_list_returns_latest_per_name() -> None:
    await workflows.register(_SOURCE)
    await workflows.register(_SOURCE)
    other = _SOURCE.replace("NAME    w", "NAME    other")
    await workflows.register(other)
    items = await workflows.list_()
    assert items == [
        {"name": "other", "latest_version": 1},
        {"name": "w", "latest_version": 2},
    ]


async def test_versions_returns_all() -> None:
    await workflows.register(_SOURCE)
    await workflows.register(_SOURCE)
    await workflows.register(_SOURCE)
    assert await workflows.versions("w") == [1, 2, 3]


async def test_versions_unknown_raises() -> None:
    with pytest.raises(workflows.WorkflowNotFound):
        await workflows.versions("ghost")


async def test_delete_removes_everything() -> None:
    await workflows.register(_SOURCE)
    await workflows.register(_SOURCE)
    await workflows.delete("w")
    assert await workflows.list_() == []
    with pytest.raises(workflows.WorkflowNotFound):
        await workflows.get("w")


async def test_delete_unknown_raises() -> None:
    with pytest.raises(workflows.WorkflowNotFound):
        await workflows.delete("ghost")


# ---- delete cascade guard (vs jobs) -----------------------------------------


async def test_delete_refused_when_jobs_exist() -> None:
    from eventstream.logic import jobs

    await workflows.register(_SOURCE)  # workflow "w", terminal-only
    await jobs.create("w")
    with pytest.raises(workflows.WorkflowHasJobs):
        await workflows.delete("w")
    # Still registered.
    assert (await workflows.get("w"))["name"] == "w"


async def test_delete_cascade_removes_jobs_and_workflow() -> None:
    from eventstream.logic import jobs

    await workflows.register(_SOURCE)
    job = await jobs.create("w")
    await workflows.delete("w", cascade=True)
    assert await workflows.list_() == []
    with pytest.raises(jobs.JobNotFound):
        await jobs.get(job["id"])


async def test_delete_without_jobs_still_works() -> None:
    await workflows.register(_SOURCE)
    await workflows.delete("w")
    assert await workflows.list_() == []
