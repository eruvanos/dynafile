import random

import pytest

from dynafile import Dynafile


@pytest.mark.perf
@pytest.mark.parametrize("count", [100, 1000, 5000])
def test_perf_put_item(tmp_path, benchmark, count):
    items = [
        {
            "PK": f"item-{i % 100}",
            "SK": str(random.randint),
        } for i in range(count)
    ]

    db = Dynafile(tmp_path / "db")
    print(tmp_path)

    @benchmark
    def execute():
        for item in items:
            db.put_item(item=item)


@pytest.mark.perf
@pytest.mark.parametrize("count", [100, 1000, 5000])
def test_perf_batch_put_item(tmp_path, benchmark, count):
    items = [
        {
            "PK": f"item-{i % 100}",
            "SK": str(random.randint),
        } for i in range(count)
    ]

    db = Dynafile(tmp_path / "db")
    print(tmp_path)

    @benchmark
    def execute():
        with db.batch_writer() as writer:
            for item in items:
                writer.put_item(item=item)
