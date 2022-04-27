import datetime

import time_machine

from dynafile import Dynafile


@time_machine.travel(datetime.datetime.now(), tick=False)
def test_ttl_keep_living_items(tmp_path):
    now = datetime.datetime.now()

    item = {"PK": "1", "SK": "2", "ttl": now.timestamp() + 1000}

    db = Dynafile(tmp_path / "db", ttl_attribute="ttl")
    db.put_item(item=item)

    assert list(db.scan()) == [item]
    assert list(db.query(item["PK"])) == [item]
    assert db.get_item(key=item) == item


@time_machine.travel(datetime.datetime.now(), tick=False)
def test_ttl_removes_expired_items_during_scan(tmp_path):
    now = datetime.datetime.now()

    item = {"PK": "1", "SK": "2", "ttl": now.timestamp() - 1000}

    db = Dynafile(tmp_path / "db", ttl_attribute="ttl")
    db.put_item(item=item)

    assert not list(db.scan())


@time_machine.travel(datetime.datetime.now(), tick=False)
def test_ttl_removes_expired_items_during_get(tmp_path):
    now = datetime.datetime.now()

    item = {"PK": "1", "SK": "2", "ttl": now.timestamp() - 1000}

    db = Dynafile(tmp_path / "db", ttl_attribute="ttl")
    db.put_item(item=item)

    assert not db.get_item(key=item)


@time_machine.travel(datetime.datetime.now(), tick=False)
def test_ttl_removes_expired_items_during_query(tmp_path):
    now = datetime.datetime.now()

    item = {"PK": "1", "SK": "2", "ttl": now.timestamp() - 1000}

    db = Dynafile(tmp_path / "db", ttl_attribute="ttl")
    db.put_item(item=item)

    assert not list(db.query(item["PK"]))
