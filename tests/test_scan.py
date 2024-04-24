from _operator import itemgetter

from dynafile import Dynafile


def test_scan_all_items(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
    }
    ab = {
        "PK": "1",
        "SK": "ab",
    }
    ac = {
        "PK": "1",
        "SK": "ac",
    }
    ba = {
        "PK": "2",
        "SK": "ba",
    }

    db.put_item(item=aa)
    db.put_item(item=ab)
    db.put_item(item=ac)
    db.put_item(item=ba)

    items = set(map(itemgetter("SK"), db.scan()))

    assert items == {"aa", "ab", "ac", "ba"}


def test_scan_with_callable_filter(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
    }
    ab = {
        "PK": "1",
        "SK": "ab",
    }
    ac = {
        "PK": "1",
        "SK": "ac",
    }
    ba = {
        "PK": "2",
        "SK": "ba",
    }

    db.put_item(item=aa)
    db.put_item(item=ab)
    db.put_item(item=ac)
    db.put_item(item=ba)

    items = set(
        map(itemgetter("SK"), db.scan(_filter=lambda i: i["SK"].startswith("a")))
    )

    assert items == {"aa", "ab", "ac"}


def test_scan_with_string_filter(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
    }
    ab = {
        "PK": "1",
        "SK": "ab",
    }
    ac = {
        "PK": "1",
        "SK": "ac",
    }
    ba = {
        "PK": "2",
        "SK": "ba",
    }

    db.put_item(item=aa)
    db.put_item(item=ab)
    db.put_item(item=ac)
    db.put_item(item=ba)

    items = set(map(itemgetter("SK"), db.scan(_filter="SK =~ /^a/")))

    assert items == {"aa", "ab", "ac"}
