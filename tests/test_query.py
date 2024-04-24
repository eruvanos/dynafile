from dynafile import Dynafile


def test_query_forward(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
        "name": "Dynafile",
    }
    ab = {
        "PK": "1",
        "SK": "ab",
        "name": "Dynafile",
    }
    ac = {
        "PK": "1",
        "SK": "ac",
        "name": "Dynafile",
    }

    db.put_item(item=aa)
    db.put_item(item=ab)
    db.put_item(item=ac)

    items = list(db.query(pk="1", starts_with="ab"))

    assert items == [ab, ac]


def test_query_backwords(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
        "name": "Dynafile",
    }
    ab = {
        "PK": "1",
        "SK": "ab",
        "name": "Dynafile",
    }
    ac = {
        "PK": "1",
        "SK": "ac",
        "name": "Dynafile",
    }

    db.put_item(item=aa)
    db.put_item(item=ab)
    db.put_item(item=ac)

    items = list(db.query(pk="1", starts_with="ab", scan_index_forward=False))

    assert items == [ab, aa]


def test_query_with_callable_filter(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
        "name": "Dynafile",
    }
    ab = {
        "PK": "1",
        "SK": "ab",
        "name": "Dynafile",
    }
    ac = {
        "PK": "1",
        "SK": "ac",
        "name": "Dynafile",
    }
    ba = {
        "PK": "1",
        "SK": "ba",
        "name": "Dynafile",
    }

    db.put_item(item=aa)
    db.put_item(item=ab)
    db.put_item(item=ac)
    db.put_item(item=ba)

    items = list(db.query(pk="1", _filter=lambda i: i["SK"].startswith("a")))

    assert items == [aa, ab, ac]


def test_query_with_string_filter(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
        "name": "Dynafile",
    }
    ab = {
        "PK": "1",
        "SK": "ab",
        "name": "Dynafile",
    }
    ac = {
        "PK": "1",
        "SK": "ac",
        "name": "Dynafile",
    }
    ba = {
        "PK": "1",
        "SK": "ba",
        "name": "Dynafile",
    }

    db.put_item(item=aa)
    db.put_item(item=ab)
    db.put_item(item=ac)
    db.put_item(item=ba)

    items = list(db.query(pk="1", _filter="SK =~ /^a/"))

    assert items == [aa, ab, ac]


def test_query_with_string_filter_nested(tmp_path):
    db = Dynafile(tmp_path / "db")

    aa = {
        "PK": "1",
        "SK": "aa",
        "data": {"count": 0},
    }
    ab = {
        "PK": "1",
        "SK": "ab",
        "data": {"count": 1},
    }

    db.put_item(item=aa)
    db.put_item(item=ab)

    items = list(db.query(pk="1", _filter="data.count > 0"))

    assert items == [ab]
