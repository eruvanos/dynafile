from dynafile import Dynafile


def test_get(tmp_path):
    db = Dynafile(tmp_path / "db")

    item = db.get_item(
        key={
            "PK": "1",
            "SK": "2",
        }
    )
    assert item is None


def test_put_get(tmp_path):
    db = Dynafile(tmp_path / "db")

    db.put_item(
        item={
            "PK": "1",
            "SK": "2",
            "name": "Dynafile",
        }
    )

    item = db.get_item(
        key={
            "PK": "1",
            "SK": "2",
        }
    )
    assert item["name"] == "Dynafile"


def test_persistent(tmp_path):
    db1 = Dynafile(tmp_path / "db")
    db1.put_item(
        item={
            "PK": "1",
            "SK": "2",
            "name": "Dynafile",
        }
    )

    db2 = Dynafile(tmp_path / "db")
    item = db2.get_item(
        key={
            "PK": "1",
            "SK": "2",
        }
    )
    assert item["name"] == "Dynafile"
