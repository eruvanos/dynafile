from dynafile import Dynafile


def test_get(tmp_path):
    db = Dynafile(tmp_path / "db")

    item = db.get_item(key={"PK": "1", "SK": "2"})

    assert item is None


def test_put_get(tmp_path):
    db = Dynafile(tmp_path / "db")

    db.put_item(item={"PK": "1", "SK": "2", "name": "Dynafile"})

    item = db.get_item(key={"PK": "1", "SK": "2", "name": "Dynafile"})
    assert item["name"] == "Dynafile"


def test_persistent(tmp_path):
    db1 = Dynafile(tmp_path / "db")
    db1.put_item(item={"PK": "1", "SK": "2", "name": "Dynafile"})

    db2 = Dynafile(tmp_path / "db")
    item = db2.get_item(key={"PK": "1", "SK": "2"})

    assert item["name"] == "Dynafile"


def test_delete(tmp_path):
    db = Dynafile(tmp_path / "db")
    db.put_item(
        item={
            "PK": "1",
            "SK": "1",
        }
    )

    db.delete_item(key={"PK": "1", "SK": "1"})

    item = db.get_item(key={"PK": "1", "SK": "1"})
    assert item is None


def test_batch_write(tmp_path):
    db = Dynafile(tmp_path / "db")

    with db.batch_writer() as writer:
        writer.put_item(item={"PK": "1", "SK": "2", "name": "Dynafile"})

    item = db.get_item(key={"PK": "1", "SK": "2"})
    assert item["name"] == "Dynafile"


def test_batch_write_with_delete(tmp_path):
    db = Dynafile(tmp_path / "db")

    with db.batch_writer() as writer:
        writer.put_item(
            item={
                "PK": "1",
                "SK": "1",
            }
        )
        writer.put_item(
            item={
                "PK": "1",
                "SK": "2",
            }
        )
        writer.delete_item(key={"PK": "1", "SK": "2"})

    item = db.get_item(key={"PK": "1", "SK": "2"})
    assert item is None

    item = db.get_item(key={"PK": "1", "SK": "1"})
    assert item is not None
