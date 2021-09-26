from dynafile import Dynafile


def test_delete_item(tmp_path):
    db = Dynafile(tmp_path / "db")

    db.put_item(
        item={
            "PK": "1",
            "SK": "1",
            "name": "Dynafile",
        }
    )

    db.delete_item(
        key={
            "PK": "1",
            "SK": "1",
        }
    )

    item = db.get_item(
        key={
            "PK": "1",
            "SK": "1",
        }
    )
    assert item is None
