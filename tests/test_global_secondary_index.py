from dynafile import Dynafile


def test_gsi_backfilled(tmp_path):
    db = Dynafile(tmp_path / "db")
    db.put_item(item={"PK": "1", "SK": "2"})

    db.create_gsi(name="gsi1", pk_attribute="SK", sk_attribute="PK")

    items = list(db.query(pk="2", index="gsi1"))
    assert items == [{"PK": "1", "SK": "2"}]


def test_gsi_spatial(tmp_path):
    db = Dynafile(tmp_path / "db")
    db.create_gsi(name="gsi1", pk_attribute="PK", sk_attribute="spatial")

    db.put_item(item={"PK": "1", "SK": "2"})
    db.put_item(item={"PK": "1", "SK": "3", "spatial": "true"})

    items = list(db.query(pk="1", index="gsi1"))
    assert items == [{"PK": "1", "SK": "3", "spatial": "true"}]


def test_gsi_query_multiple_items(tmp_path):
    db1 = Dynafile(tmp_path / "db")
    db1.create_gsi(name="gsi1", pk_attribute="GSI1_SK", sk_attribute="GSI1_PK")
    item1 = {"PK": "1", "SK": "1", "GSI1_PK": "1", "GSI1_SK": "1"}
    db1.put_item(item=item1)
    item2 = {"PK": "1", "SK": "2", "GSI1_PK": "1", "GSI1_SK": "1"}
    db1.put_item(item=item2)

    db2 = Dynafile(tmp_path / "db")

    items = list(db2.query(pk="1", index="gsi1"))
    assert items == [item1, item2]


def test_gsi_synced_put_item(tmp_path):
    db = Dynafile(tmp_path / "db")
    db.create_gsi(name="gsi1", pk_attribute="SK", sk_attribute="PK")

    db.put_item(item={"PK": "1", "SK": "2"})

    items = list(db.query(pk="2", index="gsi1"))
    assert items == [{"PK": "1", "SK": "2"}]


def test_gsi_synced_delete_item(tmp_path):
    db = Dynafile(tmp_path / "db")
    db.put_item(item={"PK": "1", "SK": "1"})
    db.put_item(item={"PK": "2", "SK": "1"})
    db.create_gsi(name="gsi1", pk_attribute="SK", sk_attribute="PK")

    db.delete_item(key={"PK": "2", "SK": "1"})

    items = list(db.query(pk="1", index="gsi1"))
    assert items == [{"PK": "1", "SK": "1"}]


def test_gsi_persists(tmp_path):
    db1 = Dynafile(tmp_path / "db")
    db1.put_item(item={"PK": "1", "SK": "1"})
    db1.put_item(item={"PK": "2", "SK": "1"})
    db1.create_gsi(name="gsi1", pk_attribute="SK", sk_attribute="PK")

    db2 = Dynafile(tmp_path / "db")
    db2.delete_item(key={"PK": "2", "SK": "1"})

    items = list(db2.query(pk="1", index="gsi1"))
    assert items == [{"PK": "1", "SK": "1"}]


def test_list_created_gsi(tmp_path):
    db1 = Dynafile(tmp_path / "db")
    db1.create_gsi(name="gsi1", pk_attribute="SK", sk_attribute="PK")
    # TODO missing test
    # assert


def test_remove_gsi(tmp_path):
    db = Dynafile(tmp_path / "db")
    # TODO missing test
