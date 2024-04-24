from dynafile import Dynafile, Event


class Observer:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))

    @property
    def latest(self):
        return self.calls[-1] if self.calls else None


def test_put_item_schedules_event(tmp_path):
    db = Dynafile(tmp_path / "db")
    observer = Observer()
    db.add_stream_listener(observer)

    db.put_item(item={"PK": "1", "SK": "aa"})

    args, kwargs = observer.latest
    assert args[0] == Event(action="PUT", new={"PK": "1", "SK": "aa"}, old=None)


def test_put_item_overwrite_schedules_event(tmp_path):
    db = Dynafile(tmp_path / "db")
    db.put_item(item={"PK": "1", "SK": "aa", "old": True})
    observer = Observer()
    db.add_stream_listener(observer)

    db.put_item(item={"PK": "1", "SK": "aa", "old": False})

    args, kwargs = observer.latest
    assert args[0] == Event(
        action="PUT",
        new={"PK": "1", "SK": "aa", "old": False},
        old={"PK": "1", "SK": "aa", "old": True},
    )


def test_delete_item_schedules_event(tmp_path):
    db = Dynafile(tmp_path / "db")
    db.put_item(item={"PK": "1", "SK": "aa"})
    observer = Observer()
    db.add_stream_listener(observer)

    db.delete_item(key={"PK": "1", "SK": "aa"})

    args, kwargs = observer.latest
    assert args[0] == Event(action="DELETE", new=None, old={"PK": "1", "SK": "aa"})


def test_batch_write_item_schedules_event(tmp_path):
    db = Dynafile(tmp_path / "db")
    observer = Observer()
    db.add_stream_listener(observer)

    with db.batch_writer() as writer:
        writer.put_item(item={"PK": "1", "SK": "aa"})
        writer.delete_item(key={"PK": "1", "SK": "aa"})

    args, kwargs = observer.calls[0]
    assert args[0] == Event(action="PUT", new={"PK": "1", "SK": "aa"}, old=None)

    args, kwargs = observer.calls[1]
    assert args[0] == Event(action="DELETE", new=None, old={"PK": "1", "SK": "aa"})
