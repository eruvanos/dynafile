import hashlib
import json
import warnings
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from itertools import groupby
from pathlib import Path
from typing import Union, Optional, List, Callable, NamedTuple, Dict

from atomicwrites import atomic_write
from sortedcontainers import SortedDict

from dynafile.dispatcher import Dispatcher, Event, EventListener

Filter = Union[Callable[[dict], bool], "str"]


class ActionType:
    PUT = "PUT"
    DELETE = "DELETE"


class Action(NamedTuple):
    op: ActionType
    data: dict  # contains only key attributes for DELETE calls or the whole item in case of PUT calls


class _Partition:
    """
    Partition represents a storage node backed by a file.

    All items in one partition need to have the same partition key, which is not enforced within the partition.
    Partition organizes items only by the sort key attribute.
    """

    def __init__(
        self, path: Path, sk_attribute: str, dispatcher: Optional[Dispatcher] = None
    ):
        self._sk_attribute = sk_attribute
        self._file = path / "data.pickle"

        self._dispatcher = dispatcher

    def _load(self) -> SortedDict:
        # TODO not thread save
        import pickle

        if self._file.exists():
            with self._file.open("rb") as file:
                return pickle.load(file)
        else:
            return SortedDict()

    def _save(self, data: SortedDict):
        # TODO not thread save
        import pickle

        self._file.parent.mkdir(parents=True, exist_ok=True)

        with atomic_write(self._file, mode="wb", overwrite=True) as file:
            pickle.dump(data, file)

    @contextmanager
    def write_access(self) -> SortedDict:
        # TODO write lock
        tree = self._load()
        yield tree
        self._save(tree)
        # TODO unlock

    @contextmanager
    def read_access(self) -> SortedDict:
        # TODO read lock
        tree = self._load()
        # TODO unlock
        yield tree

    def add_item(self, key, item: dict):
        with self.write_access() as tree:
            self._put(tree, key, item)

    def _put(self, tree, key, item):
        old = tree.get(key)
        tree[key] = item

        if self._dispatcher:
            self._dispatcher.emit(Event(action=ActionType.PUT, new=item, old=old))

    def get_item(self, key) -> Optional[dict]:
        with self.read_access() as tree:
            return _Partition._get(tree, key)

    @staticmethod
    def _get(tree, key):
        return tree.get(key)

    def delete_item(self, key):
        with self.write_access() as tree:
            self._delete(tree, key)

    def _delete(self, tree, key):
        old = tree[key]
        del tree[key]

        if self._dispatcher:
            self._dispatcher.emit(Event(action=ActionType.DELETE, new=None, old=old))

    def execute_write_batch(self, actions: List[Action]):
        """
        Provides write access within a single load/store flow.
        :param actions: Actions to execute, supports PUT and DELETE
        """
        with self.write_access() as tree:
            for action in actions:
                sk = action.data.get(self._sk_attribute)

                if action.op == ActionType.PUT:
                    self._put(tree, sk, action.data)
                elif action.op == ActionType.DELETE:
                    self._delete(tree, sk)
                else:
                    warnings.warn(f"Unknown action: {action.op}")

    def query(self, starts_with: Optional[str], scan_index_forward: bool) -> List:
        with self.read_access() as tree:
            tree: SortedDict
            if scan_index_forward:
                return [tree[sk] for sk in tree.irange(minimum=starts_with)]
            else:
                return [
                    tree[sk] for sk in tree.irange(maximum=starts_with, reverse=True)
                ]


class BatchWriter:
    def __init__(self, db: "Dynafile", pk_attribute: str):
        self._db = db
        self._queue: Optional[List[Action]] = None
        self._pk_attribute = pk_attribute

    def put_item(self, *, item: dict):
        self._queue.append(Action(ActionType.PUT, item))

    def delete_item(self, *, key: dict):
        self._queue.append(Action(ActionType.DELETE, key))

    def __enter__(self):
        if self._queue:
            warnings.warn("Unprocessed items dropped, overlapping contexts")
        self._queue = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Group by partition and batch write and delete
        queue = self._queue
        self._queue = None
        self._db.execute_batch(queue)


@dataclass
class _MetaData:
    pk_attribute: str
    sk_attribute: str


class Dynafile:
    """
    Interface to the Dynafile DB
    """

    def __init__(
        self, path: Union[str, Path] = "", pk_attribute=None, sk_attribute=None
    ):
        # TODO split constructor into a static create new table and load only constructor
        self._path = Path(path)
        self._meta_file = self._path / "meta.json"
        self._partition_path = self._path / "_partitions"
        self._gsi_path = self._path / "_gsi"

        # ensure path
        self._path.mkdir(parents=True, exist_ok=True)

        # meta data
        if self._meta_file.exists():
            with self._meta_file.open() as file:
                meta_data = _MetaData(**json.load(file))

            if pk_attribute is None and sk_attribute is None:
                pk_attribute = meta_data.pk_attribute
                sk_attribute = meta_data.sk_attribute

            elif (
                meta_data.pk_attribute != pk_attribute
                or meta_data.sk_attribute != sk_attribute
            ):
                raise Exception("PK or SK attribute different from existing data.")
        else:
            pk_attribute = pk_attribute or "PK"
            sk_attribute = sk_attribute or "SK"

            meta_data = _MetaData(pk_attribute=pk_attribute, sk_attribute=sk_attribute)
            meta_data_dict = asdict(meta_data)

            with atomic_write(self._meta_file, mode="wt", overwrite=False) as file:
                json.dump(meta_data_dict, file)

        self._pk_attribute = pk_attribute
        self._sk_attribute = sk_attribute

        self._gsis: Dict[str, Dynafile] = {}
        for path in self._gsi_path.glob("*/"):
            self._gsis[path.name] = Dynafile(path)

        self._dispatcher = Dispatcher()
        self._dispatcher.connect(self.__sync_gsi)

    def _new_partition(self, hash):
        return _Partition(
            path=self._partition_path / hash,
            sk_attribute=self._sk_attribute,
            dispatcher=self._dispatcher,
        )

    def put_item(self, *, item: dict):
        pk = item.get(self._pk_attribute)
        sk = item.get(self._sk_attribute)
        # if pk is None:
        #     raise Exception("Partition key have to be set")

        partition = self._get_partition(pk)
        partition.add_item(key=sk, item=item)

    def batch_writer(self) -> BatchWriter:
        """Allow batched `put_item` and `delete_item` calls, loading partition only ones"""
        return BatchWriter(self, self._pk_attribute)

    def get_item(self, *, key: dict) -> Optional[dict]:
        pk = key.get(self._pk_attribute)
        # if pk is None:
        #     raise Exception("Partition key have to be set")
        partition = self._get_partition((pk))

        sk = key.get(self._sk_attribute)
        # if sk is None:
        #     raise Exception("Sort key have to be set")
        return partition.get_item(sk)

    def delete_item(self, *, key: dict):
        pk = key.get(self._pk_attribute)
        # if pk is None:
        #     raise Exception("Partition key have to be set")
        partition = self._get_partition(pk)

        sk = key.get(self._sk_attribute)
        # if sk is None:
        #     raise Exception("Sort key have to be set")
        partition.delete_item(sk)

    def execute_batch(self, actions: List[Action]):
        """
        Write all batches.

        :param actions:
        :return:
        """
        # Group by partition
        per_partition = {
            partition: list(ops)
            for partition, ops in groupby(
                actions, key=lambda a: a.data.get(self._pk_attribute)
            )
        }

        # Consolidate?
        # TODO optimisation: only apply last action, drop others

        # Execute
        for key, ops in per_partition.items():
            # TODO might be parallel
            partition = self._get_partition(key)
            partition.execute_write_batch(ops)

    def _get_partition(self, partition_key: str) -> _Partition:
        """Read partition from files"""
        partition_hash = Dynafile._hash_key(partition_key)
        return self._new_partition(partition_hash)

    @staticmethod
    def _hash_key(key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest()

    def scan(self, _filter: Optional[Filter] = None):
        _filter = self.__parse_filter(_filter)

        for file in self._partition_path.glob("*/"):
            partition = _Partition(path=file, sk_attribute=self._sk_attribute)
            for item in partition.query(None, True):
                if _filter(item):
                    yield item

    def query(
        self,
        pk: str,
        starts_with="",
        scan_index_forward=True,
        _filter: Optional[Filter] = None,
        index=None,
    ):
        if index is None:
            db = self
        elif index in self._gsis:
            db = self._gsis[index]
        else:
            raise Exception("Index does not exist")

        _filter = db.__parse_filter(_filter)
        partition = db._get_partition(pk)
        for item in partition.query(
            starts_with=starts_with, scan_index_forward=scan_index_forward
        ):
            if _filter(item):
                yield item

    def __parse_filter(self, _filter: Optional[Filter]) -> Callable:
        if _filter is None:
            return bool
        elif callable(_filter):
            return _filter
        elif type(_filter) is str:
            try:
                import filtration

                return filtration.Expression.parseString(_filter)
            except ImportError:
                raise Exception(
                    "String filter expressions only available if `filtration` is installed."
                )

    def add_stream_listener(self, listener: EventListener):
        self._dispatcher.connect(listener)

    def extract_key(self, item: dict):
        return {key: item[key] for key in (self._pk_attribute, self._sk_attribute)}

    def contains_key_attributes(self, item: dict):
        return self._pk_attribute in item and self._sk_attribute in item

    def create_gsi(self, name: str, pk_attribute: str, sk_attribute: str):
        if name in self._gsis:
            raise Exception("GSI already exists")

        # add new GSI
        self._gsis[name] = Dynafile(
            path=self._path / f"_gsi/{name}",
            pk_attribute=pk_attribute,
            sk_attribute=sk_attribute,
        )

        # backfill GSI
        for item in self.scan():
            for name, gsi in self._gsis.items():
                if gsi.contains_key_attributes(item):
                    self._gsis[name].put_item(item=item)

    def __sync_gsi(self, event: Event):
        if event.action == ActionType.DELETE:
            for gsi in self._gsis.values():
                if gsi.contains_key_attributes(event.old):
                    gsi.delete_item(key=event.old)

        elif event.action == ActionType.PUT:
            for gsi in self._gsis.values():
                if gsi.contains_key_attributes(event.new):
                    gsi.put_item(item=event.new)
