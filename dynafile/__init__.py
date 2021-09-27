import hashlib
import warnings
from contextlib import contextmanager
from itertools import groupby
from pathlib import Path
from typing import Union, Optional, List, Callable, NamedTuple

from atomicwrites import atomic_write
from sortedcontainers import SortedDict

Filter = Union[Callable, "str"]


class Action(NamedTuple):
    op: str
    data: dict  # contains only key attributes for DELETE calls or the whole item in case of PUT calls


class _Partition:
    """
    Partition represents a storage node backed by a file.

    All items in one partition need to have the same partition key, which is not enforced within the partition.
    Partition organizes items only by the sort key attribute.
    """

    def __init__(self, path: Path, sk_attribute: str):
        self._sk_attribute = sk_attribute
        self._file = path / "data.pickle"

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
            _Partition._put(tree, key, item)

    @staticmethod
    def _put(tree, key, item):
        tree[key] = item

    def get_item(self, key) -> Optional[dict]:
        with self.read_access() as tree:
            return _Partition._get(tree, key)

    @staticmethod
    def _get(tree, key):
        return tree.get(key)

    def delete_item(self, key):
        with self.write_access() as tree:
            _Partition._delete(tree, key)

    @staticmethod
    def _delete(tree, key):
        del tree[key]

    def execute_write_batch(self, actions: List[Action]):
        """
        Provides write access within a single load/store flow.
        :param actions: Actions to execute, supports PUT and DELETE
        """
        with self.write_access() as tree:
            for action in actions:
                sk = action.data.get(self._sk_attribute)

                if action.op == "PUT":
                    _Partition._put(tree, sk, action.data)
                elif action.op == "DELETE":
                    _Partition._delete(tree, sk)
                else:
                    warnings.warn(f"Unknown action: {action.op}")

    def query(self, starts_with: Optional[str], scan_index_forward: bool) -> List:
        with self.read_access() as tree:
            tree: SortedDict
            if scan_index_forward:
                return [tree[sk] for sk in tree.irange(minimum=starts_with)]
            else:
                return [tree[sk] for sk in tree.irange(maximum=starts_with, reverse=True)]


class BatchWriter:
    def __init__(self, db: "Dynafile", pk_attribute: str):
        self._db = db
        self._queue: Optional[List[Action]] = None
        self._pk_attribute = pk_attribute

    def put_item(self, *, item: dict):
        self._queue.append(Action("PUT", item))

    def delete_item(self, *, key: dict):
        self._queue.append(Action("DELETE", key))

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


class Dynafile:
    """
    Interface to the Dynafile DB
    """

    def __init__(self, path: Union[str, Path] = "", pk_attribute="PK", sk_attribute="SK"):
        self._path = Path(path)
        self._partition_path = self._path / "_partitions"

        self._partitions: [bytes, _Partition] = {}

        self._pk_attribute = pk_attribute
        self._sk_attribute = sk_attribute

    def _new_pratition(self, hash):
        return _Partition(path=self._partition_path / hash, sk_attribute=self._sk_attribute)

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
            for partition, ops in groupby(actions, key=lambda a: a.data.get(self._pk_attribute))
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
        partition = self._partitions.get(partition_hash)
        if partition is None:
            partition = self._new_pratition(partition_hash)
            self._partitions[partition_key] = partition

        return partition

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

    def query(self, pk, starts_with="", scan_index_forward=True, _filter: Optional[Filter] = None):
        _filter = self.__parse_filter(_filter)

        partition = self._get_partition(pk)
        for item in partition.query(starts_with=starts_with, scan_index_forward=scan_index_forward):
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
                raise Exception("String filter expressions only available if `filtration` is installed.")
