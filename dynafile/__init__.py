import hashlib
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Union, Optional, List, Callable, Iterable

from sortedcontainers import SortedDict

from dynafile.tree import _Node, Tree

Filter = Union[Callable, "str"]


class _Partition:
    """Represents one partition"""

    @abstractmethod
    def __init__(self, *, path, sk_attribute):
        pass

    @abstractmethod
    def add_item(self, key: str, item: dict):
        pass

    @abstractmethod
    def get_item(self, key) -> Optional[dict]:
        pass

    @abstractmethod
    def query(self, starts_with: str, scan_index_forward: bool) -> Iterable:
        pass

    @abstractmethod
    def delete_item(self, key):
        pass


class _FilePartition(_Partition):
    def __init__(self, file: Path):
        self._file = file

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
        with self._file.open("wb") as file:
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
            tree: SortedDict
            tree[key] = item

    def get_item(self, key) -> Optional[dict]:
        with self.read_access() as tree:
            tree: SortedDict
            return tree.get(key)

    def delete_item(self, key):
        with self.write_access() as tree:
            tree: SortedDict
            del tree[key]

    def query(self, starts_with: Optional[str], scan_index_forward: bool) -> List:
        with self.read_access() as tree:
            tree: SortedDict
            if scan_index_forward:
                return [tree[sk] for sk in tree.irange(minimum=starts_with)]
            else:
                return [tree[sk] for sk in tree.irange(maximum=starts_with, reverse=True)]


class Dynafile:
    """
    Interface to the Dynafile DB
    """

    def __init__(self, path: Union[str, Path] = "", pk_attribute="PK", sk_attribute="SK"):
        self._path = Path(path)

        self._partitions: [bytes, _Partition] = {}

        self._pk = pk_attribute
        self._sk = sk_attribute

    def new_pratition(self, hash):
        return _FilePartition(file=self._path / "_partitions" / f"{hash}.json")

    def put_item(self, *, item: dict):
        pk = item.get(self._pk)
        sk = item.get(self._sk)
        # if pk is None:
        #     raise Exception("Partition key have to be set")

        partition = self._get_partition(pk)
        partition.add_item(key=sk, item=item)

    def get_item(self, *, key: dict) -> Optional[dict]:
        pk = key.get(self._pk)
        # if pk is None:
        #     raise Exception("Partition key have to be set")
        partition = self._get_partition((pk))

        sk = key.get(self._sk)
        # if sk is None:
        #     raise Exception("Sort key have to be set")
        return partition.get_item(sk)

    def delete_item(self, *, key: dict):
        pk = key.get(self._pk)
        # if pk is None:
        #     raise Exception("Partition key have to be set")
        partition = self._get_partition((pk))

        sk = key.get(self._sk)
        # if sk is None:
        #     raise Exception("Sort key have to be set")
        partition.delete_item(sk)

    def _get_partition(self, partition_key: str) -> _Partition:
        """Read partition from files"""
        partition_hash = Dynafile._hash_key(partition_key)
        partition = self._partitions.get(partition_hash)
        if partition is None:
            partition = self.new_pratition(partition_hash)
            self._partitions[partition_key] = partition

        return partition

    @staticmethod
    def _hash_key(key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest()

    def scan(self, _filter: Optional[Filter] = None):
        _filter = self.__parse_filter(_filter)

        for file in (self._path / "_partitions").glob("*.json"):
            partition = _FilePartition(file)
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
