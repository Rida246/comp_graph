import typing as tp
from . import operations as ops
from . import external_sort as sort


class Graph:
    """Computational graph implementation"""

    def __init__(self,
                 operation: ops.Operation,
                 dependencies: tp.List['Graph']) -> None:
        self.dependencies = dependencies
        self.operation = operation

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        :param name: name of kwarg to use as data source
        """
        graph = Graph(dependencies=[], operation=ops.FromIter(name))
        return graph

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        # TODO: graph from file
        pass

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        # TODO: map operating
        pass

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        # TODO: reduct operation
        pass

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        # TODO: sort operation

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        # TODO: join operation

    def _run(self, **kwargs: tp.Any) -> ops.TRowsGenerator:
        """Single method to start execution; data sources passed as kwargs, returns iterable object"""
        dependencies = [child_graph._run(**kwargs) for child_graph in self.dependencies]
        current_generator = self.operation(*dependencies, **kwargs)
        return current_generator

    def run(self, **kwargs: tp.Any) -> tp.List[ops.TRow]:
        """Single method to start execution; data sources passed as kwargs"""
        return list(self._run(**kwargs))
