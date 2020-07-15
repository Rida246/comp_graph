from abc import abstractmethod, ABC
import typing as tp
from operator import itemgetter
from string import punctuation
from heapq import nlargest
from itertools import groupby
from collections import defaultdict
from datetime import datetime as dt
from math import sin, cos, atan2, radians, log
import copy


TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]
TGroupGenerator = tp.Generator[tp.Tuple[tp.Any, TRowsGenerator], None, None]
Coord = tp.Tuple[float, float]
Length = float


def groupby_with_precheck(rows: TRowsIterable, key: tp.Any = None) -> TGroupGenerator:
    """
    Extension for itertools.groupby (https://docs.python.org/3/library/itertools.html#itertools.groupby)
    for grouping values by certain key, raise ValueError if not sorted rows given
    :param rows: rows to be grouped
    :param key: key on which rows are grouped
    """
    # TODO: write extended groupby and several tests for it in file test_operations
    pass


# Table Slice


class Operation(ABC):
    """
    Base Class (https://docs.python.org/3/library/abc.html) for every operation
    """
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: rows to which the operation will be applied
        """
        pass


# Operations

class FromIter(Operation):
    """
    Operation performing receiving data form iterator
    """
    def __init__(self, name: str) -> None:
        """
        :param name: key in kwargs passed on __call__ which value corresponds to iterator containing data
        """
        # TODO: resources on itemgetter: https://docs.python.org/3/library/operator.html
        self.name = name
        self.itergetter = itemgetter(self.name)

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param args: None
        :param kwargs: contains iterator containing data with key self.name
        :return: generator of input data rows
        """
        return copy.deepcopy(self.itergetter(kwargs)())


class FromFile(Operation):
    """
    Operation performing receiving data form file and parsing
    """
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        """
        :param filename:
        :param parser: yields TRow
        """
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param args: None
        :param kwargs: None
        :return: generator of input data rows
        """
        # TODO: read the self.filename file and parse each string in the file using self.parser
        pass


class Mapper(ABC):
    """Base class for mappers"""
    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        # TODO: Apply the self.mapper to each row in rows. To simplify code use yield from
        pass


class Reducer(ABC):
    """Base class for reducers"""
    @abstractmethod
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = tuple(keys)

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        if self.keys:
            # TODO: use groupby_with_precheck for grouping rows by self.keys and then apply self.reducer to each group
            pass
        else:
            yield from self.reducer(self.keys, rows)


class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def _cross_join(self, a: TRow, b: TRow, keys: tp.Sequence[str]) -> TRow:
        res = {}
        for a_key, a_value in a.items():
            if (a_key in b) and (a_key not in keys):
                res[a_key + self._a_suffix] = a_value
            else:
                res[a_key] = a_value
        for b_key, b_value in b.items():
            if (b_key in a) and (b_key not in keys):
                res[b_key + self._b_suffix] = b_value
            else:
                res[b_key] = b_value
        return res


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: left rows to be joined
        :param args: contains right rows to be joined as a first element
        :param kwargs: None
        :return:
        """
        if self.keys:
            # Constructing groups from given iterators
            left_groups = groupby_with_precheck(rows, itemgetter(*self.keys))
            right_groups = groupby_with_precheck(args[0], itemgetter(*self.keys))
            # Empty generator will be useful lately
            enpty_gen: TRowsIterable = iter(())

            lgroup_keys, l_gen = next(left_groups)
            rgroup_keys, r_gen = next(right_groups)
            while True:
                # TODO: fill all the "pass" lines with code such that all the tests on join pass successfully
                try:
                    if rgroup_keys < lgroup_keys:
                        pass
                    elif rgroup_keys == lgroup_keys:
                        pass
                    elif rgroup_keys > lgroup_keys:
                        pass
                except StopIteration:
                    break
            if rgroup_keys > lgroup_keys:
                pass
            elif rgroup_keys < lgroup_keys:
                pass

            for rgroup_keys, r_gen in right_groups:
                pass
            for lgroup_keys, l_gen in left_groups:
                pass
        else:
            yield from self.joiner(self.keys, rows, args[0])

# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _filter_punctuation(txt: str) -> str:
        # TODO: filter all the punctuation in text
        pass

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = self._filter_punctuation(row[self.column])
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        # TODO: lowercase
        pass

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""
    def __init__(self, column: str, separator: tp.Optional[str] = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        # TODO: splits text in row by the self.separator on words and next yield rows with word in self.separator
       pass


class Apply(Mapper):
    """Apply function f(x_1, x_2, x_3, .... x_N) to N columns"""
    def __init__(self, function: tp.Callable[..., tp.Any],
                 columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to apply function
        :param result_column: column name to store the result
        """
        self.function = function
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = self.function(*[row[col] for col in self.columns])
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""
    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        # TODO: filter by the self.condition
        pass


class Project(Mapper):
    """Leave only mentioned columns"""
    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        # TODO: project self.columns
        pass


class IDF(Mapper):
    """
    Compute inverce document frequency according to the formula:
    log( total number of docs / number of docs where term appears )
    """
    def __init__(self, total_number_column: str, term_occ_number_column: str, result_column: str = "idf") -> None:
        """
        :param total_number_column: name of column with total number of docs
        :param term_occ_number_column: name of column with number of docs where serten term apperas
        :param result_column: name of column to store the IDF
        """
        self.total_number_column = total_number_column
        self.term_occ_number_column = term_occ_number_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        # TODO: IDF
        pass


class TF_IDF(Mapper):
    """
    Compute term frequency according to the formula:
    number of times term appears in serten doc / total number of terms in serten doc
    """
    def __init__(self, tf_column: str = "tf", idf_column: str = "idf",  result_column: str = "tf_idf") -> None:
        """
        :param term_occ_in_doc: name of column with number of times term appears in serten doc
        :param total_term_in_doc: name of column with total number of terms in serten doc
        :param result_column: name of column to store the IDF
        """
        self.tf_column = tf_column
        self.idf_column = idf_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        # TODO: TF_IDF
        pass

# Reducers


class FilterGroup(Reducer):
    """Apply function f(x) to certain columns"""
    def __init__(self, filter_: tp.Callable[..., tp.Any], column: str) -> None:
        """
        :param filter_: filter function which is applying inside the group
        :param result_column: column name to store the result
        """
        self.filter_ = filter_
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        args = []
        for row in rows:
            args.append(row[self.column])

        if self.filter_(*args):
            yield row


class TopN(Reducer):
    """Calculate top N by value"""
    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        # TODO: the best way to do it is using heapq.nlargest
        pass


class TF(Reducer):
    """Calculate frequency of values in column"""
    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        # TODO: TF
        pass


class Mean(Reducer):
    """Mean values in column passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to sum
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        # TODO: Mean value
        pass


class Count(Reducer):
    """Count rows passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to count
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        number: int = 0
        for row in rows:
            number += 1
        filtered_row = {key: row[key] for key in group_key}
        filtered_row[self.column] = number
        yield filtered_row


class Sum(Reducer):
    """Sum values in column passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to sum
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        # TODO: Sum
        pass


# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = list(rows_b)
        for a in rows_a:
            for b in rows_b:
                yield self._cross_join(a, b, keys)


class OuterJoiner(Joiner):
    """Join with outer strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        # TODO: OuterJoiner
        pass


class LeftJoiner(Joiner):
    """Join with left strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        # TODO: LeftJoiner
        pass


class RightJoiner(Joiner):
    """Join with right strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        # TODO: RightJoiner
        pass
