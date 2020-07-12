from .lib import Graph, operations
import pathlib
import typing as tp


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(name=input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def word_count_graph_from_file(input_file_path: tp.Callable[[str], str],
                               parser: tp.Callable[[str], operations.TRow],
                               text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    path = pathlib.Path(__file__).parent
    input_file_name = input_file_path(str(path))
    return Graph.graph_from_file(filename=input_file_name, parser=parser) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])

def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""

    docs_count_column = "total_docs"
    split_words = Graph.graph_from_iter(name=input_stream_name) \
                       .map(operations.FilterPunctuation(text_column)) \
                       .map(operations.LowerCase(text_column)) \
                       .map(operations.Split(text_column))
    count_docs = Graph.graph_from_iter(name=input_stream_name) \
                      .sort([doc_column]) \
                      .reduce(operations.Count(docs_count_column), [])
    term_occ_count_column = "term_occ"
    count_idf = split_words.sort([doc_column, text_column])  \
                           .reduce(operations.FirstReducer(), (doc_column, text_column)) \
                           .sort([text_column]) \
                           .reduce(operations.Count(term_occ_count_column), [text_column]) \
                           .join(operations.InnerJoiner(suffix_a="", suffix_b=""), count_docs, []) \
                           .map(operations.IDF(docs_count_column, term_occ_count_column)) \
                           .sort([text_column])

    count_tf = split_words.sort([doc_column]) \
                          .reduce(operations.TF(text_column), [doc_column]) \
                          .sort([text_column])
    result = count_idf.join(operations.InnerJoiner(suffix_a="", suffix_b=""), count_tf, [text_column]) \
                      .map(operations.TF_IDF(result_column=result_column)) \
                      .map(operations.Project((doc_column, text_column, result_column))) \
                      .sort([text_column]) \
                      .reduce(operations.TopN(result_column, n=3), [text_column])
    return result