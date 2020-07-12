from pytest import approx
from operator import itemgetter

from .graphs import word_count_graph, word_count_graph_from_file, inverted_index_graph


def test_word_count_from_file() -> None:
    graph = word_count_graph_from_file(
        input_file_path=lambda x: x + '/resource/test_corpus.txt',
        parser=lambda x: eval(x), text_column='text', count_column='count')
    graph.run()
    assert True


def test_word_count_multiple_call() -> None:
    graph = word_count_graph('text', text_column='text', count_column='count')

    docs1 = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
    ]

    expected1 = [
        {'count': 1, 'text': 'hello'},
        {'count': 1, 'text': 'little'},
        {'count': 1, 'text': 'my'},
        {'count': 1, 'text': 'world'}
    ]

    result1 = graph.run(text=lambda: iter(docs1))

    assert expected1 == result1

    docs2 = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    expected2 = [
        {'count': 1, 'text': 'hell'},
        {'count': 1, 'text': 'world'},
        {'count': 2, 'text': 'hello'},
        {'count': 2, 'text': 'my'},
        {'count': 3, 'text': 'little'}
    ]

    result2 = graph.run(text=lambda: iter(docs2))

    assert expected2 == result2


def test_word_count_multiple_call_from_file() -> None:
    graph = word_count_graph_from_file(
        input_file_path=lambda x: x + '/resource/test_corpus.txt',
        parser=lambda x: eval(x), text_column='text', count_column='count')

    result1 = graph.run()
    result2 = graph.run()

    assert result1 == result2


def test_tf_idf() -> None:
    graph = inverted_index_graph('texts', doc_column='doc_id', text_column='text', result_column='tf_idf')

    rows = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!!'}
    ]

    expected = [
        {'doc_id': 1, 'text': 'hello', 'tf_idf': approx(0.1351, 0.001)},
        {'doc_id': 1, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

        {'doc_id': 2, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

        {'doc_id': 3, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

        {'doc_id': 4, 'text': 'hello', 'tf_idf': approx(0.1013, 0.001)},
        {'doc_id': 4, 'text': 'little', 'tf_idf': approx(0.2027, 0.001)},

        {'doc_id': 5, 'text': 'hello', 'tf_idf': approx(0.2703, 0.001)},
        {'doc_id': 5, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

        {'doc_id': 6, 'text': 'world', 'tf_idf': approx(0.3243, 0.001)}
    ]

    result = graph.run(texts=lambda: iter(rows))

    assert expected == sorted(result, key=itemgetter('doc_id', 'text'))
