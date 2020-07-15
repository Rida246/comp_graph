# Computational Graph

Overview
========

Библиотека содержит в себе реализацию вычислительного графа. Под вычислительным графом понимается заранее заданная 
последовательнсоть операций над некоторыми данными. 

Installation
=========

В файле `requirements.txt` перечисленны все необходимые для работы `comp_graph` библиотеки. Чтобы их установить, необходимо выполнить команду:

```
pip install -r requirements.txt
```

Structure
=========

- lib - основной код библиотеки
- resource - данные
- cli - интрефейс командной строки

Quick Examples
=======

Предположим в файле `data.txt` имеется набор документов. 
Пусть также есть функция `parser`, которая данные из `data.txt` привращает в итератор 
содержащий словари вида:
```
{"document_id": 1, "documnet_text": "hElLo wwworld!!!"}
{"document_id": 2, "documnet_text": "English is SPOKEN all over the world"}
...
```
Наша задача заключается в том, чтобы текст каждого документа привести к нижнему регистур, а также 
убрать знаки пунктуаци и разбить каждый текст на отдельные слова, то есть получить итератор вида:
```
{"document_id": 1, "word": "hello"}
{"document_id": 1, "word": "wwworld"}
{"document_id": 2, "word": "english"}
{"document_id": 2, "word": "is"}
{"document_id": 2, "word": "spoken"}
{"document_id": 2, "word": "all"}
{"document_id": 2, "word": "over"}
{"document_id": 2, "word": "the"}
{"document_id": 2, "word": "world"}
...
```
C помощью библиотеки `comp_graph` это можно сделать следующим образом:
```
from .lib import Graph, operations

g = Graph.graph_from_file("data.txt", parser) \
         .map(ops.FilterPunctuation(text_column)) \
         .map(ops.LowerCase(text_column)) \
         .map(ops.Split(text_column))
         .rename_column(from="documnet_text", to="word")
result = g.run()
```
Теперь в переменной `result` будет сохранен итератор с ожидаемым результатом.