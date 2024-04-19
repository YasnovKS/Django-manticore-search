import json
from typing import List, Optional

from django.db.models.fields.related import ManyToOneRel
from django_manticore_search import SearchConfig
from django_manticore_search.constants import ALLOWED_TABLE_PARAMS, FIELD_TYPES
from django_manticore_search.utils import strip_tags
from manticoresearch import (ApiClient, ApiException, IndexApi, SearchApi,
                             UtilsApi)
from manticoresearch.model import (DeleteDocumentRequest,
                                   InsertDocumentRequest, MatchOpFilter,
                                   MatchPhraseFilter, SearchRequest)


class SearchBase:
    '''Класс, реализующий создание экземпляра с указанными
    параметрами конфигурации manticore search.'''

    def __init__(self):
        self.config = SearchConfig().get_config()

    def get_session(self):
        return ApiClient(self.config)


class ManticoreObject:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return self.id

    def __repr__(self):
        return f'{self.__class__.__name__}: id={self.id}'


class QuerySelector(SearchBase):
    def __init__(self, _cls):
        self.model = _cls
        self.queryset = list()
        super().__init__()

    def get_queryset(self, response):
        '''Формирует список объектов класса ManticoreObject
        из полученных объектов поиска.'''

        queryset = response.hits.to_dict().get('hits')
        return [
                ManticoreObject(
                    id=obj.get('_id'),
                    **obj.get('_source')
                ) for obj in queryset
            ]

    def search(self, query, all=False):
        '''Метод поиска по ключевым словам в запросе.
        Совпадением считается наличие в объекте хотя бы
        одного слова из запроса.
        :attr: query - str - строка запроса для поиска,
        :attr: all - bool - определяет, нужно ли искать только
        те объекты, в которых содержатся все слова из запроса
        (порядок не важен)'''

        operator = 'and' if all else 'or'

        with ApiClient(self.config) as client:
            search_request = SearchRequest()
            search_request.index = self.model.table_name
            search_request.fulltext_filter = MatchOpFilter(
                query, '_all', operator
            )
            search_api = SearchApi(client)
            try:
                response = search_api.search(search_request)
            except ApiException as e:
                print(e)
            self.queryset = self.get_queryset(response) or None
            return self

    def phrase_search(self, query):
        '''Метод поиска объектов по точной фразе из запроса.
        Нечувствителен к регистру.'''

        with ApiClient(self.config) as client:
            search_request = SearchRequest()
            search_request.index = self.model.table_name
            search_request.fulltext_filter = MatchPhraseFilter(query, '_all')
            search_api = SearchApi(client)
            try:
                response = search_api.search(search_request)
            except ApiException as e:
                print(e)
            self.queryset = self.get_queryset(response) or None
            return self

    def all(self):
        '''Возвращает список объектов поиска, если метод поиска был использован
        до данного метода, иначе возвращает все объекты таблицы индекса.'''

        if self.queryset:
            return self.queryset
        if self.queryset is None:
            return []
        with self.get_session() as session:
            search_api = SearchApi(session)
            search_request = SearchRequest(
                index=self.model.table_name
            )
            try:
                response = search_api.search(search_request)
            except ApiException as e:
                print(e)
        return self.get_queryset(response)

    def ids(self):
        '''Возвращает список id объектов поиска, если метод поиска был
        использован до данного метода, иначе возвращает id всех объектов
        таблицы индекса.'''

        queryset = self.all()
        return [obj.id for obj in queryset]

    def first(self):
        queryset = self.all()
        return queryset[0] if queryset else None


class SearchModel(SearchBase):
    '''Класс представляет собой абстрактную модель для управления данными
    в таблицах manticore search. Последующие модели должны быть унаследованы
    от этого класса.'''

    model = None
    extra_kwargs = {}
    allowed_fields: Optional[List[str]] = None

    @property
    def table_name(self):
        return self.__class__.__name__.lower()

    @property
    def model_fields(self):
        '''Возвращает список полей модели, указанной в поле model.'''

        return self.model._meta.get_fields() if self.model else []

    @property
    def fields(self):
        '''Определяет список полей модели поиска на основе разрешенных
        для индексирования полей основной модели данных,
        указанной в поле model.'''

        if not self.model:
            return []
        return [
            f'{field.name} {FIELD_TYPES.get(field.__class__.__name__, "string")}'
            for field in self.model_fields
            if (field.name in self.__get_allowed_fields())
        ]

    @property
    def table_params(self):
        '''Возвращает список параметров таблицы индекса,
        указанных в extra_kwargs.'''

        return [
            f'{key} = \'{value}\'' for key, value in self.extra_kwargs.items()
            if key in ALLOWED_TABLE_PARAMS
        ]

    @property
    def objects(self):
        return QuerySelector(self)

    def __get_allowed_fields(self):
        '''Возвращает список разрешенных для индексирования
        полей модели, если он указан в allowed_fields.
        В противном случае - полный список полей модели.'''

        return self.allowed_fields if self.allowed_fields else [
            field.name for field in self.model_fields
            if (field.name != 'id' and not isinstance(field, ManyToOneRel))
        ]

    def __get_model_objects(self):
        '''Возвращает массив объектов связанной модели.'''

        return self.model.objects.all() if self.model else None

    def __get_query_list(self, queryset):
        '''Возвращает список строк запроса с параметрами
        индексируемых объектов.'''

        if not queryset:
            return
        allowed_fields = self.__get_allowed_fields()
        for obj in queryset:
            obj_params = {
                'index': self.table_name,
                'id': obj.id,
                'doc': {
                    field: strip_tags(getattr(obj, field))
                    for field in allowed_fields
                    if field != 'id'
                }
            }
            yield obj_params

    def __get_query_string(self, operation, data: dict):
        '''Возвращает строку запроса для выполнения CRUD операции
        в таблице БД manticore search.'''

        allowed_operations = ['insert', 'delete', 'replace', 'update',]
        if operation not in allowed_operations:
            raise ApiException(f'Operation {operation} is not allowed.')
        return getattr(self, f'get_raw_{operation}')(data)

    def get_raw_insert(self, data: dict):
        '''Возвращает строку запроса для операции вставки данных
        в таблицу manticore search.'''

        return json.dumps({'insert': data})

    def __object_exists(self, id):
        '''Проверяет наличие объекта в таблице.'''

        query = f'SELECT * FROM {self.table_name} WHERE id={id}'
        with self.get_session() as session:
            utils_api = UtilsApi(session)
            response = utils_api.sql(query)
            response = response[0]
            return response.get('total')

    def create(self, instance, data):
        '''Метод для создания объекта в таблице индекса.'''

        with self.get_session() as session:
            index_api = IndexApi(session)
            insert_data = InsertDocumentRequest(
                index=self.table_name,
                id=instance.id,
                doc=data,
            )
            try:
                index_api.insert(insert_data)
            except ApiException as e:
                print(e)

    def replace(self, instance, data):
        '''Метод для изменения объекта в таблице индекса.'''

        with self.get_session() as session:
            index_api = IndexApi(session)
            insert_data = InsertDocumentRequest(
                index=self.table_name,
                id=instance.id,
                doc=data,
            )
            try:
                index_api.replace(insert_data)
            except ApiException as e:
                print(e)

    @classmethod
    def update(cls, sender, **kwargs):
        '''Метод, выполняющий обновление объекта в таблице индекса
        (создание или изменение).'''

        self = cls()
        instance = kwargs.get('instance')
        data = {
            field: strip_tags(getattr(instance, field))
            for field in self.__get_allowed_fields()
        }
        if self.__object_exists(instance.id):
            return self.replace(instance, data)
        return self.create(instance, data)

    @classmethod
    def delete(cls, sender, **kwargs):
        '''Метод для удаления объекта из таблицы индекса.'''

        self = cls()
        instance = kwargs.get('instance')

        with self.get_session() as session:
            index_api = IndexApi(session)
            delete_request = DeleteDocumentRequest(
                index=self.table_name,
                id=instance.id
            )
        try:
            return index_api.delete(delete_request)
        except ApiException as e:
            print(e)

    def migrate_model_data(self):
        '''Выполняет операцию заполнения пустой таблицы БД manticore
        индексируемыми данными связанной модели Django.'''
        queryset = self.__get_model_objects()
        query_list = list(self.__get_query_list(queryset))
        if not query_list:
            return
        query_string = '\n'.join([
            self.__get_query_string('insert', item) for item in query_list
        ])
        with self.get_session() as session:
            index_api = IndexApi(session)
            try:
                return index_api.bulk(query_string)
            except ApiException as e:
                print(e)

    def sql(self, query):
        with self.get_session() as session:
            utils_api = UtilsApi(session)
            try:
                response = utils_api.sql(query)
                return response
            except ApiException as e:
                print(e)
