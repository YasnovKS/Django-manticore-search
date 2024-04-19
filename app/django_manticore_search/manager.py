from django_manticore_search.models import SearchBase, SearchModel
from manticoresearch import ApiException, UtilsApi


class SearchManager(SearchBase):
    '''Класс для управления моделями БД manticore search.'''

    __registered = list()

    @classmethod
    def register(cls, *args):
        '''Метод для регистрации моделей manticore search,
        объявленных в других приложениях.'''

        for search_class in args:
            if not issubclass(search_class, SearchModel):
                raise TypeError(
                    f'{search_class} must be inherited from SearchModel'
                )
            cls.__registered.append(search_class)

    def __get_search_models_stats(self):
        '''Метод сбора параметров зарегистрированных моделей в формате словаря:
        {
            model name: [model instance, attr "model" of registered model]
        }'''

        return {
            _cls.__name__: [_cls, _cls.model]
            for _cls in self.__registered if _cls.model
        }

    def get_related_models(self):
        '''Метод получения объекта модели manticore search
        и связанной c ним модели django.
        Возвращает объект dict values, который содержит списки вида
        [объект модели manticore search, объект модели django]'''

        return self.__get_search_models_stats().values()
    
    def get_search_mapping(self):
        related_models = self.get_related_models()
        return {
            model.__class__.__name__: search_model
            for search_model, model in related_models
        }

    def show_tables(self):
        '''Возвращает список существующих таблиц manticore.'''

        all_tables = list()
        with self.get_session() as session:
            utils_api = UtilsApi(session)
            try:
                data = utils_api.sql('SHOW TABLES')
            except ApiException as e:
                return e
        for item in data:
            tables = item.get('data')
            if not tables:
                continue
            table_names = [table.get('Index') for table in tables]
            all_tables.extend(table_names)
        return all_tables

    def create_table(self, model: SearchModel):
        '''Метод для создания таблицы индекса, основанной на параметрах класса
        и списке разрешенных полей.
        '''

        with self.get_session() as session:
            utils_api = UtilsApi(session)
            return utils_api.sql(
                f'CREATE TABLE {model.table_name}({", ".join(model.fields)}) '
                f'{" ".join(model.table_params)}'
            )

    def delete_table(self, model: SearchModel):
        '''Удаляет таблицу индекса, если она существует.'''

        with self.get_session() as session:
            utils_api = UtilsApi(session)
            return utils_api.sql(f'DROP TABLE IF EXISTS {model.table_name}')

    def get_registered_models(self):
        '''Возвращает список зарегистрированных поисковых моделей.'''

        return self.__registered
