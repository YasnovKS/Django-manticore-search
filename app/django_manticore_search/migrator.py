from typing import List

from django_manticore_search.manager import SearchManager
from django_manticore_search.models import SearchModel


class SearchMigrator:
    '''Класс, выполняющий операции миграции для объектов manticore search.'''

    manager = SearchManager()

    @classmethod
    def make_migrations(cls, *args, **kwargs):
        '''Метод для сбора информации об изменениях в моделях django,
        связанных с моделями manticore.
        Данный метод является колбэком для post-migrate сигнала django.
        При наличии миграций, затрагивающих модели django, связанные
        с моделями manticore будет вызван метод run_migration,
        выполняющий миграции.'''

        plan = kwargs.get('plan')
        models = cls.get_models_for_migration(plan)
        if not models:
            return
        related_models = cls.manager.get_related_models()
        migration_models = list(
            cls.collect_migration_models(models, related_models)
        )
        return cls.run_migration(migration_models)

    @classmethod
    def run_migration(cls, models: List[SearchModel]):
        '''Выполняет миграции в БД manticore при наличии изменений.'''

        for model in models:
            model = model()
            cls.manager.delete_table(model)
            cls.manager.create_table(model)
            model.migrate_model_data()

    @classmethod
    def get_models_for_migration(cls, plan):
        '''Метод получения информации о моделях django,
        которые указаны в плане миграции django.'''

        models = list()
        if not plan:
            return
        try:
            migrations = [item[0] for item in plan]
        except (IndexError, TypeError):
            return
        for migration in migrations:
            models.extend([item.model_name for item in migration.operations if hasattr(item, 'model_name')])
        return models

    @classmethod
    def collect_migration_models(cls, models, related_models):
        '''Возвращает список моделей manticore, для которых
        необходимо произвести миграции'''

        for model, related_model in related_models:
            if related_model.__name__.lower() in models:
                yield model
