from django.apps import AppConfig
from django.db.models.signals import post_delete, post_migrate, post_save


class DjangoManticoreSearchConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'django_manticore_search'

    def ready(self):
        from django_manticore_search.manager import SearchManager
        from django_manticore_search.migrator import SearchMigrator

        # Прослушивание сигналов о миграциях Django
        post_migrate.connect(SearchMigrator.make_migrations, sender=self)

        # Прослушивание сигналов о сохранении и удалении объектов
        # моделей Django, связанных с моделями manticore search
        manager = SearchManager()
        for search_model, django_model in manager.get_related_models():
            post_save.connect(
                receiver=search_model.update, sender=django_model
            )
            post_delete.connect(
                receiver=search_model.delete, sender=django_model
            )

        return super().ready()
