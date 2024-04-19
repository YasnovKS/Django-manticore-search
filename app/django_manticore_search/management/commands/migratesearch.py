from django.core.management.base import BaseCommand
from django_manticore_search.manager import SearchManager
from django_manticore_search.migrator import SearchMigrator


class Command(BaseCommand):
    help = 'Create tables from search models'

    def handle(self, *args, **kwargs):
        manager = SearchManager()
        search_models = manager.get_registered_models()
        SearchMigrator.run_migration(search_models)
        return 'Search migration complete'
