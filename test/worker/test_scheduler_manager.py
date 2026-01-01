import unittest
from unittest.mock import Mock

from dspider.worker.scheduler_manager import SchedulerManager
from dspider.worker.utils.load_module import walk_modules

class TestSchedulerManager(unittest.TestCase):
    def test_start_api_service(self):
        scheduler_manager = SchedulerManager()
        scheduler_manager.start_api_service()
        self.assertTrue(scheduler_manager.api_server.is_alive())
    
    def test_start_all(self):
        mock_scheduler = Mock()
        mock_scheduler.name = "test_scheduler"
        
        scheduler_manager = SchedulerManager()
        scheduler_manager.register_scheduler(mock_scheduler)
        scheduler_manager.start_all()
    
    def test_starta(self):
        from importlib import import_module
        from pkgutil import iter_modules
        
        scheduler_pkg = 'dspider.worker.schedulers'
        scheduler_name = 'DbToMqScheduler'
        modules = import_module(scheduler_pkg)
        
        
        for importer, modname, ispkg in iter_modules(
            modules.__path__,
            prefix=f'{modules.__name__}.',
        ):
            if not ispkg:
                mod = import_module(modname)
                if hasattr(mod, scheduler_name):
                    scheduler_class = getattr(mod, scheduler_name)
                    break

    def test_startb(self):
        scheduler_manager = SchedulerManager()
        scheduler_manager.start()
