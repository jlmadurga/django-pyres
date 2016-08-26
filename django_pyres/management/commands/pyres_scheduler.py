import logging
from django.core.management.base import BaseCommand
from django_pyres.conf import settings

from optparse import make_option
from pyres.scheduler import Scheduler
from pyres import setup_logging, setup_pidfile


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.'),
    )
    help = 'Creates a pyres scheduler'

    def handle(self, **options):
        log_level = getattr(logging, options['log_level'].upper(), 'INFO')
        setup_logging(procname='pyres_scheduler', log_level=log_level, filename=None)
        setup_pidfile(settings.PYRES_SCHEDULER_PIDFILE)
        Scheduler.run(
            server=settings.PYRES_HOST,
            password=settings.PYRES_PASSWORD,
        )
