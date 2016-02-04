import pkgutil
try:
    version = pkgutil.get_data(__name__, 'VERSION').decode('utf-8')
except IOError:                     # pragma: no cover
    version = '9999'

__version__ = version
