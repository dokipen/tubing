import pkgutil
try:
    version = pkgutil.get_data(__name__, 'VERSION')
except IOError:
    version = '9999'

__version__ = version
