from os import path
from tubing import sources
import logging

with file("{}/doc.txt.gz".format(path.dirname(path.abspath(__file__))), "rb") as f:

    objsource = sources.JSONParserSource(sources.LineReaderSource(sources.ZlibSource(f)))

    for obj in iter(objsource.readobj, None):
        print obj["_id"]

    f.seek(0)
    textsource = sources.LineReaderSource(sources.ZlibSource(f))

    for line in iter(textsource.readline, ''):
        print line
