from __future__ import print_function

from os import path
from tubing import sources

with open(
    "{}/doc.txt.gz".format(path.dirname(path.abspath(__file__))), "rb"
) as f:

    objsource = sources.JSONParserSource(
        sources.LineReaderSource(
            sources.ZlibSource(f)
        )
    )

    for obj in iter(objsource.readobj, None):
        print(obj["_id"])

    f.seek(0)
    textsource = sources.LineReaderSource(sources.ZlibSource(f))

    for line in iter(textsource.readline, ''):
        print(line)
