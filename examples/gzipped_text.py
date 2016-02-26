from __future__ import print_function

import logging
logging.basicConfig(level=logging.WARN)
from os import path
from tubing import sources, tubes, sinks

f = "{}/doc.txt.gz".format(path.dirname(path.abspath(__file__)))

apparatus = sources.File(f, "rb") \
    | tubes.Gunzip() \
    | tubes.Split() \
    | tubes.JSONParser() \
    | sinks.Objects()

for obj in apparatus.result:
    print(obj["_id"])

apparatus = sources.File(f, "rb") \
    | tubes.Gunzip() \
    | tubes.Split() \
    | sinks.Objects()

for line in apparatus.result:
    print(line)

apparatus = sources.File(f, "rb") \
    | tubes.Tee(sinks.Hash('sha1')) \
    | tubes.Tee(sinks.Hash('sha224')) \
    | tubes.Tee(sinks.Hash('sha256')) \
    | tubes.Tee(sinks.Hash('sha384')) \
    | tubes.Tee(sinks.Hash('sha512')) \
    | sinks.Hash('md5')

print("sha1 %s" % (apparatus.tubes[0].result.hexdigest()))
print("sha224 %s" % (apparatus.tubes[1].result.hexdigest()))
print("sha256 %s" % (apparatus.tubes[2].result.hexdigest()))
print("sha384 %s" % (apparatus.tubes[3].result.hexdigest()))
print("sha512 %s" % (apparatus.tubes[4].result.hexdigest()))
print("md5 %s" % (apparatus.result.hexdigest()))
