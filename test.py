########################
###    T E S T       ###
########################

# The required Modules are imported

from org.openlca.jsonld.input import JsonImport
from org.openlca.jsonld import JsonStoreReader
from org.openlca.jsonld import ZipStore
from org.openlca.app.db import Database
from org.openlca.core.database import Derby
from java.io import File
import org.openlca.core.model as model
from org.openlca.core.database import UnitGroupDao, FlowPropertyDao
from java.util import UUID
from org.openlca.app.util import UI
from org.openlca.app import App
from org.openlca.core.database import ProcessDao
from org.openlca.core.database import FlowDao
from org.openlca.core.database import CategoryDao
from org.openlca.core.database import FlowPropertyDao
import csv
import json
import os
import shutil

# The connection to the open Database is established
db         = Database.get()

# Dao objects are set. Those are used to iterate over the respective Model Type (e.g. Processes or Flows etc.)
dao_p  = ProcessDao(db)
dao_f  = FlowDao(db)

allflows      = dao_f.getAll()
allprocesses  = dao_p.getAll()

count_f = 0
for f in allflows:
  count_f = count_f + 1
  
count_p = 0
for p in allprocesses:
  count_p = count_p + 1


print("Die Anzahl der Flows ist: " + str(count_f))
print("Die Anzahl der Prozesse ist: " + str(count_p))
