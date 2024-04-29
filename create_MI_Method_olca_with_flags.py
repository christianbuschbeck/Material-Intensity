######################################
###        M O D U L E S           ###
######################################
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

######################################
###      D I R E C T O R Y         ###
######################################

# The directory where openLCA stores its databases needs to be found,
# because the LCIA-Method is later stored in that directory

def search_folder(start_path, target_folder):
    for root, dirs, files in os.walk(start_path):
        if target_folder in dirs:
            return os.path.join(root, target_folder)

    return None

start_path = '/Users'
target_folder = 'openLCA-data-1.4'

mainpath = search_folder(start_path, target_folder)

############################
### F U N C T I O N S    ###
############################

# The function CF_generate will later fill in characterization factors into .json-files

def CF_generate(mli,Val,dnames,duuid,dunit,dcatpath):
    # create dictionary with characterisation factor

        U_Mass = {
            "@type": "Unit",
            "@id": "20aadc24-a391-41cf-b340-3e4529f44bde",
            "name": "kg"}

        U_Energy = {
            "@type": "Unit",
            "@id": "52765a6c-3896-43c2-b2f4-c679acf13efe",
            "name": "MJ"}

        U_Volume = {
            "@type": "Unit",
            "@id": "1c3a9695-398d-4b1f-b07e-a8715b610f70",
            "name": "m3"}

        FP_Mass = {
            "@type": "FlowProperty",
            "@id": "93a60a56-a3c8-11da-a746-0800200b9a66",
            "name": "Mass",
            "categoryPath": [
                "Technical flow properties"]}

        FP_Energy = {
            "@type": "FlowProperty",
            "@id": "f6811440-ee37-11de-8a39-0800200c9a66",
            "name": "Energy",
            "categoryPath": [
                "Technical flow properties"]}

        FP_Volumne = {
            "@type": "FlowProperty",
            "@id": "93a60a56-a3c8-22da-a746-0800200c9a66",
            "name": "Volume",
            "categoryPath": [
                "Technical flow properties"]}

        CF = {}
        CF["@type"] = "ImpactFactor"
        CF["value"] = Val[mli]
        CF["flow"]  = {"@type": "Flow", "@id": duuid[mli],
        "name": dnames[mli],
        "categoryPath": dcatpath,
        "flowType": "ELEMENTARY_FLOW",
        "refUnit": dunit[mli]}
        if dunit[mli] == 'kg':
            CF["unit"]  = U_Mass
            CF["flowProperty"]  = FP_Mass
        elif dunit[mli] == 'MJ':
            CF["unit"]  = U_Energy
            CF["flowProperty"]  = FP_Energy
        elif dunit[mli] == 'm3':
            CF["unit"]  = U_Volume
            CF["flowProperty"]  = FP_Volumne
        else:
            None

        return CF


def median_function(l):
  n_num = l
  n = len(n_num)
  n_num.sort()

  if n % 2 == 0:
    median1 = n_num[n//2]
    median2 = n_num[n//2 - 1]
    median = (median1 + median2)/2
  else:
    median = n_num[n//2]
    return median

######################################  A D D    R E L E V A N T    F L O W S ###############################################


#######################################
###  D B   C O N N E C T I O N      ###
#######################################

# The connection to the open Database is established
db = Database.get()
ei_version = db.name

# Dao objects are set. Those are used to iterate over the respective Model Type (e.g. Processes or Flows etc.)
dao_fp = FlowPropertyDao(db)
dao_c = CategoryDao(db)
dao_p = ProcessDao(db)
dao_f = FlowDao(db)
dao_u = UnitGroupDao(db)

######################################################################
###  N E C E S S A R Y    U N I T S    A N D   P R O P E R T I E S ###
######################################################################

# Mass (in kg) is the only used unit in this LCIA-Method

units = dao_u.getAll()

for u in units:
  if u.name =="Units of mass":
    for uu in u.units:
    	if uu.name== "kg":
          kg = uu

properties = dao_fp.getAll()

for prop in properties:
  if prop.name =="Mass":
    mass = prop


#######################################
###  N E C E S S A R Y    F L O W S ###
#######################################

# All necessary flows are set. Gangue already exists and serves as template for
# the flows that are introduced.

allflows = dao_f.getAll()
allflow_names = []

for f in allflows:
  allflow_names.append(f.name)
  if "Gangue" in f.name and "bauxite" not in f.name:
      gangue_name = f.name

gangue = dao_f.getForName(gangue_name)[0]

for f in allflows:
  if "Actinium" in f.name and f.category.name == "unspecified":
    actinium = f


if "Overburden" in allflow_names:
  overburden = dao_f.getForName("Overburden")[0]
else:
  overburden = gangue.copy()
  overburden.name = 'Overburden'
  overburden.refId = "8711a380-e9dc-4bbf-be2b-91d243a8e39d"
  dao_f.insert(overburden)


if "Soil, moved" in allflow_names:
  soilmoved = dao_f.getForName("Soil, moved")[0]
else:
  soilmoved = gangue.copy()
  soilmoved.name = 'Soil, moved'
  soilmoved.refId = "676ab17e-7679-42b1-8095-76fe2340e14b"
  dao_f.insert(soilmoved)

if "Soil, compacted" in allflow_names:
  soilcompacted = dao_f.getForName("Soil, compacted")[0]
else:
  soilcompacted = gangue.copy()
  soilcompacted.name = 'Soil, compacted'
  soilcompacted.refId = "1755461b-ad4c-4a02-a7b0-67efe5bc053f"
  dao_f.insert(soilcompacted)

if "Soil, erodet" in allflow_names:
  soilerodet = dao_f.getForName("Soil, erodet")[0]
else:
  soilerodet = gangue.copy()
  soilerodet.name = 'Soil, erodet'
  soilerodet.refId = "5cb88e58-b1e2-4b10-a055-c9870eb375e7"
  dao_f.insert(soilerodet)


if "flag missing overburden" in allflow_names:
  missingoverburden_flow = dao_f.getForName("flag missing overburden")[0]
else:
  missingoverburden_flow = actinium.copy()
  missingoverburden_flow.name = 'flag missing overburden'
  missingoverburden_flow.refId = "1994dbda-47ff-4dba-9f5b-f28f84b15b30"
  dao_f.insert(missingoverburden_flow)

if "flag missing gangue" in allflow_names:
  missinggangue_flow = dao_f.getForName("flag missing gangue")[0]
else:
  missinggangue_flow = actinium.copy()
  missinggangue_flow.name = 'flag missing gangue'
  missinggangue_flow.refId = "8f27c4a2-a8d2-45e5-b15b-d2af5ef0447e"
  dao_f.insert(missinggangue_flow)

allflows = dao_f.getAll()

##############################
###  O V E R B U R D E N   ###
##############################

# In ecoinvent, Overburden that is not refilled, is recorded in waste flows.
# However, it is not possible to assign a characterization factor to waste flows.
# Therefore, the elementary flow "Overburden" is added to each process containing such a waste flow,
# with the amount being the same as in the respective waste flow.

allprocesses = dao_p.getAll()

overburden_processes   = []
overburden_amounts     = []

overburden_waste_flows = ["non-sulfidic overburden, off-site",
                      "spoil from hard coal mining",
                      "spoil from lignite mining"]

for p in allprocesses:
  isin_ov_elem  = False
  isin_ov_waste = False

  amount = 0

  if "market" not in p.name and "treatment" not in p.name:

    for ex in p.exchanges:
      if ex.flow.name =="Overburden":
        isin_ov_elem = True


      if ex.flow.name in overburden_waste_flows:
        amount = amount + ex.amount
        ex_ov = model.Exchange()
        ex_ov.isInput = True
        ex_ov.flow = overburden
        ex_ov.amount = amount
        ex_ov.unit = kg
        ex_ov.flowPropertyFactor = gangue.getReferenceFactor()

        isin_ov_waste = True

    if isin_ov_elem == True:
      for ex in p.exchanges:
        if ex.flow.name == "Overburden":
          ex.amount = amount
          dao_p.update(p)

    if isin_ov_waste == True:

      if isin_ov_elem == False:
        p.exchanges.add(ex_ov)
        dao_p.update(p)


# Besides the data from ecoinvent, the global mining database was used to update ecoinvent processes.
# In the following list the coressponding overburden values are stored.


overburden_list = [

  #Bauxite
  ["bauxite mine operation | bauxite","Global", 0.5018],    #aus ecoinvent documentation 1m dicke * 2.6 g/cm^2

  # Iron
  ["iron ore mine operation and beneficiation | iron ore concentrate","Canada, Qu",0.255],
  ["iron ore mine operation, 46% Fe | iron ore, crude ore, 46% Fe","Global",63/46 * 0.255],

  #Copper
  ["gold mine operation and refining | copper, cathode","Sweden",687],
  ["platinum group metal mine operation, ore with high palladium content | copper, cathode","Russian Federation", 687],
  ["copper production, cathode, solvent extraction and electrowinning process | copper, cathode","Global",687],
  #Copper concentrate
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Australia",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Canada",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Chile",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","China",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Indonesia",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Kazakhstan",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Rest-of-World",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Russian Federation",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","United States",150.2],
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Zambia",150.2],

  ["gold-silver mine operation and beneficiation | copper concentrate, sulfide ore","Canada, Qu",150.2],
  ["molybdenite mine operation | copper concentrate, sulfide ore","Global",150.2],

  #Gold
  ["silver-gold mine operation with refinery | gold","Chile",1977787.9],
  ["silver-gold mine operation with refinery | gold","Rest-of-World",1977787.9],

  ["gold mine operation and gold production, unrefined | gold, unrefined","South Africa",1977787.9],
  ["gold mine operation and gold production, unrefined | gold, unrefined","Rest-of-World",1977787.9],
  ["gold mine operation and refining | gold","Sweden",1977787.9],
  ["gold-silver mine operation with refinery | gold","Papua New Guinea",1977787.9],
  ["gold-silver mine operation with refinery | gold","Canada, Qu",1977787.9],
  ["gold-silver mine operation with refinery | gold","Rest-of-World",1977787.9],
  ["gold-silver mine operation with refinery | gold","Rest-of-World",1977787.9],
  ["gold production | gold","Canada",1977787.9],
  ["gold production | gold","Australia",1977787.9],
  ["gold production | gold","Tanzania, United Republic of",1977787.9],
  ["gold production | gold","United States",1977787.9],

  #Silver
  ["gold mine operation and refining | silver","Sweden",56132.2],

  ["gold-silver mine operation with refinery | silver","Papua New Guinea",56132.2],
  ["gold-silver mine operation with refinery | silver","Canada, Qu",56132.2],
  ["gold-silver mine operation with refinery | silver","Rest-of-World",56132.2],

  ["silver-gold mine operation with refinery | silver","Chile",56132.2],
  ["silver-gold mine operation with refinery | silver","Rest-of-World",56132.2],

  ["silver mine operation with extraction | silver, unrefined","Peru",56132.2],

  #Lead
  ["gold mine operation and refining | lead","Sweden",10.7],
  #Lead concentrate
  ["gold-silver mine operation and beneficiation | lead concentrate","Canada, Qu",5.7],
  ["silver mine operation with extraction | lead concentrate","Peru",5.7],

  #Molybdenite
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Canada",2211.3],
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Chile",2211.3],
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","China",2211.3],
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Rest-of-World",2211.3],
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Russian Federation",2211.3],
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","United States",2211.3],

  ["molybdenite mine operation | molybdenite","Global",2211.3],

  #Zinc
  ["gold mine operation and refining | zinc","Sweden",43.7],
  #Zinc concentrate
  ["gold-silver mine operation and beneficiation | zinc concentrate","Canada, Qu",22.8],
  ["silver mine operation with extraction | zinc concentrate","Peru",22.8]]

# The overburden list is iterated and the overburden values are inserted in the respective processes


for mp in overburden_list:
  mining_process_name_raw = mp[0]
  location                = mp[1]
  overburden_amount       = mp[2]

  for ap in allprocesses:
    if mining_process_name_raw in ap.name:
      if "market" not in ap.name:
        mining_processes_name_explicit = ap.name

  mining_processes = dao_p.getForName(mining_processes_name_explicit)
  foundsomething = False
  for p in mining_processes:

    if location in p.location.name:
      foundsomething = True
      isin = False

      for ex in p.exchanges:
        if ex.isInput == True:
          if ex.flow.name == overburden.name:
            ex.amount = overburden_amount
            #ex.unit = kg
            dao_p.update(p)
            isin = True
            break

      if isin == False:

        ex = model.Exchange()
        ex.isInput = True
        ex.flow = overburden
        ex.amount = overburden_amount
        ex.unit = kg
        ex.flowPropertyFactor = gangue.getReferenceFactor()
        p.exchanges.add(ex)
        dao_p.update(p)

  if foundsomething == False:
    print("nothing found for:" + mining_processes_name_explicit + location)


"""
# In order to estimate the robustness of abiotic material, the mining processes that lack overburden get the elementary flow
# "missing_overburden" with an amount of 1.


############################
###     G A N G U E      ###
############################

# Gangue is also not given in every mining process.
# Therefore corresponding values for gangue are retrived from the global mining database and ecoinvent.

allprocesses = dao_p.getAll()


gangue_list = [


  # Copper
  ["platinum group metal mine operation, ore with high palladium content | copper, cathode |", 108.6],
  ["gold mine operation and refining | copper, cathode |",108.6],
  ["copper production, cathode, solvent extraction and electrowinning process | copper, cathode",108.6],
  # Copper concentrate
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore",28.9],
  ["molybdenite mine operation | copper concentrate, sulfide ore |",28.9],
  ["gold-silver mine operation and beneficiation | copper concentrate, sulfide ore ",28.9],


  # Gold
  ["gold mine operation and gold production, unrefined | gold, unrefined",597162.6],
  ["gold mine operation and refining | gold |",597162.6],
  ["gold production | gold |",597162.6],
  ["silver-gold mine operation with refinery | gold |",597162.6],
  ["gold-silver mine operation with refinery | gold |",597162.6],

  # Molybdenum
  ["molybdenite mine operation | molybdenite |",439.7],

  # Iron
  ["iron ore mine operation and beneficiation | iron ore concentrate |",0.4995],       # aus ecoinvent
  ["iron ore mine operation, 46% Fe | iron ore, crude ore, 46% Fe |",63/46 * 0.4995],  # aus ecoinvent

  # Silver
  ["gold-silver mine operation with refinery | silver |",3709.3],
  ["gold mine operation and refining | silver |",3709.3],
  ["silver-gold mine operation with refinery | silver |",3709.3],

  # Lead
  ["gold mine operation and refining | lead",5.4],
  # Lead concentrate
  ["gold-silver mine operation and beneficiation | lead concentrate",3.3],

  # Zinc
  ["gold mine operation and refining | zinc",7.6],
  # Zinc concentrate
  ["gold-silver mine operation and beneficiation | zinc concentrate",4.4],

  # Nickel
  ["platinum group metal mine operation, ore with high palladium content | nickel, class",57.9],

  # Palladium
  ["platinum group metal mine operation, ore with high palladium content | palladium",290685.2],

  # Platinum
  ["platinum group metal mine operation, ore with high palladium content | platinum",222769.7],

  # Rhodium
  ["platinum group metal mine operation, ore with high palladium content | rhodium",864627.3],

  # Uranium
  ["uranium production, in yellowcake, in-situ leaching | uranium, in yellowcake",2134.3]]


# Mining processes are updated with the respective value

for mp in gangue_list:
  mining_process_name_raw = mp[0]
  gangue_amount    = mp[1]

  for ap in allprocesses:
    if mining_process_name_raw in ap.name:
      if "market" not in ap.name:
        mining_processes_name_explicit = ap.name

  mining_processes = dao_p.getForName(mining_processes_name_explicit)

  for p in mining_processes:

    isin = False

    for ex in p.exchanges:
      if ex.isInput == True:
        if ex.flow.name == gangue.name:
          ex.amount = gangue_amount
          #ex.unit = kg
          dao_p.update(p)
          isin = True
          break

    if isin == False:

      ex = model.Exchange()
      ex.isInput = True
      ex.flow = gangue
      ex.amount = gangue_amount
      ex.unit = kg
      ex.flowPropertyFactor = gangue.getReferenceFactor()
      p.exchanges.add(ex)
      dao_p.update(p)



####################################
### M I S S I N G   F L O W S    ###
####################################


allprocesses = dao_p.getAll()
allcategories =dao_c.getAll()

mining_cat_parents = []

for c in allcategories:
  if c.name in ["05:Mining of coal and lignite","07:Mining of metal ores","08:Other mining and quarrying"]:
    mining_cat_parents.append(c)

children_list_glob=[]

def children(parent):
  children_list = []
  if len(parent.childCategories)>0:
    for child in parent.childCategories:
      children_list.append(child)
      children_list_glob.append(child.name)
  return children_list

for par in mining_cat_parents:
  childs = children(par)
  for gc in childs:
    children(gc)


missing_overburden = []
missing_gangue     = []
missing_outside = []


for p in allprocesses:
  if p.category.name in children_list_glob:

    isin_ig = False
    isin_ga = False
    isin_ov = False

    for ex in p.exchanges:
      if "in ground" in ex.flow.category.name:
        isin_ig = True
      if ex.flow.name == gangue_name:
        isin_ga = True
      if ex.flow.name == "Overburden":
        isin_ov = True

    if isin_ig == True:
      if isin_ga ==False:
        if "07" in p.category.name:
          missing_gangue.append(p.name)

          ex = model.Exchange()
          ex.isInput = True
          ex.flow = missinggangue_flow
          ex.amount = 1
          ex.unit = kg
          ex.flowPropertyFactor = gangue.getReferenceFactor()
          p.exchanges.add(ex)
          dao_p.update(p)

      if isin_ov ==False:
        if "05" in p.category.name or "07" in p.category.name:
          missing_overburden.append(p.name)

          ex = model.Exchange()
          ex.isInput = True
          ex.flow = missingoverburden_flow
          ex.amount = 1
          ex.unit = kg
          ex.flowPropertyFactor = gangue.getReferenceFactor()
          p.exchanges.add(ex)
          dao_p.update(p)

  if p.category.name not in children_list_glob:

    isin_ig = False
    isin_ga = False
    isin_ov = False

    for ex in p.exchanges:
      if "in ground" in ex.flow.category.name:
        if "Soil," not in ex.flow.name:
          isin_ig = True
      if ex.flow.name == gangue_name:
        isin_ga = True
      if ex.flow.name == "Overburden":
        isin_ov = True

    if isin_ig == True:
      if isin_ga == False or isin_ov == False:
        missing_outside.append(p.name)


missing_overburden = list(set(missing_overburden))
missing_gangue = list(set(missing_gangue))
missing_outside = list(set(missing_outside))



############################
### T I L L A G E        ###
############################

# The elementary flow soil moved should be added to all processes that tillage the soil.
# How much soil is moved is calculated according to the following formulas

density_soil = 1.4 /1000 * 100**3 # g/cm^3 -> kg/m^3

moved_soil_harrowing = 0.15 * 10000 * density_soil
moved_soil_ploughing = 0.2 * 10000 * density_soil
moved_soil_subsoil   = 0.3 * 10000 * density_soil


tillage_list = [

  # harrowing
  ["tillage, harrowing, ",moved_soil_harrowing],

  # ploughing
  ["tillage, ploughing | tillage, ploughing |",moved_soil_ploughing],

  # subsoiling
  ["tillage, subsoiling, by subsoiler plow | tillage, subsoiling, by subsoiler plow |",moved_soil_subsoil]]


for tp in tillage_list:

  tillage_process_name_raw = tp[0]
  soilmoved_amount = tp[1]

  for ap in allprocesses:
    if tillage_process_name_raw in ap.name:
      if "market" not in ap.name:
        tillage_processes_name_explicit = ap.name

  tillage_processes = dao_p.getForName(tillage_processes_name_explicit)

  for p in tillage_processes:

    isin = False

    for ex in p.exchanges:
      if ex.isInput == True:
        if ex.flow.name == soilmoved.name:
          ex.amount = soilmoved_amount
          dao_p.update(p)
          isin = True
          break

    if isin == False:

      ex = model.Exchange()
      ex.isInput = True
      ex.flow = soilmoved
      ex.amount = soilmoved_amount
      ex.unit = kg
      ex.flowPropertyFactor = soilmoved.getReferenceFactor()
      p.exchanges.add(ex)
      dao_p.update(p)


############################
### C O M P A C T I N G  ###
############################

# The elementary flow soil compacted should be added to all processes where soil is compacted.
# How much soil is compacted is calculated according to the following formulas



# For agriculture

def compacting(MW):
  TW = 0.7
  CPA = 0.2 * 2 * TW
  L = 10000 / MW
  res = L*CPA*density_soil
  return res

# For forestry
road_area_factor = 367

compacting_list = [
  # Plant protection
  ["application of plant protection product, by field sprayer | application of plant protection product, by field sprayer",compacting(7)],

  # Harvesting
  ["combine harvesting | combine harvesting |",compacting(7)],
  ["harvesting, by complete harvester, beets | harvesting, by complete harvester, beets |",compacting(7)],
  ["harvesting, by complete harvester, ground crops | harvesting, by complete harvester, ground crops |",compacting(7)],
  ["harvesting, sugarcane | harvesting, sugarcane |",compacting(7)],
  ["chopping, maize | chopping, maize |",compacting(7)],

  # Fertilizing
  ["fertilising, by broadcaster | fertilising, by broadcaster |",compacting(5)],
  ["fertilising, by rig fertiliser, sugarcane | fertilising, by rig fertiliser, sugarcane |",compacting(10)],

  # Haying
  ["haying, by rotary tedder | haying, by rotary tedder |",compacting(5)],

  # Hoeing
  ["hoeing | hoeing |",compacting(3)],

  # Mowing
  ["mowing, by motor mower | mowing, by motor mower |",compacting(7)],

  # Mulching
  ["mulching | mulching |",compacting(2.5)],

  # Planting
  ["planting | planting |",compacting(2.5)],
  ["planting, potato | potato planting |",compacting(3.7)],
  ["planting, sugarcane | planting, sugarcane |",compacting(3.7)],

  # Cutting
  ["potato haulm cutting | potato haulm cutting |",compacting(2)],

  # Sowing
  ["sowing | sowing |",compacting(3)],

  # Swath
  ["swath, by rotary windrower | swath, by rotary windrower |",compacting(4)],

  # Cultivating
  ["tillage, cultivating, chiselling | tillage, cultivating, chiselling |",compacting(3.4)],

  # Currying
  ["tillage, currying, by weeder | tillage, currying, by weeder |",compacting(6)],

  #Harrowing
  ["tillage, harrowing, by offset disk harrow | tillage, harrowing, by offset disk harrow |",compacting(7)],
  ["tillage, harrowing, by offset leveling disc harrow | tillage, harrowing, by offset leveling disc harrow |",compacting(7)],
  ["tillage, harrowing, by rotary harrow | tillage, harrowing, by rotary harrow |",compacting(7)],
  ["tillage, hoeing and earthing-up, potatoes | tillage, hoeing and earthing-up, potatoes",compacting(7)],
  ["tillage, harrowing, by spring tine harrow | tillage, harrowing, by spring tine harrow |",compacting(3)],

  # Ploughing
  ["tillage, ploughing | tillage, ploughing |",compacting(7)],

  # Rolling
  ["tillage, rolling | tillage, rolling |",compacting(3)],

  # Rotary cultivator
  ["tillage, rotary cultivator | tillage, rotary cultivator |",compacting(7.8)]]

for p in allprocesses:
  if p.category.name == "0220:Logging":
    for ex in p.exchanges:
      if ex.isInput == True:
        if ex.flow.name == "Transformation, from traffic area, rail/road embankment":

          compacting_list.append([p.name,ex.amount * road_area_factor])


for cp in compacting_list:

  compacting_process_name_raw = cp[0]
  soilcompacted_amount = cp[1]

  for ap in allprocesses:
    if compacting_process_name_raw in ap.name:
      if "market" not in ap.name:
        compacting_processes_name_explicit = ap.name

  compacting_processes = dao_p.getForName(compacting_processes_name_explicit)

  for p in compacting_processes:

    isin = False

    for ex in p.exchanges:
      if ex.isInput == True:
        if ex.flow.name == soilcompacted.name:
          ex.amount = soilcompacted_amount
          ex.unit = kg
          dao_p.update(p)
          isin = True
          break

    if isin == False:

      ex = model.Exchange()
      ex.isInput = True
      ex.flow = soilcompacted
      ex.amount = soilcompacted_amount
      ex.unit = kg
      ex.flowPropertyFactor = soilcompacted.getReferenceFactor()
      p.exchanges.add(ex)
      dao_p.update(p)


########################################## C R E A T E    L C I A    M E T H O D  #############################


######################################
### E M P T Y   M E T H O D        ###
######################################

# The directories for the .json files of the LCIA Method are created

path_lcia_categories = mainpath + "/Material Intensity/Material Intensity METHOD_"+ei_version +"/lcia_categories"
path_lcia_methods = mainpath + "/Material Intensity/Material Intensity METHOD_"+ei_version +"/lcia_methods"

if not os.path.exists(path_lcia_categories):
    os.makedirs(path_lcia_categories)
if not os.path.exists(path_lcia_methods):
    os.makedirs(path_lcia_methods)


# Dictionaries for the .json files are created (here in python)

method={"@type":"ImpactMethod","category":"Material Intensity","@id":"56c9a436-2c1d-4ead-87d5-17ac168b0191","name":"Material Intensity","version":"1.0",
"impactCategories":[
{"@type":"ImpactCategory","category":"Material Intensity","@id":"90768cd8-9b26-11ee-b9d1-0242ac120002","name":"Abiotic RMI","refUnit":"kg"},
{"@type":"ImpactCategory","category":"Material Intensity","@id":"0e79d9c7-8add-4a23-a25e-1f55a4e82d2d","name":"Abiotic TMR","refUnit":"kg"},
{"@type":"ImpactCategory","category":"Material Intensity","@id":"90768f6c-9b26-11ee-b9d1-0242ac120002","name":"Biotic RMI","refUnit":"kg"},
{"@type":"ImpactCategory","category":"Material Intensity","@id":"3d2d5656-2e3e-4453-8003-c3d485356045","name":"Biotic TMR","refUnit":"kg"},
{"@type":"ImpactCategory","category":"Material Intensity","@id":"8f07827d-72c0-4ae7-a708-a18e684b3f54","name":"Water","refUnit":"kg"},
{"@type":"ImpactCategory","category":"Material Intensity","@id":"68be8652-dba7-443e-b439-c57d058f0388","name":"Moved Soil","refUnit":"kg"},
{"@type":"ImpactCategory","category":"Material Intensity","@id":"cd9a353b-371c-4aa9-bdec-89e98891d5dd","name":"ESTIMATE MISSING ABIOTIC","refUnit":"kg"}]}


empty_abiotic_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "90768cd8-9b26-11ee-b9d1-0242ac120002",
    "name": "Abiotic RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "90768cd8-9b26-11ee-b9d1-0242ac120002.json"}

empty_abiotic_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "0e79d9c7-8add-4a23-a25e-1f55a4e82d2d",
    "name": "Abiotic TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "0e79d9c7-8add-4a23-a25e-1f55a4e82d2d.json"}


empty_biotic_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "90768f6c-9b26-11ee-b9d1-0242ac120002",
    "name": "Biotic RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "90768f6c-9b26-11ee-b9d1-0242ac120002.json"}


empty_biotic_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "3d2d5656-2e3e-4453-8003-c3d485356045",
    "name": "Biotic TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "3d2d5656-2e3e-4453-8003-c3d485356045.json"}


empty_water  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "8f07827d-72c0-4ae7-a708-a18e684b3f54",
    "name": "Water",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "8f07827d-72c0-4ae7-a708-a18e684b3f54.json"}

empty_soil  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "68be8652-dba7-443e-b439-c57d058f0388",
    "name": "Moved Soil",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "68be8652-dba7-443e-b439-c57d058f0388.json"}


empty_missing  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "cd9a353b-371c-4aa9-bdec-89e98891d5dd",
    "name": "ESTIMATE MISSING ABIOTIC",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "cd9a353b-371c-4aa9-bdec-89e98891d5dd.json"}

# These dictionaries are saved as .json-files

with open(path_lcia_methods + '/56c9a436-2c1d-4ead-87d5-17ac168b0191.json', 'w') as fp:
  json.dump(method, fp)

with open(path_lcia_categories +'/90768cd8-9b26-11ee-b9d1-0242ac120002.json', 'w') as fp:
  json.dump(empty_abiotic_rmi, fp)
with open(path_lcia_categories +'/0e79d9c7-8add-4a23-a25e-1f55a4e82d2d.json', 'w') as fp:
  json.dump(empty_abiotic_tmr, fp)

with open(path_lcia_categories +'/90768f6c-9b26-11ee-b9d1-0242ac120002.json', 'w') as fp:
  json.dump(empty_biotic_rmi, fp)
with open(path_lcia_categories +'/3d2d5656-2e3e-4453-8003-c3d485356045.json', 'w') as fp:
  json.dump(empty_biotic_tmr, fp)

with open(path_lcia_categories +'/8f07827d-72c0-4ae7-a708-a18e684b3f54.json', 'w') as fp:
  json.dump(empty_water, fp)
with open(path_lcia_categories +'/68be8652-dba7-443e-b439-c57d058f0388.json', 'w') as fp:
  json.dump(empty_soil, fp)

with open(path_lcia_categories +'/cd9a353b-371c-4aa9-bdec-89e98891d5dd.json', 'w') as fp:
  json.dump(empty_missing, fp)

###############################################################################
### C R E A T E    L I S T S    F O R    I M P A C T   C A T E G O R I E S  ###
###############################################################################

###### A B I O T I C   R E S O U R C E S

# Lists for abiotic resources are filled with the respective elementary flows.

abiotic_rmi_uuid =[]
abiotic_rmi_names =[]
abiotic_rmi_catpath = []

abiotic_tmr_uuid =[]
abiotic_tmr_names =[]
abiotic_tmr_catpath = []


for f in allflows:
    if f.category != None:
        if f.category.name == "in ground":
          if f.referenceFlowProperty.name =="Mass":
            if "Soil," not in f.name:
              if "Overburden" not in f.name:
                abiotic_rmi_names.append(f.name)
                abiotic_rmi_uuid.append(f.refId)
                abiotic_rmi_catpath.append(f.category.name)
              abiotic_tmr_names.append(f.name)
              abiotic_tmr_uuid.append(f.refId)
              abiotic_tmr_catpath.append(f.category.name)


abiotic_rmi_units  = ["kg"]*len(abiotic_rmi_uuid)
abiotic_rmi_values = [1]*len(abiotic_rmi_uuid)

abiotic_tmr_units  = ["kg"]*len(abiotic_tmr_uuid)
abiotic_tmr_values = [1]*len(abiotic_tmr_uuid)

abiotic_rmi_dict ={
    "names": abiotic_rmi_names,
    "uuids": abiotic_rmi_uuid,
    "units": abiotic_rmi_units,
    "values":abiotic_rmi_values,
    "catpath":abiotic_rmi_catpath}

abiotic_tmr_dict ={
    "names": abiotic_tmr_names,
    "uuids": abiotic_tmr_uuid,
    "units": abiotic_tmr_units,
    "values":abiotic_tmr_values,
    "catpath":abiotic_tmr_catpath}


######  B I O T I C   R E S O U R C E S

# Lists for biotic resources are filled with the respective elementary flows.


biotic_rmi_uuid    = []
biotic_rmi_names   = []
biotic_rmi_catpath = []
biotic_rmi_units   = []
biotic_rmi_values  = []

biotic_tmr_uuid    = []
biotic_tmr_names   = []
biotic_tmr_catpath = []
biotic_tmr_units   = []
biotic_tmr_values  = []

for f in allflows:
  if "Fish," in f.name:
	biotic_rmi_uuid.append(f.refId)
	biotic_rmi_names.append(f.name)
	biotic_rmi_catpath.append(f.category.name)
	biotic_rmi_units.append("kg")
	biotic_rmi_values.append(1)

  	biotic_tmr_uuid.append(f.refId)
	biotic_tmr_names.append(f.name)
	biotic_tmr_catpath.append(f.category.name)
	biotic_tmr_units.append("kg")
	biotic_tmr_values.append(1)


for f in allflows:
  if "Energy, gross calorific value, in biomass" in f.name:
    if "correction" not in f.name:
	  biotic_rmi_uuid.append(f.refId)
	  biotic_rmi_names.append(f.name)
	  biotic_rmi_catpath.append(f.category.name)
	  biotic_rmi_units.append("MJ")
	  biotic_rmi_values.append(0.1025/2)

	  biotic_tmr_uuid.append(f.refId)
	  biotic_tmr_names.append(f.name)
	  biotic_tmr_catpath.append(f.category.name)
	  biotic_tmr_units.append("MJ")
	  biotic_tmr_values.append(0.1025)


biotic_rmi_dict ={
    "names": biotic_rmi_names,
    "uuids": biotic_rmi_uuid,
    "units": biotic_rmi_units,
    "values":biotic_rmi_values,
    "catpath":biotic_rmi_catpath}



biotic_tmr_dict ={
    "names": biotic_tmr_names,
    "uuids": biotic_tmr_uuid,
    "units": biotic_tmr_units,
    "values":biotic_tmr_values,
    "catpath":biotic_tmr_catpath}


######  W A T E R

# Lists for water are filled with the respective elementary flows.

water_names = []
water_uuid =[]
water_catpath = []
water_units = []
water_values = []

for f in allflows:
    if f.category != None:
        if f.category.name == "in water" or f.category.name == "in ground" :
            if "Water," in f.name and "turbine" not in f.name and "salt" not in f.name:
                water_uuid.append(f.refId)
                water_names.append(f.name)
                water_catpath.append(f.category.name)
                if f.referenceFlowProperty.name == "Mass":
                  water_units.append("kg")
                  water_values.append(1)
                if f.referenceFlowProperty.name == "Volume":
                  water_units.append("m3")
                  water_values.append(1000)


water_dict ={
    "names": water_names,
    "uuids": water_uuid,
    "units": water_units,
    "values":water_values,
    "catpath":water_catpath}



######  M O V E D    S O I L

# Lists for Soil are filled with the respective elementary flows.



movedsoil_uuid =[]
movedsoil_names =[]
movedsoil_catpath = []
movedsoil_units = []
movedsoil_values = []

for f in allflows:
  if f.name == "Soil, moved" or f.name == "Soil, compacted" or f.name == "Soil, erodet":
    if f.category != None:
      if f.category.name == "in ground":
        movedsoil_uuid.append(f.refId)
        movedsoil_names.append(f.name)
        movedsoil_catpath.append(f.category.name)
        movedsoil_units.append("kg")
        movedsoil_values.append(1)


movedsoil_dict ={
    "names": movedsoil_names,
    "uuids": movedsoil_uuid,
    "units": movedsoil_units,
    "values":movedsoil_values,
    "catpath":movedsoil_catpath}



######  E S T I M A T E   M I S S I N G    A B I O T I C

# Lists for Soil are filled with the respective elementary flows.

overburden_amounts = []
gangue_amounts = []

for p in allprocesses:
  if "07" in p.category.name:
    for ex in p.exchanges:
      if ex.flow.name == gangue.name:
        gangue_amounts.append(ex.amount)

  if "07" in p.category.name or "05" in p.category.name:
    if ex.flow.name == overburden.name:
      overburden_amounts.append(ex.amount)




missing_uuid =[]
missing_names =[]
missing_catpath = []
missing_units = []
missing_values = []

for f in allflows:
  if f.name == "flag missing overburden":
    missing_uuid.append(f.refId)
    missing_names.append(f.name)
    missing_catpath.append(f.category.name)
    missing_units.append("kg")
    missing_values.append(median_function(overburden_amounts))

  if f.name == "flag missing gangue":
    missing_uuid.append(f.refId)
    missing_names.append(f.name)
    missing_catpath.append(f.category.name)
    missing_units.append("kg")
    missing_values.append(median_function(gangue_amounts))


missing_dict ={
    "names": missing_names,
    "uuids": missing_uuid,
    "units": missing_units,
    "values":missing_values,
    "catpath":missing_catpath}


##################################################
### P O P U L A T E     .J S O N - F I L E S #####
##################################################


MI_uuid = ["90768cd8-9b26-11ee-b9d1-0242ac120002.json",
             "0e79d9c7-8add-4a23-a25e-1f55a4e82d2d.json",
             "90768f6c-9b26-11ee-b9d1-0242ac120002.json",
             "3d2d5656-2e3e-4453-8003-c3d485356045.json",
             "8f07827d-72c0-4ae7-a708-a18e684b3f54.json",
             "68be8652-dba7-443e-b439-c57d058f0388.json",
             "cd9a353b-371c-4aa9-bdec-89e98891d5dd.json"]


MI_dict = {
    "Abiotic RMI": abiotic_rmi_dict,
  	"Abiotic TMR": abiotic_tmr_dict,
    "Biotic RMI": biotic_rmi_dict,
  	"Biotic TMR": biotic_tmr_dict,
    "Water": water_dict,
    "Moved Soil": movedsoil_dict,
	"ESTIMATE MISSING ABIOTIC": missing_dict}

cat_names = list(MI_dict.keys())


for i in range(0,7):
    f_in = os.path.join(path_lcia_categories,MI_uuid[i])
    with open(f_in, 'r+') as f:
        thisd = json.load(f)
        thisd['name'] = cat_names[i]
        thisd['id']   = MI_uuid[i]
        del thisd['impactFactors'][0:len(thisd['impactFactors'])] # delete the two factors that are still there from copying the files

        for mli in range(0,len(MI_dict[cat_names[i]]["values"])):

            CF = CF_generate(mli,
                             Val = MI_dict[cat_names[i]]["values"],
                             dnames = MI_dict[cat_names[i]]["names"],
                             duuid = MI_dict[cat_names[i]]["uuids"],
                             dcatpath = "",
                             dunit = MI_dict[cat_names[i]]["units"])


            # add new CF to json file:
            if CF["value"] > 0:
                thisd['impactFactors'].append(CF)
        # wrap up and save
        f.seek(0)        # reset file position to the beginning.
        json.dump(thisd, f, indent=4)
        f.truncate()     # remove remaining part
        f.close()



shutil.make_archive(mainpath + "/Material Intensity/Material Intensity METHOD_"+ei_version, 'zip', root_dir=mainpath + "/Material Intensity/Material Intensity METHOD_"+ ei_version)
method_dir = mainpath + "/Material Intensity/Material Intensity METHOD_" + ei_version + ".zip"
print("Material Intensity method created :)")

#####################
###  I M P O R T  ###
#####################

reader = ZipStore.open(File(method_dir))
i = JsonImport(reader,db)
i.run()
reader.close()



########################################################
###  P R O C E S S E S    M I S S I N G    F L O W S ###
########################################################

# For transparency and analytical reasons, csv files are created that contain information about
# process data sets where information regarding gangue and/or overburden is missing.

csv_filename = mainpath + "/Material Intensity/missing_overburden.csv"

# CSV-Datei zum Schreiben öffnen
with open(csv_filename, mode='w') as csv_file:
    # CSV-Writer erstellen
    csv_writer = csv.writer(csv_file)

    # Schreibe die Liste in die CSV-Datei
    csv_writer.writerow(["Processname"])  # Überschriftenzeile
    for name in missing_overburden:
        csv_writer.writerow([name])


csv_filename = mainpath + "/Material Intensity/missing_gangue.csv"

# CSV-Datei zum Schreiben öffnen
with open(csv_filename, mode='w') as csv_file:
    # CSV-Writer erstellen
    csv_writer = csv.writer(csv_file)

    # Schreibe die Liste in die CSV-Datei
    csv_writer.writerow(["Processname"])  # Überschriftenzeile
    for name in missing_gangue:
        csv_writer.writerow([name])


csv_filename = mainpath + "/Material Intensity/missing_outside.csv"

# CSV-Datei zum Schreiben öffnen
with open(csv_filename, mode='w') as csv_file:
    # CSV-Writer erstellen
    csv_writer = csv.writer(csv_file)

    # Schreibe die Liste in die CSV-Datei
    csv_writer.writerow(["Processname"])  # Überschriftenzeile
    for name in missing_outside:
        csv_writer.writerow([name])
