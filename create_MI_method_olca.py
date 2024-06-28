### M a t e r i a l   I n t e n s i t y   L C I A - M e t h o d ###
###################################################################

# This script creates a new LCIA-Method called Material Intensity. It is based on 
# the MIPS Methodology and the Material Footprint. Hence, it is an indput based
# indicator that describes the mass of material which is extracted in order to 
# produce a certain product or fulfill a certain service.

# The script has the following structure

# 1. Setup                  (line x to y)
# 2. Add relevant flows     (line x to y)
# 3. Create LCIA-Method     (line x to y)
# 4. Import                 (line x to y)
# 5. Document Missing Flows (line x to y)

# !! It is important to note, that for the LCIA-Method to work, the script has to be run
# at least once for each database. Exporting and Importing the LCIA-Method leads to wrong
# results because the required elementary flows are not created. !!

##################################### 1. SETUP #####################################


######################################
###        M O D U L E S           ###
######################################

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


###############################
###     A N A L Y S I S     ###
###############################

# This variable defines, whether two additional impact categories, which give additional insight
# into the databasis of the result, shall be given or not.

analysis = False 


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

start_path    = '/Users'
target_folder = 'openLCA-data-1.4'

mainpath      = search_folder(start_path, target_folder)

############################
### F U N C T I O N S    ###
############################

# The function CF_generate will later fill in characterization factors into .json-files, which make up the LCIA-Method

def CF_generate(mli, Val, dnames, duuid, dunit, dcatpath):
    # Create dictionary with characterisation factors
    
    # Define the unit for mass
    U_Mass = {
        "@type": "Unit",
        "@id": "20aadc24-a391-41cf-b340-3e4529f44bde",
        "name": "kg"
    }

    # Define the unit for energy
    U_Energy = {
        "@type": "Unit",
        "@id": "52765a6c-3896-43c2-b2f4-c679acf13efe",
        "name": "MJ"
    }

    # Define the unit for volume
    U_Volume = {
        "@type": "Unit",
        "@id": "1c3a9695-398d-4b1f-b07e-a8715b610f70",
        "name": "m3"
    }

    # Define the flow property for mass
    FP_Mass = {
        "@type": "FlowProperty",
        "@id": "93a60a56-a3c8-11da-a746-0800200b9a66",
        "name": "Mass",
        "categoryPath": ["Technical flow properties"]
    }

    # Define the flow property for energy
    FP_Energy = {
        "@type": "FlowProperty",
        "@id": "f6811440-ee37-11de-8a39-0800200c9a66",
        "name": "Energy",
        "categoryPath": ["Technical flow properties"]
    }

    # Define the flow property for volume
    FP_Volumne = {
        "@type": "FlowProperty",
        "@id": "93a60a56-a3c8-22da-a746-0800200c9a66",
        "name": "Volume",
        "categoryPath": ["Technical flow properties"]
    }

    # Initialize the characterization factor dictionary
    CF = {}
    CF["@type"] = "ImpactFactor"
    CF["value"] = Val[mli]
    
    # Set the flow details in the characterization factor dictionary
    CF["flow"] = {
        "@type": "Flow", 
        "@id": duuid[mli],
        "name": dnames[mli],
        "categoryPath": dcatpath,
        "flowType": "ELEMENTARY_FLOW",
        "refUnit": dunit[mli]
    }
    
    # Assign the appropriate unit and flow property based on the unit type
    if dunit[mli] == 'kg':
        CF["unit"] = U_Mass
        CF["flowProperty"] = FP_Mass
    elif dunit[mli] == 'MJ':
        CF["unit"] = U_Energy
        CF["flowProperty"] = FP_Energy
    elif dunit[mli] == 'm3':
        CF["unit"] = U_Volume
        CF["flowProperty"] = FP_Volumne
    else:
        None

    # Return the completed characterization factor dictionary
    return CF

  
# Becausse numpy is not available in the openLCA python console, basic functions as
# mean or median have to be defined

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

def mean_function(x):
  return sum(x)/len(x)
  

##################################### 2. Add relevant flows #####################################

# For the method to work, several flows have to added. On the one hand side
# elementary flows that are used in the impact assessment. On the other hand side
# flows that are present in some processes and missing in others.


#######################################
###  D B   C O N N E C T I O N      ###
#######################################

# The connection to the open Database is established
db         = Database.get()
ei_version = db.name

# Dao objects are set. Those are used to iterate over the respective Model Type (e.g. Processes or Flows etc.)
dao_fp = FlowPropertyDao(db)
dao_c  = CategoryDao(db)
dao_p  = ProcessDao(db)
dao_f  = FlowDao(db)
dao_u  = UnitGroupDao(db)
dao_m  = ImpactMethodDao(db)
dao_i = ImpactCategoryDao(db)

allflows      = dao_f.getAll()
allprocesses  = dao_p.getAll()
allmethods    = dao_m.getAll()
allimpcat     = dao_i.getAll()
allcategories = dao_c.getAll()

###################################################
###  D e l e t e  e x i s t i n g   M e t h o d  ##    ###
###################################################

for meth in allmethods:
  if meth.name == "Material Intensity":
    dao_m.delete(meth)

for ic in allimpcat:
  if ic.category.name == "Material Intensity":
    dao_i.delete(ic)

for c in allcategories:
  
  if c.name == "Material Intensity":
    dao_c.delete(c)

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
# flows that are introduced and should be stored in the category "in ground".

allflow_names = []

for f in allflows:
  allflow_names.append(f.name)
  if "Gangue" in f.name and "bauxite" not in f.name:
      gangue_name = f.name

gangue = dao_f.getForName(gangue_name)[0]
elem_flow_ground = gangue

# An arbitrary flow from the category "biotic" is extracted to serve as template for
# flows that are introduced and should be stored in the category "biotic resources"

for f in allflows:
  if "Anhydrite" in f.name and f.category.name == "biotic":
    elem_flow_biotic = f

# An arbitrary flow from the category "unspecified" is extracted to serve as template for
# flows that are introduced and should be stored in the category "unspecified"

for f in allflows:
  if "Actinium" in f.name and f.category.name == "unspecified":
    elem_flow_unspec = f


# The follwing elementary flows are created (if not already present)

# - Overbruden
# - Biomass, used
# - Biomass, unused
# - Soil, moved
# - Soil, compacted
# - Soil, erodet
# - flag missing overburden
# - flag missing gangue
# - flag external data

if "Overburden" in allflow_names:
  overburden = dao_f.getForName("Overburden")[0]
else:
  overburden       = elem_flow_ground.copy()
  overburden.name  = 'Overburden'
  overburden.refId = "8711a380-e9dc-4bbf-be2b-91d243a8e39d"
  dao_f.insert(overburden)

  
if "Biomass, used" in allflow_names:
  biomass_used = dao_f.getForName("Biomass, used")[0]
else:
  biomass_used       = elem_flow_biotic.copy()
  biomass_used.name  = 'Biomass, used'
  biomass_used.refId = "9442f771-1473-40d6-8dab-8ffbb94fec1d"
  dao_f.insert(biomass_used)

  
if "Biomass, unused" in allflow_names:
  biomass_unused = dao_f.getForName("Biomass, unused")[0]
else:
  biomass_unused       = elem_flow_biotic.copy()
  biomass_unused.name  = 'Biomass, unused'
  biomass_unused.refId = "bfb3e97d-cb6b-4f02-867c-e8908601a8f3"
  dao_f.insert(biomass_unused)


if "Soil, moved" in allflow_names:
  soilmoved = dao_f.getForName("Soil, moved")[0]
else:
  soilmoved       = elem_flow_ground.copy()
  soilmoved.name  = 'Soil, moved'
  soilmoved.refId = "676ab17e-7679-42b1-8095-76fe2340e14b"
  dao_f.insert(soilmoved)

if "Soil, compacted" in allflow_names:
  soilcompacted = dao_f.getForName("Soil, compacted")[0]
else:
  soilcompacted       = elem_flow_ground.copy()
  soilcompacted.name  = 'Soil, compacted'
  soilcompacted.refId = "1755461b-ad4c-4a02-a7b0-67efe5bc053f"
  dao_f.insert(soilcompacted)

if "Soil, erodet" in allflow_names:
  soilerodet = dao_f.getForName("Soil, erodet")[0]
else:
  soilerodet       = elem_flow_ground.copy()
  soilerodet.name  = 'Soil, erodet'
  soilerodet.refId = "5cb88e58-b1e2-4b10-a055-c9870eb375e7"
  dao_f.insert(soilerodet)

  
if "flag missing overburden" in allflow_names:
  missingoverburden_flow = dao_f.getForName("flag missing overburden")[0]
else:
  missingoverburden_flow       = elem_flow_unspec.copy()
  missingoverburden_flow.name  = 'flag missing overburden'
  missingoverburden_flow.refId = "1994dbda-47ff-4dba-9f5b-f28f84b15b30"
  dao_f.insert(missingoverburden_flow)

if "flag missing gangue" in allflow_names:
  missinggangue_flow = dao_f.getForName("flag missing gangue")[0]
else:
  missinggangue_flow       = elem_flow_unspec.copy()
  missinggangue_flow.name  = 'flag missing gangue'
  missinggangue_flow.refId = "8f27c4a2-a8d2-45e5-b15b-d2af5ef0447e"
  dao_f.insert(missinggangue_flow)

  
if "flag external data" in allflow_names:
  external_data_flow = dao_f.getForName("flag external data")[0]
else:
  external_data_flow       = elem_flow_unspec.copy()
  external_data_flow.name  = 'flag external data'
  external_data_flow.refId = "db01c0ff-a5ca-454a-8834-0595e7b59814"
  dao_f.insert(external_data_flow)

  
##############################
###  O V E R B U R D E N   ###
##############################

# In ecoinvent, Overburden that is not refilled, is partly recorded with 3 different waste flows.
# However, it is not possible to assign a characterization factor to waste flows.
# Therefore, the elementary flow "Overburden" is added to each process containing one or more of these waste flows.
# The amount of overburden is the sum of the amounts of these waste flows.

overburden_processes   = []
overburden_amounts     = []

# The waste flows indicating overburden are specified
overburden_waste_flows = ["non-sulfidic overburden, off-site",
                          "spoil from hard coal mining",
                          "spoil from lignite mining"]

for p in allprocesses:
  
  # The booleans isin_x indicate, whether the respective flow is present in a process
  isin_ov_elem  = False   # overburden as elementar flow
  isin_ov_waste = False   # overburden as waste flow

  amount = 0
  
  # Market and treatment activities that deal with waste flows are not mining activities
  # and should therefore not include overburden
  if "market" not in p.name and "treatment" not in p.name:

    for ex in p.exchanges:
      if ex.flow.name =="Overburden":
        isin_ov_elem = True

	  # If the process contains one of the waste flows, an exchange for overburden is created.
      # Because one process can have several overburden waste flows, their amounts is cumulated.
      if ex.flow.name in overburden_waste_flows:
        amount = amount + ex.amount 
        ex_ov = model.Exchange()
        ex_ov.isInput = True
        ex_ov.flow = overburden
        ex_ov.amount = amount
        ex_ov.unit = kg
        ex_ov.flowPropertyFactor = gangue.getReferenceFactor()

        isin_ov_waste = True

    # If the process contains overburden, its value is updated
    if isin_ov_elem == True:
      for ex in p.exchanges:
        if ex.flow.name == "Overburden":
          ex.amount = amount
          dao_p.update(p)
    
    # If the process contains an overburden waste flow, but no overburden elementary flow,
    # the exchange for the overburden elementary flow is added
    if isin_ov_waste == True:
      if isin_ov_elem == False:
        p.exchanges.add(ex_ov)
        dao_p.update(p)


        
# Because not every mining process contains information regarding overburden, 
# external data was used to fill these gaps. 
# In the following list the coressponding overburden values are stored.
# The sources given are given as comments.

overburden_list = [
  
  #Bauxite
  ["bauxite mine operation | bauxite","Global", 0.1904762],    # ecoinvent report 10 v.2.1: 1m cap thickness vs. 3 to 7m ore thickness  
  
  #Baryte
  ["barite production | barite","Rest-of-World", 0.09],            # ANDHRA PRADESH MINERAL DEVELOPMENT CORPORATION LIMITEDreport
  ["barite production | barite","Europe", 0.09],                   # ANDHRA PRADESH MINERAL DEVELOPMENT CORPORATION LIMITEDreport
  ["barite production | barite","Canada, Qu", 0.09],               # ANDHRA PRADESH MINERAL DEVELOPMENT CORPORATION LIMITEDreport
  
  # Iron
  ["iron ore mine operation, 63% Fe | iron ore, crude ore, 63% Fe","India",1.85],         # ecoinvent report v2.1 report No 10 dataset "Iron ore 46% Fe, at mine" remarks
  ["iron ore mine operation, 46% Fe | iron ore, crude ore, 46% Fe","Global", 1.85],       # ecoinvent report v2.1 report No 10 dataset "Iron ore 46% Fe, at mine" remarks
  ["iron ore mine operation and beneficiation | iron ore concentrate","Canada, Qu",1.85], # ecoinvent report v2.1 report No 10 dataset "Iron ore 46% Fe, at mine" remarks
	
  #Copper
  ["gold mine operation and refining | copper, cathode","Sweden",291],                                                  #global database, generic
  ["platinum group metal mine operation, ore with high palladium content | copper, cathode","Russian Federation", 291], #global database, generic
  ["copper production, cathode, solvent extraction and electrowinning process | copper, cathode","Global",291],         #global database, generic
  #Copper concentrate
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Australia",70],          #global database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Canada",69],             #global database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Chile",121],             #global database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","China",75],              #global database, generic
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Indonesia",109],         #global database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Kazakhstan",60],         #global database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Rest-of-World",75],      #global database, generic
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Russian Federation",75], #global database, generic
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","United States",309],     #global database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Zambia",264],            #global database, country specific
  
  ["gold-silver mine operation and beneficiation | copper concentrate, sulfide ore","Canada, Qu",69], #global database, country specific
  ["molybdenite mine operation | copper concentrate, sulfide ore","Global",75],                       #global database, generic

  #Gold
  ["silver-gold mine operation with refinery | gold","Chile",1912966],         #global database, generic
  ["silver-gold mine operation with refinery | gold","Rest-of-World",1912966], #global database, generic
 
  ["gold mine operation and gold production, unrefined | gold, unrefined","South Africa",1912966], #global database, generic
  ["gold mine operation and gold production, unrefined | gold, unrefined","Rest-of-World",1912966],#global database, generic
  ["gold mine operation and refining | gold","Sweden",1912966],                                    #global database, generic
  ["gold-silver mine operation with refinery | gold","Papua New Guinea",200793],                   #global database, country specific
  ["gold-silver mine operation with refinery | gold","Canada, Qu",253051],                         #global database, country specific
  ["gold-silver mine operation with refinery | gold","Rest-of-World",1912966],                     #global database, generic
  ["gold-silver mine operation with refinery | gold","Rest-of-World",1912966],                     #global database, generic
  ["gold production | gold","Canada",253051],                                                      #global database, country specific
  ["gold production | gold","Australia",1573988],                                                  #global database, country specific
  ["gold production | gold","Tanzania, United Republic of",1793320],                               #global database, country specific
  ["gold production | gold","United States",5093769],                                              #global database, country specific

  #Silver
  ["gold mine operation and refining | silver","Sweden",30970],                  #global database, generic
  
  ["gold-silver mine operation with refinery | silver","Papua New Guinea",30970],#global database, generic
  ["gold-silver mine operation with refinery | silver","Canada, Qu",27409],      #global database,country specific
  ["gold-silver mine operation with refinery | silver","Rest-of-World",30970],   #global database, generic
  
  ["silver-gold mine operation with refinery | silver","Chile",30970],           #global database, generic
  ["silver-gold mine operation with refinery | silver","Rest-of-World",30970],   #global database, generic
  
  ["silver mine operation with extraction | silver, unrefined","Peru",26580],    #global database,country specific

  #Lead
  #["gold mine operation and refining | lead","Sweden",10.7],                             # !! CHECK !!
  #Lead concentrate
  #["gold-silver mine operation and beneficiation | lead concentrate","Canada, Qu",5.7],  # !! CHECK !!
  #["silver mine operation with extraction | lead concentrate","Peru",5.7],               # !! CHECK !!

  #Molybdenite
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Canada",1125],              #global database, country specific
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Chile",1134],               #global database, generic
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","China",1134],               #global database, generic
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Rest-of-World",1134],       #global database, generic
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","Russian Federation",1134],  #global database, generic
  ["copper mine operation and beneficiation, sulfide ore | molybdenite","United States",1134],       #global database, generic
  
  ["molybdenite mine operation | molybdenite","Global",1136],                                        #global database, generic

  #Zinc
  ["gold mine operation and refining | zinc","Sweden",76], #global database, generic
  #Zinc concentrate
  ["gold-silver mine operation and beneficiation | zinc concentrate","Canada, Qu",40], #global database, country specific
  ["silver mine operation with extraction | zinc concentrate","Peru",40]]              #global database, country specific


# In a next step, the overburden list is iterated and the overburden values are inserted 
# in the respective processes
  
  
for mp in overburden_list:
  
  #name, location and amount are extracted from the list
  mining_process_name_raw = mp[0]
  location                = mp[1]
  overburden_amount       = mp[2]

  for ap in allprocesses:
    
    # All mining processes are identified based on the name in the list.
    # Market activities are excluded. 
    if mining_process_name_raw in ap.name:
      if "market" not in ap.name:
        mining_processes_name_explicit = ap.name

        
  mining_processes = dao_p.getForName(mining_processes_name_explicit)
  foundsomething = False    # This boolean is used to print a warning message, if no process could be found
  for p in mining_processes:
    
    
    if location in p.location.name:
      foundsomething = True      
      
      # The booleans isin_x indicate, whether the respective flow is present in a process
      isin_ov  = False   # Overburden as elementary flow
      isin_ext = False   # Flow indicating external data

      # Iterating over exchanges and report whether overburden as elementary flow and 
      # the flow indicating external data are present. If so, the amount is updated.
      for ex in p.exchanges:
        if ex.isInput == True:
          if ex.flow.name == overburden.name:
            ex.amount = overburden_amount
            dao_p.update(p)
            isin_ov = True
            break
          
          if ex.flow.name == external_data_flow.name:
            ex.amount = overburden_amount
            dao_p.update(p)
            isin_ext = True
            break

      # If overburden is not present, a new exchange is created and added.   
      if isin_ov == False:

        ex = model.Exchange()
        ex.isInput = True
        ex.flow = overburden
        ex.amount = overburden_amount
        ex.unit = kg
        ex.flowPropertyFactor = gangue.getReferenceFactor()
        p.exchanges.add(ex)
        dao_p.update(p)

      # If the flow indicating external data is not present, a new exchange is created and added.   
      if isin_ext == False:
        
        ex = model.Exchange()
        ex.isInput = True
        ex.flow = external_data_flow
        ex.amount = overburden_amount
        ex.unit = kg
        ex.flowPropertyFactor = gangue.getReferenceFactor()
        p.exchanges.add(ex)
        dao_p.update(p)
      
  # If no process with the given name and location can be found, a warning message is printed.    
  if foundsomething == False:
    print("nothing found for:" + mining_processes_name_explicit + location)


############################
###     G A N G U E      ###
############################

# Because not every mining process contains information regarding gangue, 
# external data was used to fill these gaps. 
# In the following list the coressponding gangue values are stored.
# The sources given are given as comments.

allprocesses = dao_p.getAll()


gangue_list = [
  
  #Manganese
  #["manganese concentrate production | manganese concentrate","Global", 0],    # !! CHECK !!
  
  #Bauxite
  ["bauxite mine operation | bauxite","Global", 0],    # Bauxite is the mined ore. Therefore gangue is set to 0
  
  # Barite
  ["barite production | barite |","Europe", 0.333],           # ore grade in ecoinvent documentation https://ecoquery.ecoinvent.org/3.9.1/cutoff/dataset/7053/documentation
  ["barite production | barite |","Rest-of-World",0.333],     # ore grade in ecoinvent documentation https://ecoquery.ecoinvent.org/3.9.1/cutoff/dataset/7053/documentation
  ["barite production | barite |","Canada, Qu", 0.333],       # ore grade in ecoinvent documentation https://ecoquery.ecoinvent.org/3.9.1/cutoff/dataset/7053/documentation
  
  # Iron
  ["iron ore mine operation and beneficiation | iron ore concentrate |","Canada, Q",46/100 * 0.4995],    # ecoinvent process: iron ore mine operation, 63% Fe
  ["iron ore mine operation, 46% Fe | iron ore, crude ore, 46% Fe |","Global",46/63 * 0.4995],           # ecoinvent process: iron ore mine operation, 63% Fe

  # Copper
  ["platinum group metal mine operation, ore with high palladium content | copper, cathode |","Russian Federation", 212.0], # gobal database, country specific
  ["gold mine operation and refining | copper, cathode |","Sweden",39.0],                                                   # gobal database, country specific
  ["copper production, cathode, solvent extraction and electrowinning process | copper, cathode","Global",119.0],           # gobal database, generic
  
  # Copper concentrate
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Australia",22.0],         # gobal database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Canada",14.0],            # gobal database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Chile",67.0],             # gobal database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","China",75.0],             # gobal database, generic
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Indonesia",75.0],         # gobal database, generic
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Kazakhstan",29.0],        # gobal database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Rest-of-World",75.0],     # gobal database, generic
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Russian Federation",75.0],# gobal database, generic
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","United States",85.0],     # gobal database, country specific
  ["copper mine operation and beneficiation, sulfide ore | copper concentrate, sulfide ore","Zambia",40.0],            # gobal database, country specific
  
  ["molybdenite mine operation | copper concentrate, sulfide ore |","Global",75.0],                      # gobal database, generic
  ["gold-silver mine operation and beneficiation | copper concentrate, sulfide ore","Canada, Qu",14.0],  # gobal database, country specific


  # Gold
  ["gold mine operation and gold production, unrefined | gold, unrefined","Rest-of-World",511257.0],  # gobal database, generic
  ["gold mine operation and gold production, unrefined | gold, unrefined","Zambia",34.0],             # gobal database, country specific
  ["gold mine operation and refining | gold |","Sweden",1000340.0],                                   # gobal database, country specific
  ["gold production | gold |","Australia",1627.0],                                                    # gobal database, country specific
  ["gold production | gold |","Canada",520026.0],                                                     # gobal database, country specific
  ["gold production | gold |","Tanzania, United Republic of",334465.0],                               # gobal database, country specific
  ["gold production | gold |","United States",509221.0],                                              # gobal database, country specific
  ["silver-gold mine operation with refinery | gold |","Chile",938750.0],                             # gobal database, country specific
  ["silver-gold mine operation with refinery | gold |","Rest-of-World",511257.0],                     # gobal database, generic
  ["gold-silver mine operation with refinery | gold |","Canada, Q",520026.0],                         # gobal database, country specific
  ["gold-silver mine operation with refinery | gold |","Papua New Guinea",1258757.0],                 # gobal database, country specific
  ["gold-silver mine operation with refinery | gold |","Rest-of-World",511257.0],                     # gobal database, generic

  # Molybdenum
  ["molybdenite mine operation | molybdenite |","Global",1414],   # gobal database

  # Silver
  ["gold-silver mine operation with refinery | silver |","Canada, Q",14185.0],        # gobal database, country specific
  ["gold-silver mine operation with refinery | silver |","Papua New Guinea",21172.0], # gobal database, country specific
  ["gold-silver mine operation with refinery | silver |","Rest-of-World",9432.0],     # gobal database, generic
  
  ["gold mine operation and refining | silver |","Sweden",16334.0],                 # gobal database, country specific
  ["silver-gold mine operation with refinery | silver |","Chile",9089.0],           # gobal database, country specific
  ["silver-gold mine operation with refinery | silver |","Rest-of-World",9432.0],   # gobal database, generic

  # Lead
  ["gold mine operation and refining | lead","Sweden",31.0],                             # gobal database, generic
  # Lead concentrate
  ["gold-silver mine operation and beneficiation | lead concentrate","Canada, Q",10.0],  # gobal database, country specific

  # Zinc
  ["gold mine operation and refining | zinc","Sweden",11.0],                             # gobal database, country ,specific
  # Zinc concentrate
  ["gold-silver mine operation and beneficiation | zinc concentrate","Canada, Q",18.0],  # gobal database, country specific

  # Nickel
  ["platinum group metal mine operation, ore with high palladium content | nickel, class","Russian Federation",93.0],  # gobal database, country specific

  # Palladium
  ["platinum group metal mine operation, ore with high palladium content | palladium","Russian Federation",3127297.0], # gobal database, country specific

  # Platinum
  ["platinum group metal mine operation, ore with high palladium content | platinum","Russian Federation",2534893.0],  # gobal database, country specific

  # Rhodium
  ["platinum group metal mine operation, ore with high palladium content | rhodium","Russian Federation",28.0],        # gobal database, country specific

  # Uranium
  ["uranium production, in yellowcake, in-situ leaching | uranium, in yellowcake","Global",8384]]  # gobal database, generic


# In a next step, mining processes are updated with the respective value


for mp in gangue_list:
  
  #name, location and amount are extracted from the list
  mining_process_name_raw = mp[0]
  location                = mp[1]
  gangue_amount           = mp[2]

  for ap in allprocesses:
    # All mining processes are identified based on the name in the list.
    # Market activities are excluded. 
    if mining_process_name_raw in ap.name:
      if "market" not in ap.name:
        mining_processes_name_explicit = ap.name

  mining_processes = dao_p.getForName(mining_processes_name_explicit)
  foundsomething = False
  for p in mining_processes:
    if location in p.location.name:
      foundsomething = True
      
      # The booleans isin_x indicate, whether the respective flow is present in a process
      isin_ga  = False  # Gangue as elementary flow
      isin_ext = False  # Flow indicating external data
      
      
      for ex in p.exchanges:
        if ex.isInput == True:
          if ex.flow.name == gangue.name:
            ex.amount = gangue_amount
            dao_p.update(p)
            isin_ga = True
            break
          
          if ex.flow.name == external_data_flow.name:
            ex.amount = gangue_amount
            dao_p.update(p)
            isin_ext = True
            break
      

      if isin_ga == False:
        
        ex = model.Exchange()
        ex.isInput = True
        ex.flow = gangue
        ex.amount = gangue_amount
        ex.unit = kg
        ex.flowPropertyFactor = gangue.getReferenceFactor()
        p.exchanges.add(ex)
        dao_p.update(p)

      if isin_ext == False:
        
        ex = model.Exchange()
        ex.isInput = True
        ex.flow = external_data_flow
        ex.amount = gangue_amount
        ex.unit = kg
        ex.flowPropertyFactor = gangue.getReferenceFactor()
        p.exchanges.add(ex)
        dao_p.update(p)

      
####################################
### M I S S I N G   F L O W S    ###
####################################
        

allprocesses  = dao_p.getAll()
allcategories = dao_c.getAll()

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
missing_outside    = []
  

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
missing_gangue     = list(set(missing_gangue))
missing_outside    = list(set(missing_outside))


#######################################
### B I O T I C   R E S O U R C E S ###
#######################################

### Used Biomass ###

#category lists 

agriculture_categories_list = [
  "0111:Growing of cereals (except rice), leguminous crops and oil seeds",
  "0112:Growing of rice",
  "0113:Growing of vegetables and melons, roots and tubers",
  "0114:Growing of sugar cane",
  "0116:Growing of fibre crops",
  "0119:Growing of other non-perennial crops",
  
  "0121:Growing of grapes",
  "0122:Growing of tropical and subtropical fruits",
  "0123:Growing of citrus fruits",
  "0124:Growing of pome fruits and stone fruits",
  "0125:Growing of other tree and bush fruits and nuts",
  "0126:Growing of oleaginous fruits",
  "0127:Growing of beverage crops",
  "0128:Growing of spices, aromatic, drug and pharmaceutical crops",
  "0129:Growing of other perennial crops",
]

forestry_categories_list = [
  "0210:Silviculture and other forestry activities",
  "0220:Logging"
]

animal_categories_list = [
  
  "0141:Raising of cattle and buffaloes",
  "0144:Raising of sheep and goats",
  "0145:Raising of swine|pigs",
  "0146:Raising of poultry",
  "0149:Raising of other animals"
  
]

# Argriculture

for p in allprocesses:
  
  if p.category.name in agriculture_categories_list:
    if "market" not in p.name:
      isin_biomass = False
      
      for ex in p.exchanges:
        if "Biomass, used" in ex.flow.name:
          isin_biomass = True

      if p.quantitativeReference.unit.name == "kg" and isin_biomass == False:
        ex_used         = model.Exchange()
        ex_used.isInput = True
        ex_used.flow    = biomass_used
        ex_used.amount  = 1
        ex_used.unit    = kg
        ex_used.flowPropertyFactor = gangue.getReferenceFactor()
        p.exchanges.add(ex_used)
        dao_p.update(p)
      
      if p.quantitativeReference.unit.name == "kg" and isin_biomass == True:
        for ex in p.exchanges:
          if ex.isInput == True:
            if ex.flow.name == biomass_used.name:
              ex.amount = 1
              dao_p.update(p)
      
      
      
# Forestry



for p in allprocesses:
  
  if p.category.name in forestry_categories_list:
    
    isin_energy  = False
    isin_biomass = False
    
    mass_per_energy = 1.0/15.5  #  ecoinvent report 9 Wood fuel construction
    
    if "hardwood" in p.name:
      mass_per_energy = 1.0/15.0 # ecoinvent report 9 Wood fuel construction
    if "softwood" in p.name:
      mass_per_energy = 1.0/15.7 # ecoinvent report 9 Wood fuel construction
      
 
    for ex in p.exchanges:
      if "Biomass, used" in ex.flow.name:
        isin_biomass = True
        
      if "Energy, gross calorific value, in biomass" in ex.flow.name:
        if "correction" not in ex.flow.name:
          energy_amount = ex.amount
          isin_energy = True
    
    if isin_energy == True and isin_biomass == False:

      ex_used         = model.Exchange()
      ex_used.isInput = True
      ex_used.flow    = biomass_used
      ex_used.amount  = mass_per_energy * energy_amount
      ex_used.unit    = kg
      ex_used.flowPropertyFactor = gangue.getReferenceFactor()
      p.exchanges.add(ex_used)
      dao_p.update(p)
      
    if isin_energy == True and isin_biomass == True:
      for ex in p.exchanges:
        if ex.isInput == True:
          if ex.flow.name == biomass_used.name:
            ex.amount = mass_per_energy * energy_amount
            dao_p.update(p)


            
# Rest
mass_per_energy_average = 1.0/19.0

for p in allprocesses:
  
  isin_energy  = False
  isin_biomass = False
  
  for ex in p.exchanges:
    if "Biomass, used" in ex.flow.name:
      isin_biomass = True
    
    if "Energy, gross calorific value, in biomass" in ex.flow.name:
      if "correction" not in ex.flow.name:
        energy_amount = ex.amount
        isin_energy = True
        
  if isin_energy == True and isin_biomass == False:
    
    ex_used         = model.Exchange()
    ex_used.isInput = True
    ex_used.flow    = biomass_used
    ex_used.amount  = mass_per_energy_average * energy_amount
    ex_used.unit    = kg
    ex_used.flowPropertyFactor = gangue.getReferenceFactor()
    p.exchanges.add(ex_used)
    dao_p.update(p)

    
### Unused Biomass ###            

for p in allprocesses:
  
  isin_biomass_used = False
  isin_biomass_unused = False
  for ex in p.exchanges:
    if "Biomass, used" in ex.flow.name:
      isin_biomass_used = True
      biomass_used_amount = ex.amount

  for ex in p.exchanges:
    if "Biomass, unused" in ex.flow.name:
      isin_biomass_unused = True
      
  if isin_biomass_used == True and isin_biomass_unused == False:
    residue_ratio = 1
    
    if p.category.name in agriculture_categories_list:
      if "wheat" in p.name:
        residue_ratio = mean_function([1.3,1.2,1.34,1.75,0.6,1,
                                      1.7,1.7,1.6,0.8,1.7,
                                      1.3,1.3,1.5,0.9,1.3])        
      if "barley" in p.name:
        residue_ratio = mean_function([1.3,1.5,1,1.75,1,1.24,1.2,1])
      if "rye" in p.name:
        residue_ratio = mean_function([1.75,1.7])
      if "maize" in p.name or "corn" in p.name:
        residue_ratio = mean_function([1,1,0.9,2,1.3,1,0.7,1,1,1])
      if "sunflower" in p.name:
        residue_ratio = mean_function([1.5,2.6,1.4,])
      if "rape" in p.name:
        residue_ratio = mean_function([1.1,1.7,1.7])
      if "rice" in p.name:
        residue_ratio = mean_function([1.76,1])
    
    if p.category.name in forestry_categories_list:
      residue_ratio = 6.0 / 4.0                      # https://wgbis.ces.iisc.ac.in/energy/HC270799/RWEDP/acrobat/p_residues.pdf
      
    if p.category.name in animal_categories_list:
      
      residue_ratio = 0                     # meadows do not have residues
    
    ex_unused         = model.Exchange()
    ex_unused.isInput = True
    ex_unused.flow    = biomass_unused
    ex_unused.amount  = biomass_used_amount * residue_ratio
    ex_unused.unit    = kg
    ex_unused.flowPropertyFactor = gangue.getReferenceFactor()
    p.exchanges.add(ex_unused)
    dao_p.update(p)    
      
       

        
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


##################################### 3. Create LCIA-Method  #####################################


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

if analysis == True:
  
  method={"@type":"ImpactMethod","category":"Material Intensity","@id":"56c9a436-2c1d-4ead-87d5-17ac168b0191","name":"Material Intensity","version":"1.0",
  "impactCategories":[
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"90768cd8-9b26-11ee-b9d1-0242ac120002","name":"Abiotic RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"0e79d9c7-8add-4a23-a25e-1f55a4e82d2d","name":"Abiotic TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"90768f6c-9b26-11ee-b9d1-0242ac120002","name":"Biotic RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"3d2d5656-2e3e-4453-8003-c3d485356045","name":"Biotic TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"8f07827d-72c0-4ae7-a708-a18e684b3f54","name":"Water","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"68be8652-dba7-443e-b439-c57d058f0388","name":"Moved Soil","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"cd9a353b-371c-4aa9-bdec-89e98891d5dd","name":"ESTIMATE MISSING ABIOTIC","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"edeb416b-5d59-4524-95dc-9c0a9261c0d0","name":"FLAG EXTERNAL DATA","refUnit":"kg"}]}

if analysis == False:
  
  method={"@type":"ImpactMethod","category":"Material Intensity","@id":"56c9a436-2c1d-4ead-87d5-17ac168b0191","name":"Material Intensity","version":"1.0",
  "impactCategories":[
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"90768cd8-9b26-11ee-b9d1-0242ac120002","name":"Abiotic RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"0e79d9c7-8add-4a23-a25e-1f55a4e82d2d","name":"Abiotic TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"90768f6c-9b26-11ee-b9d1-0242ac120002","name":"Biotic RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"3d2d5656-2e3e-4453-8003-c3d485356045","name":"Biotic TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"8f07827d-72c0-4ae7-a708-a18e684b3f54","name":"Water","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"Material Intensity","@id":"68be8652-dba7-443e-b439-c57d058f0388","name":"Moved Soil","refUnit":"kg"}]}


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


empty_external  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"Material Intensity",
    "@id": "edeb416b-5d59-4524-95dc-9c0a9261c0d0",
    "name": "FLAG EXTERNAL DATA",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": "edeb416b-5d59-4524-95dc-9c0a9261c0d0.json"}

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
  
with open(path_lcia_categories +'/edeb416b-5d59-4524-95dc-9c0a9261c0d0.json', 'w') as fp:
  json.dump(empty_external, fp)

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
  if f.name == "Biomass, used":
    
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

  if f.name == "Biomass, unused":
    
    biotic_tmr_uuid.append(f.refId)
    biotic_tmr_names.append(f.name)
    biotic_tmr_catpath.append(f.category.name)
    biotic_tmr_units.append("kg")
    biotic_tmr_values.append(1)


    

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

# Lists regarding missing abiotic values are filled with the respective elementary flows.

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

print(gangue_amounts)
print(overburden_amounts)

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

print(missing_dict)
######  F L A G   E X T E R N A L   D A T A 

# Lists regarding flags of external data are filled with the respective elementary flows.


external_uuid =[]
external_names =[]
external_catpath = []
external_units = []
external_values = []

for f in allflows:
  if f.name == "flag external data":
    external_uuid.append(f.refId)
    external_names.append(f.name)
    external_catpath.append(f.category.name)
    external_units.append("kg")
    external_values.append(1)

external_dict ={
    "names": external_names,
    "uuids": external_uuid,
    "units": external_units,
    "values":external_values,
    "catpath":external_catpath}



##################################################
### P O P U L A T E     .J S O N - F I L E S #####
##################################################

if analysis == True:
  MI_uuid = ["90768cd8-9b26-11ee-b9d1-0242ac120002.json",
             "0e79d9c7-8add-4a23-a25e-1f55a4e82d2d.json",
             "90768f6c-9b26-11ee-b9d1-0242ac120002.json",
             "3d2d5656-2e3e-4453-8003-c3d485356045.json",
             "8f07827d-72c0-4ae7-a708-a18e684b3f54.json",
             "68be8652-dba7-443e-b439-c57d058f0388.json",
             "cd9a353b-371c-4aa9-bdec-89e98891d5dd.json",
          	 "edeb416b-5d59-4524-95dc-9c0a9261c0d0.json"]


  MI_dict = {
      "Abiotic RMI": abiotic_rmi_dict,
      "Abiotic TMR": abiotic_tmr_dict,
      "Biotic RMI": biotic_rmi_dict,
      "Biotic TMR": biotic_tmr_dict,
      "Water": water_dict,
      "Moved Soil": movedsoil_dict,
      "ESTIMATE MISSING ABIOTIC": missing_dict,
      "FLAG EXTERNAL DATA": external_dict}

  cat_names = ["Abiotic RMI",
              "Abiotic TMR",
              "Biotic RMI",
              "Biotic TMR",
              "Water",
              "Moved Soil",
              "ESTIMATE MISSING ABIOTIC",
              "FLAG EXTERNAL DATA"]


if analysis == False:
  
  MI_uuid = ["90768cd8-9b26-11ee-b9d1-0242ac120002.json",
             "0e79d9c7-8add-4a23-a25e-1f55a4e82d2d.json",
             "90768f6c-9b26-11ee-b9d1-0242ac120002.json",
             "3d2d5656-2e3e-4453-8003-c3d485356045.json",
             "8f07827d-72c0-4ae7-a708-a18e684b3f54.json",
             "68be8652-dba7-443e-b439-c57d058f0388.json"]


  MI_dict = {
      "Abiotic RMI": abiotic_rmi_dict,
      "Abiotic TMR": abiotic_tmr_dict,
      "Biotic RMI": biotic_rmi_dict,
      "Biotic TMR": biotic_tmr_dict,
      "Water": water_dict,
      "Moved Soil": movedsoil_dict}

  cat_names = ["Abiotic RMI",
              "Abiotic TMR",
              "Biotic RMI",
              "Biotic TMR",
              "Water",
              "Moved Soil"]

  
print(len(MI_uuid))

for i in range(0,len(MI_uuid)):
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

##################################### 4. Import #####################################


reader = ZipStore.open(File(method_dir))
i = JsonImport(reader,db)
i.run()
reader.close()



##################################### 5. Document Missing Flows #####################################

# For transparency and analytical reasons, csv files are created that contain information about
# process data sets where information regarding gangue and/or overburden is missing.

if analysis == True:


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
