# -*- coding:utf-8 -*-
import sys 
import os
import datetime
import calendar
import time
import re
import json
from urllib.request import urlopen
from urllib.request import Request
from urllib.request import urlretrieve
import collections
import logging
import argparse
import pypyodbc
import random
import time

today = str(datetime.date.today())
yesterday = str(datetime.date.today() + datetime.timedelta(-1))
def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    fh = logging.FileHandler('%slog.log'%name)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(handler)
    return logger

logger = setup_custom_logger("TerraformModulesBI")

def get_results(url):
    try:
        req =  Request(url)
        response = urlopen(req,timeout=30).read()
        results = json.loads(response.decode())
        return results
    except:
        logger.exception("Exception Logged")
        return []

def getPublished(source,version):
    url = 'https://registry.terraform.io/v1/modules/'+source+'/'+version
    info = get_results(url)
    if(info == []):
        logger.info("CAN NOT get published %s"%url)
        return "",""
    return info["published_at"][0:10], info["owner"]

def getVersions(source):
    url = 'https://registry.terraform.io/v1/modules/'+source+'/versions'
    results = get_results(url)
    if(results == []):
        logger.info("CANNOT get Versions %s"%url)
        return []
    versions = results["modules"][0]["versions"]
    module_versions = []
    for version in versions:
        module_versions.append(version["version"])
    return module_versions

def processModules(modules):
    provider_dict = {}
    rgx_source = r'(.*)/'
    for module in modules:
        itemList = {}
        itemList["source"] = re.findall(rgx_source,module["id"])[0]
        itemList["downloads"] = module["downloads"]
        itemList["namespace"] = module["namespace"]
        itemList["verified"] = module["verified"]
        module_versions = {}
        start = time.time()
        for version in getVersions(itemList["source"]): 
            module_versions[version],owner = getPublished(itemList["source"],version)
        end = time.time()
        print(itemList["source"] + ": %s"%(end - start))
        itemList["owner"] = owner
        itemList["versions"] = module_versions
        if(module["provider"] not in provider_dict.keys()):
           provider_dict[module["provider"]] = {}
        provider_dict[module["provider"]][itemList["source"]] = itemList
    return provider_dict

def createOriginalSQL(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    
    sql_create = """CREATE TABLE OriginalModules(
        date Date,
        provider varchar(50),
        module varchar(255),
        namespace varchar(255),
        owner varchar(255),
        downloads INT,
        verified BIT,
        PRIMARY KEY(date,module)
    )"""
    cursor.execute(sql_create)
    cursor.commit()

    for provider in provider_dict.keys():
        for module in provider_dict[provider].keys():
            sql_insert = ("INSERT INTO OriginalModules" 
            "(date, provider, module, namespace, owner, downloads, verified)"
            "VALUES (?,?,?,?,?,?,?)")
            Values = [today, provider, module, provider_dict[provider][module]["namespace"], provider_dict[provider][module]["owner"],provider_dict[provider][module]["downloads"],provider_dict[provider][module]["verified"]]
            cursor.execute(sql_insert, Values)
    cursor.commit()
    connection.close()

def updateOriginalSQL(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    for provider in provider_dict.keys():
        for module in provider_dict[provider].keys():
            sql_insert = ("INSERT INTO OriginalModules" 
            "(date, provider, module, namespace, owner, downloads, verified)"
            "VALUES (?,?,?,?,?,?,?)")
            Values = [today, provider, module, provider_dict[provider][module]["namespace"], provider_dict[provider][module]["owner"],provider_dict[provider][module]["downloads"],provider_dict[provider][module]["verified"]]
            cursor.execute(sql_insert, Values)
    connection.commit()
    connection.close()
def getEveryDay(begin_date,end_date):
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date,"%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list

def getBeforeMonth(month_numbers):
    month_list = []
    first_day = datetime.date.today().replace(day=1)
    month_list.append(first_day)
    while(month_numbers):
        first_day = (first_day-datetime.timedelta(1)).replace(day=1)
        month_numbers = month_numbers - 1
        month_list.append(first_day)
    return month_list

def getBeforeModuleNum(days, modules):
    moduleNums = {}
    for date in days:
        num = 0
        for module in modules.keys():
            versions = modules[module]["versions"].keys()
            version = min(versions)
            if(modules[module]["versions"][version] < date):
                num += 1
        moduleNums[date] = num
    return moduleNums

def createModuleNumSQL(uid, pwd):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    with open("Provider-Module-Nums.json", "r") as f:
        provider_dict = json.load(f)
    f.close()
    aws_moduleNum = provider_dict["aws"]
    google_moduleNum = provider_dict["google"]
    azure_moduleNum = provider_dict["azurerm"]
    start = '2018-01-26'
    
    days = getEveryDay(start, today)
    
    sql_create = """CREATE TABLE ModuleNums ( 
        date Date,
        aws INT,
        azure INT,
        google INT,
        PRIMARY KEY(date)
        )"""
    cursor.execute(sql_create)


    for date in days:
        sql_insert = ("INSERT INTO ModuleNums" 
        "(date, aws, azure, google)"
        "VALUES (?,?,?,? )")
        Values = [date, aws_moduleNum[date], azure_moduleNum[date], google_moduleNum[date]]
        cursor.execute(sql_insert, Values)
    connection.commit()
    
    sql_create = """CREATE TABLE TodayModuleContribution ( 
        provider TEXT,
        modulenums INT
        )"""
    cursor.execute(sql_create)
    sql_insert = ("INSERT INTO TodayModuleContribution" 
        "(provider, modulenums)"
        "VALUES (?,?)")
    Values = ['aws',aws_moduleNum[today]]
    cursor.execute(sql_insert, Values)
    Values = ['azurerm', azure_moduleNum[today]]
    cursor.execute(sql_insert, Values)
    Values = ['google', google_moduleNum[today]]
    cursor.execute(sql_insert, Values)
    connection.commit()
    
    sql_create = """CREATE TABLE TodayModuleNums ( 
        aws INT,
        azure INT,
        google INT
        )"""
    cursor.execute(sql_create)
    sql_insert = ("INSERT INTO TodayModuleNums" 
        "(aws, azure, google)"
        "VALUES (?,?,?)")
    Values = [aws_moduleNum[today],azure_moduleNum[today],google_moduleNum[today]]
    cursor.execute(sql_insert, Values)
    connection.commit()
    connection.close()

def createNamespaceSQL(uid, pwd):
    with open("Provider-Module-Namespace.json", "r") as f:
        provider_dict = json.load(f)
    f.close()
    days = provider_dict["aws"].keys()
    today = str(datetime.date.today()+datetime.timedelta(-1))
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    
    sql_create = """CREATE TABLE NamespaceNums ( 
        date Date,
        aws INT, 
        azure INT,
        google INT,
        PRIMARY KEY(date)
        )"""
    cursor.execute(sql_create)
    print("Create NamespaceNums Table...")
    for date in days:
        sql_insert = ("INSERT INTO NamespaceNums" 
        "(date, aws, azure, google)"
        "VALUES (?,?,?,? )")
        Values = [date, len(provider_dict["aws"][date]), len(provider_dict["azurerm"][date]),len(provider_dict["google"][date])]
        print(Values)
        cursor.execute(sql_insert, Values)
    connection.commit()
    
    provider_list = ["aws","azurerm","google"]
    for provider in provider_list:
        print("Create %sTodayNamespaceContribution Table..."%provider)
        sql_create = """CREATE TABLE %sTodayNamespaceContribution (
            namespace TEXT,
            moduleNums INT
        )"""%(provider)
        cursor.execute(sql_create)
        th = 2
        for namespace in provider_dict[provider][today].keys():
            if(provider_dict[provider][today][namespace] < th):
                continue
            sql_insert = ("INSERT INTO %sTodayNamespaceContribution"
            "(namespace, moduleNums)"
            "VALUES (?,?)"%provider)
            Values = [namespace, provider_dict[provider][today][namespace]]
            cursor.execute(sql_insert, Values)
        connection.commit()
    connection.close()
def createModulesSQL(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    provider_list = ["aws","azurerm","google"]
    rgx_word = r'/(.*?)/'
    for provider in provider_list:
        print("Create %sModules Table..."%provider)
        sql_create = """CREATE TABLE %sModules(
            keyword varchar(255),
            module varchar(255),
            namespace varchar(255),
            owner varchar(255),
            downloads INT,
            verified BIT,
            PRIMARY KEY(module)
        )"""%provider
        cursor.execute(sql_create)
        connection.commit()
        for module in provider_dict[provider].keys():
            word = re.findall(rgx_word, module)[0]
            sql_insert = ("INSERT INTO %sModules"
            "(keyword, module,namespace,owner,downloads,verified)"
            "VALUES (?,?,?,?,?,?)"%provider)
            Values = [word, module, provider_dict[provider][module]["namespace"], provider_dict[provider][module]["owner"],provider_dict[provider][module]["downloads"],provider_dict[provider][module]["verified"]]
            print(Values)
            cursor.execute(sql_insert, Values)
        connection.commit()
    connection.close()

def updateModulesSQL(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    provider_list = ["aws","azurerm","google"]
    rgx_word = r'/(.*?)/'
    for provider in provider_list:
        print("Update %sModules Table..."%provider)
        for module in provider_dict[provider].keys():
            word = re.findall(rgx_word, module)[0]
            try:
                sql_update = ("UPDATE %sModules SET downloads = %d,verified=%d WHERE module = '%s'"%(provider,provider_dict[provider][module]["downloads"],provider_dict[provider][module]["verified"],module))
                cursor.execute(sql_update)
            except:
                sql_insert = ("INSERT INTO %sModules"
                "(keyword, module,namespace,owner,downloads,verified)"
                "VALUES (?,?,?,?,?,?)"%provider)
                Values = [word, module, provider_dict[provider][module]["namespace"], provider_dict[provider][module]["owner"],provider_dict[provider][module]["downloads"],provider_dict[provider][module]["verified"]]
                print(Values)
                cursor.execute(sql_insert, Values)
        connection.commit()
    connection.close()

def createOwnerSQL(uid, pwd):
    with open("Provider-Module-Owners.json", "r") as f:
        provider_dict = json.load(f)
    f.close()
    today = str(datetime.date.today()+datetime.timedelta(-1))
    days = provider_dict["aws"].keys()
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    
    sql_create = """CREATE TABLE OwnerNums ( 
        date Date,
        aws INT, 
        azure INT,
        google INT,
        PRIMARY KEY(date)
        )"""
    cursor.execute(sql_create)
    print("Create OwnerNums Table...")
    for date in days:
        sql_insert = ("INSERT INTO OwnerNums" 
        "(date, aws, azure, google)"
        "VALUES (?,?,?,? )")
        Values = [date, len(provider_dict["aws"][date]), len(provider_dict["azurerm"][date]),len(provider_dict["google"][date])]
        print(Values)
        cursor.execute(sql_insert, Values)
    connection.commit()
    
    provider_list = ["aws","azurerm","google"]
    for provider in provider_list:
        print("Create %sTodayOwnerContribution Table..."%provider)
        th = 2
        sql_create = """CREATE TABLE %sTodayOwnerContribution (
            owner TEXT,
            moduleNums INT
        )"""%(provider)
        cursor.execute(sql_create)
        for owner in provider_dict[provider][today].keys():
            if(provider_dict[provider][today][owner] < th):
                continue
            sql_insert = ("INSERT INTO %sTodayOwnerContribution"
            "(owner, moduleNums)"
            "VALUES (?,?)"%provider)
            Values = [owner, provider_dict[provider][today][owner]]
            cursor.execute(sql_insert, Values)
        connection.commit()
    connection.close()
def createOwnerModulesSQL(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    provider_list = ["aws","azurerm","google"]
    for provider in provider_list:
        print("Create %sOwnerModules Table..."%provider)
        sql_create = """CREATE TABLE %sownerModules(
            owner TEXT,
            module TEXT
        )"""%provider
        cursor.execute(sql_create)
        connection.commit()
        for module in provider_dict[provider].keys():
            sql_insert = ("INSERT INTO %sOwnerModules"
            "(owner,module)"
            "VALUES (?,?)"%provider)
            Values = [provider_dict[provider][module]["owner"], module]
            cursor.execute(sql_insert, Values)
        connection.commit()

def createDownloadSQL(uid, pwd, provider_dict):
    with open("Provider-Module-Downloads.json", "r") as f:
        provider_modules = json.load(f)
    f.close()
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    
    provider_list = ["aws","azurerm","google"]
    
    for provider in provider_list:
        print("Create %sModuleDownloads table..."%provider)
        sql_create = """CREATE TABLE %sModuleDownloads(
            date Date,
            module TEXT,
            downloads INT
        )"""%provider
        cursor.execute(sql_create)
        connection.commit()
        for module in provider_modules[provider].keys():
            for date in provider_modules[provider][module].keys():
                sql_insert = ("INSERT INTO %sModuleDownloads"
                "(date, module, downloads)"
                "VALUES (?,?,?)"%provider)
                Values = [date,module, provider_modules[provider][module][date]]
                cursor.execute(sql_insert, Values)
        connection.commit()

    for provider in provider_list:
        print("Create %sModuleDownloadsChange table..."%provider)
        sql_create = """CREATE TABLE %sModuleDownloadsChange(
            module TEXT,
            difference INT
        )"""%provider
        cursor.execute(sql_create)
        connection.commit()
        for module in provider_modules[provider].keys():
            if(yesterday not in provider_modules[provider][module].keys()):
                continue
            yesterday_downloads = provider_modules[provider][module][yesterday]
            Change = provider_dict[provider][module]["downloads"] - yesterday_downloads
            sql_insert = ("INSERT INTO %sModuleDownloadsChange"
            "( module, difference)"
            "VALUES (?,?)"%provider)
            Values = [module, Change]
            print(Values)
            cursor.execute(sql_insert, Values)
        connection.commit()
    
    days = provider_modules["aws"]['terraform-aws-modules/vpc/aws'].keys()
    print("Create TotalDownloads table...")
    sql_create = """CREATE TABLE TotalDownloads ( 
        aws INT, 
        azure INT,
        google INT
        )"""
    cursor.execute(sql_create)
    
    totalDownloads = []
    maxModuleDownloads = []
    columns = "date Date,"
    items = "date,"
    for provider in provider_list:
        downloads = 0
        maxDownloads = 0
        print(provider)
        for module in provider_dict[provider].keys():
            downloads += provider_dict[provider][module][today]
            if(maxDownloads < provider_dict[provider][module][today]):
                maxDownloads = provider_dict[provider][module][today]
                maxDownloadModule = module  
        columns += "[%s] INT,"%maxDownloadModule 
        items += "[%s],"%maxDownloadModule
        print("%s Total Downloads:%d"%(provider, downloads))
        print("%s Max Download Modules:%d"%(maxDownloadModule, provider_dict[provider][maxDownloadModule][today]))
        totalDownloads.append(downloads)
        maxModuleDownloads.append(maxDownloadModule)
    items = items[:-1]
    sql_insert = ("INSERT INTO TotalDownloads" 
        "(aws, azure, google)"
        "VALUES (?,?,? )")
    cursor.execute(sql_insert, totalDownloads)
    connection.commit()
    
    print("Create TotalDownloadsChange table...")
    sql_create = """CREATE TABLE TotalDownloadsChange ( 
        provider TEXT, 
        difference INT
        )"""
    cursor.execute(sql_create)
    
    for provider in provider_list:
        sql_insert = ("INSERT INTO TotalDownloadsChange"
        "(provider, difference)"
        "VALUES (?,?)")
        Values = [provider, 0]
        cursor.execute(sql_insert, Values)
    connection.commit()
    connection.close()

def createVersionSQL(uid, pwd, provider_dict):
    provider_list = ["aws", "azurerm","google"] 
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    
    for provider in provider_dict:
        print("Create %sVersionUpdate Table..."%provider)
        sql_create =  """CREATE TABLE %sVersionUpdate ( 
            name TEXT, 
            updatedays FLOAT
            )"""%provider
        cursor.execute(sql_create)
        for module in provider_dict[provider].keys():
            if(provider_dict[provider][module]["verified"] == "false"):
                continue
            versionUpdate = computeVersionUpdate(module)
            if(versionUpdate == -1):
                continue
            sql_insert = ("INSERT INTO %sVersionUpdate" 
                "(name, updatedays)"
                "VALUES (?,? )"%provider)
            Values = [module, versionUpdate]
            print(Values)
            cursor.execute(sql_insert, Values)
        connection.commit()
    connection.close()

def updateVersions(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()

    ### Update Provider Module Versions JSON file
    with open("Provider-Module-Versions.json", "r") as f:
        provider_modules = json.load(f)
    f.close()
    
    for provider in provider_dict.keys():
        if(provider not in provider_modules.keys()):
                provider_modules[provider] = {}
        for module in provider_dict[provider].keys():   
            today_versions = [] 
            
            module_versions = provider_dict[provider][module]["versions"]
            if(module_versions == {}):
                for version in getVersions(module): 
                    module_versions[version],owner = getPublished(module,version)
            new_dict = collections.OrderedDict(sorted(module_versions.items(), key=lambda t: t[1], reverse=True))
            versions = list(new_dict.keys())
            published = list(new_dict.values())
            print(module, versions, published)
            if(published[0] < yesterday):
                latest_version = "0.0.0"
                for i in range(0, len(published)):
                    if(v2num(versions[i]) < v2num(latest_version)):
                        continue
                    latest_version = versions[i]
                today_versions.append(latest_version)
             
            if(module not in provider_modules[provider].keys()):
                provider_modules[provider][module] = {} 
                today_versions = versions
                
            if(today_versions == []):
                logger.info("module:%s, today_version:%s"%(module, today_versions))
            provider_modules[provider][module][today] =  today_versions

    with open("Provider-Module-Versions.json", "w") as f:    
        json.dump(provider_modules, f)
    f.close()
    
    # Update VersionUpdate Table...
    provider_list = ["aws", "azurerm","google"] 
    for provider in provider_list:
        print("Update %sVersionUpdate Table..."%provider)
        for module in provider_dict[provider].keys():
            print(module)
            if(provider_dict[provider][module]["verified"] == "false"):
                continue
            module_versions = provider_dict[provider][module]["versions"]
            new_dict = collections.OrderedDict(sorted(module_versions.items(), key=lambda t: t[1], reverse=True))
            versions = list(new_dict.keys())
            published = list(new_dict.values())
            if(published[0] == yesterday):
                versionUpdate = computeVersionUpdate(published)
                if(versionUpdate == -1):
                    continue
                print(versionUpdate)
                try:
                    sql_update = ("UPDATE %sVersionUpdate SET updatedays = %f WHERE name LIKE '%s'"%(provider, versionUpdate,module))
                    cursor.execute(sql_update)
                except:
                    sql_insert = ("INSERT INTO %sVersionUpdate" 
                        "(name, updatedays)"
                        "VALUES (?,?)"%provider)
                    Values = [module, versionUpdate]
                    cursor.execute(sql_insert, Values)         
        connection.commit()
    connection.close()

def getProviderModulesNum(modules):
    provider_modules = {}
    for module in modules:
        provider = module["provider"]
        if(provider not in provider_modules.keys()):
            provider_modules[provider] = 1
        else:
            provider_modules[provider] += 1
    return provider_modules

def updateModuleNums(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()

    ### Update Provider Module Nums JSON file
    with open("Provider-Module-Nums.json", "r") as f:
        provider_modules = json.load(f)
    f.close()
    
    with open("Provider-Module-Nums.json", "w") as f:
        for provider in provider_dict.keys():
            if provider not in provider_modules.keys():
                provider_modules[provider] = {}
            provider_modules[provider][today] = len(provider_dict[provider])
            print("%s:%s"%(provider, provider_modules[provider][today]))
        json.dump(provider_modules, f)
    f.close()
    
    ### Update Provider Module Nums SQL
    print("Start insert into ModuleNums table...")
    sql_insert = ("INSERT INTO ModuleNums" 
        "(date, aws, azure, google)"
        "VALUES (?,?,?,? )")
    Values = [today, provider_modules["aws"][today], provider_modules["azurerm"][today], provider_modules["google"][today]]
    print(Values)
    cursor.execute(sql_insert, Values)
    connection.commit()
    
    print("Update TodayModuleContribution table...")
    print("aws:%d"%provider_modules["aws"][today])
    sql_update_aws = ("UPDATE TodayModuleContribution set modulenums = %d WHERE provider LIKE 'aws'"%provider_modules["aws"][today])
    cursor.execute(sql_update_aws)
    print("azure:%d"%provider_modules["azurerm"][today])
    sql_update_azure = ("UPDATE TodayModuleContribution set modulenums = %d WHERE provider LIKE 'azurerm'"%provider_modules["azurerm"][today])
    cursor.execute(sql_update_azure)
    print("google:%d"%provider_modules["google"][today])
    sql_update_google = ("UPDATE TodayModuleContribution set modulenums = %d WHERE provider LIKE 'google'"%provider_modules["google"][today])
    cursor.execute(sql_update_google)
    connection.commit()

    print("Update TodayModuleNums table...")
    sql_update = ("UPDATE TodayModuleNums set aws = %d, azure = %d, google = %d"%(provider_modules["aws"][today],provider_modules["azurerm"][today],provider_modules["google"][today]))
    cursor.execute(sql_update)
    connection.commit()
    connection.close()
def getProviderModulesNamespace(modules):
    modules_namespace = {}
    for module in modules.keys():
        namespace = modules[module]["namespace"]
        if(namespace not in modules_namespace.keys()):
            modules_namespace[namespace] = 1
        else:
            modules_namespace[namespace] += 1
    return modules_namespace

def getProviderModulesOwner(modules):
    modules_owner = {}
    for module in modules.keys():
        owner = modules[module]["owner"]
        if(owner not in modules_owner.keys()):
            modules_owner[owner] = 1
        else:
            modules_owner[owner] += 1
    return modules_owner

def updateNamespace(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor= connection.cursor()
    ### Update Provider Module Namespace JSON file
    with open("Provider-Module-Namespace.json", "r") as f:
        provider_modules = json.load(f)
    f.close()
    provider_list = ["aws","azurerm","google"]
    for provider in provider_dict.keys():
        if(provider not in provider_modules.keys()):
            provider_modules[provider] = {}
        if(today not in provider_modules[provider].keys()):
            provider_modules[provider][today] = {}
        provider_modules[provider][today] = getProviderModulesNamespace(provider_dict[provider])
    
    with open("Provider-Module-Namespace.json", "w") as f:    
        json.dump(provider_modules, f)
    f.close()
    
    print("Update NamespaceNums table...")
    sql_insert = ("INSERT INTO NamespaceNums" 
        "(date, aws, azure, google)"
        "VALUES (?,?,?,? )")
    Values = [today, len(provider_modules["aws"][today]), len(provider_modules["azurerm"][today]),len(provider_modules["google"][today])]      
    print(Values)
    cursor.execute(sql_insert, Values)
    connection.commit()
    
    for provider in provider_list:
        print("Update %sTodayNamespaceContribution table.."%provider)
        for namespace in provider_modules[provider][today].keys(): 
            try:
                sql_update = ("UPDATE %sTodayNamespaceContribution set moduleNums= %d WHERE namespace LIKE '%s'"%(provider,provider_modules[provider][today][namespace], namespace))
                cursor.execute(sql_update)
            except:
                sql_insert = ("INSERT INTO %sTodayNamespaceContribution"
                "(namespace, moduleNums)"
                "VALUES (?,?)"%provider)
                Values = [namespace, provider_modules[provider][today][namespace]]
                cursor.execute(sql_insert, Values)
        connection.commit()
    
    for provider in provider_list:
        print("Update %sNamespaceModules Table..."%provider)
        for module in provider_dict[provider].keys():
            try:
                sql_insert = ("INSERT INTO %sNamespaceModules"
                "(namespace,module)"
                "VALUES (?,?)"%provider)
                Values = [provider_dict[provider][module]["namespace"], module]
                print(Values)
                cursor.execute(sql_insert, Values)
            except:
                print("%s Exists"%module)
        connection.commit()
    connection.close()

def updateOwner(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor= connection.cursor()
    ### Update Provider Module owner JSON file
    with open("Provider-Module-Owners.json", "r") as f:
        provider_modules = json.load(f)
    f.close()
    provider_list = ["aws","azurerm","google"]
    for provider in provider_dict.keys():
        if(provider not in provider_modules.keys()):
            provider_modules[provider] = {}
        if(today not in provider_modules[provider].keys()):
            provider_modules[provider][today] = {}
        provider_modules[provider][today] = getProviderModulesOwner(provider_dict[provider])
    
    with open("Provider-Module-Owners.json", "w") as f:    
        json.dump(provider_modules, f)
    f.close()
    
    print("Update OwnerNums table...")
    sql_insert = ("INSERT INTO OwnerNums" 
        "(date, aws, azure, google)"
        "VALUES (?,?,?,? )")
    Values = [today, len(provider_modules["aws"][today]), len(provider_modules["azurerm"][today]),len(provider_modules["google"][today])]      
    print(Values)
    cursor.execute(sql_insert, Values)
    connection.commit()
    
    for provider in provider_list:
        print("Update %sTodayOwnerContribution table.."%provider)
        for owner in provider_modules[provider][today].keys(): 
            try:
                sql_update = ("UPDATE %sTodayOwnerContribution set moduleNums = %d WHERE owner LIKE '%s'"%(provider, provider_modules[provider][today][owner], owner))
                cursor.execute(sql_update)
            except:
                sql_insert = ("INSERT INTO %sTodayOwnerContribution"
                "(owner, moduleNums)"
                "VALUES (?,?)"%provider)
                Values = [owner, provider_modules[provider][today][owner]]
                cursor.execute(sql_insert, Values)
        connection.commit()

    for provider in provider_list:
        print("Update %sownerModules Table..."%provider)
        for module in provider_dict[provider].keys():
            try:
                sql_insert = ("INSERT INTO %sOwnerModules"
                "(owner,module)"
                "VALUES (?,?)"%provider)
                Values = [provider_dict[provider][module]["owner"], module]
                print(Values)
                cursor.execute(sql_insert, Values)
            except:
                print("%s Exists"%module)
        connection.commit()
    connection.close()

def updateDownload(uid, pwd, provider_dict):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    with open("Provider-Module-Downloads.json", "r") as f:
        provider_modules = json.load(f)
    f.close()
    provider_list = ["aws","azurerm","google"]
    for provider in provider_dict.keys():
        if(provider not in provider_modules.keys()):
                provider_modules[provider] = {}
        for module in provider_dict[provider].keys():
            if(module not in provider_modules[provider].keys()):
                provider_modules[provider][module] = {}
            provider_modules[provider][module][today] = provider_dict[provider][module]["downloads"]
    with open("Provider-Module-Downloads.json", "w") as f:    
        json.dump(provider_modules, f)
    f.close()
    
    provider_list = ["aws","azurerm","google"]
    for provider in provider_list:
        print("Update %sModuleDownloads table..."%provider)
        for module in provider_dict[provider].keys():  
            sql_insert = ("INSERT INTO %sModuleDownloads"
            "(date, module, downloads)"
            "VALUES (?,?,?)"%provider)
            Values = [today, module, provider_dict[provider][module]["downloads"]]
            cursor.execute(sql_insert, Values)
          
        connection.commit()
    
    for provider in provider_list:
        sql_group = ("SELECT sum(downloads) as totalDownloads FROM %sModuleDownloads GROUP BY date HAVING date='%s'"%(provider, yesterday))
        cursor.execute(sql_group)
        row = cursor.fetchone()
        yesterday_downloads = row[0]
        
        sql_group = ("SELECT sum(downloads) as totalDownloads FROM %sModuleDownloads GROUP BY date HAVING date='%s'"%(provider, today))
        cursor.execute(sql_group)
        row = cursor.fetchone()
        today_downloads = row[0]
        change = today_downloads-yesterday_downloads
        print("%s Total Downloads:%d"%(provider, today_downloads))
        print("%s Total Downloads Increase:%d"%(provider, change))
        sql_update = ("UPDATE TotalDownloadsChange SET difference = %d WHERE provider LIKE '%s'"%(change, provider))
        cursor.execute(sql_update)
        sql_update = ("UPDATE TotalDownloads set %s = %d"%(provider, today_downloads))
        cursor.execute(sql_update)
    connection.commit()
   
    for provider in provider_list:
        print("Update %sModuleDownloadsChange..."%provider)
        for module in provider_dict[provider].keys():
            sql_select = ("SELECT downloads FROM %sModuleDownloads WHERE date = '%s'AND module = '%s'"%(provider, yesterday, module))
            cursor.execute(sql_select)
            row = cursor.fetchone()
            if(row):
                yesterday_downloads = row[0]
            else:
                continue

            change = provider_dict[provider][module]["downloads"] - yesterday_downloads
            print(module, change)
            try:
                sql_update = ("UPDATE %sModuleDownloadsChange SET difference = %d WHERE module LIKE '%s'"%(provider, change, module))
                cursor.execute(sql_update)
            except:
                sql_insert = ("INSERT INTO %sModuleDownloadsChange"
                "(module, difference)"
                "VALUES (?,?)"%provider)
                Values = [module, change]
                cursor.execute(sql_insert, Values)
        connection.commit()
    connection.close()
    del provider_modules#, today_modules
    print("Update Provider Module Downloads DONE!\n")

def computeVersionUpdate(published):
    if(len(published) == 1):
        return -1
    allversions = len(published)-1
    alldays = len(getEveryDay(published[-1], published[0]))-1
    return alldays/allversions

def v2num(version): 
    nums = version.split('.')
    if(nums == []):
        return -1
    s = 0
    f = 1   
    if(len(nums)==4):
        s+=0.1*int(nums[3])
        nums.pop(3)

    for num in reversed(nums):
        if(re.search('[a-zA-z]', num)):
            num = num[0]
        if(num == ''):
            continue
        n = int(num)
        s += n*f 
        f *= 100    
    return s
def getProviderModulesVersions(modules):
    provider_modules = {}
    rgx_source = r'(.*)/'
    for module in modules:
        provider = module["provider"]
        if(provider not in provider_modules.keys()):
            provider_modules[provider] = {}
        module_source = re.findall(rgx_source,module["id"])[0]
        provider_modules[provider][module_source] = module["version"]
    return provider_modules

def processOriginalSQL(uid,pwd):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
                "Server=terraformmodules.database.windows.net;"
                "Database=terraformModules;"
                "uid=%s;pwd=%s"%(uid,pwd))
    cursor = connection.cursor()
    sql_select = "SELECT * from OriginalModules WHERE date = '2018-08-15'"
    cursor.execute(sql_select)
    items = cursor.fetchall()
    provider_dict = {}
    for item in items:
        itemList = {}
        itemList["source"] = item[1]
        itemList["downloads"] = item[5]
        itemList["namespace"] = item[3]
        itemList["verified"] = item[6]
        module_versions = {}
        start = time.time()
        for version in getVersions(itemList["source"]): 
            module_versions[version],owner = getPublished(itemList["source"],version)
        end = time.time()
        print(itemList["source"] + ": %s"%(end - start))
        itemList["owner"] = owner
        itemList["versions"] = module_versions
        if(item[2] not in provider_dict.keys()):
           provider_dict[item[2]] = {}
        provider_dict[item[2]][itemList["source"]] = itemList
    return provider_dict

def main():
    run_opt = 2 
    uid = "zunli"
    pwd = ""
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_opt', type=str, default=run_opt, help='(Required) 1 for create Tables, 2 for update Tables')
    parser.add_argument('--uid', type=str, default=uid, help='(Optional) Define Azure SQL Server database uid')
    parser.add_argument('--pwd', type=str, default=pwd, help='(Required) Define Azure SQL Server password for uid.')
    args = parser.parse_args()

    if(args.run_opt):
        run_opt = args.run_opt
    if(args.uid):
        uid = args.uid
    if(args.pwd):
        pwd = args.pwd

    url = 'https://registry.terraform.io/v1/modules?limit=100'
    results = get_results(url)
    try:
        next_offset = results["meta"]["next_offset"]
    except:
        next_offset = []
    print("Offset: 0")
    modules = results["modules"]
    while(next_offset):
        url = 'https://registry.terraform.io/v1/modules?limit=100&offset=%s'%next_offset
        results = get_results(url)
        try:
            next_offset = results["meta"]["next_offset"]
        except:
            next_offset = []
        print("Offset: %s"%results["meta"]["current_offset"])
        modules+= results["modules"]
    
    provider_dict = processModules(modules)
    
    #provider_dict = processOriginalSQL(connection)
    with open("%sprovider_dict.json"%today, "wt") as f:
        json.dump(provider_dict, f)
    f.close()
    '''
    with open("%sprovider_dict.json"%today, "r") as f:
        provider_dict = json.load(f)
    f.close()
    '''
    if(run_opt == 1): # Create table  
        createOriginalSQL(uid, pwd, provider_dict)
        createModulesSQL(uid, pwd, provider_dict)
        createModuleNumSQL(uid,pwd)
        createNamespaceSQL(uid,pwd)
        createOwnerSQL(uid,pwd)
        createDownloadSQL(uid, pwd, provider_dict)
        createVersionSQL(uid, pwd, provider_dict)
    if(run_opt == 2): # Update table
        updateOriginalSQL(uid, pwd, provider_dict)
        updateModulesSQL(uid, pwd, provider_dict)
        updateVersions(uid, pwd, provider_dict)
        updateModuleNums(uid, pwd, provider_dict)
        updateNamespace(uid, pwd, provider_dict)
        updateOwner(uid, pwd, provider_dict)
        updateDownload(uid, pwd, provider_dict)
                   

if __name__ == '__main__':
    main()    