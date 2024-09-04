class OpsUtil(ServerUtil):

    def __init__(self, client, ssh, dbConfig, workers=None, isUtil=False, sqlType='pgsql'):
        super(OpsUtil, self).__init__(ssh, 'traffic_ops', 'traffic_ops', isUtil=isUtil)
        global MAX_WORKERS
        self.client = client
        self.sqlType = sqlType
        self.dbConfig = dbConfig
        if sqlType == 'mysql':
            self.mysqlConfig = dbConfig
        if sqlType == 'pgresql':
            self.pgsql = dbConfig

        if workers:
            MAX_WORKERS = workers

        self.api_util = OpsAPIUtil(self.client)

    def __getDomainProfileId(self, name):
        profiles = self.__getDomains()
        for profile in profiles['response']:
            if name == profile['profileName']:
                return profile['profileId']
        logging.error("No domain profile " + name + "found in config!!!")
        return 0

    def __getDomains(self):
        path = '/api/3.0/cdns/domains'
        response = self.client.get(path)
        if response:
            config = JsonUtil.json_str(response.text)
            logging.debug('Get profiles: \n' + str(config))
            return config
        return None

    def setDSOptions(self, ds_temp, ds_option):
        logging.debug("delivery service template: " + str(ds_temp))
        logging.debug("delivery service option: " + str(ds_option))
        ds = ds_temp['template']
        for opt in ds_option.keys():
            if isinstance(ds_option[opt], dict):
                if 'options' in ds_option[opt]:
                    opt_sel = ds_option[opt]['options']
                    ds[opt] = ds_temp['options'][opt][opt_sel]
                elif 'profile' in ds_option[opt]:
                    domain_profile = ds_option[opt]['profile']
                    ds[opt] = self.__getDomainProfileId(domain_profile)
                else:
                    logging.error("Invalid option: " + str(ds_option))
            else:
                ds[opt] = ds_option[opt]
        logging.debug("delivery service: " + str(ds))
        return ds
    
    def getOpsTypeName(self, useInTable, objtype):
        name_map = {
                     "server" : {
                        "router": "CCR",
                        "edge": "EDGE",
                        "mid": "MID",
                        "origin": "ORG",
                        "monitor": "RASCAL",
                        "riak": "RIAK",
                        "analytics": "TRAFFIC_ANALYTICS",
                        "ops": "TRAFFIC_OPS",
                        "opsdb": "TRAFFIC_OPS_DB",
                        "stats": "TRAFFIC_STATS",
                       },
                     "cachegroup":{
                       "edge": "EDGE_LOC",
                       "mid": "MID_LOC",
                       "origin": "ORG_LOC",
                       "router": "MID_LOC",
                       "monitor": "MID_LOC" 
                       },
                     "deliveryservice":{
                       "http": "HTTP",
                       "dns": "DNS",
                       "live": "HTTP_LIVE",
                       "nocache": "HTTP_NO_CACHE"
                       }
                   }
        return name_map[useInTable][objtype]

    def login(self, user, password):
        auth = {
                'u': user,
                'p': password,
                }
        auth_str = json.dumps(auth)
        path = '/api/3.0/user/login'
        logging.info('login ops with auth: ' + auth_str + ', path: ' +client.post(path, auth_str, headers)
        if response is None or not OpsSetting.checkResponse(path, response):
            logging.error('failed to login the ops with path: ' + path)
            return False

        return True

    def setVault(self, username, password):
        path = '/opt/traffic_ops/app/conf/production/riak.conf'
        content = self.sshUtil.fileGetJson(path)
        content['user'] = username
        content['password'] = password
        return self.sshUtil.fileDump(path, JsonUtil.str_json(content, 4), False)

    def associateWithVault(self, username, password):
        return self.setVault(username, password)


    #CRUD Delivery Service
    def createDeliveryService(self, config):
        ds = self.api_util.createDsAssert(config)
        if not ds:
            logging.error('failed to create delivery service ' + config['displayName'])
            return None

        return ds[0]

    def getDeliveryService(self, name, uid=None):
        ds = self.getDsByNameByApi(name, uid)
        if ds:
            return DsConfig(ds)
        logging.error('failed to get delivery service ' + name)
        return ds

    def updateDeliveryService(self, config, name, uid=None):
        if 'id' not in config and uid == None:
            ds = self.getDeliveryService(name)
            uid = ds['id']
            config['id'] = uid

        if 'id' in config and config['id'] != uid:
            logging.error('failed to update delivery service due to mismatch in id')
            return None
        updateDs = self.api_util.updateDsAssert(config, config['id'])
        ds = self.getDeliveryService(name)        
        return ds
    
    def deleteDeliveryService(self, name, uid=None):
        if uid == None:
            ds = self.getDeliveryService(name)
            uid = ds['id']
        return self.deleteDsByApi(uid)

    def assignServersToDs(self, dsName, server, ipIds):
        dsConfig = self.getDeliveryService(dsName)
        if not dsConfig:
            return None
        self.assignServersToDsByApi(dsConfig,server, ipIds)
        ds = self.getDeliveryService(dsName)
        if ds:
            return ds
        logging.error('failed to assign the servers to ds ' + dsName)
        return None    

    def getAssignedServers(self, dsSetting, edgeServers):
        servers = self.api_util.getAssignedServerinDs(dsSetting['id'])
        if not servers:
            logging.error('no assigned servers in ds' + dsSetting['xmlId'])
        assignedEdges = list()
        for srv in servers:
            assignedEdges.append(srv['hostName'])
        return assignedEdges
            
    def updatePrepManifest(self, dsId):
        path = '/ds/' + str(dsId) + '/update_manifest'
        response = self.client.post(path, None)
        if response is None or not OpsSetting.checkResponse(path, response):
            logging.error('failed to update manifest to ds with path: ' + path)
            return False
        return True

    #CRUD profile
    def createProfile(self, config):
        name = config['name']
        profile = self.api_util.addProfileAssert(config)
        if name == profile['name']:
            profile = self.api_util.getProfileAssert(name, 'name')
            profileById = self.api_util.getProfilebyIdAssert(profile['id'])
            return profileById
        else:
          logging.error('failed to create profile ' + str(name))
          return None 

    def getProfile(self, name):
        profile = self.api_util.getProfileAssert(name, 'name')
        if profile:
            return profile
        else:
            logging.error('failed to get profile ' + str(name))
            return None
         
    def getProfileById(self, uid):
        profileById = self.api_util.getProfilebyIdAssert(uid)
        if profileById:
            return profileById
        else:
            logging.error('failed to get profile with id - ' + uid)
            return None

    def updateProfile(self, config, name, uid=None):
        if not uid:
            profile = self.api_util.getProfileAssert(name, 'name')
            uid = profile['id']

        profile = self.api_util.updateProfileAssert(config, uid)
        if not profile:
            logging.error('failed to update profile ' + str(name))
            return None
        return profile
    
    def deleteProfile(self, name, uid=None):
        if not uid:
            profile = self.api_util.getProfileAssert(name,'name')
            uid = profile['id']
        profile = self.api_util.deleteProfileAssert(uid)
        if not  profile:
            logging.error('failed to delete profile ' + str(name))
            return None
        return profile
         
    def __buildOpsObj(self, nameId, config):
        if not config:
            return None

        obj = OpsObj(nameId)
        obj.update(config)
        return obj

    #CRUD server
    def createServer(self, config, options=None):
        server = self.api_util.addServerAssert(config)
        if not server:
            logging.error('ops - failed to create server ' + config['name'])            
            return None
        return server

    def getServer(self, name, uid=None):
        if not uid:
            server = self.api_util.getAllServerAssert(name, 'hostName')
            if server:
                uid = server['id']
            else:
                logging.error('ops - failed to get server with name: ' + str(name) + ', uid: ' + str(uid))
                return None
        server = self.api_util.getServerAssert(uid)
        if not server:
            logging.error('ops - failed to get server with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return server

    def getAllServers(self):
        serverList = self.api_util.getAllServerAssert()
        if not serverList:
            logging.error('ops - failed to get servers list ')
            return None
        return serverList

    def updateServer(self, config, name, uid=None):
        if 'id' not in config and not uid:
            server = self.api_util.getAllServerAssert(name, 'hostName')
            if server:
                config['id'] = server['id']
        elif 'id' not in config and uid:
            config['id'] = uid
        
        server = self.api_util.updateServerAssert(config)
        if not server:
            logging.error('ops - failed to update server with name: ' + str(name))
            return None
        return server

    def deleteServer(self, name, uid=None):
        if not uid:
            server = self.api_util.getAllServerAssert(name, 'hostName')
            if server:
                uid = server['id']
        server = self.api_util.delServerAssert(uid)
        if not server:
            logging.error('ops - failed to delete server with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return server

    def onlineServer(self, name, uid=None, config=None):
        if not config:
            config = self.getServer(name, uid)
            if not config:
                logging.error('ops - failed to get server: ' + str(name))
                return None

        ctype = config['type']
        if ctype == 'EDGE':
            status = 'REPORTED'
        else:
            status = 'ONLINE'

        newConfig = {'admin_status': status}
        return self.updateServer(newConfig, name, uid)

    def offlineServer(self, name, uid=None, config=None):
        if not config:
            config = self.getServer(name, uid)
            if not config:
                logging.error('ops - failed to get server: ' + str(name))
                return None

        ctype = config['type']
        if ctype == 'EDGE':
            status = 'ADMIN_DOWN'
        else:
            status = 'OFFLINE'
        newConfig = {'admin_status': status}
        return self.updateServer(newConfig, name, uid)

    #CRUD Cache-Group
    def createCacheGroup(self, config):
        cg = self.api_util.addCgAssert(config)
        if not cg:
            logging.error('ops - failed to create cache group ' + config['name'])            
            return None
        return cg

    def getCacheGroup(self, name, uid=None):
        
        if not uid:
            cg = self.api_util.getAllCgAssert(name, 'name')
            if cg:
                uid = cg['id']
            else:
                logging.error('ops - failed to get cg with name: ' + str(name) + ', uid: ' + str(uid))
                return None

        cg = self.api_util.getCgAssert(uid)
        if not cg:
            logging.error('ops - failed to get cg with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return cg

    def getAllCacheGroups(self):
        cgList = self.api_util.getAllCgAssert()
        if not cgList:
            logging.error('ops - failed to get cache group list ')
            return None
        return cgList

    def updateCacheGroup(self, config, name, uid=None):
        if 'id' not in config and not uid:
            cg = self.api_util.getAllCgAssert(name, 'name')
            if cg:
                uid = cg['id']
                config['id'] = uid

        if 'id' not in config and uid:
            config['id'] = uid
       
        if uid and  config['id'] != uid:
            logging.error('ops - failed to update cache group due to mismatch in id')
            return None
        cg = self.api_util.updateCgAssert(config)
        if not cg:
            logging.error('ops - failed to update cg with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return cg

    def deleteCacheGroup(self, name, uid=None):
        if not uid:
            cg = self.api_util.getAllCgAssert(name, 'name')
            if cg:
                uid = cg['id']
        cg = self.api_util.delCgAssert(uid)
        if not cg:
            logging.error('failed to delete cache group with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return cg

    def activateEdgeCacheGroups(self, ccrProfile, idList):
        cdnParam = self.getProfileParameter(ccrProfile, 'CDN_name')
        if not cdnParam:
            logging.error('no CDN_name parameter found in ccrProfile: ' + ccrProfile)
            return False

        path = '/cachegroupparameter/create'
        for cgid in idList:
            config = {'parameter': cdnParam['id'], 'cachegroup': cgid}
            response = self.client.post(path, config)
            if not self.api_util.checkResponse(path, response):
                logging.error('failed to add the cachegroup to cdn: ' + cgid)
                return False
        return True

    #CRUD division
    def createDivision(self, config):
        div = self.api_util.addDivAssert(config)
        if not div:
            logging.error('failed to create division ' + config['name'])            
            return None
        return div

    def getDivision(self, name, uid=None):
        if not uid:
            div = self.api_util.getAllDivAssert(name, 'name')
            if div:
                uid = div['id']
            else:
                logging.error('ops - failed to get division with name: ' + str(name) + ', uid: ' + str(uid))
                return None

        div = self.api_util.getDivAssert(uid)
        if not div:
            logging.error('failed to get division with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return div

    def updateDivision(self, config, name, uid=None):
        if not uid:
            div = self.api_util.getAllDivAssert(name, 'name')
            if div:
                uid = div['id']

        if config['id'] != uid:
            logging.error('failed to update division due to mismatch in id')
            return None
        div = self.api_util.updateDivAssert(config)
        if not div:
            logging.error('failed to update division with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return div

    def deleteDivision(self, name, uid=None):
        if not uid:
            div = self.api_util.getAllDivAssert(name, 'name')
            if div:
                uid = div['id']
        div = self.api_util.delDivAssert(uid)
        if not div:
            logging.error('failed to delete division with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return div

    #CRUD region
    def createRegion(self, config):
        reg = self.api_util.addRegAssert(config)
        if not reg:
            logging.error('failed to create region ' + config['name'])            
            return None
        return reg

    def getRegion(self, name, uid=None):
        if not uid:
            reg = self.api_util.getAllRegAssert(name, 'name')
            if reg:
                uid = reg['id']
            else:
                logging.error('ops - failed to get division with name: ' + str(name) + ', uid: ' + str(uid))
                return None

        reg = self.api_util.getRegAssert(uid)
        if not reg:
            logging.error('failed to get division with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return reg

    def updateRegion(self, config, name, uid=None):
        if not uid:
            reg = self.api_util.getAllRegAssert(name, 'name')
            if reg:
                uid = reg['id']

        if config['id'] != uid:
            logging.error('failed to update region due to mismatch in id')
            return None
        reg = self.api_util.updateRegAssert(config)
        if not reg:
            logging.error('failed to update region with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return reg

    def deleteRegion(self, name, uid=None):
        if not uid:
            reg = self.api_util.getAllRegAssert(name, 'name')
            if reg:
                uid = reg['id']
        reg = self.api_util.delRegAssert(uid)
        if not reg:
            logging.error('failed to delete division with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return reg

    #CRUD location
    def createLocation(self, config):
        loc = self.api_util.addLocationAssert(config)
        if not loc:
            logging.error('failed to create location ' + config['name'])            
            return None
        return loc

    def getLocation(self, name, uid=None):
        if not uid:
            loc = self.api_util.getAllLocationAssert(name, 'name')
            if loc:
                uid = loc['id']
            else:
                logging.error('ops - failed to get location with name: ' + str(name) + ', uid: ' + str(uid))
                return None

        loc = self.api_util.getLocationAssert(uid)
        if not loc:
            logging.error('failed to get location with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return loc

    def updateLocation(self, config, name, uid=None):
        if not uid:
            loc = self.api_util.getAllLocationAssert(name, 'name')
            if loc:
                uid = loc['id']

        if config['id'] != uid:
            logging.error('failed to update location due to mismatch in id')
            return None
        loc = self.api_util.updateLocationAssert(config)
        if not loc:
            logging.error('failed to update location with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return loc

    def deleteLocation(self, name, uid=None):
        if not uid:
            loc = self.api_util.getAllLocationAssert(name, 'name')
            if loc:
                uid = loc['id']
        loc = self.api_util.delLocationAssert(uid)
        if not loc:
            logging.error('failed to delete location with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return loc

    #CRUD parameter
    def addParameterToProfile(self, profileName, paramId):
        profile = self.getProfile(profileName)
        if not profile:
            return None 
        config = {'profileId': profile['id'], 'paramIds': [paramId]}
        profile =  self.api_util.addProfileParameterAssert(config)
        if not profile:
            logging.error('failed to add profile parameter : ' + str(paramId) + ', for profile: ' + str(profileName))
            return None
        return profile

    def deleteParameterFromProfile(self, profileName, paramId):
        profile = self.getProfile(profileName)
        if not profile:
            return None
        
        profile = self.api_util.deleteProfileParameterAssert(profile['id'], paramId)
        if not profile:
            logging.error('failed to delete profile parameter : ' + str(paramId) + ', for profile: ' + str(profileName))
            return None

        return profile


    def createParameter(self, param, profile=None):
        if profile is None:
            profile = param.get('profiles')

        if profile is None:
            logging.error('profile field must be set, cur param: ' + str(param))
            return None

        newParam = dict()
        newParam.update(param)
        if 'profiles' in newParam:
            del newParam['profiles']
        param = self.api_util.addParametersAssert(newParam)
        if not param:
            logging.error('create parameter failed with param: ' + str(newParam))
            return None

        #try to create the profileparameter
        if profile:
            profile = self.addParameterToProfile(profile, param['id'])
            if not profile:
                logging.error('addParameterToProfile failed')
                return None
        param = self.api_util.getParametersbyIdAssert(param['id'])
        if not param:
            logging.error('create parameter failed with param: ' + str(newParam))
            return None
        return param

    def findParameters(self, param, profile=None):
     logging.info("find param : %s in profile %s",param, profile)
     dictparam=dict()
     nparam=[]
 
     if profile:
         nparam = self.api_util.getProfileParameterAssert(profile)
         rparam = param
         if 'profiles' in rparam.keys():
             del rparam['profiles']
         for eparam in nparam:
             if JsonUtil.contains(eparam, rparam): 
                 dictparam.update(eparam)
     elif 'id' in param.keys(): 
         nparam = self.api_util.getParametersbyIdAssert(param['id'])
         for eparam in nparam:
             if JsonUtil.contains(eparam, param): 
                 dictparam.update(eparam)
     else:
         nparam = self.api_util.getParametersAssert()
         for eparam in nparam:
             if JsonUtil.contains(eparam, param): 
                 dictparam.update(eparam)

     logging.debug("param list :%s", nparam ) 
     if not dictparam:
         logging.error('cannot find the config with param: ' + str(param))
         return None
     retList = [dictparam]
     logging.info(" param config found : %s ",retList)  
     return retList


    def getParameter(self, param, uid=None, profile=None):
        if 'uid' != None and 'id' not in param.keys():
            param['id'] = uid
           
        paramList = self.findParameters(param, param['profiles'])
        nparam = paramList[0] 
        if not nparam:
            logging.error('cannot find parameter ' + str(param) + 'uid ' + str(uid) + 'profile ' + profile)
            return None
        return nparam 
    
    def updateParameter(self, newConfig, oldConfig, uid=None, profile=None):
        if 'profiles' not in newConfig and profile is None:
            logging.error('profile field must be set in the param: ' + str(newConfig))
            return None
        if profile is not None:
            newConfig['profiles'] = profile

        orgProfile = newConfig['profiles']
        newParam = dict()
        newParam.update(newConfig)
        del newParam['profiles']
        if not uid:
            oldParam = self.getParameter(oldConfig, profile=profile)
            if not oldParam:
                return None

            uid = oldParam['id']
        updated = self.api_util.updateParametersAssert(newParam, uid)
        if not updated:
            return None

        updated['profiles'] = orgProfile
        updated = self.getParameter(updated, uid, orgProfile)
        if not updated:
            return None

        if not JsonUtil.contains(updated, newParam):
            logging.error('the param is not updated successfully, you may need to open the debug level to check the details')
            return None

        return updated

    def deleteParameter(self, param, uid=None, profile=None):

        if profile:
            param['profiles'] = profile
        
        paramId = uid
        #remove the profileparameter first
        if 'profiles' in param and param['profiles'] != 'NONE':
            paramSetting = self.getParameter(param, uid, profile=profile)
            if not paramSetting:
                return False
            paramId = paramSetting['id']  
            profile = self.deleteParameterFromProfile(param['profiles'], paramSetting['id'])
            if not profile:
                return False
            param['profiles'] = 'NONE'
        newConfig = self.api_util.deleteParametersAssert(paramId)

        if not newConfig:
            logging.error('cannot delete parameter ' + str(param) + 'uid, ' + str(uid) + 'profile, ' + str(profile))
        return newConfig

    def getProfileParameter(self, profile, name, configFile=None, uid=None):
        if uid:
            return self.getParameter(None, uid, profile=profile)
        else:
            cond = {'profiles': profile, 'name': name}
            if configFile is not None:
                cond['configFile'] = configFile
            paramList = self.findParameters(cond, profile=profile)
            if paramList is None:
                logging.error(" No param found with "+name+" name")
                return None
            elif len(paramList) > 1:
                logging.error('getProfileParameter only used for unique parameter: ' + name + ' in the profile: ' + profile + ', but more parameters found: ' + str(paramList))
                return None
            return paramList[0]

    def updateProfileParameter(self, profile, newConfig, name, configFile=None, uid=None):
        oldConfig = self.getProfileParameter(profile, name, configFile, uid)
        if not oldConfig:
            return (None, None)

        newConfig['profiles'] = profile
        for key,value in oldConfig.items():
            if key not in newConfig.keys() and key != 'lastUpdated':
                newConfig[key] = value
        return (self.updateParameter(newConfig, oldConfig, oldConfig['id'], profile=profile), oldConfig)

    def updateOrCreateProfileParameter(self, profile, newConfig, name, configFile=None, uid=None):
        if not profile:
            logging.error('profile must be set in updateProfileParameter, newConfig: ' + str(newConfig))
            return (None, None)

        if 'profiles' not in newConfig:
            newConfig['profiles'] = profile
        if 'name' not in newConfig:
            newConfig['name'] = name

        (param, oldParam) = self.updateProfileParameter(profile, newConfig, name, configFile, uid)
        if param:
            return (param, oldParam)

        if oldParam:
            if not self.deleteParameterFromProfile(profile, oldParam['id']):
                logging.error('delete the old param failed')
                return (None, oldParam)

        #try to find if the param already exists
        del newConfig['profiles']
        logging.info('try to find the parameter: ' + str(newConfig))
        paramList = self.findParameters(newConfig)
        if paramList:
            param = paramList[0]
            pset = self.addParameterToProfile(profile, param['id'])
            if not pset:
                logging.error('attach param: ' + str(param) + ' to profile ' + profile + ' failed')
                return (None, None)
            return (param, None)

        logging.info('parameter not exists, try to create it with config: ' + str(newConfig))
        #try create the parameter
        newConfig['profiles'] = profile
        newConfig['secure'] = False
        if configFile is not None:
            newConfig['configFile'] = configFile
        param = self.createParameter(newConfig, profile=profile)
        oldParam = None

        return (param, oldParam)

    def getCrConfig(self, chooseCDN):
        path = '/CRConfig-Snapshots/' + chooseCDN + '/CRConfig.json'
        response = self.client.get(path)
        if response:
            config = JsonUtil.json_str(response.text)
            logging.debug('Get profiles: \n' + str(config))
            return config
        return None


    #CRUD cdn
    def hasCdnApi(self):
        response = self.api_util.getCdn()
        if not response:
            return False
        return response

    def findCdns(self):
        cdns = self.hasCdnApi()
        if cdns:
            return cdns
        else:
            return list()

    def createCdn(self, config):
        cdn = self.api_util.addCdnAssert(config)
        if not cdn:
            logging.error('failed to create cdn ' + config['name'])            
            return None
        return cdn

    def getCdn(self, name, uid=None):
        if not uid:
            cdn = self.api_util.getCdnAssert(name, 'name')
            if cdn:
                uid = cdn['id']
            else:
                logging.error('ops - failed to get cdn with name: ' + str(name) + ', uid: ' + str(uid))
                return None
        cdn = self.api_util.getCdnAssert(uid)
        if not cdn:
            logging.error('failed to get cdn with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return cdn

    def updateCdn(self, config, name, uid=None):
        if not uid:
            cdn = self.api_util.getCdnAssert(name, 'name')
            if cdn:
                uid = cdn['id']

        if 'id' in config and config['id'] != uid:
            logging.error('failed to update cdn due to mismatch in id')
            return None
        cdn = self.api_util.updateCdnAssert(config, uid)
        if not cdn:
            logging.error('failed to update cdn with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return cdn

    
    def deleteCdn(self, name, uid=None):
        cdn = None
        if not uid:
            cdn = self.api_util.getCdnAssert(name, 'name')
            if cdn:
                uid = cdn['id']
        if uid:
            cdn = self.api_util.delCdnAssert(uid)
        if not cdn:
            logging.error('failed to delete cdn with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return cdn

    def createOrUpdateCdn(self, config):
        cdn = self.getCdn(config['cdn_data.name'])
        if cdn:
            logging.info('cdn: ' + str(config) + ' already exists, return it directly')
            return cdn

        return self.createCdn(config)

    #CRUD user
    def createUser(self, config):
        usr = self.api_util.addUserAssert(config)
        if not usr:
            logging.error('failed to create user ' + config['name'])            
            return None
        return usr

    def createSimpleUser(self, name, password, role, email=None):
        if email is None:
            email = name + '@cisco.com'
        config = {
                'tm_user.full_name': name,
                'tm_user.username': name,
                'tm_user.email': email,
                'tm_user.local_passwd': password,
                'tm_user.confirm_local_passwd': password,
                'tm_user.role': role,
            }

        return self.createUser(config)

    def getUser(self, name, uid=None):
        if not uid:
            usr = self.api_util.getUserAssert(name, 'name')
            if usr:
                uid = cdn['id']
            else:
                logging.error('ops - failed to get user with name: ' + str(name) + ', uid: ' + str(uid))
                return None
        usr = self.api_util.getUserAssert(uid)
        if not usr:
            logging.error('failed to get user with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return usr

    def updateUser(self, config, name, uid=None, options=None):
        #setting = OpsUtil.__getSetting('user')
        #if options is None:
        #    options = dict()

        #sl = options.get('skipRegexList', [])
        #sl.append('deliveryservices')
        #options['skipRegexList'] = sl
        #return setting.update(self.client, config, name, uid, options=options)
        
        #TODO - skipRegexList
        if not uid:
            usr = self.api_util.getUserAssert(name, 'name')
            if usr:
                uid = usr['id']

        if config['id'] != uid:
            logging.error('failed to update cdn due to mismatch in id')
            return None
        usr = self.api_util.updateUserAssert(config, uid)
        if not usr:
            logging.error('failed to update cdn with name: ' + str(name) + ', uid: ' + str(uid))
            return None
        return usr


    def updateUserPassword(self, name, oldPassword, newPassword):
        user = self.getUser(name)
        if not user:
            logging.error('failed to get user with name: ' + name)
            return None

        user['tm_user.local_passwd'] = newPassword
        user['tm_user.confirm_local_passwd'] = newPassword
        return self.updateUser(user, name, user['id'], options={'skipRegexList': ['.*\.id$']})
    
    def deleteUser(self, name, uid=None):
        # has no delete api in ops
        # delete by mysql cmd
        if uid is None:
            user = self.getUser(name)
            if user is None:
                logging.info('user ' + str(name) + ' already deleted')
                return True
            uid = user['id']

        cmd = 'mysql -u {0} --password={1} {2} -e "delete from log where tm_user={3};"'.format(self.mysqlConfig['user'], 
                self.mysqlConfig['passwd'], self.mysqlConfig['db'], uid)

        self.sshUtil.execCheck(cmd)

        cmd = 'mysql -u {0} --password={1} {2} -e "delete from tm_user where id={3};"'.format(self.mysqlConfig['user'], 
                self.mysqlConfig['passwd'], self.mysqlConfig['db'], uid)

        return self.sshUtil.execCheck(cmd)

    def genSslKeys(self, cdn_name, cdn_domain, ds_id, ds_xmlid, sub_domain, routing_type, to_ssl_fields, version):
        path = '/api/3.0/deliveryservices/sslkeys/generate'
        return self.__createSslKeys(path, cdn_name, cdn_domain, ds_id, ds_xmlid, sub_domain, routing_type, to_ssl_fields, version)

    def addSslKeys(self, cdn_name, cdn_domain, ds_id, ds_xmlid, sub_domain, routing_type, to_ssl_fields, version):
        path = '/api/3.0/deliveryservices/sslkeys/add'
        return self.__createSslKeys(path, cdn_name, cdn_domain, ds_id, ds_xmlid, sub_domain, routing_type, to_ssl_fields, version)

    def getSslHostName(self, sub_domain, domain_name, routing_type):
        up_type = routing_type.upper()
        if up_type == 'HTTP':
            hostname = '*.' + sub_domain + '.' + domain_name
        elif up_type == 'DNS':
            hostname = 'edge.' + sub_domain + '.' + domain_name
        else:
            logging.error('Unsupported routing type %s' % routing_type)
            return None

        return hostname

    def __createSslKeys(self, path, cdn_name, cdn_domain, ds_id, ds_xmlid, sub_domain, routing_type, to_ssl_fields, version):
        hostname = self.getSslHostName(sub_domain, cdn_domain, routing_type)
        if not hostname:
            logging.error('Failed to create ssl keys, Unsupported routing type %s' % routing_type)
            return False

        ssl_obj = {
            "key": 'ds_%s' % ds_id,
            "version": version,
            "hostname": hostname,
            "deliveryservice": ds_xmlid,
            "cdn": cdn_name,
               }
        ssl_obj.update(to_ssl_fields)

        rsp = self.client.post(path, json.dumps(ssl_obj), headers={'Content-Type': 'application/json'})
        if rsp.status_code != 200:
            return False

        return True

    def delSSLkey(self, name):
        path = '/api/3.0/deliveryservices/xmlId/{}/sslkeys/delete.json'.format(name)
        logging.info('delete SSLkey for deliveryservices: ' + name + ', path: ' + path)

        response = self.client.get(path)
        if response.status_code == 200:
            return True
        else:
            logging.error('failed to deleted ssl keys for deliveryservices: ' + name)
        return False

    # ......

    # commit any config change in ops
    def commitConfigChange(self, cdn):
        return self.__diffCRConfig(cdn)

    def dumpDb(self, bk_path=None):
        if not bk_path:
            bk_path = '/tmp/db_bkup_' + str(uuid.uuid4())[:6]
        cfg = self.dbConfig
        if self.sqlType == 'mysql':
            ret = self.sys_util.mysqlBackup(cfg['user'], cfg['passwd'], cfg['db'], bk_path)
        else:
            ret = self.sys_util.pgsqlBackup(cfg['user'], cfg['passwd'], cfg['db'], bk_path)
        if not ret:
            return None

        return bk_path

    def restoreDb(self, bk_path):
        cfg = self.dbConfig
        if self.sqlType == 'mysql':
            return self.sys_util.mysqlRecover(cfg['user'], cfg['passwd'], cfg['db'], bk_path)
        else:
            ret = self.sys_util.pgsqlRecover(cfg['user'], cfg['passwd'], cfg['db'], bk_path)
            ret &= self.restartService()
            return ret

    def delDbBkup(self, bk_path):
        return self.sys_util.execCheck('rm -rf %s' % bk_path)

    def __diffCRConfig(self, chooseCDN):
        path = '/api/3.0/snapshot?cdn=' + chooseCDN
        payload = {}
        response = self.client.put(path, payload)
        if response is None or not OpsSetting.checkResponse(path, response):
            return False

        return True


    def __checkResponse(self, response):
        try:
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError as e:
            logging.error("You get an HTTPError: " + e.message)
            return False

    # CRUD Delivery Service by Ops Restful API
    def createDsByApi(self, config):
        path = '/api/3.0/deliveryservices'
        response = self.client.post(path, json.dumps(config))
        if response is None or not OpsSetting.checkResponse(path, response):
            logging.error('failed to add ds with path: ' + path)
            return None
        # dsConfig = self.getDsByApi(config['id'])
        if 'response' in response.json():
            return response.json()['response'][0]
        else:
            logging.error('failed to add ds with path: ' + path)
            return None

    def getDsByApi(self, ds):
        path = '/api/3.0/deliveryservices?id=%s' % ds['id']
        response = self.client.get(path)
        if response.status_code == 200 and 'response' in response.json():
            return response.json()['response'][0]
        else:
            logging.error('failed to get deliveryservices: ' + ds['id'])

        return False

    def getDsByNameByApi(self, name, uid = None):
        #path = '/api/3.0/deliveryservices/'
        #response = self.client.get(path)
        #if response.status_code == 200 and 'response' in response.json():
        #    rsp = response.json()['response']
        #    for ds in rsp:
        #        if ds['xmlId'] == name:
        if uid:
            ds = self.api_util.getDsByIdAssert(uid)
        else:
            ds = self.api_util.getDsAssert( match=name, key='xmlId')
        if not ds:
            return None
        urlTagList = ds['exampleURLs']
        if not urlTagList:
            logging.info('no urlTagList in the ds page: ' + ds['xmlId'])
            return ds

        logging.debug('found urlTagList: ' + str(urlTagList))
        fqdnList = list()
        for urlTag in urlTagList:
            url = urlTag.strip()
            idx = url.find('://')
            if idx < 0:
                logging.error('invalid ds url: ' + url + ', please check if the ds page is changed')
                return None

            fqdn = url[idx+3: len(url)]
            fqdnList.append(fqdn)

        ds['fqdnList'] = fqdnList
        logging.info('parsed fqdnList: ' + str(fqdnList) + ' in ds: ' + ds['xmlId'])
        return ds
 
    def assignServersToDsByApi(self, ds, servers, ipIds):
        path = '/api/3.0/deliveryservices/%s/servers' % ds['xmlId']
        config = {"serverNames": servers, "ipIds":ipIds}
        response = self.client.post(path, json.dumps(config))
        if response.status_code == 200 and 'response' in response.json():
            return response.json()['response']
        else:
            logging.error('failed to assign server to ds : ' + ds['xmlId'])
            assert False

    def deleteDsByApi(self, dsId):
        path = '/api/3.0/deliveryservices/%s' % dsId
        response = self.client.delete(path)
        if response.status_code == 200 and 'alerts' in response.json():
            return response.json()['alerts']
        else:
            logging.error('failed to del deliveryservices: ' + dsId)

        return False
