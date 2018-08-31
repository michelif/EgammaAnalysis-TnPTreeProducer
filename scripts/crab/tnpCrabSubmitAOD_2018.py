from CRABClient.UserUtilities import config, getUsernameFromSiteDB
import sys

# this will use CRAB client API
from CRABAPI.RawCommand import crabCommand

# talk to DBS to get list of files in this dataset
from dbs.apis.dbsClient import DbsApi
dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

dataset = '/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISpring18DRPremix-100X_upgrade2018_realistic_v10-v2/AODSIM'
fileDictList=dbs.listFiles(dataset=dataset)

print ("dataset %s has %d files" % (dataset, len(fileDictList)))

# DBS client returns a list of dictionaries, but we want a list of Logical File Names
lfnList = [ dic['logical_file_name'] for dic in fileDictList ]

# this now standard CRAB configuration

from WMCore.Configuration import Configuration

config = config()

submitVersion ="2018Data_1"
doEleTree = 'doEleID=True'
doPhoTree = 'doPhoID=True'
doHLTTree = 'doTrigger=True'
doReco = 'doRECO=True'
isAOD = 'isAOD=True'
#calibEn   = 'useCalibEn=False'

mainOutputDir = '/store/group/phys_egamma/micheli/TnP/ntuples_20180831/%s' % submitVersion

config.General.transferLogs = False

config.JobType.pluginName  = 'Analysis'

# Name of the CMSSW configuration file
config.JobType.psetName  = '/afs/cern.ch/work/m/micheli/tnp_production_2018_20180831/CMSSW_10_2_0/src/EgammaAnalysis/TnPTreeProducer/python/TnPTreeProducer_cfg.py'
#config.Data.allowNonValidInputDataset = False
config.JobType.sendExternalFolder     = True

config.Data.inputDBS = 'global'
config.Data.publication = False
config.Data.allowNonValidInputDataset = True
#config.Data.publishDataName = 

config.Site.storageSite = 'T2_CH_CERN'


 
if __name__ == '__main__':

    from CRABAPI.RawCommand import crabCommand
    from CRABClient.ClientExceptions import ClientException
    from httplib import HTTPException

    # We want to put all the CRAB project directories from the tasks we submit here into one common directory.
    # That's why we need to set this parameter (here or above in the configuration file, it does not matter, we will not overwrite it).
    config.General.workArea = 'crab_%s' % submitVersion

    def submit(config):
        try:
            crabCommand('submit', config = config)
        except HTTPException as hte:
            print "Failed submitting task: %s" % (hte.headers)
        except ClientException as cle:
            print "Failed submitting task: %s" % (cle)


    ##### submit MC

#    config.Data.splitting     = 'FileBased'
#    config.Data.unitsPerJob   = 10
#    config.Data.inputDataset    = '/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISpring18MiniAOD-100X_upgrade2018_realistic_v10-v2/MINIAODSIM'

    config.Data.outLFNDirBase = '%s/%s/' % (mainOutputDir,'mc')
    config.JobType.pyCfgParams  = ['isMC=True',doEleTree,doPhoTree,doHLTTree,doReco,isAOD,'GT=100X_upgrade2018_realistic_v10']
#    config.Data.userInputFiles = lfnList #for mc remember to update this parameter at the beginning of this script. for submitting data comment this line
    config.Data.splitting = 'FileBased'
    config.General.requestName  = 'DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8'
    config.Data.unitsPerJob = 1
#    submit(config)


    ##### now submit DATA
    config.Data.outLFNDirBase = '%s/%s/' % (mainOutputDir,'data')
    config.Data.splitting     = 'LumiBased'
    config.Data.lumiMask      = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/PromptReco/Cert_314472-321221_13TeV_PromptReco_Collisions18_JSON.txt'
    config.Data.unitsPerJob   = 100
    config.JobType.pyCfgParams  = ['isMC=False',doEleTree,doPhoTree,doHLTTree,doReco,isAOD,'GT=101X_dataRun2_Prompt_v9']
 
    config.General.requestName  = 'Prompt2018_RunA_v1'
    config.Data.inputDataset    = '/EGamma/Run2018A-PromptReco-v1/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunA_v2'
    config.Data.inputDataset    = '/EGamma/Run2018A-PromptReco-v2/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunA_v3'
    config.Data.inputDataset    = '/EGamma/Run2018A-PromptReco-v3/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunB_v1'
    config.Data.inputDataset    = '/EGamma/Run2018B-PromptReco-v1/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunB_v2'
    config.Data.inputDataset    = '/EGamma/Run2018B-PromptReco-v2/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunC_v1'
    config.Data.inputDataset    = '/EGamma/Run2018C-PromptReco-v1/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunC_v2'
    config.Data.inputDataset    = '/EGamma/Run2018C-PromptReco-v2/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunC_v3'
    config.Data.inputDataset    = '/EGamma/Run2018C-PromptReco-v3/AOD'
#    submit(config)    


    config.General.requestName  = 'Prompt2018_RunD_v1'
    config.Data.inputDataset    = '/EGamma/Run2018D-PromptReco-v1/AOD'
#    submit(config)    

    config.General.requestName  = 'Prompt2018_RunD_v2'
    config.Data.inputDataset    = '/EGamma/Run2018D-PromptReco-v2/AOD'
    submit(config)    
