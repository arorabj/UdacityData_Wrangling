import os
import glob
import configparser as cp

env='dev'
conf=cp.RawConfigParser()
conf.read('../../../resources/application.properties')
filepath = ''
#def process_data(cur, conn, filepath):

log_files =  glob(filepath,'*.')