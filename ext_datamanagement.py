# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 15:56:23 2022

@author: hendrik_gt
"""

# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2022 Deltares
#       Gerrit Hendriksen
#       gerrit.hendriksen@deltares.nl   
#       Nathalie Dees (nathalie.dees@deltares.nl)
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.


# setup connection with sftp box
import pysftp
import configparser
from urllib.parse import urlparse
import os
import shutil

#load local procedures to write to db
from ext_loaddataintodatamodel import metadata_location, timeseries_todb, update_location
from ts_helpders import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep
from sftp_tools import Sftp

#configfile = r'D:\projecten\datamanagement\Nederland\BodembewegingNL\tools\config.txt'
configfile = r'C:\projecten\rws\2022\extensometer\config.txt'
cf = configparser.ConfigParser() 
cf.read(configfile)      

sftp = Sftp(
        hostname=cf.get('FTP','url'),
        username=cf.get('FTP','user'),
        password=cf.get('FTP','password'),
        port=cf.get('FTP','port'),
    )
    

# list items + paths for input + outputs
lstdir = ['cabauw','bleskensgraaf','berkenwoude']
lpath = 'C:\\projecten\\rws\\2022\\extensometer\\data'
ppath= 'P:\\extensometer\\peilen\\'

#metadata sourcefile
sf = r'C:\projecten\rws\2022\extensometer\metadata_test.xlsx'

#updating the metadata table, if source files changes, edit this function in loaddataintodatamodel.py
update_location(sf)

#tetrieving the metadata table
metadata=metadata_location()

# Connect to SFTP
sftp.connect()
#sftp.download(rmpath,lpath)
for dir in lstdir:
    rmpath = './{rm}/'.format(rm=dir) #remote path is link to the sftp and name of the folder 
    sftp.listdir_attr(rmpath)
    for i in sftp.listdir_attr(rmpath):
        filepath=(lpath+'\\{rm}\\').format(rm=dir)+i.filename #assigning the right filename to file
        sftp.download(rmpath+i.filename,filepath) #download from ftp to local location

        # hier moet een stuk komen die de file upload naar de database
        # rekening houden met location, serieskeys, input is lstdir voor de name en de file wat in de db moet komen

        timeseries_todb(filepath, metadata, dir) #put in db

        # if dir does not exist in root then make dir
        if not os.path.exists(ppath+dir):
            os.mkdir(ppath+dir)

        #na succsevol inladen in de database data naar de p verplaatsen,
        shutil.move(filepath, (os.path.join(ppath,dir)+'\\'+i.filename)) #write to p drive
        
        # na succesvol inladen in de database data van de ftp verwijderen
        sftp.remove_file(rmpath+i.filename) #delete from ftp


#close the connection to SFTP
sftp.disconnect()