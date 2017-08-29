#!/usr/local/bin/python3
'''
glacierize.glacierize -- shortdesc

glacierize.glacierize is a description

It defines classes_and_methods

@author:     user_name

@copyright:  2017 organization_name. All rights reserved.

@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import sys
import os
import boto3
import tarfile
import uuid
import hashlib
import csv
import threading
import getpass

from tqdm import tqdm
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from queue import Queue, Empty, Full
from time import sleep
from subprocess import Popen, PIPE
from tempfile import mkdtemp
from botocore.utils import calculate_tree_hash

__all__ = []
__version__ = 0.1
__date__ = '2017-08-22'
__updated__ = '2017-08-22'

DEBUG = 1
terminateASAP = False

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

class interceptingFileReader():
    def __init__(self, fileObj, messageQueueObj):
        self.file = fileObj
        self.previousTell = 0
        self.messageQueue = messageQueueObj

    def read(self, size):
        self.messageQueue.put(['UPDATE', size - self.previousTell])
        self.previousTell += size
        return self.file.read(size)

    def __getattr__(self, name):
        return getattr(self.file, name)

def uploadWorker(messageQueue, id, vaultName):
    global terminateASAP
    try:
        uploadMessageQueue = Queue()
        client = boto3.client('glacier')
        while True:
            curMessage = messageQueue.get()
            curCommand = curMessage[0]
            if curCommand == 'EXIT':
                return
            elif curCommand == 'SEND':
                uploadBarThread = threading.Thread(target=progressBarWorker, args=[uploadMessageQueue, os.path.getsize(curMessage[1]), id, "Upload Thread %d" % (int(id)-1)])
                uploadBarThread.setDaemon(True)
                uploadBarThread.start()
                with open(curMessage[1], 'rb') as f:
                    response = client.initiate_multipart_upload(vaultName=vaultName,
                        archiveDescription=curMessage[2],
                        partSize=str(268435456) #256MB
                        )
                    uploadID = response.get('uploadId')
                    uploadMessageQueue.put(['UPDATE', 0, None])
                    
                    prevTell = 0
                    #interceptedF = interceptingFileReader(f, uploadMessageQueue)
                    while True:
                        buf = f.read(268435456)
                        if not buf: #eof
                            break      
                        response = client.upload_multipart_part(vaultName=vaultName,
                            uploadId=uploadID,
                            #Thanks amazon...
                            range='bytes %d-%d/*' % (prevTell, prevTell+len(buf)-1),
                            body=buf
                            )
                        prevTell = prevTell + len(buf)
                        uploadMessageQueue.put(['UPDATE', len(buf), None])
              
                    f.seek(0)
                    response = client.complete_multipart_upload(vaultName=vaultName,
                        uploadId=uploadID,
                        archiveSize=str(os.path.getsize(curMessage[1])),
                        checksum=calculate_tree_hash(f)
                        )
                    with open(curMessage[3], 'a') as manifest:
                        manifest.write('----\n')
                        manifest.write(str(response.get('location')))
                uploadMessageQueue.put(['EXIT'])
                uploadMessageQueue.join()
                os.remove(curMessage[1]) #Delete the local copy of the file we just uploaded
                uploadBarThread.join()
            messageQueue.task_done()
    except Exception as e:
        terminateASAP = True
        raise e

            
def progressBarWorker(messageQueue, totalSize, id, desc):
    pbar = tqdm(total=totalSize, unit='B', unit_scale=True, dynamic_ncols=True, desc=desc, position=id, smoothing=0)
    
    pbar.monitor_interval = 0
    pbar.dynamic_miniters = 0
    pbar.miniters = 1
    
    while True:
        curMessage = messageQueue.get()
        curCommand = curMessage[0]
        if curCommand == 'EXIT':
            return
        elif curCommand == 'UPDATE':
            pbar.update(curMessage[1])
        elif curCommand == 'STATUS':
            #It looks like there are several bugs in tqdm which cause this not to update frequently
            #There are some workarounds, but they are fairly hacky. Stay tuned 
            pbar.set_postfix({'State':curMessage[1]})
        messageQueue.task_done()
    
def md5OfFile(fileName):
    runningHash = hashlib.md5()
    with open(fileName, 'rb') as f:
        while True:
            buf = f.read(2**20)
            if not buf: #eof
                break
            runningHash.update( buf )
    return runningHash.hexdigest()
    
def createArchive(archiveDstDir,manifestDir,fileList,printQueue,uploadQueues,archivePassword):
    global terminateASAP
    archiveUUID = uuid.uuid1()
    tarName = os.path.join(archiveDstDir,str(archiveUUID)+'.tar.gz')
    tar = tarfile.open(tarName, 'x:gz')
    
    manifestName = os.path.join(manifestDir,str(archiveUUID)+'.manifest')
    manifest = open(manifestName, 'w')
    manifestCsv = csv.writer(manifest, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    manifestCsv.writerow(['Name','Size','MTime','MD5Sum'])
    for fileName in fileList:
        printQueue.put(['STATUS', 'HASH....'])
        fileHash = md5OfFile(fileName)
        
        printQueue.put(['STATUS', 'COMPRESS'])
        tar.add(fileName)
        
        manifestCsv.writerow([fileName, os.path.getsize(fileName), os.path.getmtime(fileName), str(fileHash)])
        
        printQueue.put(['UPDATE', os.path.getsize(fileName)])

        #Stay apprised of the state of the program
        if terminateASAP == True:
            raise Exception('A child thread has terminated abnormally')
        
    manifest.close()
    tar.close()
    printQueue.put(['STATUS', 'ENCRYPT.'])
    
    encName = tarName + '.gpg'
    #We are not using the gnupg module because it is horribly slow
    #Instead we use the system gpg
    gpgProc = Popen(args=['gpg', '--batch', '-z', '0', 
                                   '-o', encName, 
                                   '--passphrase-fd', '0', '--symmetric', tarName],
                             stdin=PIPE,
                             stdout=None,
                             stderr=None,
                             shell=False)
    gpgProc.stdin.write(bytes(archivePassword+'\n','utf-8'))
    gpgProc.stdin.flush()
    gpgret = gpgProc.wait()
    if gpgret != 0:
        raise Exception('GPG failure, returned ' + gpgret)
    os.remove(tarName)
    
    printQueue.put(['STATUS', 'UPLOAD..'])
    emplaceSuccessful = False
    while emplaceSuccessful == False:
        for uploadQueue in uploadQueues:
            try:
                uploadQueue.put_nowait(['SEND', encName, str(archiveUUID), manifestName])
                emplaceSuccessful = True
                break
            except Full:
                continue
        #If there is no free upload queue, wait for the first one
        if emplaceSuccessful == False:
            sleep(0.1)
                            

def archiveFolderRecursive(paths,workingDir,manifestDir,vaultName,archivePassword):
    bigSetOFiles = set()
    totalSize = 0
    with tqdm(desc='Enumerating files', unit='f', dynamic_ncols=True) as fbar:
        for path in paths:
            for root, dirs, files in os.walk(path, topdown=False):
                for file in files:
                    if os.path.islink(file) == False:
                        fullpath = os.path.abspath(os.path.join(root, file))
                        totalSize += os.path.getsize(fullpath)
                        bigSetOFiles.add(fullpath)
                        fbar.update(1)
    

    with tqdm(desc='Evaluating manifest entries', unit='f', dynamic_ncols=True, position=0) as ebar:
        with tqdm(desc='Unchanged files', unit='f', dynamic_ncols=True, position=1) as fbar:
            manifestFiles = [os.path.join(manifestDir,fn) for fn in next(os.walk(manifestDir))[2] if fn.endswith('.manifest')]
            for manifestFilePath in manifestFiles:
                with open(manifestFilePath, newline='') as manifestFile:
                    manifestFileCSV = csv.reader(manifestFile, delimiter=',')
                    next(manifestFileCSV)
                    setOfItemsToRemove = set()
                    sizeToRemoveFromTotal = 0
                    validManifest = False
                    for row in manifestFileCSV:
                        if len(row) == 1 and row[0] == '----':
                            #This manifest is valid, commit the differences
                            validManifest = True
                            bigSetOFiles.difference_update(setOfItemsToRemove)
                            totalSize -= sizeToRemoveFromTotal
                            fbar.update(len(setOfItemsToRemove))
                            break
                        #If the file in the manifest exists
                        if row[0] in bigSetOFiles:
                            ebar.update(1)
                            #Is it the same size?
                            if os.path.getsize(row[0]) == int(row[1]):
                                #Same hash?
                                if str(md5OfFile(row[0])) == row[3]:
                                    #If so, don't backup the file
                                    setOfItemsToRemove.add(row[0])
                                    sizeToRemoveFromTotal += os.path.getsize(row[0])
                    if validManifest == False:
                        print('\nManifest %s is invalid' % os.path.basename(manifestFilePath))
    
    print('\nDone, creating archive...')

    printQueue = Queue() 
    printThread = threading.Thread(target=progressBarWorker, args=[printQueue, totalSize, 0, 'Total Progress'])
    printThread.setDaemon(True)
    printThread.start()
    
    uploadQueues = []
    uploadThreads = []
    for threadNo in range(2):
        uploadQueues.append(Queue(maxsize=1))
        uploadThreads.append(threading.Thread(target=uploadWorker, args=[uploadQueues[threadNo], threadNo+1, vaultName]))
        uploadThreads[threadNo].setDaemon(True)
        uploadThreads[threadNo].start()
    

    currentArchiveSize = 0
    currentFileList = []
    maximumArchiveSize = 1*1024*1024*1024 #1GB
    
    for path in paths:
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                if os.path.islink(file) == False:
                    file = os.path.abspath(os.path.join(root, file))
                    #We could just operate on the bigSetOFiles, but we lose the ordering
                    #so as a compromise, we just skip ones not in the set
                    if file in bigSetOFiles:                    
                        #If the current file is larger than the standard archive size, make one just for it
                        if os.path.getsize(file) > maximumArchiveSize:
                            createArchive(archiveDstDir=workingDir,
                                          manifestDir=manifestDir,
                                          fileList=[file],
                                          printQueue=printQueue,
                                          uploadQueues=uploadQueues,
                                          archivePassword=archivePassword
                                          )
                        else: #Otherwise, add it to the list
                            currentFileList.append(file)
                            currentArchiveSize = currentArchiveSize + os.path.getsize(file)
                            #If the list is now larger than the standard archive size, pack it up
                            if currentArchiveSize > maximumArchiveSize:
                                createArchive(archiveDstDir=workingDir,
                                              manifestDir=manifestDir,
                                              fileList=currentFileList,
                                              printQueue=printQueue,
                                              uploadQueues=uploadQueues,
                                              archivePassword=archivePassword
                                              )
                                currentFileList.clear()
                                currentArchiveSize = 0
                
    #Spin off an archive for the remaining files
    if len(currentFileList) > 0:
        createArchive(archiveDstDir=workingDir,
                      manifestDir=manifestDir,
                      fileList=currentFileList,
                      printQueue=printQueue,
                      uploadQueues=uploadQueues,
                      archivePassword=archivePassword
                      )
    
    for threadNo in range(2):
        uploadQueues[threadNo].put(['EXIT'])
        uploadThreads[threadNo].join()
    printQueue.put(['STATUS', 'DONE....'])
    printQueue.put(['EXIT'])
    printThread.join(5)
    
def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv == None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Copyright (C) 2017 Christopher Parish
  
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.
  
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
  
  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

USAGE
''' % (program_shortdesc)

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument('-t', dest='tempdir', metavar='path', nargs='?', help='Temporary directory for the archive files')
        parser.add_argument('-m', dest='manifestdir', metavar='path', required=True, help='Directory for the manifest files. Any manifests already present will be compared against, allow for incremental backup.')
        parser.add_argument('-n', dest='vaultname', metavar='vault', required=True, help='Name of destination glacier vault')
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument(dest="paths", help="paths to folder(s) to recursively backup [default: %(default)s]", metavar="path", nargs='+')

        # Process arguments
        args = parser.parse_args()

        paths = args.paths
        tempDir = args.tempdir
        manifestDir = args.manifestdir
        vaultName = args.vaultname
        
        if tempDir == None:
            tempDir = mkdtemp()
        
        pw1 = getpass.getpass('Desired encryption password:')
        pw2 = getpass.getpass('Retype:')
        
        if pw1 != pw2:
            print('Passwords do not match')
            raise Exception("Input password mismatch")
        
        #This will build archives relative to the path here
        #For example, if you provide a full path to a folder
        #(/home/foo/, the resulting archive will have 
        #the full paths /home/foo/file1, /home/foo/file2, 
        #etc. inside. Relative paths are usually the way
        #to go here
        archiveFolderRecursive(paths=paths,
                               workingDir=tempDir,
                               manifestDir=manifestDir,
                               vaultName=vaultName,
                               archivePassword=pw1
                               )
        print('\nAll Operations Complete')
            
        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0

if __name__ == "__main__":     
    sys.exit(main())
