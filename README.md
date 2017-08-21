# glacierize
Python based incremental file backup using large archives for minimal cost AWS Glacier storage

## Overview
This software is based on the concept of glacier as an archival backup-of-the-backups, useful for a disaster recovery scenario if the on-site backups were to be destroyed.

Because the most likely scenario is a complete restoration of all the data at once, I deal with files in large globs (say 1-3GB).  This strikes a balance between the number of archives (money in per-request costs) and the archive size (money in unrelated bandwidth), leaning heavily towards reducing the archive count.  The archives are still small enough to allow a limited restoration should some unusual selective loss of data occur.

### Full Backup
`WIP`

In general this process is synchronous, compressing and uploading a few GB at a time. This cuts down on temp storage usage and should leave us in a easily recoverable state in case of interruption.

#### File search function:
For each file (in natural depth-first order)  
If file is larger than glob, make a unique archive for it  
Else, add file to archive list  
If archive size is greater than glob size (worst case we are at 1.999 size now), make archive from list  
rinse and repeat until done


#### Archive maker function:
make manifest file with UUIDish name (save first line for amazon archive ID)  
make targz with files in it  
put file names/paths in manifest file  
encrypt archive (how?)  
upload archive with description set to the UUID name of manifest file  
once upload is complete, put amazon ID into the first line of the manifest  
return

### Incremental Backup

`TODO`
