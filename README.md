# Recipe for running CEDA FBS on the whole archive

## Login to jasmin-sci2 server and locate yourself

```
$ ssh ${USER}@jasmin-sci2.ceda.ac.uk
$ cd /group_workspaces/jasmin/cedaproc/${USER}/
$ mkdir fbs
$ export BASEDIR=$PWD
$ cd fbs/
```

## Get and install ceda-fbs code from Git (with install script)

```
$ wget https://raw.githubusercontent.com/cedadev/ceda-fbs/master/install-ceda-fbs.sh
$ .  ./install-ceda-fbs.sh
```

This will build you a `virtualenv` locally so your environment should look like:

```
$ ls
ceda-fbs  install-ceda-fbs.sh  venv-ceda-fbs
```

## Create a little setup script

```
$ cat setup_env.sh
export BASEDIR=/group_workspaces/jasmin/cedaproc/$USER/fbs
export PYTHONPATH=$BASEDIR/ceda-fbs/python:$BASEDIR/ceda-fbs/python/src/fbs:$PYTHONPATH
export PATH=$PATH:$BASEDIR/ceda-fbs/python/src/fbs/cmdline
. venv-ceda-fbs/bin/activate
```

## Configure servers and Elasticsearch index

You need to tell `ceda-fbs` some key things in the config file (ceda_fbs.ini) at:

`$BASEDIR/ceda-fbs/python/config/ceda_fbs.ini`

You will need to edit the following sections:

```
 log-path = /group_workspaces/jasmin/cedaproc/__INSERT_USERID_HERE__/fbs/log
 es-host = jasmin-es1ceda.ac.uk.							
 es-index = ceda-archive-level-2							
 es-index-settings = /group_workspaces/jasmin/cedaproc/__INSERT_USERID_HERE__/fbs/ceda-fbs/elasticsearch/mapping/level_3_settings_1.json	
 num-files = 10000		
 num-processes = 128		
```

NOTE: change `__INSERT_USERID_HERE__` to your userid.

## Check that your userid has access to the required groups to read the archive

The CEDA archive is made up of numerous datasets that are managed through Unix group permissions. You will need access to the following in order to successfully read files across the archive:

* byacl
* open
* badcint
* gws_specs
* cmip5_research
* esacat1
* ecmwf
* ukmo
* eurosat
* ukmo_wx
* ukmo_clim

## 1. Scan the file system for a list of all CEDA datasets

```
$ ceda-fbs/python/src/fbs/cmdline/create_datasets_ini_file.sh
Wrote datasets file to: ceda_all_datasets.ini
```

You should now have an INI file that maps identifiers to dataset paths, i.e.:

```
$ head -3 ceda_all_datasets.ini
badc__abacus=/badc/abacus/data
badc__accacia=/badc/accacia/data
badc__accmip=/badc/accmip/data
```

## 2. Create file lists for every dataset (ready for the actual scanning)

Make directories ready for file lists and log files:

```
$ mkdir logs datasets
```
!WARNING: files lists can be *many Gbytes in size* so don't do this in your home directory.

Now run the first LOTUS jobs to generate lists of all files in each dataset.

```
$ make_file_lists.py -f ceda_all_datasets.ini -m $BASEDIR/datasets --host lotus -p 256
```

This will submit lots of jobs to LOTUS...and wait...and try to submit more.

*NOTE:* To run a subset of these jobs locally you might do:

```
$ make_file_lists.py -f redo_datasets.ini -m $BASEDIR/datasets --host localhost
```

*NOTE:* And to run just one you can do:

```
$ scan_dataset.py -f redo_datasets.ini -d  badc__ukmo-nimrod --make-list /$BASEDIR/datasets/badc__ukmo-nimrod.txt
```

At this stage you might want to examine which datasets were not scanned - and why. The above command gives you a method of running for individual datasets.

## 3. Create a set of commands to run the full scan

Create a set of commands ready to send to LOTUS that will scan the entire archive. They will use the file list files from (2) as their inputs.

```
$ scan_archive.py --file-paths-dir $BASEDIR/datasets --num-files 10000 --level 2 --host lotus
```

This generates a file inside the current directory called: `lotus_commands.txt`. Each command specifies a list of up to 10,000 data files that are to be scanned when the job runs on LOTUS. (The `lotus_commands.txt` file will contain about 25,000 lines/commands).

## 4. Execute the scan commands on LOTUS

Before you do this: Create: `~/.forward` (containing just your email address) - so that LOTUS messages will be mailed to you.

Next, run the `run_commands_in_lotus.py` script to work its way through the list of commands inside the `lotus_commands.txt` file by submitting up to 128 at any one time.

On `jasmin-sci[12].ceda.ac.uk`, run:

```
$ nohup run_commands_in_lotus.py -f lotus_commands.txt -p 128 2>&1 > scan.output.txt &
```

## 5. Watch the file count building

You can see how things are progressing in the web-interface:

 http://jasmin-es1.ceda.ac.uk:9200/_plugin/head/
 
Or, you can use the `Sense` plugin in Chrome, and try: 

`GET jasmin-es1.ceda.ac.uk:9200/ceda-archive-level-2/_count`

## 6. Make some optimisations to the Elasticsearch settings

Make these settings using `curl`, `wget` or the `Sense` plugin.

Set the Index to NOT use replica shards by calling the following:

```
PUT jasmin-es1.ceda.ac.uk:9200/ceda-archive-level-2/_settings
{
    "number_of_replicas": 0
}
```

Set the number of shards for each host to 1 by calling the following:

```
PUT jasmin-es1.ceda.ac.uk:9200/ceda-archive-level-2/_settings
{
    "index.routing.allocation.total_shards_per_node": 1

}
```
