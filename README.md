# SDE_CRA

SDE_CRA is a utility for running Esri's "CRA" maintenance tools
(Compress, Re-index, Analyze) on a database.  This tool is based on our
understanding of Esri's recommendations and tailored to suit our needs,
so use at your own risk.  We still hope it might be helpful to others.

When we first started running the maintenance tools on our database, it
took a while to figure out how to run the tools in the proper order,
and with the right privileges and settings for each.  Hopefully this
script can give others a starting point.

The project also includes run_sde_cra.py, which demonstrates how SDE_CRA
can be set up to perform weekly maintenance of a database, using several
connection files, and using the logging module and BufferingSMTPHandler
for email reports.  A file such as run_sde_cra.py can then be set up to
be triggered by a batch file or scheduled task.

The fundamental maintenance workflow recommended by Esri, to the best of
our understanding, is as follows:

1. arcpy.AnalyzeDatasets_management
2. arcpy.Compress_management
3. arcpy.RebuildIndexes_management
4. arcpy.AnalyzeDatasets_management

The first analyze is optional, and serves to speed up the compress
(though in some of our cases it takes much more time than it saves, so
we tend to leave it out - test for yourself).

Compress is run on the SDE connection, while analyze and rebuild must be
 run both for sde and data owner connections (with different
 parameters).

ArcGIS 10.5 online resources:

* Using Python scripting to batch reconcile and post versions
  * http://desktop.arcgis.com/en/arcmap/latest/manage-data/geodatabases/using-python-scripting-to-batch-reconcile-and-post-versions.htm
* Analyze Datasets (Data Management)
  * http://desktop.arcgis.com/en/arcmap/latest/tools/data-management-toolbox/analyze-datasets.htm
* Compress (Data Management)
  * http://desktop.arcgis.com/en/arcmap/latest/tools/data-management-toolbox/compress.htm
* Rebuild Indexes (Data Management)
  * http://desktop.arcgis.com/en/arcmap/latest/tools/data-management-toolbox/rebuild-indexes.htm
