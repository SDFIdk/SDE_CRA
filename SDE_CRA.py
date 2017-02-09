# -*- coding: utf-8 -*-
"""
A utility for running Esri's "CRA" tools (Compress, Re-index, Analyze) on a database connection.

This tool is based on our understanding of Esri's recommendations and tailored to suit our needs,
so use at your own risk. We still hope it might be helpful to others.

The basic functionality is inherited from Esri's online help, with some modifications for our practices.
E.g. we don't kick user connections, and we don't force reconcile or post.

The fundamental maintenance workflow recommended by Esri, to the best of our understanding, is as follows:
1. arcpy.AnalyzeDatasets_management
2. arcpy.Compress_management
3. arcpy.RebuildIndexes_management
4. arcpy.AnalyzeDatasets_management

The first analyze is optional, and serves to speed up the compress (though in some of our cases it takes
much more time than it saves, so we tend to leave it out - test for yourself).

Compress is run on the SDE connection, while analyze and rebuild must be run both for sde and data owner
connections (with different parameters).

ArcGIS 10.5 online resources:
    Using Python scripting to batch reconcile and post versions
        http://desktop.arcgis.com/en/arcmap/latest/manage-data/geodatabases/using-python-scripting-to-batch-reconcile-and-post-versions.htm
    Analyze Datasets (Data Management)
        http://desktop.arcgis.com/en/arcmap/latest/tools/data-management-toolbox/analyze-datasets.htm
    Compress (Data Management)
        http://desktop.arcgis.com/en/arcmap/latest/tools/data-management-toolbox/compress.htm
    Rebuild Indexes (Data Management)
        http://desktop.arcgis.com/en/arcmap/latest/tools/data-management-toolbox/rebuild-indexes.htm

History:
Ver. 1.0.0 - First working version /mahvi
   Basically the original script, just chopped up in sub functions and added modes and runtime profiling
Ver. 1.0.1 /mahvi
   Logging to file, to debug problems with windows scheduler
Ver. 1.1.0 /mahvi
   Implementing Shara's (shollis@esri.com) suggestions '131210/mahvi
   Putting a 'try:' on RebuildIndexes() so a single locked layer won't kill the entire script.
Ver. 1.1.2 /halpe
   Now at least Analyze works again
   Timer report should work now
Ver. 1.1.3 /halpe, 13 Oct 2014
   Updating the series of procedures that's run, according to Shara's directions,
     doing analyze for both sde and data owner connections
   - sde and owner connections take different parameters for Analyze, etc.
   Include a note in the report if there are active users or existing versions
   Include timer output in report
Ver. 1.1.4 /halpe, 1 Sep 2016
    Allowing several connections with different data owners for different datasets
    Introducing use of logging module
    Introducing topo_utils.sendEmail()
    Added analyze_by_fc() and rebuild_by_fc() for data analysis
    Dropping SendEmailWarning()
Ver. 1.1.5 /halpe, Jan 2017
    Introducing the logging module;
    - then the user can set it up with buffering_smtp_handler, from
      https://github.com/mikecharles/python-buffering-smtp-handler and we don't need to worry about email

Created by: Martin Hvidberg <mahvi@gst.dk> (first versions)
            Hanne L. Petersen <halpe@sdfe.dk> (recent versions)
"""
import sys
import logging
import arcpy
import re
import socket
from datetime import datetime as dt, timedelta

individual_analyze = False


def list_datasets(workspace):
    """
    Get a list of datasets owned by the workspace user.

    This assumes you are using database authentication.
    OS authentication connection files do not have a 'user' property.
    """
    arcpy.env.workspace = workspace
    if arcpy.Describe(arcpy.env.workspace).dataType != "Workspace":
        logging.error("Workspace not recognised - something is wrong. Please check your TNS settings.")
    # Get the user name for the workspace.
    user_name = arcpy.Describe(arcpy.env.workspace).connectionProperties.user
    all_users = user_name + '.*'  # For non-Oracle try: '*.' + user_name + '.*'

    # Get a list of all the datasets the user has access to.
    # First, get all the stand alone tables, feature classes and rasters.
    data_lst = arcpy.ListTables(all_users) + arcpy.ListFeatureClasses(all_users) + arcpy.ListRasters(all_users)

    # Next, for feature datasets get all of the feature classes
    # from the list and add them to the master list.
    for dataset in arcpy.ListDatasets(all_users):
        data_lst += arcpy.ListFeatureClasses(feature_dataset=dataset)

    return data_lst


def analyze_data_owner(workspace, data_lst):
    """Analyze the workspace, with settings appropriate for the data owner."""
    # Note: don't use the "SYSTEM" option unless user is an administrator
    arcpy.AnalyzeDatasets_management(workspace, "NO_SYSTEM", data_lst,
                                     "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")
    return 0


def analyze_sde(workspace):
    """Analyze the workspace with settings appropriate for SDE user."""
    arcpy.AnalyzeDatasets_management(workspace, "SYSTEM")
    return 0


def compress(workspace):
    """Run arcpy.Compress_management on workspace."""
    arcpy.Compress_management(workspace)
    return 0


def rebuild_indexes(workspace, data_lst):
    """Run arcpy.RebuildIndexes_management on the datasets in the list."""
    # Note: to use the "SYSTEM" option the user must be an administrator.
    # To access the actual data sets, we need the data owner.
    # So we need to run once for each.
    try:
        logging.debug("rebuild: " + workspace + ", " + str(data_lst))
        if data_lst == '' or data_lst == []:  # workspace is sde
            arcpy.RebuildIndexes_management(workspace, "SYSTEM", "", "ALL")
        else:  # workspace is data owner
            arcpy.RebuildIndexes_management(workspace, "NO_SYSTEM", data_lst, "ALL")
        logging.info("      > Rebuild successful: " + str(data_lst))
        return 0
    except arcpy.ExecuteError as x:
        logging.error("      > ExecuteError is: "+repr(x))
        return 1
    except BaseException as e:
        logging.error("      > Non-arcpy problem with: " + str(data_lst) + repr(e))
        logging.error(arcpy.GetMessages())
        return 1


def get_sde_id(pattern, string, n=1):
    """Extract an id from an sde string. Return the first part of string that matches pattern, or the whole string."""
    m = re.search(pattern, string)
    if m is not None:
        return m.group(n)
    return string


def perform_maintenance(con_dba, con_geo, mode, sde_id_match_pattern):
    """
    Run maintenance routines specified by mode on the database connections.

    con_GEO can be a string or an array of strings.

    Valid modes can contain: cra, acra, aca, analyze, compress, rebuild, report, block, kick
    """
    logging.info("Running perform_maintenance() on {}".format(socket.gethostname()))
    timer = ECtimes()

    t0 = dt.now()
    timer.time_stamp('Initialize', 'start', 'Start of Main()')

    logging.info(" * Auto Compress SDE - Main *")
    logging.info("   Start time: {}".format(t0))
    logging.info("   Connection DB admin: {}".format(con_dba))
    logging.info("   Connection data owners: {}".format(con_geo))
    logging.info("   Mode: {}".format(mode))

    if isinstance(con_geo, basestring):
        con_geo = [con_geo]

    # List all versions for each connection
    for con in con_geo:
        lst_versions = arcpy.da.ListVersions(con)
        if len(lst_versions) > 1:
            logging.info("        Current versions (any but SDE.DEFAULT will prevent optimal compression): " +
                         str([v.name for v in lst_versions]))
        else:
            logging.info("        No edit versions for {}.".format(con))

    timer.time_stamp('Initialize', 'stop', 'Start of Main()')

    # Block new connections to the database.
    if 'block' in mode:
        arcpy.AcceptConnections(con_dba, False)

    if 'kick' in mode:
        arcpy.DisconnectUser(con_dba, "ALL")

    timer.time_stamp('main', 'start', '')  # Start main timer

    # Build a list of datasets owned by each data owner to the rebuild indexes and analyze datasets tools.
    # SDE only owns COMPRESS_LOG
    if mode != ['report']:
        logging.info("   1. Get List Of Data Sets")
        timer.time_stamp('list_data', 'start', '')
        dict_conn2versions = {}
        for con in con_geo:
            dict_conn2versions[con] = list_datasets(con)
        timer.time_stamp('list_data', 'stop', '')

    # First Analyze
    # (this is supposed to improve performance of Compress - but it can take so long it doesn't seem worthwhile)
    if 'acra' in mode or 'aca' in mode:
        logging.info("   1b. Analyze")
        for con in con_geo:
            logging.info("      " + con)
            if len(dict_conn2versions[con]) > 0:
                sde_id = get_sde_id(sde_id_match_pattern, con)
                timer.time_stamp('analyze1_'+sde_id, 'start', '')
                if individual_analyze:
                    for fc in dict_conn2versions[con]:
                        start = dt.now()
                        analyze_data_owner(con, fc)
                        logging.debug("Duration of {} in {}: {}".format(fc, con, str(dt.now() - start)[:-2]))
                else:
                    start = dt.now()
                    analyze_data_owner(con, dict_conn2versions[con])
                    logging.error("Duration of all fcs in {}: {}".format(con, str(dt.now() - start)[:-2]))
                timer.time_stamp('analyze1_'+sde_id, 'stop', '')
            else:
                logging.info("Skipping empty data: "+con)
        timer.time_stamp('analyze1_sde', 'start', '')
        analyze_sde(con_dba)
        timer.time_stamp('analyze1_sde', 'stop', '')

    # Do the actual Compress
    if 'cra' in mode or 'acra' in mode or 'aca' in mode or 'compress' in mode:
        logging.info("   2. Compress")
        timer.time_stamp('compress', 'start', '')
        compress(con_dba)
        timer.time_stamp('compress', 'stop', '')

    # Do the Rebuild Indexes
    if 'cra' in mode or 'acra' in mode or 'rebuild' in mode:
        logging.info("   3. Rebuild Indexes")
        for con in con_geo:
            if len(dict_conn2versions[con]) > 0:
                sde_id = get_sde_id(sde_id_match_pattern, con)
                timer.time_stamp('rebuild_index_'+sde_id, 'start', '')
                logging.info("   Start time rebuild indexes: " + str(dt.now()))
                rebuild_indexes(con, dict_conn2versions[con])
                timer.time_stamp('rebuild_index_'+sde_id, 'stop', '')
            else:
                logging.info("Skipping empty data: " + con)
        timer.time_stamp('rebuild_index_sde', 'start', '')
        rebuild_indexes(con_dba, "")
        timer.time_stamp('rebuild_index_sde', 'stop', '')

    # Second Analyze
    # Running Analyze AFTER Compress is the important thing
    if 'cra' in mode or 'acra' in mode or 'aca' in mode or 'analyze' in mode:
        logging.info("   4. Analyze")
        for con in con_geo:
            if len(dict_conn2versions[con]) > 0:
                sde_id = get_sde_id(sde_id_match_pattern, con)
                timer.time_stamp('analyze2_'+sde_id, 'start', '')
                logging.info("   Start time analyze: " + str(dt.now()))
                analyze_data_owner(con, dict_conn2versions[con])
                timer.time_stamp('analyze2_'+sde_id, 'stop', '')
        timer.time_stamp('analyze2_sde', 'start', '')
        analyze_sde(con_dba)
        timer.time_stamp('analyze2_sde', 'stop', '')

    timer.time_stamp('main', 'stop', '')  # Stop main timer

    # Allow the database to begin accepting connections again
    if 'block' in mode:
        # Input connection must be administrator
        arcpy.AcceptConnections(con_dba, True)

    # Compile and log the profiling report
    if 'report' in mode:
        logging.info(" * Compile a report")
        time_profile_report = timer.time_report()
        logging.info("Time profile report:" + time_profile_report)

    # All Done - Cleaning up
    tz = dt.now()
    duration = tz - t0
    logging.info("End time: " + str(tz))
    logging.info("Python script duration (h:mm:ss.dddd): " + str(duration)[:-2])


class ECtimes:
    """
    Module to facilitate timing of individual program sections and reporting.

    Created by: mahvi@gst.dk
    Class wrapping added by halpe@sdfe.dk
    """
    lst_time = list()  # a list object to hold timing events

    def __init__(self):
        """Clear all records and start anew."""
        self.lst_time = list()

    def time_stamp(self, group, stst, text):
        """
        Create a timestamp in lst_time.

        group <text> : Time spend is accumulated by group
        stst <text> : ['start'|'stop']
        text <text> : user defined text/comment
        """
        if stst not in ['start', 'stop']:
            stst = 'stst'
        self.lst_time.append([group, dt.now(), stst, text])
        return 0

    def time_report(self):
        """Analyse timestamps in lst_time, and generate a report."""
        dic_groups = dict()
        dic_report = dict()
        str_report = ""
        for stamp in self.lst_time:
            if stamp[0] not in dic_groups.keys():
                dic_groups[stamp[0]] = list()
            dic_groups[stamp[0]].append(stamp[1:])
        for group in dic_groups.keys():
            series = dic_groups[group]
            deltatime = timedelta()
            if len(series) % 2 == 1:  # Check if length is even
                continue
            series.sort()
            for i in range(len(series))[::2]:
                if series[i][1] == 'start':
                    if series[i+1][1] == 'stop':
                        dur_s = series[i+1][0] - series[i][0]  # note this is a timedelta object, not just a number
                        deltatime += dur_s
                    else:
                        # TODO: error handling
                        pass
                else:
                    # TODO: error handling
                    pass
            dic_report[group] = "group: {} = {} seconds".format(group, deltatime.total_seconds())
        rep_keys = dic_report.keys()
        rep_keys.sort()
        for key_r in rep_keys:
            str_report += "\n" + dic_report[key_r]
        return str_report
# End class ECtimes


def main():
    """Perform maintenance on command line inputs."""
    if len(sys.argv) == 5:
        perform_maintenance(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        print("Usage: " + __file__ + " CONN_SDE CONN_DO MODE ID_REGEX")


if __name__ == "__main__":
    main()
    pass
