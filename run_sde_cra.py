# -*- coding: utf-8 -*-
"""
A sample script for calling and using SDE_CRA.py.

Uses BufferingSMTPHandler from https://github.com/mikecharles/python-buffering-smtp-handler
"""
import sys
import logging
from datetime import datetime as dt

from buffering_smtp_handler import BufferingSMTPHandler

import SDE_CRA

log_level = logging.DEBUG


def run():
    smtp = "mail.organisation.com"
    sender = "Python smtp <batch@organisation.com>"
    email_report_to = ['myuser@organisation.com']

    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")
    root_logger = logging.getLogger()

    logfile = "SDE_CRA_log.txt"
    fh = logging.FileHandler(filename=logfile)
    fh.setLevel(log_level)
    fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    root_logger.addHandler(fh)
    print("Running SDE_CRA through " + __file__ + ", logged output goes to " + logfile)

    # Connection files for compress, rebuild, and analyze
    con_sde = r"C:\data\local\sys_SDE.sde"
    con_data_owners = [
        r'..\DatabaseConnections\sys_BASE.sde',
        r'..\DatabaseConnections\sys_s10.sde',
        r'..\DatabaseConnections\sys_s50.sde',
        r'..\DatabaseConnections\sys_s100.sde',
    ]
    # Regex pattern to extract id/name from connection file name (for reporting)
    id_pattern = "sys_(BASE|s\d+m?)"

    # Options:
    # - cra: Compress-RebuildIndexes-Analyze
    # - acra: Analyze-Compress-RebuildIndexes-Analyze
    # - aca: Analyze-Compress-Analyze
    # - report: log timer report
    # - block: block for connections to the database while running the script
    # - kick: kick all existing connections

    # Sample weekly mode for our organisation:
    # - no kicking users, and no forced reconcile/post.
    # - don't use block, it's in the middle of the night, no one is on anyway, and if they are, they have a reason
    weekly_mode = ['cra', 'report']

    email_subject = ",".join([SDE_CRA.get_sde_id(id_pattern, c) for c in con_data_owners])
    email_subject = "Report from SDE_CRA - " + email_subject
    bsh = BufferingSMTPHandler(smtp, sender, email_report_to, email_subject, 1000, "%(asctime)s %(message)s")
    bsh.setLevel(logging.INFO)
    root_logger.addHandler(bsh)

    logging.info("Perform maintenance:")
    # SDE_CRA.perform_maintenance(con_sde, con_data_owners, ['report'], id_pattern)
    # SDE_CRA.perform_maintenance(con_sde, con_data_owners[2], ['analyze', 'report'], email_report_to, id_pattern)
    SDE_CRA.perform_maintenance(con_sde, con_data_owners, weekly_mode, id_pattern)
    logging.info("Completed maintenance!")


def analyze_by_fc(conn, first, last):
    """Analyze one feature class at a time and report duration of each."""
    print("Analyzing data sets, one feature class at a time, indexes {}-{}...".format(first, last))
    t0 = dt.now()
    logging.info("Start time: " + str(t0))

    conn2versions_lst = sorted(SDE_CRA.list_datasets(conn))

    if len(conn2versions_lst) > 0:
        if last == -1:
            last = len(conn2versions_lst) - 1
            print("  Indexes {}-{}".format(first, last))
        for i in range(first, 1+last):
            start = dt.now()
            print("{}: Running Analyze on {} in {}, started {}...".format(i, conn2versions_lst[i],
                                                                          conn.split('\\')[-1:], str(start)))
            SDE_CRA.analyze_data_owner(conn, conn2versions_lst[i])
            print("      Duration:  {}".format(dt.now() - start))

    tz = dt.now()
    duration = tz - t0
    logging.info("End time: {}".format(tz))
    logging.info("Python script duration (h:mm:ss.dddd): " + str(duration)[:-2])


def rebuild_by_fc(conn, first, last):
    """Rebuild indexes for one feature class at a time and report duration of each."""
    print("Rebuilding indexes for data sets, one feature class at a time, indexes {}-{}...".format(first, last))
    t0 = dt.now()
    logging.info("Start time: {}".format(t0))

    conn2versions_lst = sorted(SDE_CRA.list_datasets(conn))

    if len(conn2versions_lst) > 0:
        if last == -1:
            last = len(conn2versions_lst) - 1
            print("  Indexes {}-{}".format(first, last))
        for i in range(first, 1+last):
            start = dt.now()
            print("{}: Running Rebuild Indexes on {} in {}, started {}...".format(i, conn2versions_lst[i],
                                                                                  conn.split('\\')[-1:], str(start)))
            SDE_CRA.rebuild_indexes(conn, [conn2versions_lst[i]])  # 2nd arg must be a list
            print("      Duration:  {}".format(str(dt.now() - start)))

    tz = dt.now()
    duration = tz - t0
    logging.info("End time: " + str(tz))
    logging.info("Python script duration (h:mm:ss.dddd): " + str(duration)[:-2])


if __name__ == "__main__":
    run()
