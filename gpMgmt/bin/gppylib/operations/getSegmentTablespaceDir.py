#!/usr/bin/env python3

from gppylib.db import dbconn
from gppylib import gplog

logger = gplog.get_default_logger()


class GetTablespaces:

    def __init__(self):
        self.tablespace_oids = []
        self.tablespace_dirs = []

    def get_tablespace_oids(self):
        get_tablespace_oids_sql = "SELECT oid FROM pg_tablespace WHERE spcname NOT IN ('pg_default', 'pg_global')"
        with dbconn.connect(dbconn.DbURL()) as conn:
            pg_tablespace_data = dbconn.query(conn, get_tablespace_oids_sql)
        if pg_tablespace_data:
            for oid in pg_tablespace_data:
                self.tablespace_oids.append(oid[0]) if oid[0] not in self.tablespace_oids else self.tablespace_oids
        return self.tablespace_oids

    # get tablespace location for user tablspaces
    def fetch_tablespace_dirs(self, all_hosts, tablespace_oids=None, data_directory=None):
        oid_subq = """ (SELECT *
                        FROM (
                            SELECT oid FROM pg_tablespace
                            WHERE spcname NOT IN ('pg_default', 'pg_global')
                            ) AS _q1,
                            LATERAL gp_tablespace_location(_q1.oid)
                        ) AS t """
        if tablespace_oids:
            with dbconn.connect(dbconn.DbURL()) as conn:
                if all_hosts:
                    TABLESPACE_LOCATION = """
                        SELECT c.hostname, t.tblspc_loc||'/'||c.dbid tblspc_loc
                        FROM {oid_subq}
                            JOIN gp_segment_configuration AS c
                            ON t.gp_segment_id = c.content
                        """ .format(oid_subq=oid_subq)
                else:
                    TABLESPACE_LOCATION = """
                        SELECT c.hostname,c.content, t.tblspc_loc||'/'||c.dbid tblspc_loc
                        FROM {oid_subq}
                            JOIN gp_segment_configuration AS c
                            ON t.gp_segment_id = c.content
                            AND c.role='m' AND c.datadir ='{data_directory}'
                        """ .format(oid_subq=oid_subq, data_directory=data_directory)
                res = dbconn.query(conn, TABLESPACE_LOCATION)
                for r in res:
                    self.tablespace_dirs.append(r)
        return self.tablespace_dirs

    def get_tablespace_dirs(self, all_hosts, data_directory=None):
        # fetch non default tablespace oids
        self.tablespace_oids = self.get_tablespace_oids()
        # fetch tablespace locations
        self.tablespace_dirs = self.fetch_tablespace_dirs(all_hosts, self.tablespace_oids, data_directory)
        return self.tablespace_dirs
