#!/usr/bin/python 
#Description: Split the backup files into smaller chunks

import sqlite3

class Backup:
    def __init__(self, src, dest_prefix, chunk_size):
        self.dest_prefix = dest_prefix
        self.chunk_size = chunk_size
        connection = sqlite3.connect(src)
        self.cursor = connection.cursor()

    def show(self):
        rows = self.cursor.execute('select vbucket_id,cpoint_id,seq,op,key,flg,exp,cas,val,length(val) from cpoint_op')
        for row in rows:
            print row

    def _create_tables(self, cursor):
        if cursor != None:
            cursor.execute('create table if not exists cpoint_op(vbucket_id integer, cpoint_id integer, seq integer, op text, key varchar(250), flg integer, exp integer, cas integer, val blob);')
            cursor.execute('create table if not exists cpoint_state(vbucket_id integer, cpoint_id integer, prev_cpoint_id integer, state varchar(1), source varchar(250), updated_at text);')

    def create_splits(self):
        shard = 0
        create_new_sqlite = True
        size = 0
        output_cursor = connection = None
        cpoint_ids = []
        cstate = self.cursor.execute('select * from cpoint_state')
        cstate = cstate.fetchall()

        rows = self.cursor.execute('select vbucket_id,cpoint_id,seq,op,key,flg,exp,cas,val,length(val) from cpoint_op')
        for row in rows:
            if row[1] not in cpoint_ids:
                cpoint_ids.append(row[1])

            size += int(row[-1])
            if size >= self.chunk_size and connection!=None:
                for cstate_row in cstate:
                    if cstate_row[1] in cpoint_ids:
                        output_cursor.execute('insert into cpoint_state values(?,?,?,?,?,?)', (cstate_row[0],cstate_row[1],cstate_row[2],cstate_row[3],cstate_row[4],cstate_row[5]))
                
                connection.commit()
                connection.close()
                create_new_sqlite = True
                size = int(row[-1])
                shard +=1
                
            if create_new_sqlite:
                print 'Creating file %s-%d.mbb' %(self.dest_prefix, shard)
                connection = sqlite3.connect("%s-%d.mbb" %(self.dest_prefix, shard))
                output_cursor = connection.cursor()
                self._create_tables(output_cursor)
                create_new_sqlite = False
                cpoint_ids = []
            output_cursor.execute('insert into cpoint_op(vbucket_id,cpoint_id,seq,op,key,flg,exp,cas,val) values(?,?,?,?,?,?,?,?,?)', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))

        for cstate_row in cstate:
            if cstate_row[1] in cpoint_ids:
                output_cursor.execute('insert into cpoint_state values(?,?,?,?,?,?)', (cstate_row[0],cstate_row[1],cstate_row[2],cstate_row[3],cstate_row[4],cstate_row[5]))
 
        connection.commit()
        connection.close()


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print
        print "Usage: %s input.mbb dest_path/file_prefix" %sys.argv[0]
        print "Files of approx 512 MB chunks will be created as dest_path/file_prefix-%.mbb"
        sys.exit(1)

    b = Backup(sys.argv[1], sys.argv[2], 536870912)   
    b.create_splits()
    #b.show()

