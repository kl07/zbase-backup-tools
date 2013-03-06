#!/usr/bin/env python

import memcacheConstants
import mc_bin_client
import struct
import sqlite3
import os
import select
import datetime
import consts

MBB_VERSION = "2"
TIMEOUT = 0
TXN_SIZE = 100
EXT_LEN_WITHOUT_QTIME = 16

class ConnectException(Exception):
    def __str__(self):
        return "TAP Connect error"

cmdInfo = {
    memcacheConstants.CMD_TAP_MUTATION: ('mutation', 'm'),
    memcacheConstants.CMD_TAP_DELETE: ('delete', 'd'),
    memcacheConstants.CMD_TAP_FLUSH: ('flush', 'f'),
}

chkpoint_stmt = "INSERT into cpoint_state" \
                    "(vbucket_id, cpoint_id, prev_cpoint_id, state, source, updated_at)" \
                    " VALUES (?, ?, ?, \"closed\", ?, ?)"

tap_stmt = "INSERT OR REPLACE into cpoint_op" \
            "(vbucket_id, cpoint_id, seq, op, key, flg, exp, cas, cksum, val)" \
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

del_chk_start_stmt = "DELETE FROM cpoint_state WHERE state=\"closed\" and cpoint_id=%d"

def readTap(mc):
    ext = ''
    key = ''
    val = ''
    cmd, vbucketId, opaque, cas, keylen, extlen, data = mc._recvMsg()
    if data:
        ext = data[0:extlen]
        key = data[extlen:extlen+keylen]
        val = data[extlen+keylen:]
    return cmd, opaque, cas, vbucketId, key, ext, val

def encodeTAPConnectOpts(opts, backfill=False):
    header = 0
    val = []
    for op in sorted(opts.keys()):
        header |= op
        if op in memcacheConstants.TAP_FLAG_TYPES:
            val.append(struct.pack(memcacheConstants.TAP_FLAG_TYPES[op],
                                   opts[op]))
        elif backfill and op == memcacheConstants.TAP_FLAG_CHECKPOINT:
            if opts[op][2] >= 0:
                val.append(struct.pack(">HHQ", opts[op][0], opts[op][1], opts[op][2]))
        else:
            val.append(opts[op])
    return struct.pack(">I", header), ''.join(val)

def parseTapExt(ext):
    if len(ext) == 8:
        flg = exp = cksum_len = 0
        eng_length, flags, ttl = \
            struct.unpack(memcacheConstants.TAP_GENERAL_PKT_FMT, ext)
    else:
        eng_length, flags, ttl, cksum_len, flg, exp  = \
                struct.unpack(memcacheConstants.TAP_MUTATION_PKT_FMT, ext[:EXT_LEN_WITHOUT_QTIME])

    needAck = flags & memcacheConstants.TAP_FLAG_ACK

    return eng_length, flags, ttl, flg, exp, needAck, cksum_len

def add_record(cursor, stmt, fields):
    result = True
    try:
        cursor.execute(stmt, fields)
    except sqlite3.Error, e: ## Can't find the better exeception code for database full error.
        result = False
    return result

def create_backup_db(backup_file_name, max_backup_size, split_backup, deduplicate):
    db = None
    db = sqlite3.connect(backup_file_name) # TODO: Revisit isolation level
    db.text_factory = str
    cur = db.execute("pragma user_version").fetchall()[0][0] # File's version.
    if (int(cur) != 0):
        raise Exception("ERROR: unexpected db user version: " + str(cur))
    if deduplicate:
        db.executescript("""
        BEGIN;
        CREATE TABLE cpoint_op
        (vbucket_id integer, cpoint_id integer, seq integer, op text,
        key varchar(250), flg integer, exp integer, cas integer, cksum varchar(100), val blob,
        primary key(vbucket_id, key));
        CREATE TABLE cpoint_state
        (vbucket_id integer, cpoint_id integer, prev_cpoint_id integer, state varchar(1),
        source varchar(250), updated_at text);
        pragma user_version=%s;
        COMMIT;
        """ % (MBB_VERSION))
    else:
        db.executescript("""
        BEGIN;
        CREATE TABLE cpoint_op
        (vbucket_id integer, cpoint_id integer, seq integer, op text,
        key varchar(250), flg integer, exp integer, cas integer, cksum varchar(100), val blob);
        CREATE TABLE cpoint_state
        (vbucket_id integer, cpoint_id integer, prev_cpoint_id integer, state varchar(1),
        source varchar(250), updated_at text);
        pragma user_version=%s;
        COMMIT;
        """ % (MBB_VERSION))

    if split_backup:
        max_backup_size = max_backup_size * 1024 * 1024 # Convert MB to bytes
        db_page_size = db.execute("pragma page_size").fetchone()[0]
        db_max_page_count = max_backup_size / db_page_size
        db.execute("pragma max_page_count=%d" % (db_max_page_count))
        db.execute("pragma journal_mode=MEMORY")
    return db


class BackupFactory:
    """
    Backup class to generate backup split on demand
    """

    def __init__(self, base_filepath, backup_type, tapname,logger, host, port,
            txn_size=None):
        self.base_filepath = base_filepath
        self.backup_type = backup_type
        self.logger = logger
        self.split_no = 0
        self.current_split = None
        self.split_backup_files = []
        self.full_backup = False
        self.backfill_chk_start = False
        self.current_checkpoint_id = 0
        self.update_count = 0
        self.source = tapname
        self.complete = False
        self.host = host
        self.port = port

        if txn_size:
            self.txn_size = txn_size
        else:
            self.txn_size = TXN_SIZE

        if backup_type == "full":
            self.full_backup = True
            mc = mc_bin_client.MemcachedClient(host,port)
            mc.deregister_tap_client(tapname)
            ext, val = encodeTAPConnectOpts({
            memcacheConstants.TAP_FLAG_CHECKPOINT: (1, 0, 0),
            memcacheConstants.TAP_FLAG_SUPPORT_ACK: '',
            memcacheConstants.TAP_FLAG_REGISTERED_CLIENT: 0x01, # "value > 0" means "closed checkpoints only"
            memcacheConstants.TAP_FLAG_BACKFILL: 0x00000000,
            memcacheConstants.TAP_FLAG_CKSUM: ''
            }, True)

            mc._sendCmd(memcacheConstants.CMD_TAP_CONNECT, tapname, val, 0, ext)
            cmd, opaque, cas, vbucketId, key, ext, val = readTap(mc)
            if cmd != memcacheConstants.CMD_TAP_OPAQUE:
                raise Exception("ERROR: Could not register tap: %s" %tapname)
            mc.close()

            sclient = mc_bin_client.MemcachedClient(self.host, self.port)
            self.max_cpoint_id = int(sclient.stats('checkpoint')['vb_0:open_checkpoint_id'])
            sclient.close()

        self.mc = mc_bin_client.MemcachedClient(host, port)
        ext, val = encodeTAPConnectOpts({
          memcacheConstants.TAP_FLAG_CHECKPOINT: '',
          memcacheConstants.TAP_FLAG_SUPPORT_ACK: '',
          memcacheConstants.TAP_FLAG_REGISTERED_CLIENT: 0x01, # "value > 0" means "closed checkpoints only"
          memcacheConstants.TAP_FLAG_BACKFILL: 0xffffffff,
          memcacheConstants.TAP_FLAG_CKSUM: ''
        })

        self.mc._sendCmd(memcacheConstants.CMD_TAP_CONNECT, tapname, val, 0, ext)
        self.sinput = [self.mc.s]
        self.op_records = []

        self.vbmap = {} # Key is vbucketId, value is [checkpointId, seq].

    def __del__(self):
        if self.mc:
            self.mc.close()

    def _get_next_file(self, buffer_path):
        path = os.path.join(buffer_path, self.base_filepath)
        path = path.replace('%', str(self.split_no).zfill(5))
        return path

    def is_complete(self):
        return self.complete

    def list_splits(self):
        return self.split_backup_files

    def get_current_split(self):
        return self.current_split
       
    def create_next_split(self, buffer_path):
        if self.complete:
            return None

        commits_count = 0

        filepath = self._get_next_file(buffer_path)
        self.current_split = filepath
        if os.path.exists(filepath):
            raise Exception("File already exists")
        else:
            backup_dir = os.path.dirname(filepath)
            if not os.path.exists(backup_dir):
                try:
                    os.makedirs(backup_dir)
                except Exception, e:
                    raise Exception ("FAILED: Creating Backup directory %s (%s)" %(backup_dir, e.strerror))

        self.logger.log("Creating Backup file : %s" %(filepath))
        db = create_backup_db(filepath, consts.SPLIT_SIZE, True, True)
        self.split_backup_files.append((buffer_path, filepath))
        c = db.cursor()
        if self.current_checkpoint_id > 0:
            t = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            if self.full_backup:
                for i in range(1, self.max_cpoint_id):
                    result = add_record(c, chkpoint_stmt, (0, i, -1, self.source, t))
            else:
                add_record(c, chkpoint_stmt, (0, self.current_checkpoint_id, -1, self.source, t))

            db.commit()

        ## Insert all the records that belong to the rollbacked transaction.
        failed = False
        for record in self.op_records:
            result = add_record(c, tap_stmt, record)
            if result == False:
                failed = True
        if failed == True:
            db.rollback()
            for record in self.op_records[:-1]:
                result = add_record(c, tap_stmt, record)
            self.op_records = [self.op_records[-1]]
            self.update_count = 1
            db.commit()
            db.close()
            self.split_no += 1
            return filepath

        last_checkpoint_id = -1
        while True:
            if TIMEOUT > 0:
                iready, oready, eready = select.select(self.sinput, [], [], TIMEOUT)
                if (not iready) and (not oready) and (not eready):
                    raise Exception("EXIT: timeout after " + str(TIMEOUT) + " seconds of inactivity")

            cmd, opaque, cas, vbucketId, key, ext, val = readTap(self.mc)

            needAck = False

            if (cmd == memcacheConstants.CMD_TAP_MUTATION or
                cmd == memcacheConstants.CMD_TAP_DELETE or
                cmd == memcacheConstants.CMD_TAP_FLUSH):
                cmdName, cmdOp = cmdInfo[cmd]

                if self.full_backup and self.backfill_chk_start == False and len(self.vbmap) == 0:
                    self.vbmap[vbucketId] = [1, 0]
                    t = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                    for i in range(1, self.max_cpoint_id):
                        result = add_record(c, chkpoint_stmt,
                                        (vbucketId, i, -1, self.source, t))
                        if result == False:
                            raise Exception("ERROR: Unable to write checkpoint states"
                                                                "to backup file")
                    db.commit()
                    self.update_count = 0
                    self.op_records = []
                    self.current_checkpoint_id = i
                    self.backfill_chk_start = True

                if not vbucketId in self.vbmap:
                    msg = "%s with unknown vbucketId: %s" % (cmdName, vbucketId)
                    msg += '\n'
                    msg += "ERROR: received %s without checkpoint in vbucket: %s\n" \
                             "Perhaps the server is an older version?" % (cmdName, vbucketId)
                    raise Exception(msg)

                c_s = self.vbmap[vbucketId]
                checkpointId = c_s[0]
                seq          = c_s[1] = c_s[1] + 1

                eng_length, flags, ttl, flg, exp, needAck, cksum_len = parseTapExt(ext)
                cksum = "";

                if cksum_len > 0:
                    cksum_offset = len(val) - cksum_len
                    cksum = val[cksum_offset:]
                    val = val[:cksum_offset]

                val = sqlite3.Binary(val)
                self.op_records.append((vbucketId, checkpointId, seq, cmdOp,key, flg, exp, cas, cksum, val))
                result = add_record(c, tap_stmt, (vbucketId, checkpointId, seq, cmdOp,
                                                  key, flg, exp, cas, cksum, val))
                self.update_count = self.update_count + 1
                if result == False:
                    ## The current backup db file is full
                    try:
                        db.rollback()
                        if commits_count == 0:
                            for record in self.op_records[:-1]:
                                result = add_record(c, tap_stmt, record)
                            db.commit()
                            self.op_records = [self.op_records[-1]]
                            self.update_count = 1
                    except sqlite3.Error, e: ## Can't find the better error for rollback failure.
                        self.logger.log("WARNING: Insertion transaction was already rollbacked: " + e.args[0])
                    c.close()
                    db.close()
                    self.split_no += 1
                    return filepath

            elif cmd == memcacheConstants.CMD_TAP_CHECKPOINT_START:
                if len(ext) > 0:
                    eng_length, flags, ttl, flg, exp, needAck, cksum = parseTapExt(ext)
                checkpoint_id = struct.unpack(">Q", val)
                checkpointStartExists = False
                self.current_checkpoint_id = checkpoint_id[0]

                # While backfill, if it receives START, it indiciates backup is complete.
                if self.backfill_chk_start:
                    if self.update_count > 0:
                        db.commit()
                    self.complete = True
                    return filepath
                elif last_checkpoint_id > 0 and last_checkpoint_id == checkpoint_id[0] - 1:
                    del self.vbmap[vbucketId]
                elif last_checkpoint_id > 0 and last_checkpoint_id != checkpoint_id[0]:
                    raise Exception("Checkpoints received are not continous. cpoint_id:%d " \
                            "received after cpoint_id:%d" %(checkpoint_id[0], last_checkpoint_id))

                if vbucketId in self.vbmap:
                    if self.vbmap[vbucketId][0] == checkpoint_id[0]:
                        checkpointStartExists = True

                if checkpointStartExists == False:
                    self.vbmap[vbucketId] = [checkpoint_id[0], 0]
                    t = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                    result = add_record(c, chkpoint_stmt,
                                        (vbucketId, checkpoint_id[0], -1, self.source, t))
                    if result == False:
                        ## The current backup db file is full and closed.
                        c.close()
                        db.close()
                        self.split_no += 1
                        return filepath
                    else:
                        db.commit()
                        self.op_records = []
                        self.update_count = 0
                    last_checkpoint_id = checkpoint_id[0]

            elif cmd == memcacheConstants.CMD_TAP_CHECKPOINT_END:
                if len(ext) > 0:
                    eng_length, flags, ttl, flg, exp, needAck, cksum = parseTapExt(ext)

            elif cmd == memcacheConstants.CMD_TAP_OPAQUE:
                if len(ext) > 0:
                    eng_length, flags, ttl, flg, exp, needAck, cksum = parseTapExt(ext)
                    opaque_opcode = struct.unpack(">I" , val[0:eng_length])
                    if opaque_opcode[0] == memcacheConstants.TAP_OPAQUE_OPEN_CHECKPOINT:
                        if self.update_count > 0:
                            db.commit()
                            self.op_records = []
                            self.update_count = 0

                        db.commit()
                        self.update_count = 0
                        self.complete = True
                        return filepath

            elif cmd == memcacheConstants.CMD_TAP_CONNECT:
                db.close()
                os.remove(filepath)
                self.mc.close()
                self.mc = None
                raise ConnectException

            elif cmd == memcacheConstants.CMD_NOOP:
                pass
            else:
                raise Exception("ERROR: unhandled cmd " + str(cmd))

            if self.update_count == self.txn_size:
                db.commit()
                commits_count += 1
                self.op_records = []
                self.update_count = 0

            if needAck:
                self.mc._sendMsg(cmd, '', '', opaque,
                            vbucketId=0,
                            fmt=memcacheConstants.RES_PKT_FMT,
                            magic=memcacheConstants.RES_MAGIC_BYTE)


