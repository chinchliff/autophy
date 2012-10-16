import sqlite3, time

secs_minute = 60.0

def patient_commit(dbcon, max_time=300, wait_interval=5):
    # if the database is locked, re-attempt commits for the specified time before quitting
    global secs_minute
    success = False
    time_waited = 0
    while not success:
        if time_waited > max_time:
            raise sqlite3.OperationalError("Database has been locked for over %s minutes. Quitting." \
                                               % (max_time / secs_minute))
        try:
            dbcon.commit()
            success = True
        except sqlite3.OperationalError as error:
            if error.message == "database is locked":
                print "Database temporarily locked. Trying again in %s seconds" % wait_interval
                time.sleep(wait_interval)
                time_waited += wait_interval

def patient_execute(dbcur, query, values = None, max_time=300, wait_interval=5):
    # if the database is locked, re-attempt the query execute for the specified time before quitting
    global secs_minute
    success = False
    time_waited = 0
    while not success:
        if time_waited > max_time:
            raise sqlite3.OperationalError("Database has been locked for over %s minutes. Quitting." \
                                               % (max_time / secs_minute))
        try:
            if values == None:
                dbcur.execute(query)
                success = True
            else:
                dbcur.execute(query, values)
                success = True
        except sqlite3.OperationalError as error:
            if error.message == "database is locked":
                print "Database temporarily locked. Trying again in %s seconds" % wait_interval
                time.sleep(wait_interval)
                time_waited += wait_interval

class safecursor(sqlite3.Cursor):      
    # protected execute methods catch benign exceptions to prevent program halts
    # this class can be passed to the sqlite3.cursor() instance to inherit these methods

    def pexecute(self, statement, values = None):
        try:
            if values is None:
                self.execute(statement)
	    else:
                self.execute(statement, values)

	except sqlite3.OperationalError as error:
            error_tokens = error.message.split()
	    if error_tokens[0] == "table" and error_tokens[3] == "exists":
                print "Table %s already exists." % error_tokens[1]
            else:
                raise error

	except sqlite3.IntegrityError as error:
            if error.message == "column name is not unique":
                print "Add record failed: %s" % error.message
            else:
                raise error

    def pexecutemany(self,statements):
        if hasattr(statements, "__iter__"):
            for line in statements:
                self.pexecute(line, values)
	else:
            self.pexecute(statements, values)
