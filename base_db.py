import os
import sqlite3
import numpy as np
import pandas as pd
sqlite3.register_adapter(np.int64, lambda val: int(val))

class BaseDB:
    def __init__(self,
                 path: str,
                 create: bool = False
                ):

        self._connected = False
        self.path = os.path.normpath(path) # reformats path based on operating system (mac vs windows, etc)
        self._check_exists(create)
        return

    def run_query(self, 
                  sql: str, 
                  params: tuple|dict = None
                 ) -> pd.DataFrame:
        
        self._connect()
        try:
            results = pd.read_sql(sql, self._conn, params=params)
        except Exception as e:
            # add some info about the query, and then re-raise the exception
            raise type(e)(f'sql: {sql}\nparams: {params}') from e
        finally:
            self._close()
        return results

    def run_action(self, 
                   sql: str, 
                   params: tuple|dict = None,
                   commit: bool = False,
                   keep_open: bool = False, # not going to commit but closes connection unless said otherwise
                  ) -> int:
        
        if not self._connected:
            self._connect()
            
        try:
            if params is None:
                self._curs.execute(sql)
            else:
                self._curs.execute(sql, params)

            if commit: 
                self._conn.commit()
        except Exception as e:
            self._conn.rollback() # undo all changes since the last commit
            self._conn.close()
            raise type(e)(f'sql: {sql}\nparams: {params}') from e

        if not keep_open:
            self._close()

        # lastrowid is the last id created for an autoincrement field
        return self._curs.lastrowid
        

    def _connect(self) -> None:
        self._conn = sqlite3.connect(self.path)
        self._curs = self._conn.cursor()
        
        # foreign key constraints are not enabled by default,
        # we need to turn them on
        self._curs.execute("PRAGMA foreign_keys=ON;")
        self._connected = True
        #print('database connection opened')
        return

    def _close(self) -> None:
        self._conn.close()
        self._connected = False
        #print('database connection closed')
        return
        
    def _check_exists(self, create: bool) -> bool:
        self._existed = True
        path_parts = self.path.split(os.sep)
        
        n = len(path_parts)
        for i in range(n):
            part = os.sep.join(path_parts[:i+1])
            if not os.path.exists(part):
                self._existed = False
                if not create:
                    raise FileNotFoundError(f'{part} does not exist')
                if i == n-1:
                    print('Creating db')
                    self._connect()
                    self._close()
                else:
                    os.mkdir(part)
        return