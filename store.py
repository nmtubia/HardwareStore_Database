from base_db import BaseDB
import os
import pandas as pd
import sqlite3
import numpy as np
from glob import glob
sqlite3.register_adapter(np.int64, lambda val: int(val))

class StoreDB(BaseDB):
    '''
    This class extends BaseDB with code specific to the store database.
    '''
    FOLDER = 'data/'
    SALES_FOLDER = FOLDER + 'to_load/'
    DB_FOLDER = 'db/'
    DB_NAME = 'store.sqlite'

    PRODUCTS = FOLDER + 'products.csv'
    #SALES_SAMPLES = glob(SALES_FOLDER + 'Sales_*.csv')
    STATES = FOLDER + 'states.csv'
    ZIPS = FOLDER + 'zips.csv'
    
    PATH = DB_FOLDER + DB_NAME
    
    def __init__(self, 
                 create: bool = False
                ):
        # call the constructor for the parent class
        super().__init__(self.PATH, create)

        if not self._existed:
            print('Creating database....')
            self._create_tables()
            self._load_static_data()
            #self._load_continuous_data()
        return

    def _create_tables(self) -> None:
        sql = """
            CREATE TABLE tProd(
                prod_id INTEGER PRIMARY KEY,
                prod_desc TEXT NOT NULL,
                unit_price INTEGER NOT NULL
            )
            ;"""
        self.run_action(sql)

        sql = """
            CREATE TABLE tInv(
                invoice_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cust_id INTEGER NOT NULL REFERENCES tCust(cust_id),
                day INTEGER NOT NULL CHECK(day BETWEEN 1 and 31),
                month INTEGER NOT NULL CHECK(month BETWEEN 1 and 12),
                year INTEGER NOT NULL CHECK(length(year) = 4),
                time TEXT NOT NULL
            )
            ;"""
        self.run_action(sql)
        
        sql = """
            CREATE TABLE tInvDetail(
                invoice_id INTEGER NOT NULL REFERENCES tInv(invoice_id),
                prod_id INTEGER NOT NULL REFERENCES tProd(prod_id),
                qty INTEGER NOT NULL,
                PRIMARY KEY (invoice_id, prod_id)
            )
            ;"""
        self.run_action(sql)
        
        sql = """
            CREATE TABLE tCust(
                cust_id INTEGER PRIMARY KEY AUTOINCREMENT,
                first TEXT NOT NULL,
                last TEXT NOT NULL,
                addr TEXT NOT NULL,
                zip TEXT NOT NULL REFERENCES tZip(zip)
            )
            ;"""
        self.run_action(sql)
        
        sql = """
            CREATE TABLE tZip(
                zip TEXT PRIMARY KEY CHECK(length(zip) = 5),
                city TEXT NOT NULL,
                state_id TEXT NOT NULL REFERENCES tState(state_id)
            )
            ;"""
        self.run_action(sql)
        
        sql = """
            CREATE TABLE tState(
                state_id TEXT PRIMARY KEY CHECK(length(state_id) = 2),
                state TEXT NOT NULL
            )
            ;"""
        self.run_action(sql)
        
        return

    def _load_static_data(self) -> None:
        products = pd.read_csv(self.PRODUCTS)

        sql = """
            INSERT INTO tProd(prod_id, prod_desc, unit_price)
            VALUES (:prod_id, :prod_desc, :unit_price)
            ;"""

        for i, row in enumerate(products.to_dict(orient='records')):
            try:
                self.run_action(sql, params=row, commit=False, keep_open=True)
            except Exception as e:
                raise type(e)(f'Error on row {i}') from e

        self._conn.commit()
        self._close()
# --------------------------------------------------------------------------- Loading in tState        
        states = pd.read_csv(self.STATES)

        sql = """
            INSERT INTO tState(state_id, state)
            VALUES (:state_id, :state)
            ;"""
        
        for i, row in enumerate(states.to_dict(orient='records')):
            try:
                self.run_action(sql, params=row, commit=False, keep_open=True)
            except Exception as e:
                raise type(e)(f'Error on row {i}') from e

        self._conn.commit()
        self._close()
# --------------------------------------------------------------------------- Loading in tZip        
        zips = pd.read_csv(self.ZIPS, dtype={'zip':str})
        zips['zip'] = zips['zip'].astype(str)
        
        sql = """
            INSERT INTO tZip(zip, city, state_id)
            VALUES (:zip, :city, :state_id)
            ;"""

        for i, row in enumerate(zips.to_dict(orient='records')):
            try:
                self.run_action(sql, params=row, commit=False, keep_open=True)
            except Exception as e:
                raise type(e)(f'Error on row {i}') from e

        self._conn.commit()
        self._close()
        return
        
    def _load_continuous_data(self) -> None:
        #for sales_file in self.SALES_SAMPLES:
        for sales_file in glob(self.SALES_FOLDER + 'Sales_*.csv'):
            sales = pd.read_csv(sales_file, dtype={'zip':str})
            sales['zip'] = sales['zip'].astype(str)
            
            for index, row in sales.iterrows():
                date_parts = row['date'].split('-')
                year = date_parts[0]
                month = date_parts[1]
                day = date_parts[2].split()[0]
                time = row['date'].split(' ')[1]

                state_id = row['st']

                check_state = """
                    SELECT state_id
                    FROM tState
                    WHERE state_id = :state_id
                    ;"""

                state_exists = self.run_query(check_state, params={'state_id': state_id})

                if state_exists.empty:
                    raise ValueError(f'Invalid state_id on row {index}')
                
                prod_info = {'prod_id': row['prod_id'],
                             'prod_desc': row['prod_desc'],
                             'unit_price': row['unit_price']}

                check_prod = """
                    SELECT prod_id
                    FROM tProd
                    WHERE prod_desc = :prod_desc
                        AND unit_price = :unit_price
                    ;"""

                valid_prod_id = self.run_query(check_prod, params=prod_info)

                if valid_prod_id.empty:
                    raise ValueError(f'Invalid prod_id on row {index}')
                    
                customers = {
                    'first': row['first'],
                    'last': row['last'],
                    'addr': row['addr'],
                    'zip': row['zip']
                    }
    
                check = """
                    SELECT cust_id
                    FROM tCust
                    WHERE first = :first
                        AND last = :last
                        AND addr = :addr
                        AND zip = :zip
                ;"""
        
                customer = self.run_query(check, params=customers)
    
                if customer.empty:
                    
                    sql = """
                         INSERT INTO tCust(first, last, addr, zip)
                         VALUES (:first, :last, :addr, :zip)
                         ;"""
                    try:
                        self.run_action(sql, params=customers, keep_open=True, commit=True)
                    except Exception as e:
                        raise type(e)(f'Error on row {index}') from e
                        
                    customer_id = self._conn.execute("SELECT last_insert_rowid();").fetchone()[0]
                else:
                    customer_id = customer.iloc[0]['cust_id']
    
                invoice_info = {
                    'cust_id': customer_id,
                    'day': day,
                    'month': month,
                    'year': year,
                    'time': time
                    }

                check_invoice = """
                SELECT *
                FROM tInv
                WHERE cust_id = :cust_id
                    AND day = :day
                    AND month = :month
                    AND year = :year
                    AND time = :time
                ;"""

                invoices = self.run_query(check_invoice, params=invoice_info)

                if invoices.empty:
                    
                    sql = """
                        INSERT INTO tInv(cust_id, day, month, year, time)
                        VALUES (:cust_id, :day, :month, :year, :time)
                        ;"""
                    try:
                        self.run_action(sql, params=invoice_info, keep_open=True, commit=True)
                    except Exception as e:
                        raise type(e)(f'Error on row {index}') from e
                        
                    invoice_id = self._conn.execute("SELECT last_insert_rowid();").fetchone()[0]
                else:
                    invoice_id = invoices.iloc[0]['invoice_id']
    
                invoice_detail_info = {
                    'invoice_id': invoice_id,
                    'prod_id': row['prod_id'],
                    'qty': row['qty']
                    }
    
                sql = """
                        INSERT INTO tInvDetail(invoice_id, prod_id, qty)
                        VALUES (:invoice_id, :prod_id, :qty)
                        ;"""
                try:
                    self.run_action(sql, params=invoice_detail_info, commit=True)
                except Exception as e:
                    raise type(e)(f'Error on row {index}') from e
            
            file_name = os.path.basename(sales_file)
            new_path = os.path.join('data', 'loaded', file_name)
            os.rename(sales_file, new_path)
            
        self._close()

        return

    

        