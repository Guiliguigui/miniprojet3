"""
The DBRegister module is used to insert or update the IBM_Db2 database with values from a csv file.

"""
__author__ = "Guillaume Mairesse and Andréas Pierre"

import atexit
import sqlite3
import logging
import os
import argparse
import csv
import sys
from datetime import datetime

def close_connection(conection):
    if conection:
        conection.close()

def create_SIV(cursor):
    """
    Create the SIV table.

    Parameters
    ----------
    cursor : sqlite3.crusor
        the cursor udsed to execute the creation request.
    """

    cursor.execute('''CREATE TABLE SIV (
        adresse_titulaire           TEXT    NOT NULL,
        nom                         TEXT    NOT NULL,
        prenom                      TEXT    NOT NULL,
        immatriculation             TEXT    NOT NULL,
        date_immatriculation        TEXT    NOT NULL,
        vin                         TEXT    NOT NULL,
        marque                      TEXT    NOT NULL,
        denomination_commerciale    TEXT    NOT NULL,
        couleur                     TEXT    NOT NULL,
        carrosserie                 TEXT    NOT NULL,
        categorie                   TEXT    NOT NULL,
        cylindree                   TEXT    NOT NULL,
        energie                     TEXT    NOT NULL,
        places                      TEXT    NOT NULL,
        poids                       TEXT    NOT NULL,
        puissance                   TEXT    NOT NULL,
        type                        TEXT    NOT NULL,
        variante                    TEXT    NOT NULL,
        version                     TEXT    NOT NULL)''')

def update_vehicle(row):
    """
    Update a specific row in the SIV table with a corresponding row from the csv file.

    Parameters
    ----------
    row : dict
        A specific row from the csv file where all the vehicle data is stocked.
    """
    values=tuple(row.values())
    values=values+(row['immatriculation'],)
    cursor.execute('''UPDATE SIV SET 
    adresse_titulaire           = ?,
    nom                         = ?,
    prenom                      = ?,
    immatriculation             = ?,
    date_immatriculation        = ?,
    vin                         = ?,
    marque                      = ?,
    denomination_commerciale    = ?,
    couleur                     = ?,
    carrosserie                 = ?,
    categorie                   = ?,
    cylindree                   = ?,
    energie                     = ?,
    places                      = ?,
    poids                       = ?,
    puissance                   = ?,
    type                        = ?,
    variante                    = ?,
    version                     = ?
    WHERE immatriculation=?''', values)

def insert_vehicle(row):
    """
    Insert a specific row in the SIV table from the csv file.

    Parameters
    ----------
    row : dict
        A specific row from the csv file where all the vehicle data is stocked.
    """
    values=tuple(row.values())
    cursor.execute('INSERT INTO SIV VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', values)



if __name__ == "__main__":
    log_file_name = 'DBRegister-' + datetime.today().strftime('%Y-%m-%d') + '.log'
    logging.basicConfig(filename=log_file_name, level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('infile')
    args=parser.parse_args()
    logging.info('Started')

    if not os.path.isfile(args.infile):
        logging.error('ERROR 101: \'%s\' is not a file!' % args.infile)
        sys.exit(101)
    else:
        connection=sqlite3.connect('IBM_Db2.sqlite3')
        logging.info('Connected to the database.') 

    atexit.register(close_connection,connection)

    cursor = connection.cursor()

    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='SIV' ''')
    if cursor.fetchone()[0]==1 :
	    logging.info('Table SIV exists.')
    else:
        create_SIV(cursor)
        connection.commit()
        logging.info('Table SIV created.')

    csvfile = open(args.infile, 'r', newline='')
    logging.info('Csv file openned.')
    reader = csv.DictReader(csvfile, delimiter=';')

    updated=0
    inserted=0

    for row in reader:
        values=(row['immatriculation'],)
        cursor.execute(' SELECT COUNT(immatriculation) FROM SIV WHERE immatriculation=? ', values)
        if cursor.fetchone()[0]==1 :
            updated+=1
            update_vehicle(row)
        else:
            inserted+=1
            insert_vehicle(row)
            

    logging.info('%d vehicle inserted, %d vehicle updated.', inserted, updated)

    connection.commit()

    logging.info('Finished')

