import unittest
from DBRegister import insert_vehicle
from DBRegister import update_vehicle
from DBRegister import create_SIV
import sqlite3
import os

class TestDBRegisterFunctions(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect('IBM_Db2.sqlite3')
        self.cursor = self.connection.cursor()
        self.row = dict([('adresse_titulaire', '40619 Louis Crossing Port Sierrafurt, RI 27062'), ('nom', 'Hernandez'), ('prenom', 'Kelsey'), ('immatriculation', '44I 563'), ('date_immatriculation', '1960-02-15'), ('vin', '9780891834090'), ('marque', 'Glover and Sons'), ('denomination_commerciale', 'Exclusive holistic utilization'), ('couleur', 'Maroon'), ('carrosserie', '65-3098983'), ('categorie', '50-3645074'), ('cylindree', '4802'), ('energie', '29384754'), ('places', '30'), ('poids', '3107'), ('puissance', '320'), ('type', 'and Sons'), ('variante', '38-4233314'), ('version', '96545300')])
        self.cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='SIV' ''')
        if self.cursor.fetchone()[0]!=1 :
            create_SIV(self.cursor)
            self.connection.commit()

    def tearDown(self):
        self.connection.close()
        os.remove('IBM_Db2.sqlite3')

    def test_insert_vehicle(self):
        self.cursor.execute(' SELECT COUNT(immatriculation) FROM SIV ')
        if self.cursor.fetchone()[0]==0 :
            insert_vehicle(self.row, self.cursor)
            self.cursor.execute(' SELECT COUNT(immatriculation) FROM SIV ')
            self.assertEqual(self.cursor.fetchone()[0],1)

    def test_update_vehicle(self):
        self.cursor.execute(' SELECT COUNT(immatriculation) FROM SIV ')
        if self.cursor.fetchone()[0]==0 :
            insert_vehicle(self.row, self.cursor)
            self.cursor.execute(' SELECT COUNT(immatriculation) FROM SIV ')
            self.assertEqual(self.cursor.fetchone()[0],1)
        update_vehicle(self.row,self.cursor)
