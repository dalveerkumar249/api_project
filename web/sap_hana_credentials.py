from hdbcli import dbapi

from flask import Flask, request, render_template
#from flask_mysqldb import MySQL
#from flask_mysqldb import MySQL
#import MySQLdb.cursors

import pymysql

application = Flask(__name__)

#connection = dbapi.connect(
#        address="35.232.37.67",
#        port=39013,
#        user="ULTEST",
#        password="Singular2021"
#	)   


# connection = dbapi.connect(
#         #address="18.132.191.229",
#         address="13.41.70.144",
#         port=39013,
#         user="SYSTEM",
#         password="SingHana2021"
# 	)   

# connection = dbapi.connect(         
#         address="44.230.140.21",
#         port=3306,
#         user="admin",
#         password="F1rstl1n3**"
# 	)   


#connection = None
# Required

# application.config['MYSQL_HOST'] = 'localhost'
# application.config['MYSQL_USER'] = 'root'
# application.config['MYSQL_PASSWORD'] = 'Winter@123'
# application.config['MYSQL_DB'] = 'ULGROWTH20'
# application.config['MYSQL_CURSORCLASS'] = 'DictCursor'

# mysql = MySQL(application)

connection = pymysql.connect(
        host="141.147.116.13",
        user="root",
        password="admin123",
        database="ULGROWTH20",
        port=5000
    )
#connection=credentials
