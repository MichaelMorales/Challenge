from flask import Flask, request, jsonify
import pandas as pd
import sqlite3
import os
import fastavro
import datetime
import requests
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 18:48:04 2023

@author: Usuario Autorizado
"""
database_file = 'test1.db'
def create_table_file_avro(archivo,tabla):

  # Ruta del archivo Avro
  archivo_avro = archivo

  # Conexión a la base de datos SQLite
  conexion = sqlite3.connect(database_file)
  cursor = conexion.cursor()

  # Leer el archivo Avro y cargar los datos en una variable
  with open(archivo_avro, 'rb') as archivo:
      datos_avro = fastavro.reader(archivo)

      # Obtener los nombres de los campos del primer registro
      primer_registro = next(datos_avro)
      campos = list(primer_registro.keys())

      # Crear la tabla en la base de datos
      nombre_tabla = tabla
      consulta_creacion = f'CREATE TABLE {nombre_tabla} ({", ".join(f"{campo} TEXT" for campo in campos)})'
      cursor.execute(consulta_creacion)
      conexion.commit()

      # Insertar los datos en la tabla
      consulta_insercion = f'INSERT INTO {nombre_tabla} VALUES ({", ".join("?" for _ in campos)})'
      cursor.executemany(consulta_insercion, (tuple(registro.values()) for registro in [primer_registro] + list(datos_avro)))
      conexion.commit()

  # Cerrar la conexión a la base de datos
  conexion.close()
  return ("Se creo la tabla: ",tabla, " tomando el archivo ",archivo,".")

def crear_archivo_avro(tabla):


  # Conexión a la base de datos SQLite
  conn = sqlite3.connect(database_file)
  cursor = conn.cursor()

  # Nombre de la tabla a exportar
  nombre_tabla = tabla

  # Obtener los nombres de las columnas
  cursor.execute("PRAGMA table_info({})".format(nombre_tabla))
  columnas = [columna[1] for columna in cursor.fetchall()]

  # Definir el esquema Avro basado en las columnas de la tabla
  esquema = {
      "type": "record",
      "name": nombre_tabla,
      "fields": [{"name": columna, "type": "string","default": ''} for columna in columnas]
  }

  # Obtener los datos de la tabla
  cursor.execute("SELECT * FROM {}".format(nombre_tabla))
  datos = cursor.fetchall()

  lista_columnas = columnas
  lista_valores = datos
  lista_resultante = []

  for i in lista_valores:
      res = {}
      for p, j in enumerate(lista_columnas):
          res[j]= str(i[p])
      lista_resultante.append(res)




  # Obtener la fecha y hora actual
  fecha_hora_actual = datetime.datetime.now()
  # Formatear la fecha y hora
  fecha_hora_formateada = str(fecha_hora_actual.strftime("%d-%m-%Y-%H-%M-%S"))
  archivo = '{}-{}.avro'.format(nombre_tabla,fecha_hora_formateada)
  #archivo = '{}-.avro'.format(nombre_tabla)

  with open(archivo, 'wb') as archivo_avro:
      fastavro.writer(archivo_avro, esquema, lista_resultante)
  print('datos 1')

  '''
  # leer y verificar archivo
  with open(archivo, 'rb') as archivo_avro:
      print('datos 2')
      avro_reader = fastavro.reader(archivo_avro)
      for dato in avro_reader:
          print(dato)
  '''

  # Cerrar el cursor y la conexión
  cursor.close()
  conn.close()
  return ("Se creo el correctamente el archivo: ",archivo)


def leer_tabla(tabla):

  # Conexión a la base de datos SQLite
  conn = sqlite3.connect(database_file)
  cursor = conn.cursor()

  # Nombre de la tabla a describir
  nombre_tabla = tabla

  # Ejecutar consulta SELECT
  cursor.execute("SELECT * FROM {}".format(nombre_tabla))

  # Obtener los nombres de las columnas
  columnas = [descripcion[0] for descripcion in cursor.description]
  

  # Obtener los valores de las filas
  filas = cursor.fetchall()
  
  
  json_data = pd.DataFrame(filas, columns=columnas).to_json(orient='records')


  # Imprimir los nombres de las columnas
  print("Nombres de las columnas de la tabla {}:".format(nombre_tabla))
  for columna in columnas:
      #print(columna)
      None

  # Imprimir los valores de las filas
  print("Valores de las filas de la tabla {}:".format(nombre_tabla))
  for fila in filas:
      for valor in fila:
          #print(valor, end=" ")
          None
     

  # Cerrar el cursor y la conexión
  cursor.close()
  conn.close()
  #return ("Termino correctamente ejecucion de: ",nombre_tabla) 
  return json_data




def create_table(csv,campos):
  # Configuración de la conexión a SQLite
  

  # Cargar datos desde el archivo CSV utilizando pandas
  csv_file_path = csv

  # Obtener el nombre del archivo sin la extensión
  nombre_archivo = os.path.splitext(os.path.basename(csv_file_path))[0]

  table_name = nombre_archivo
  #data = pd.read_csv(csv_file_path,names=('ID','Descripcion'))
  data = pd.read_csv(csv_file_path,header=None)
  # Generar nombres de columnas automáticos
  #column_names = ['columna' + str(i+1) for i in range(len(data.columns))]
  column_names = campos

  # Asignar los nombres de columnas al DataFrame
  data.columns = column_names

  # Crear una conexión a SQLite
  conn = sqlite3.connect(database_file)
  drop =  f"DROP TABLE IF EXISTS {table_name} ;"
  conn.execute(drop)
  # Crear un cursor para ejecutar comandos SQL
  cursor = conn.cursor()

  # Crear la tabla en la base de datos
  create_table_query = f"CREATE TABLE {table_name} ("

  # Iterar sobre las columnas del archivo CSV para definir los tipos de datos de la tabla
  for column in data.columns:
      column_name = column.strip()  # Eliminar espacios en blanco adicionales en el nombre de la columna
      data_type = 'String'  # Definir el tipo de dato como TEXT por defecto
      create_table_query += f"{column_name} {data_type}, "

  create_table_query = create_table_query.rstrip(', ') + ")"  # Eliminar la última coma y espacio
  cursor.execute(create_table_query)
  conn.commit()

  # Insertar los datos en la tabla
  insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in data.columns])})"
  values = [tuple(row) for row in data.values]
  cursor.executemany(insert_query, values)
  conn.commit()

  # Cerrar la conexión y el cursor
  cursor.close()
  conn.close()

  return ("Tabla creada correctamente: ",table_name)

'''
lista_tablas = [
    ['jobs.csv',['id_job','desc_job']],
    ['departments.csv',['id_departments','desc_department']],
    ['hired_employees.csv',['id_employees','desc_employees','date','fk_id_job','fk_id_departments']]
     ]
print(lista_tablas)

for i in lista_tablas:
  resultado = create_table(i[0],i[1])
  #print(resultado)
'''
 
crear_archivo_avro('departments')
crear_archivo_avro('jobs')
crear_archivo_avro('hired_employees')
'''create_table_file_avro('departments-02-06-2023-20-58-11.avro','hired_employees1')'''


app = Flask(__name__)
@app.route('/api/infotables/departments', methods=['GET'])
def get_data_t1():
    # Lógica para manejar la solicitud GET
    var_departments     = leer_tabla('departments')
    #data = {'message': '¡Hola desde el API desktop!'}
    #return jsonify(data)
    #response = requests.get(url, params=params)
    return var_departments

@app.route('/api/infotables/jobs', methods=['GET'])
def get_data_t2():
    # Lógica para manejar la solicitud GET
    var_jobs            = leer_tabla('jobs')
    return var_jobs

@app.route('/api/infotables/hired_employees', methods=['GET'])
def get_data_t3():
    # Lógica para manejar la solicitud GET
    var_hired_employees = leer_tabla('hired_employees')

    return var_hired_employees

@app.route('/api/infotables/avro', methods=['GET'])
def get_data_t4():
    # Lógica para manejar la solicitud GET
    avro_response = crear_archivo_avro('departments')
    data = {'message': avro_response}

    return data 

if __name__ == '__main__':
    app.run()
