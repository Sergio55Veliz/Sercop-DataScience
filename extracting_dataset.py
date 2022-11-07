import pandas as pd
import requests
import json 

import time
from datetime import date, datetime as dt
import pytz # Para determinar zona horaria
from json.decoder import JSONDecodeError
import os


URL = 'https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds?year='
url_proyect = "D:/Sergio/OneDrive - Escuela Superior Politécnica del Litoral/Documentos/DataScience/"

def getPages_per_year(year):
  x = requests.get(URL+year)
  if x.status_code==200:
    return json.loads(x.content)['pages']
  else:
    raise Exception("Status Conection ERROR !!")
  

def getData(year, page):
  x = requests.get(URL+year+'&page='+page)
  if x.status_code==200:
    return json.loads(x.content)['data']
  else:
    raise Exception("Status Conection ERROR !!")


def generateDatasets(year,
                     total_pages=10,
                     size_batch=1500,
                     total_batches=None):

  try:
    os.makedirs(url_proyect+'logs')
  except FileExistsError as fer:
    print(str(fer))

  try:
    os.makedirs(url_proyect+'data')
  except FileExistsError as fer:
    print(str(fer))

  t_start = time.time()
  listAux = []
  batch = 0

  time_zone = pytz.timezone('America/Guayaquil')
  dt_string = dt.now(time_zone).strftime("%d-%m-%Y %Hh%Mm%Ss")
  txt_name = "logError" + str(year) + " - " + dt_string + ".txt"
  with open(url_proyect + 'logs/' + txt_name, 'w') as file:
    file.write("page,date_error,time_error,error_message\n") # Encabezado

    for page in range(1, total_pages + 1):
      if page % 50 == 0:  # Tiempo de espera cada 50 peticiones
        time.sleep(30)  # añade un retraso de 30s

      try:
        res = getData(year, str(page))
        listAux += res
        print("Añadida página:", page)
      except JSONDecodeError as jsondecErr:
        error_message = str(jsondecErr)
        time_error = dt.now(time_zone).strftime("%H:%M:%S")
        date_error = dt.now(time_zone).strftime("%d/%m/%Y")
        file.write(str(page) + ',' + date_error + ',' + time_error + ',' + error_message + "\n")

        print("\n\t" + error_message + "\n\tin page", str(page) + "\n")
        pass
      except Exception as err:
        error_message = str(err)
        time_error = dt.now(time_zone).strftime("%H:%M:%S")
        date_error = dt.now(time_zone).strftime("%d/%m/%Y")
        file.write(str(page) + ',' + date_error + ',' + time_error + ',' + error_message + "\n")

        print("\n\t" + error_message + "\n\tin page", str(page) + "\n")
        pass

      if page % size_batch == 0 or page == total_pages:
        batch += 1
        name_csv = 'dataset' + str(year) + '_batch' + str(batch) + '.csv'
        print(name_csv)
        pd.DataFrame(listAux).to_csv(url_proyect+'data/' + name_csv, index=False)  # se guarda dataset en el drive
        print("\tBATCH " + str(batch) + ' ejecutado y guardado  ---   tiempo actual de ejecución: ',
              round(time.time() - t_start, 2), end='\n\n')
        listAux.clear()

      if total_batches == batch: break  # Por defecto esto no se ejecuta
                                        # así que por defecto se ejecutan todos los batches

  print("\nSe terminó de ejecutar TODO!!")
  print('Tiempo total de ejecución: ', time.time() - t_start)


if __name__ == '__main__':
  year = '2021'
  generateDatasets(year=year, total_pages=getPages_per_year(year))


