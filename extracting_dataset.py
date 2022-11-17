import pandas as pd
import numpy as np
from math import ceil
import requests
import json

import time
from datetime import date, datetime as dt
import pytz  # Para determinar zona horaria
from json.decoder import JSONDecodeError
import os

URL_per_year = 'https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds?year='
URL_per_ocid = 'https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/record?ocid='
url_proyect = os.getcwd()


def getPages_per_year(year):
    x = requests.get(URL_per_year + year)
    if x.status_code == 200:
        return json.loads(x.content)['pages']
    else:
        raise Exception("Status Conection ERROR !!")


def getData(label_content,
            year=None,
            iterable_code=None  # page / ocid code
            ):
    if year is None:  # Consulta a la data completa por ocid
        x = requests.get(URL_per_ocid + iterable_code)
    else:  # Consulta al resumen por año
        x = requests.get(URL_per_year + year + '&page=' + iterable_code)

    if x.status_code == 200:
        return json.loads(x.content)[label_content]
    else:
        raise Exception("Status Conection ERROR !!")


def generateDatasets(year,
                     total_pages=10,
                     size_batch=1500,
                     total_batches=None):
    try:
        os.makedirs(url_proyect + '\\logs')
    except FileExistsError as fer:
        print(str(fer))

    try:
        os.makedirs(url_proyect + '\\data\\resume\\' + str(year))
    except FileExistsError as fer:
        print(str(fer))

    t_start = time.time()
    listAux = []
    batch = 0

    time_zone = pytz.timezone('America/Guayaquil')
    dt_string = dt.now(time_zone).strftime("%d-%m-%Y %Hh%Mm%Ss")
    txt_name = "logError" + str(year) + " - " + dt_string + ".txt"
    with open(url_proyect + '\\logs\\' + txt_name, 'w') as file:
        file.write("page,date_error,time_error,error_message\n")  # Encabezado

        for page in range(1, total_pages + 1):
            if page % 50 == 0:  # Tiempo de espera cada 50 peticiones
                time.sleep(30)  # añade un retraso de 30s

            try:
                res = getData(label_content='data', year=year, iterable_code=str(page))
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
                pd.DataFrame(listAux).to_csv(url_proyect + '\\data\\' + name_csv,
                                             index=False)  # se guarda dataset en el drive
                print("\tBATCH " + str(batch) + ' ejecutado y guardado  ---   tiempo actual de ejecución: ',
                      round(time.time() - t_start, 2), end='\n\n')
                listAux.clear()

            if total_batches == batch: break  # Por defecto esto no se ejecuta
            # así que por defecto se ejecutan todos los batches

    print("\nSe terminó de ejecutar TODO!!")
    print('Tiempo total de ejecución: ', time.time() - t_start)


def load_data(year):
    batches = 0
    try:
        directory = os.getcwd() + '\\data\\resume\\' + str(year)
        batches = len(os.listdir(directory))
    except FileNotFoundError as fnf_err:
        print("\n\tDirectory not found")
        print(str(fnf_err))
        return

    if batches == 0: raise ValueError("No data found in Directory: " + directory)

    list_dataFrames = [pd.read_csv(directory + "\\dataset" + str(year) + "_batch" + str(batch) + ".csv") for batch in
                       range(1, batches + 1)]
    return pd.concat(list_dataFrames)


def getComplete_data(year,
                     size_batch=15000,
                     total_batches=None,
                     starts=0,
                     ends=None
                     ):
    df = load_data(year).dropna(subset=["ocid"]).sort_values(by="date", ascending=True)

    ocid_codes = df.loc[:, "ocid"].unique().tolist()
    total_batches = ceil(len(ocid_codes) / size_batch)

    if ends is None: ends = len(ocid_codes)

    print("Total batches:    ", total_batches)
    print("Total ocid codes: ", len(ocid_codes))
    print("\nstarts: ", starts)
    print("ends:   ", ends, end="\n\n")

    data = {
        "year": int(year),
        "total_ocid": len(ocid_codes),
        "content": []
    }
    data_per_batch = {
        "year": int(year),
        "total_ocid": len(ocid_codes),
        "batches": total_batches,
        "batch": 1,
        "content": []
    }

    batch = 0
    for i in range(starts, ends):
        ocid = ocid_codes[i]
        if i % 50 == 0:  # Tiempo de espera cada 50 peticiones
            time.sleep(30)  # añade un retraso de 30s

        # TODO validar la conexión tipo 200. Si no es 200, esperar 30s y volver a intentar hasta que si se pueda
        '''
        time_to_wait = 30
        while condition:
            try:
               data["content"]+getData(label_content='releases', iterable_code=ocid)
            except xxx xxx:
                #...
                time.sleep(time_to_wait)
                time_to_wait+=10
        '''

        if i % size_batch == 0 or i == ends - 1:
            batch += 1
            name = "dataComplete" + str(year) + "_batch" + str(batch) + ".json"
            with open(name, 'w') as file:
                json.dump(data_per_batch, file, indent=4)
                # TODO verificar si el indent=4 hace que se cree un archivo innecesariamente muy grande
            if batch != total_batches: data_per_batch = {"year": int(year), "total_ocid": len(ocid_codes),
                                                         "batches": total_batches, "batch": batch + 1,
                                                         "content": []}

    name = "dataComplete" + str(year) + "_all.json"
    with open(name, 'w') as file:
        json.dump(data_per_batch, file, indent=4)
        # TODO verificar si el indent=4 hace que se cree un archivo innecesariamente muy grande


if __name__ == '__main__':
    year = '2020'
    # generateDatasets(year=year, total_pages=getPages_per_year(year))
    # df = getComplete_data(year)
    # print(df.columns)
    # print(df.head().to_string())
    # print(getComplete_data(year))

    # print(getData(label_content='releases', iterable_code='ocds-5wno2w-SIE-HIESSRIO-46-2020-2561'))
