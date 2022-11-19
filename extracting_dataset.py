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

from utilities import *


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
    with open(url_proyect + '\\logs\\' + txt_name, 'w') as file_errors:
        file_errors.write("page,date_error,time_error,error_message\n")  # Encabezado

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
                file_errors.write(str(page) + ',' + date_error + ',' + time_error + ',' + error_message + "\n")

                print("\n\t" + error_message + "\n\tin page", str(page) + "\n")
                pass
            except Exception as err:
                error_message = str(err)
                time_error = dt.now(time_zone).strftime("%H:%M:%S")
                date_error = dt.now(time_zone).strftime("%d/%m/%Y")
                file_errors.write(str(page) + ',' + date_error + ',' + time_error + ',' + error_message + "\n")

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


def load_resume_data(year):
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
                     size_batch=10000, # No usar tamaños mayores a 12000 o sino github no permitirá subirlos al repo
                     ready_made_batches=0,
                     batches=None
                     ):
    df = load_resume_data(year).dropna(subset=["ocid"]).sort_values(by="date", ascending=True)

    ocid_codes = df.loc[:, "ocid"].unique().tolist()

    starts = ready_made_batches * size_batch
    if batches is None:
        ends = len(ocid_codes)
    else:
        ends = (ready_made_batches + batches) * size_batch

    total_batches = ceil(ends / size_batch)
    print("Total batches:    ", total_batches)
    print("Total ocid codes: ", len(ocid_codes))
    print("\nstarts: ", starts)
    print("ends:   ", ends, end="\n\n")

    data_per_batch = {
        "year": int(year),
        "total_ocid": len(ocid_codes),
        "total_posible_batches": ceil(len(ocid_codes) / size_batch),
        "batches": total_batches,
        "batch": ready_made_batches + 1,
        "content": []
    }

    try:
        os.makedirs(url_proyect + '/data/complete/' + str(year))
    except FileExistsError as fer:
        print('\tLa carpeta \''+url_proyect + '/data/complete/' + str(year)+'\' ya ha sido creada')

    conection_errors = 0
    json_errors = 0

    time_zone = pytz.timezone('America/Guayaquil')
    dt_string = dt.now(time_zone).strftime("%d-%m-%Y %Hh%Mm%Ss")
    txt_name = "logError_CompleteData" + str(year) + " - " + dt_string + ".txt"
    with open(url_proyect + '/logs/' + txt_name, 'w') as file_errors:
        t_begin = time.time()
        file_errors.write("iteration_num,times_tried,ocid_code,date_error,time_error,error_message\n")  # Encabezado

        for n_consulta in range(starts):
            progress_bar(actual_value=n_consulta+1,
                         max_value=ends,
                         initial_message="Progress:",
                         end_message=' |  ' + get_str_time_running(t_begin))

        batch = ready_made_batches
        for i in range(starts, ends):
            errors_text = '⚠errors: 🌐'+str(conection_errors) + '  📃'+str(json_errors)
            ocid = ocid_codes[i]
            if (i + 1) % 50 == 0:  # Tiempo de espera cada 50 peticiones
                progress_bar(actual_value=i,
                             max_value=ends,
                             initial_message="Progress:",
                             end_message='⏳|  ' + get_str_time_running(t_begin) + '  |  ' + errors_text + '  |  ocid code: ' + ocid
                             )
                time.sleep(30)  # añade un retraso de 30s

            times_tried = 0
            time_to_wait = 30
            reload_page = True
            while reload_page:  # Si surgen errores volvemos a hacer el request luego de un tiempo (time_to_wait)
                times_tried += 1
                try:
                    ocid_process = getData(label_content='releases', iterable_code=ocid)
                    data_per_batch["content"] += ocid_process
                    reload_page = False
                except JSONDecodeError as jsondecErr:
                    json_errors+=1
                    progress_bar(actual_value=i + 1,
                                 max_value=ends,
                                 initial_message="Progress:",
                                 end_message='⏳|  ' + get_str_time_running(t_begin) + '  |  ' + errors_text + '  |  ocid code: ' + ocid,
                                 error_indicator=True
                                 )
                    time.sleep(time_to_wait)
                    time_to_wait += 10
                    reload_page = True

                    error_message = str(jsondecErr)
                    time_error = dt.now(time_zone).strftime("%H:%M:%S")
                    date_error = dt.now(time_zone).strftime("%d/%m/%Y")

                    file_errors.write(str(i) + ',' + str(times_tried) + ',' + str(ocid) + ',' + date_error + ',' + time_error + ',' + error_message + "\n")

                    #print(color.LIGHTRED_EX + "\n\t" + error_message + "\n\tin ocid", str(ocid) + "\n")
                except Exception as err:
                    conection_errors+=1
                    progress_bar(actual_value=i + 1,
                                 max_value=ends,
                                 initial_message="Progress:",
                                 end_message='⏳|  ' + get_str_time_running(t_begin) + '  |  ' + errors_text + '  |  ocid code: ' + ocid,
                                 error_indicator=True
                                 )
                    time.sleep(time_to_wait)
                    time_to_wait += 10
                    reload_page = times_tried < 5  # o va a haber más de 5 intentos por un error de conexion al api
                    # Al quinto intento se dejará de intentar consultar ese código ocid

                    error_message = str(err)
                    time_error = dt.now(time_zone).strftime("%H:%M:%S")
                    date_error = dt.now(time_zone).strftime("%d/%m/%Y")
                    file_errors.write(str(i) + ',' + str(times_tried) + ',' + str(ocid) + ',' + date_error + ',' + time_error + ',' + error_message + "\n")

                    #print(color.LIGHTRED_EX + "\n\t" + error_message + "\n\tin ocid", str(ocid) + "\n")

            if (i + 1) % size_batch == 0 or (i + 1) == ends:
                batch += 1
                name = "dataComplete" + str(year) + "_batch" + str(batch) + ".json"
                with open(url_proyect + '/data/complete/' + str(year) + '/' + name, 'w') as file_batch:
                    json.dump(data_per_batch, file_batch, indent=4)
                if batch != total_batches:
                    data_per_batch = {
                        "year": int(year),
                        "total_ocid": len(ocid_codes),
                        "total_posible_batches": ceil(len(ocid_codes) / size_batch),
                        "batches": total_batches,
                        "batch": batch + 1,
                        "content": []
                    }

            progress_bar(actual_value = i+1,
                         max_value=ends,
                         initial_message="Progress:",
                         end_message='⏳|  ' + get_str_time_running(t_begin) + '  |  ' + errors_text + '  |  ocid code: ' + ocid
                         )




if __name__ == '__main__':
    year = '2022'
    # generateDatasets(year=year, total_pages=getPages_per_year(year))
    getComplete_data(year=year)
    #getComplete_data(year=year, ready_made_batches=1)