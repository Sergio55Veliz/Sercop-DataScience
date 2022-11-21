from utilities.__init__ import *
import zipfile
from utilities.utilities import verify_create_folder
from utilities.utilities import size_archive
from utilities.utilities import progress_bar


def compress_data_to_zip(dir_folder_tosave,
                         dir_folder_toread,
                         name_zip,
                         limit_to_zip=100,
                         extension='.json'
                         ):
    verify_create_folder(dir_folder_tosave, verbose=False)
    verify_create_folder(dir_folder_toread, verbose=False)

    if not name_zip.endswith('.zip'):
        name_zip += '.zip'

    zip = None  # variable del comprimido

    zips_created = 0
    archives_zipped = 0
    size_zipped = 0
    # folder, subfolders, files
    folder, _, files = list(os.walk(dir_folder_toread))[0]  # Solo obtenemos datos de la carpeta especificada como argumento
    files = [f for f in files if f.endswith(extension)]
    print("\nProceso de compresión... ")
    progress_bar(actual_value=archives_zipped,
                 max_value=len(files),
                 initial_message="Archivos comprimidos:",
                 end_message=' | Total zips: '+str(zips_created))
    for index in range(len(files)):
        file = files[index]

        size_mb = size_archive(os.path.join(folder, file))*0.1

        if (size_zipped + size_mb) >= limit_to_zip:
            # Crear el zip al alcanzar el límite o al llegar al último archivo
            zip.close()
            size_zipped = 0
            zips_created += 1
            progress_bar(actual_value=archives_zipped,
                         max_value=len(files),
                         initial_message="Archivos comprimidos:",
                         end_message=' | Total zips: '+str(zips_created))

        if size_zipped == 0:
            name = name_zip.split('.')[0] + "_"+str(zips_created+1) + '.' + name_zip.split('.')[1]
            zip = zipfile.ZipFile(os.path.join(dir_folder_tosave, name), 'w')

        if (size_zipped+size_mb) < limit_to_zip:  # Límite en MB, 100 por default
            zip.write(os.path.join(folder, file),
                      os.path.relpath(os.path.join(folder, file), dir_folder_toread),
                      compress_type=zipfile.ZIP_DEFLATED)
            size_zipped += size_mb
            archives_zipped += 1
            if index == (len(files) - 1):
                zip.close()
                size_zipped = 0
                zips_created += 1

    progress_bar(actual_value=archives_zipped,
                 max_value=len(files),
                 initial_message="Archivos comprimidos:",
                 end_message=' | Total zips: '+str(zips_created))
    print("\n\tZipped file successfully!!")


def extract_data_from_zip(dir_folder_tosave,
                          dir_zip  # Contiene el nombre del archivo a descomprimir
                          ):
    fantasy_zip = zipfile.ZipFile(dir_zip)

    verify_create_folder(dir_folder_tosave, verbose=False)

    fantasy_zip.extractall(dir_folder_tosave)
    fantasy_zip.close()
    print("\n\tExtracted files successfully!!")
