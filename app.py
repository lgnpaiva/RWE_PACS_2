import os
import re
import psycopg2
import modalities
import pydicom
from dateutil import parser
from io import BytesIO
from datetime import datetime
from unidecode import unidecode
import logging
from google.cloud import logging as cloud_logging
from pydicom.uid import UID_dictionary
from pynetdicom import AE
from pynetdicom import debug_logger
from pynetdicom import evt
from pynetdicom import AllStoragePresentationContexts
from pynetdicom import ALL_TRANSFER_SYNTAXES
from google.cloud import storage
# debug_logger()

def connection():
    conn = psycopg2.connect(
        host='10.12.32.7',
        database='telemed',
        user='postgres',
        password='DK&xb|=J}7fULKB6',
        port=5432
    )
    cur = conn.cursor()
    return conn, cur

def close_connection(conn, cur):
    if cur:
        cur.close()
    if conn:
        conn.close()

def convert_dicom_date(dicom_date):
    if re.match(r'^\d{8}$', dicom_date):
        formatted_date = datetime.strptime(dicom_date, '%Y%m%d').date()
        return formatted_date.strftime('%Y-%m-%d')

    try:
        parsed_date = parser.parse(dicom_date)
        return parsed_date.strftime('%Y-%m-%d')
    except ValueError:
        return datetime.now().strftime("%Y-%m-%d")

def convert_dicom_time(dicom_time):
    if re.match(r'^\d{8}$', dicom_time):
        formatted_date = datetime.strptime(dicom_time, '%H:%M:%S').date()
        return formatted_date.strftime('%H:%M:%S')

    try:
        parsed_date = parser.parse(dicom_time)
        return parsed_date.strftime('%H:%M:%S')
    except ValueError:
        return datetime.now().strftime("%H:%M:%S")

def convert_born_age(birth_date):
    birth_date = datetime.strptime(birth_date, "%Y-%m-%d")
    today = datetime.now()
    age = today.year - birth_date.year

    if (today.month, today.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age

# Inicializar cliente do Google Cloud Logging
client = cloud_logging.Client()
client.setup_logging()

# Configurar o logger
logger = logging.getLogger("pynetdicomLogger")
logger.setLevel(logging.INFO)

def log(status, message):
    try:
        logger.log_text(message, severity=status)
    except Exception as e:
        logger.error(f"Erro encontrado: {e}")
    # directory = 'logs'
    # folder = os.path.join(directory, datetime.now().strftime('%Y%m%d'))
    # os.makedirs(folder, exist_ok=True)
    # log = open(os.path.join(folder, status + '.log'), mode='a', encoding='utf-8')
    # log.write(message)
    # log.close()

associations = {}

def store(event):
    ds = event.dataset
    ds.file_meta = event.file_meta

    dictionary = {}

    if 'InstitutionName' in ds and ds.InstitutionName:
        dictionary['InstitutionName'] = ds.InstitutionName.replace('.','').strip()
    else:
        dictionary['InstitutionName'] = ''

    if 'SOPClassUID' in ds and ds.SOPClassUID in UID_dictionary:
        dictionary['SOPClassUID'] = UID_dictionary[ds.SOPClassUID][0]
    else:
        dictionary['SOPClassUID'] = ''

    if 'PatientID' in ds and ds.PatientID:
        dictionary['PatientID'] = ds.PatientID
    else:
        dictionary['PatientID'] = ''

    if 'PatientName' in ds and ds.PatientName:
        dictionary['PatientName'] = re.sub(r'\^+', ' ', ' '.join(ds.PatientName.components)).strip()
    else:
        dictionary['PatientName'] = ''

    if 'PatientSex' in ds and ds.PatientSex:
        if ds.PatientSex == 'M':
            dictionary['PatientSex'] = 'Masculino'
        elif ds.PatientSex == 'F':
            dictionary['PatientSex'] = 'Feminino'
        else:
            dictionary['PatientSex'] = 'Outros'
    else:
        dictionary['PatientSex'] = ''

    if 'PatientBirthDate' in ds and ds.PatientBirthDate:
        dictionary['PatientBirthDate'] = convert_dicom_date(ds.PatientBirthDate)
        dictionary['PatientAge'] = convert_born_age(dictionary['PatientBirthDate'])
    else:
        dictionary['PatientBirthDate'] = str(datetime.now().strftime("%Y-%m-%d"))
        dictionary['PatientAge'] = 0

    if 'Modality' in ds:
        # dictionary['Modality'] = modalities.modality(ds.Modality)
        dictionary['Modality'] = ds.Modality
    else:
        dictionary['Modality'] = ''

    if 'StudyDate' in ds:
        dictionary['StudyDate'] = str(convert_dicom_date(ds.StudyDate))

    if 'StudyTime' in ds:
        dictionary['StudyTime'] = str(convert_dicom_time(ds.StudyTime))

    if 'StudyInstanceUID' in ds:
        dictionary['StudyInstanceUID'] = ds.StudyInstanceUID
    else:
        dictionary['StudyInstanceUID'] = ''

    if 'SeriesInstanceUID' in ds:
        dictionary['SeriesInstanceUID'] = ds.SeriesInstanceUID
    else:
        dictionary['SeriesInstanceUID'] = ''

    if 'InstanceNumber' in ds:
        dictionary['InstanceNumber'] = ds.InstanceNumber
    else:
        dictionary['InstanceNumber'] = ''

    if 'SeriesNumber' in ds:
        dictionary['SeriesNumber'] = ds.SeriesNumber
    else:
        dictionary['SeriesNumber'] = ''

    if 'AccessionNumber' in ds:
        dictionary['AccessionNumber'] = ds.AccessionNumber
    else:
        dictionary['AccessionNumber'] = ''

    if 'StudyDescription' in ds:
        dictionary['StudyDescription'] = ds.StudyDescription
    else:
        dictionary['StudyDescription'] = ''

    dictionary['SOPInstanceUID'] = ds.SOPInstanceUID
    dictionary['AffectedSOPClassUID'] = event.request.AffectedSOPClassUID.split(' - ')[0]

    # VARIABLES
    study_id = ''
    serie_id = ''
    InstitutionName = re.sub(r'[^\w_]', '', unidecode(ds.InstitutionName)).strip().lower()
    file_path = os.path.join('dicom', f'{ds.SOPInstanceUID}.dcm')
    dicom_stream = BytesIO()
    ds.save_as(dicom_stream, write_like_original=False)
    file_size = dicom_stream.getbuffer().nbytes
    dicom_stream.seek(0)

    # TEST CONN POSTGRESQL
    try:
        connect = connection()
        conn = connect[0]
        cur = connect[1]

    except Exception as e:
        print("connection error: ", e)
        write = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n--> Connection error: {e}\n\n"
        log('ERROR', write)
        return 0xC000

    # NO INSTITUTION NAME
    if not 'InstitutionName' in ds or not dictionary['InstitutionName']:
        query = f"SELECT study_instance_uid FROM pacs_logs WHERE study_instance_uid = '{ds.StudyInstanceUID}' LIMIT 1"
        try:
            cur.execute(query)

        except Exception as e:
            write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error select pacs_logs: {e}\n\n"
            log('ERROR', write)
            if conn:
                conn.rollback()

            close_connection(conn, cur)
            return 0xC000
        else:
            row = cur.fetchone()
            if row is None:
                columns = (
                    "patient_name",
                    "study_instance_uid",
                    "message",
                    "created_at"
                )

                values = (
                    dictionary['PatientName'],
                    dictionary['StudyInstanceUID'],
                    "Institution name não informado!",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )

                columns_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))

                query = f"INSERT INTO pacs_logs ({columns_str}) VALUES ({placeholders})"
                try:
                    cur.execute(query, values)
                    # conn.commit()

                except Exception as e:
                    write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error 'pacs_logs': {e}\n\n"
                    log('ERROR', write)
                    if conn:
                        conn.rollback()

                    close_connection(conn, cur)

                else:
                    conn.commit()
                    close_connection(conn, cur)
            else:
                update = f"UPDATE pacs_logs SET message = 'Institution name não informado!' WHERE study_instance_uid = '{ds.StudyInstanceUID}' RETURNING *"
                print('update 2.2')
                cur.execute(update)
                conn.commit()
                close_connection(conn, cur)

        return 0x0000

    # INSTITUTION NAME WITHOUT CLIENT
    if 'InstitutionName' in ds or dictionary['InstitutionName']:
        query = f"select id from client_institutions where institution_name = '{ds.InstitutionName}' LIMIT 1"
        try:
            cur.execute(query)
            print('try log 2')

        except Exception as e:
            write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error select in 'client_institutions': {e}\n\n"
            log('ERROR', write)
            if conn:
                conn.rollback()

            close_connection(conn, cur)
            print('except log 2')
            print(write)
            return 0xC000

        else:
            print('else log 2.1')
            institution = cur.fetchone()
            print(institution)
            if institution is None:
                query = f"select id from pacs_logs where study_instance_uid = '{ds.StudyInstanceUID}' LIMIT 1"
                try:
                    cur.execute(query)
                    print('try log 2.2')

                except Exception as e:
                    write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error select in 'pacs_logs': {e}\n\n"
                    log('ERROR', write)
                    if conn:
                        conn.rollback()

                    close_connection(conn, cur)
                    print('except log 2.2')
                    print(write)
                    return 0xC000

                else:
                    StudyInstanceUID = cur.fetchone()
                    print('else log 2.2')
                    if StudyInstanceUID is None:
                        columns = (
                            "institution_name",
                            "patient_name",
                            "study_instance_uid",
                            "message",
                            "created_at"
                        )

                        values = (
                            dictionary['InstitutionName'],
                            dictionary['PatientName'],
                            dictionary['StudyInstanceUID'],
                            "Institution name informado e sem associação!",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )

                        columns_str = ", ".join(columns)
                        placeholders = ", ".join(["%s"] * len(columns))

                        query = f"INSERT INTO pacs_logs ({columns_str}) VALUES ({placeholders})"
                        try:
                            cur.execute(query, values)
                            print('try log 2.2')
                            # conn.commit()

                        except Exception as e:
                            write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error 'pacs_logs': {e}\n\n"
                            log('ERROR', write)
                            print('Exception log 2.2')
                            print(write)
                            if conn:
                                conn.rollback()

                            close_connection(conn, cur)

                        else:
                            print('else log 2.2')
                            conn.commit()
                            close_connection(conn, cur)
                    else:
                        update = f"UPDATE pacs_logs SET message = 'Institution name informado e sem associação!' WHERE study_instance_uid = '{ds.StudyInstanceUID}' RETURNING *"
                        print('update 2.2')
                        cur.execute(update)
                        conn.commit()
                        close_connection(conn, cur)

        # return 0x0000

    # TABLE STUDIES
    query = f"SELECT study_instance_uid, id FROM studies WHERE study_instance_uid = '{ds.StudyInstanceUID}' LIMIT 1"
    try:
        cur.execute(query)
        print('try 1')

    except Exception as e:
        write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error select in 'studies': {e}\n\n"
        log('ERROR', write)
        if conn:
            conn.rollback()

        close_connection(conn, cur)
        print('except 1')
        return 0xC000

    else:
        print('else 1')
        study = cur.fetchone()
        if study is None:
            columns = (
                "institution_name",
                "patient_id",
                "patient_name",
                "patient_sex",
                "patient_birth_date",
                "patient_age",
                "accession_number",
                "modality",
                "study_description",
                "study_instance_uid",
                "study_date_time",
                "status",
            )
            values = (
                dictionary['InstitutionName'],
                dictionary['PatientID'],
                dictionary['PatientName'],
                dictionary['PatientSex'],
                dictionary['PatientBirthDate'],
                dictionary['PatientAge'],
                dictionary['AccessionNumber'],
                dictionary['Modality'],
                dictionary['StudyDescription'],
                dictionary['StudyInstanceUID'],
                f"{dictionary['StudyDate']} {dictionary['StudyTime']}",
                'receiving'
            )
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            query = f"INSERT INTO studies ({columns_str}) VALUES ({placeholders}) RETURNING id"

            try:
                cur.execute(query, values)
                study_id = cur.fetchone()[0]
                print('try 2')
                # conn.commit()
            except Exception as e:
                write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error insert in 'studies': {e}\n\n"
                log('ERROR', write)
                if conn:
                    conn.rollback()

                close_connection(conn, cur)
                print('except 2')
                print(write)
                return 0xC000

        else:
            study_id = study[1]
            print('else 2')

    # TABLE SERIES
    query = f"SELECT series_instance_uid, id FROM series WHERE series_instance_uid = '{ds.SeriesInstanceUID}' LIMIT 1"
    try:
        cur.execute(query)
        print('else 3')

    except Exception as e:
        write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error select in 'series': {e}\n\n"
        log('ERROR', write)
        if conn:
            conn.rollback()

        close_connection(conn, cur)
        print('except 3')
        print(write)
        return 0xC000

    else:
        print('else 3')
        serie = cur.fetchone()
        if serie is None:
            columns = (
                "study_id",
                "series_instance_uid",
                "series_number",
            )
            values = (
                study_id,
                dictionary['SeriesInstanceUID'],
                dictionary['SeriesNumber'],
            )
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            query = f"INSERT INTO series ({columns_str}) VALUES ({placeholders}) RETURNING id"

            try:
                cur.execute(query, values)
                serie_id = cur.fetchone()[0]
                print('try 4')
                # conn.commit()
            except Exception as e:
                write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error insert 'series': {e}\n\n"
                log('ERROR', write)
                if conn:
                    conn.rollback()

                close_connection(conn, cur)
                print('except 4')
                print(write)
                return 0xC000
        else:
            serie_id = serie[1]
            print('else 4')

    # TABLE INSTANCES
    query = f"SELECT sop_instance_uid FROM instances WHERE sop_instance_uid = '{ds.SOPInstanceUID}' LIMIT 1"
    try:
        cur.execute(query)
        print('else 5')

    except Exception as e:
        write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error select in 'instances': {e}\n\n"
        log('ERROR', write)
        if conn:
            conn.rollback()

        close_connection(conn, cur)
        print('except 5')
        return 0xC000

    else:
        print('else 5')
        instance = cur.fetchone()
        if instance is None:
            columns = (
                "serie_id",
                "sop_instance_uid",
                "file_path",
                "file_size",
                "instance_number",
                "created_at"
            )
            values = (
                serie_id,
                dictionary['SOPInstanceUID'],
                file_path,
                file_size,
                dictionary['InstanceNumber'],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            query = f"INSERT INTO instances ({columns_str}) VALUES ({placeholders})"

            try:
                print('try 6')
                cur.execute(query, values)
                # conn.commit()
            except Exception as e:
                write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error insert instances: {e}\n\n"
                log('ERROR', write)
                if conn:
                    conn.rollback()

                close_connection(conn, cur)
                print('except 6')
                return 0xC000

            # Save
            try:
                print('try 7')
                # path = os.path.join('/home/lgnpaiva/Projetos/backend-telemed-4/storage/app/dicom')
                # os.makedirs(path, exist_ok=True)
                # ds.save_as(os.path.join(path, f'{ds.SOPInstanceUID}.dcm'), write_like_original=False)

                # Bucket Google Storage
                gcpKey = '/etc/pacs/key.json'
                # gcpKey = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                client = storage.Client.from_service_account_json(gcpKey)
                bucket = client.get_bucket('storage-telemed-4')
                blob = bucket.blob(file_path)
                blob.upload_from_file(dicom_stream, content_type='application/dicom')
            except Exception as e:
                write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Error save in folder studies: {e}\n\n"
                log('ERROR', write)
                if conn:
                    conn.rollback()
                print('except 7')
                print(write)
                return 0xC000

            assoc_key = f"{event.assoc.remote['address']}:{event.assoc.remote['port']}"
            if assoc_key not in associations:
                associations[assoc_key] = []

            if ds.StudyInstanceUID not in associations[assoc_key]:
                associations[assoc_key].append(ds.StudyInstanceUID)

            conn.commit()
            return 0x0000

    close_connection(conn, cur)
    return 0x0000

def echo(event):
    print(event.assoc.remote)
    write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Echo received from {event.assoc.remote.as_tuple()}\n\n"
    log('INFO', write)

    return 0x0000

def released(event):
    assoc_key = f"{event.assoc.remote['address']}:{event.assoc.remote['port']}"

    if assoc_key in associations:
        try:
            connect = connection()
            conn = connect[0]
            cur = connect[1]

        except Exception as e:
            print("connection error: ", e)
            write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error released: {e}\n\n"
            log('ERROR', write)
            return 0xC000
        else:
            uids = ", ".join(f"'{uid}'" for uid in associations[assoc_key])
            update = f"UPDATE studies SET status = 'completed' WHERE study_instance_uid in ({uids}) RETURNING *"
            print(update)
            cur.execute(update)
            conn.commit()
            print(cur.statusmessage)
            close_connection(conn, cur)

        print(associations[assoc_key])
        del associations[assoc_key]

    return 0x0000

def aborted(event):
    assoc_key = f"{event.assoc.remote['address']}:{event.assoc.remote['port']}"

    if assoc_key in associations:
        try:
            connect = connection()
            conn = connect[0]
            cur = connect[1]

        except Exception as e:
            print("connection error: ", e)
            write = f"{str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n--> Connection error released: {e}\n\n"
            log('ERROR', write)
            return 0xC000
        else:
            uids = ", ".join(f"'{uid}'" for uid in associations[assoc_key])
            update = f"UPDATE studies SET status = 'aborted' WHERE study_instance_uid in ({uids}) RETURNING *"
            print(update)
            cur.execute(update)
            conn.commit()
            print(cur.statusmessage)
            close_connection(conn, cur)

        print(associations[assoc_key])
        del associations[assoc_key]

    return 0x0000

handlers = [
    (evt.EVT_C_ECHO, echo),
    (evt.EVT_C_STORE, store),
    (evt.EVT_RELEASED, released),
    (evt.EVT_ABORTED, aborted)
]

ae = AE()
# ae.maximum_pdu_size = 0
#ae.acse_timeout = 1200
#ae.connection_timeout = 1200
#ae.dimse_timeout = 1200
#ae.network_timeout = 1200
ae.maximum_associations = 100
ae.implementation_version_name = 'RWEPACS-3.0'
ae.ae_title = 'RWEPACS'

ae.add_supported_context('1.2.840.10008.5.1.4.31', '1.2.840.10008.1.2')
ae.add_supported_context('1.2.840.10008.1.1', '1.2.840.10008.1.2')
storageSopClasses = [
    cx.abstract_syntax for cx in AllStoragePresentationContexts
]
for uid in storageSopClasses:
    ae.add_supported_context(uid, ALL_TRANSFER_SYNTAXES)

ae.start_server(('0.0.0.0', 11190), evt_handlers=handlers)
