import psycopg2
import os
from datetime import datetime
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def log(status, message):
    directory = 'logs'
    folder = os.path.join(directory, datetime.now().strftime('%Y%m%d'))
    os.makedirs(folder, exist_ok=True)
    log = open(os.path.join(folder, status + '.log'), mode='a', encoding='utf-8')
    log.write(message)
    log.close()

def create_tables():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="127.0.0.1",
            port="5432",
            dbname="telemed"
        )
    except psycopg2.Error as e:
        write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao conectar ao banco de dados: {e}\n\n"
        log('error', write)
        return

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    drop_query = "DROP TABLE IF EXISTS client_institutions;"
    try:
        cursor.execute(drop_query)

    except Exception as e:
        write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao excluir tabela 'client_institutions': {e}\n\n"
        log('error', write)
        if connection:
            connection.rollback()
        return

    else:
        print("Tabela 'client_institutions' excluída com sucesso.")
        create_query = """
            CREATE TABLE IF NOT EXISTS client_institutions (
                client_id UUID NOT NULL,
                institution_name VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT (date_trunc('second', CURRENT_TIMESTAMP)),
                updated_at TIMESTAMP,
                deleted_at TIMESTAMP,
                PRIMARY KEY (client_id, institution_name)
            );
        """
        try:
            cursor.execute(create_query)

        except Exception as e:
            write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao criar tabela 'client_institutions': {e}\n\n"
            log('error', write)
            if connection:
                connection.rollback()
            return

        else:
            print("Tabela 'client_institutions' criada com sucesso.")

            drop_query = "DROP TABLE IF EXISTS studies CASCADE;"
            try:
                cursor.execute(drop_query)

            except Exception as e:
                write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao excluir tabela 'studies': {e}\n\n"
                log('error', write)
                if connection:
                    connection.rollback()
                return

            else:
                print("Tabela 'studies' excluída com sucesso.")

                create_query = """
                    CREATE TABLE IF NOT EXISTS studies (
                        id SERIAL PRIMARY KEY,
                        patient_id VARCHAR NOT NULL,
                        institution_name VARCHAR NOT NULL,
                        patient_name VARCHAR NOT NULL,
                        patient_sex VARCHAR(20) NOT NULL,
                        patient_birth_date DATE NOT NULL,
                        patient_age INTEGER NOT NULL,
                        accession_number VARCHAR NOT NULL,
                        study_description VARCHAR NOT NULL,
                        study_instance_uid VARCHAR NOT NULL,
                        study_date_time TIMESTAMP NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'receiving',
                        created_at TIMESTAMP NOT NULL DEFAULT (date_trunc('second', CURRENT_TIMESTAMP)),
                        updated_at TIMESTAMP,
                        deleted_at TIMESTAMP
                    );
                """

                try:
                    cursor.execute(create_query)

                except Exception as e:
                    write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao excluir tabela 'client_institutions': {e}\n\n"
                    log('error', write)
                    if connection:
                        connection.rollback()
                    return
                else:
                    print("Tabela 'studies' criada com sucesso.")


                    drop_query = "DROP TABLE IF EXISTS series CASCADE;"
                    try:
                        cursor.execute(drop_query)

                    except Exception as e:
                        write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao excluir tabela 'series': {e}\n\n"
                        log('error', write)
                        if connection:
                            connection.rollback()
                        return

                    else:
                        print("Tabela 'series' excluída com sucesso.")
                        create_query = """
                            CREATE TABLE IF NOT EXISTS series (
                                id SERIAL PRIMARY KEY,
                                study_id INTEGER NOT NULL,
                                series_instance_uid VARCHAR NOT NULL,
                                series_number INTEGER NOT NULL,
                                created_at TIMESTAMP NOT NULL DEFAULT (date_trunc('second', CURRENT_TIMESTAMP)),
                                updated_at TIMESTAMP,
                                deleted_at TIMESTAMP,
                                FOREIGN KEY (study_id) REFERENCES studies(id) ON DELETE CASCADE
                            );
                        """
                        try:
                            cursor.execute(create_query)

                        except Exception as e:
                            write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao criar tabela 'series': {e}\n\n"
                            log('error', write)
                            if connection:
                                connection.rollback()
                            return
                        else:
                            print("Tabela 'series' criada com sucesso.")
                            create_index = """CREATE INDEX IF NOT EXISTS series_study_id_idx ON series(study_id);"""
                            cursor.execute(create_index)
                            print("Index 'study_id' em 'series' criada com sucesso.")


                            drop_query = "DROP TABLE IF EXISTS instances CASCADE;"
                            try:
                                cursor.execute(drop_query)

                            except Exception as e:
                                write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao excluir tabela 'instances': {e}\n\n"
                                log('error', write)
                                if connection:
                                    connection.rollback()
                                return

                            else:
                                print("Tabela 'instances' excluída com sucesso.")

                                create_query = """
                                    CREATE TABLE IF NOT EXISTS instances (
                                        id SERIAL PRIMARY KEY,
                                        serie_id INTEGER NOT NULL,
                                        sop_instance_uid VARCHAR NOT NULL,
                                        instance_number INTEGER NOT NULL,
                                        file_path VARCHAR NOT NULL,
                                        file_size BIGINT NOT NULL,
                                        created_at TIMESTAMP NOT NULL DEFAULT (date_trunc('second', CURRENT_TIMESTAMP)),
                                        updated_at TIMESTAMP,
                                        deleted_at TIMESTAMP,
                                        FOREIGN KEY (serie_id) REFERENCES series(id) ON DELETE CASCADE
                                    );
                                """

                                try:
                                    cursor.execute(create_query)

                                except Exception as e:
                                    write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao criar tabela 'instances': {e}\n\n"
                                    log('error', write)
                                    if connection:
                                        connection.rollback()
                                    return

                                else:
                                    print("Tabela 'instances' criada com sucesso.")
                                    create_index = """CREATE INDEX IF NOT EXISTS instances_serie_id_idx ON instances(serie_id);"""
                                    cursor.execute(create_index)
                                    print("Index 'serie_id' em 'instances' criada com sucesso.")

                                    drop_query = "DROP TABLE IF EXISTS pacs_logs CASCADE;"
                                    try:
                                        cursor.execute(drop_query)

                                    except Exception as e:
                                        write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao excluir tabela 'pacs_logs': {e}\n\n"
                                        log('error', write)
                                        if connection:
                                            connection.rollback()
                                        return

                                    else:
                                        print("Tabela 'pacs_logs' excluída com sucesso.")

                                        create_query = """
                                            CREATE TABLE IF NOT EXISTS pacs_logs (
                                                id SERIAL PRIMARY KEY,
                                                study_id INTEGER NOT NULL,
                                                message VARCHAR NOT NULL,
                                                created_at TIMESTAMP NOT NULL DEFAULT (date_trunc('second', CURRENT_TIMESTAMP)),
                                                updated_at TIMESTAMP,
                                                deleted_at TIMESTAMP
                                            );
                                        """

                                        try:
                                            cursor.execute(create_query)

                                        except Exception as e:
                                            write = f"{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}\n--> Erro ao criar tabela 'pacs_logs': {e}\n\n"
                                            log('error', write)
                                            if connection:
                                                connection.rollback()
                                            return

                                        else:
                                            print("Tabela 'pacs_logs' criada com sucesso.")

    cursor.close()
    connection.close()

create_tables()
