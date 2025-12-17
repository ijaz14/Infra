import time,datetime,traceback,json
import pandas as pd
import geopandas as gpd
from hawq_orm.sqlalchemy_helper import create_engine
from hawq_utilities.hawq_config.config_parser import *
from hawq_utilities.db_session_handler import db_session_context, PostgreSqlDbSettings
from hawq_orm.hawq_s18_table import *


class Hawq:
    def __init__(self,logger):
        self.logger = logger
        self.svt_connection_string = PostgreSqlDbSettings(
            get_config_details("db")["svt_prod_db_settings"] #svt_prod_db_settings
        ).get_database_url()
        self.svt_engine = create_engine(self.svt_connection_string, echo=False)
        self.hawq_connection_string = PostgreSqlDbSettings(
            get_config_details("db")["hawq-db-settings"]
        ).get_database_url()
        self.hawq_engine = create_engine(
            self.hawq_connection_string,
            executemany_mode="values",
            executemany_values_page_size=10000,
            executemany_batch_page_size=500,
        )
        self.error_df = pd.DataFrame()
        self.job_id_list =[]
        self.s18_layer_dict = {
            "activity_code": ActivityCode,
            "awr": Awr,
            "awr_derived": AwrDerived,
            "aoi": Aoi,
            "blocker": Blocker,
            "boundary": Boundary,
            "cable": Cable,
            "cable_assignment": CableAssignment,
            "cadastre": Cadastre,
            "duct": Duct,
            "duct_route": DuctRoute,
            "equipment": Equipment,
            "equipment_hfc": EquipmentHfc,
            "equipment_port": EquipmentPort,
            "equipment_port_frequency": EquipmentPortFrequency,
            "equipment_power": EquipmentPower,
            "metadata": Metadata,
            "planning_brief": PlanningBrief,
            "route": Route,
            "range": Range,
            "sl": Sl,
            "sl_to_copperpair": SlToCopperpair,
            "sl_to_ne": SlToNe,
            "site": Site,
            "sjr": Sjr,
            "structurepoint": Structurepoint,
            "splitter_to_enclosure": SplitterToEnclosure,
        }

    def get_db_json_query_results(self, query, db_url, query_type="JSON"):
        with db_session_context(db_url=db_url) as session:
            res = session.execute(query)
            if query_type == "JSON":
                l_dict = [x[0] for x in res]
            else:
                l_dict = [x for x in res]
            return l_dict

    def get_max_hawq_job_id(self):
        query = f"select job_id from register.job_tracker order by job_id desc limit 1"
        data = self.get_db_json_query_results(query, db_url=self.hawq_connection_string)
        return data[0]

    def load_tracker_table(self):
        max_hawq_job_id = self.get_max_hawq_job_id()
        self.logger.info(f"Max Hawq job_id: {max_hawq_job_id}")
        self.job_id_list = self.get_job_ids_to_process(max_hawq_job_id)
        # if len(self.job_id_list)==1:
        #     self.job_id_list =self.job_id_list[0]
        # else:
        #uncomment for backfill process
        # self.job_id_list = self.remaining_job_id()
        tracker_data = {'table_name': 'register.job_tracker', 'row_count': '', 'status': '', 'time_taken': '',
                        'load_time': ''}
        self.logger.info(f'no of job_id_list:{len(self.job_id_list)}')
        if len(self.job_id_list) > 1:
        #     query = f""" select * from register.job_tracker where job_id ={self.job_id_list[0]}"""
        # else:
            query = f""" select * from register.job_tracker where job_id IN {tuple(self.job_id_list)}"""
        df = self.load_data_frame(sql=query, engine=self.svt_engine)
        t0 = time.time()
        df = df.drop(['additional_info'],axis=1)
        try:
            df.to_sql(
                name='job_tracker',
                con=self.hawq_engine,
                schema='register',
                if_exists="append",
                index=False,
                chunksize=1000,
                # dtype={"bets": sqlalchemy.types.JSON},
            )
        except Exception as e:
            print("e",e)
            print(traceback.format_exc(e))
        tracker_data['row_count'] = len(df)
        tracker_data['status'] = True
        tracker_data['time_taken'] = time.time() - t0
        tracker_data['load_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        tracker_df = pd.DataFrame([tracker_data])
        self.error_df.append(tracker_df)

    def remaining_job_id(self):
        query = f"select job_id from register.job_tracker where job_id not in (select job_id from s18.duct_route) order by job_id desc limit 100"
        data = self.get_db_json_query_results(query, db_url=self.hawq_connection_string)
        return data

    def get_job_ids_to_process(self,hawq_job_id):
        query = f" WITH job_tracker_in_scope AS (SELECT a.job_id,a.payload_type,a.sam_name FROM register.job_tracker a WHERE a.job_status::text = 'COMPLETED'::text AND a.package_name::text !~* '.*ISC|USC|ASC.*'::text)," \
                f"max_job_id as (SELECT max(a.job_id) AS max_job_id FROM job_tracker_in_scope a GROUP BY a.sam_name, a.payload_type)" \
                f"select j.max_job_id from max_job_id j where j.max_job_id>{hawq_job_id}"
        data = self.get_db_json_query_results(query, db_url=self.svt_connection_string)
        return data

    def load_core_orm(self,orm_obj_list):
        t0 = time.time()
        status = True
        try:
            # config = PostgreSqlDbSettings(get_config_details("db")["svt_db_settings"])
            # db_url = config.get_database_url()
            # engine = create_engine(
            #     db_url,
            #     echo=False,
            #     connect_args={"options": "-c timezone=utc"},
            #     encoding="utf8",
            #     executemany_mode="values",
            #     executemany_values_page_size=10000,
            #     executemany_batch_page_size=500,
            # )

            if len(orm_obj_list) > 0:

                obj = orm_obj_list[0].__table__
                transformed_data = []
                orm_object_list_old = orm_obj_list.copy()
                for each in orm_object_list_old:
                    each.__dict__.pop("_sa_instance_state")
                    transformed_data.append(each.__dict__)
                if len(transformed_data) > 10000:
                    split_transformed_data = [
                        transformed_data[x: x + 100]
                        for x in range(0, len(transformed_data), 100)
                    ]
                else:
                    split_transformed_data = [transformed_data]

                for ind, each in enumerate(split_transformed_data):
                    try:
                        self.hawq_engine.execute(obj.insert(), each)
                    except Exception as e:
                        print(e)
                        # If batch has issue with loading falling back to individual records load on
                        for each_rec in each:
                            self.hawq_engine.execute(obj.insert(), each_rec)

            # return status, time.time() - t0
        except Exception as e:
            status = False
            # raise
        finally:
            return status, time.time() - t0

    def load_orm(self, results ,table):
        try:
            all_data = []
            tracker_data = {'table_name': table, 'row_count': '', 'status': '', 'time_taken': '',
                            'load_time': ''}
            self.logger.info(f"started db load for {table}")
            layer = str(table).split('.')[-1].lower()
            table = self.s18_layer_dict.get(layer)
            for data in results:
                current_data = data
                if "geometry" in list(data.keys()):
                    geometry = data.pop("geometry")
                    current_data["geometry"] = (
                        f"SRID=4283;{geometry.wkt}" if hasattr(geometry, "wkt") else None
                    )


                awr_derived = table(**current_data)
                all_data.append(awr_derived)
                self.logger.info(f'table:{table} and data count :{len(all_data)}')

            if all_data:
                load_status, time_taken = self.load_core_orm(all_data)
                self.logger.info(f"Time taken to insert {time_taken}")
                tracker_data['row_count'] = len(all_data)
                tracker_data['status'] = load_status
                tracker_data['time_taken'] = time_taken
                tracker_data['load_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                tracker_df = pd.DataFrame([tracker_data])
                return tracker_df

        except Exception as e:
            print(e)
            self.logger.error(traceback.format_exc())
            raise

    def load_data_frame(self,sql,engine):
        try:
            df = gpd.read_postgis(sql, engine, geom_col="geometry")
        except ValueError as e:
            df = pd.read_sql(sql, engine)

        for col_name, col_type in df.dtypes.iteritems():
            if col_type == "int64":
              df = df.astype({col_name: "float"})
        # if "u_id" in df.columns:
        #     df.drop(columns=["u_id"], inplace=True)
        # df.drop_duplicates(inplace=True)
        return df

    def get_delta_job_ids(self):
        query = f"select job_id::text FROM s18_delta.s18_delta_tracker where status='COMPLETED' and (job_id_v2 in {tuple(self.job_id_list)} or job_id_v1 in {tuple(self.job_id_list)})"
        data = self.get_db_json_query_results(query,db_url=self.svt_connection_string)
        return data

    def load_s18_delta(self,s18_table_list):
        delta_job_id_list = self.get_delta_job_ids()
        # self.logger.info(f"Delta_job_id_list: {delta_job_id_list}")
        for table in s18_table_list:
            tracker_data = {'table_name': table, 'row_count': '', 'status': '', 'time_taken': '',
                            'load_time': ''}
            query = f""" select * from {table} where job_id IN {tuple(delta_job_id_list)}"""
            # self.logger.info(f"{query}")
            df = self.load_data_frame(sql=query, engine=self.svt_engine)
            t0 = time.time()
            df.to_sql(
                name=table.split('.')[-1],
                con=self.hawq_engine,
                schema=table.split('.')[0],
                if_exists="append",
                index=False,
                chunksize=1000,
            )
            tracker_data['row_count'] = len(df)
            tracker_data['status'] = True
            tracker_data['time_taken'] = time.time() - t0
            tracker_data['load_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            tracker_df = pd.DataFrame([tracker_data])
            self.error_df.append(tracker_df)

    def load_s18_layer_data(self,table_list):
        # self.logger.info(f"S18 JOB_ID_LIST: {self.job_id_list}")
        for table in table_list:
            query = f""" select * from {table} where job_id IN {tuple(self.job_id_list )}"""
            # self.logger.info(f"{query}")
            df = self.load_data_frame(sql= query, engine=self.svt_engine)
            if not df.empty:
                df = gpd.GeoDataFrame(df)
                tracker_df = self.load_orm(
                    results=df.T.to_dict().values() ,table = table
                )
                self.error_df.append(tracker_df)


if __name__ == "__main__":
    # Hawq().get_diff_job_id('2022-04-10')
    # data = Hawq(logger='HAWQ').get_max_hawq_job_id()
    data = Hawq(logger='HAWQ')
    data.load_tracker_table()
    # print(data.svt_connection_string)
