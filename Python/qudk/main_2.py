from Python.qudk.utils import req_file
from connect_sql import maria_access
from utils import maria_util, log_config

# json 키
table_data= 'TableData'
table_info= 'TableInfo'
real_data= 'RealData'

filename='Result_File_1.json'
file_path=f'json_data/{filename}'

if __name__=='__main__':
    log_config.configure()

    # req_file.get_file(file_path)

    json_data= req_file.read_json_data(file_path)

    tableInfo=json_data[table_data][table_info]
    schema_name=list(tableInfo.keys())[0]

    table_info_list=json_data[table_data][table_info][schema_name]
    real_data_dict = json_data[real_data][schema_name]

    tb_ddl_list, fk_info_dict, tb_col_type_dict = maria_util.build_create_table(table_info_list)
    insert_query_dict = maria_util.build_insert_values(real_data_dict, tb_col_type_dict)
    fk_schema_except={}
    fk_ddl_dict = maria_util.build_add_fk(fk_info_dict, fk_schema_except)

    tb_err_list = []
    # maria_access.execute_create_tables(tb_ddl_list, tb_err_list)

    insert_err_dict = {}
    maria_access.execute_insert_values(insert_query_dict, tb_err_list, insert_err_dict)
    # maria_access.insert_data_total(schema_name, real_data_dict, tb_err_list, insert_err_dict)

    fk_err_dict={}
    # maria_access.execute_add_fk(fk_ddl_dict, tb_err_list, fk_err_dict)

