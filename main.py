from fastapi import FastAPI, Response  # 导⼊FastAPI，⽤于定义API
import pandas as pd
import re
from collections import Counter
import requests
import json
import dmPython
import uvicorn
import multiprocessing
import logging.handlers
import io
import os
import configparser

requests.adapters.DEFAULT_RETRIES = 5
session = requests.session()
session.keep_alive = False

app = FastAPI()  # 创建FastAPI实例
# 创建配置解析器对象
config = configparser.ConfigParser()
# 获取当前文件（main.py）的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
print("当前main.py的目录：" + current_dir)
# 构建配置文件的路径
config_path = os.path.join(current_dir, 'configData.ini')
print(config_path)
# 读取配置文件
config.read(config_path)
host = config['dm']['host']
port = config['dm']['port']
user = config['dm']['username']
password = config['dm']['password']

"""配置日志输出"""
log_folder_path = os.path.join(current_dir, 'log')
if not os.path.exists(log_folder_path):
    os.makedirs(log_folder_path)

log_file_path = os.path.join(log_folder_path, "main.log")
if not os.path.exists(log_file_path):
    # 如果文件不存在，创建文件
    open(log_file_path, 'w').close()


# def beijing(sec, what):
#     beijing_time = datetime.datetime.now() + datetime.timedelta(hours=8)
#     return beijing_time.timetuple()
#
#
# logging.Formatter.converter = beijing
#
# # logging.basicConfig(
# #     format="%(asctime)s %(levelname)s: %(message)s",
# #     level=logging.INFO,
# #     datefmt="%Y-%m-%d %H:%M:%S",
# # )

# 配置日志记录器
logger = logging.getLogger('hz_part')
logger.setLevel(logging.DEBUG)  # 设置日志级别
# 配置日志处理器
# TimedRotatingFileHandler：根据时间间隔轮转日志
handler = logging.handlers.TimedRotatingFileHandler(
    log_file_path,  # 日志文件名
    when='midnight',  # 轮转时间：每天午夜
    interval=1,  # 间隔：1天
    backupCount=7  # 保留的备份文件数量：7天
)
handler.setLevel(logging.DEBUG)  # 设置处理器的日志级别
# 配置日志格式
formatter = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
# 将处理器添加到记录器
logger.addHandler(handler)

"""
    #连接达梦数据库并进行update、insert
    #:param   sql--->sql语句 str
"""
def connect_dm_dml(sql):
    try:
        conn = dmPython.connect(
            host=host,  # 数据库地址
            port=port,  # 默认端口
            user=user,  # 系统管理员账号
            password=password  # 初始密码
        )
        logger.info("成功连接达梦数据库")
        """创建游标"""
        cursor = conn.cursor()
        """执行SQL"""
        cursor.execute(sql)
        """提交"""
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("更新或插入成功")
    except Exception as e:
        logger.info("更新或插入SQL:" + sql + f"更新或插入失败: {e}")
        """回滚事务"""
        conn.rollback()
        return "更新或插入失败，并回滚"


"""
    #连接达梦数据库并进行查询
    #:param   sql--->sql语句 str
    #:return  results--->查询结果 []
"""
def connect_dm_select(sql):
    try:
        conn = dmPython.connect(
            host=host,  # 数据库地址
            port=port,  # 默认端口
            user=user,  # 系统管理员账号
            password=password  # 初始密码
        )
        logger.info("成功连接达梦数据库")
        """创建游标"""
        cursor = conn.cursor()
        """执行SQL"""
        cursor.execute(sql)
        """获取所有结果"""
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info("查询成功")
        return results
    except Exception as e:
        logger.info("查询SQL:" + sql + f" 查询失败: {e}")
        """回滚事务"""
        conn.rollback()
        return "查询失败"


"""
翻译备件的物质中文描述
"""
def fun_fan_yi(text_for_tran):
    """参数需根据所监控的服务进行适应性修改----开始"""
    """ url=http://aepgw-t.gnpjvc.cgnpc.com.cn/translation/getTranslationResult """
    url = """http://aepgw.gnpjvc.cgnpc.com.cn/translation/getTranslationResult"""
    headers = {
        "Accept": "*/*",
        "requestId": "1",
        "version": "1",
        "appId": "BJRPA",
        # "appKey": "3ae67de24e01411ca12a76097f1e9215",
        "appKey": "5100a1e012124af0ab7168defa117aea",
        "appMethod": "1",
        "timestamp": "1",
        # "signInfo": "2639ce05eb874c51a481b8b98b87594a",
        "signInfo": "c0a28d3e4cbc363dd67135659100e80b",
        "tenantId": "1",
        "Content-Type": "application/json",
        "format": "json"
    }
    http_type = "post"
    body = {
        "from": "zh",
        "q": text_for_tran,
        "to": "en"
    }
    body = json.dumps(body) if ('Content-Type' in headers and 'json' in headers.get('Content-Type')) else body
    try:
        if http_type == 'post' and body == 'no':
            response = session.post(url, headers=headers, timeout=60)
        if http_type == 'post' and body != 'no':
            response = session.post(url, body, headers=headers, timeout=60)
        if http_type == 'get':
            response = session.get(url, headers=headers, timeout=60)
        # 返回信息
        text = response.text
        return text
    except Exception as e:
        return e


"""
    #群场数据获取
    #广核群厂备件清单汇总最新.XLSX  --->RPA_PLATFORM_DEV.AUX_GUANGHE_GROUP_SPARE_PARTS_LIST
    #:param   
            chineseName --->描述 str
            material--->材料  str 
    #:return  result--->自定义字典项 {}
"""


def get_history_data(chinese_name, material):
    result = {"english_name": "", "material_class": "", "life_num": "", "basic_unit": "", "store_level": "C",
              "deposit_style": "","pack_style": "", "check_style": "", "store_unit": "", "cycle_device": "N"}
    # result = {"物料英文描述": "", "物资类别": "", "总货架寿命": "", "基本单位": "", "存储等级": "C", "存放方式": "",
    #           "包装方式": "", "验收方式": "", "库存单位": "", "循环设备": "N"}
    """
    material = Replace(material, "(", "", False)
    material = Replace(material, ")", "", False)
    chineseName = Replace(chineseName, "(", "\\(", False)
    chineseName = Replace(chineseName, ")", "\\)", False)
    chineseName = Replace(chineseName, "\n", "", False)
    chineseName = Replace(chineseName, "\"", "", False)
    nameOther = chineseName & "\\["
    """
    search_result = []
    """仅匹配描述"""
    if len(chineseName) != 0:
        sql = "SELECT * FROM RPA_PLATFORM_DEV.AUX_GUANGHE_GROUP_SPARE_PARTS_LIST WHERE DESCRIPTION  LIKE " + \
              "'%" + chinese_name + "%'" + \
              "OR DESCRIPTION LIKE" + \
              "'%" + chinese_name + "[';"
        logger.info("仅匹配描述SQL:" + sql)
        search_result = connect_dm_select(sql)
        if len(search_result) > 0:
            english_description_array = []
            """物资类别"""
            material_category_array = []
            """基本单位"""
            base_unit_array = []
            """存放方式"""
            storage_method_array = []
            """验收方式"""
            acceptance_method_array = []
            for value in search_result:
                english_description_array.append(value[1])
                material_category_array.append(value[2])
                base_unit_array.append(value[6])
                storage_method_array.append(value[8])
                acceptance_method_array.append(value[10])
            result["english_name"] = Counter(english_description_array).most_common(1)[0][0]
            result["english_name"] = result["english_name"].replace("=", "")
            logger.info(result["english_name"])
            result["material_class"] = Counter(material_category_array).most_common(1)[0][0]
            logger.info(result["material_class"])
            result["basic_unit"] = Counter(base_unit_array).most_common(1)[0][0]
            logger.info(result["basic_unit"])
            result["deposit_style"] = Counter(storage_method_array).most_common(1)[0][0]
            logger.info(result["deposit_style"])
            result["check_style"] = Counter(acceptance_method_array).most_common(1)[0][0]
            logger.info(result["check_style"])
            logger.info(result)

    """匹配描述+材料"""
    if len(search_result) > 0 and material != "":
        sql = "SELECT * FROM RPA_PLATFORM_DEV.AUX_GUANGHE_GROUP_SPARE_PARTS_LIST WHERE " + \
              "(" + \
              "DESCRIPTION  LIKE " + \
              "'%" + chineseName + "%'" + \
              "OR DESCRIPTION LIKE" + \
              "'%" + chineseName + "['" + \
              ")" + \
              "AND  MANUFACTURING_MATERIAL LIKE  " + \
              "'%" + material + "%';"
        logger.info("匹配描述+材料SQL:" + sql)
        both_search_result = connect_dm_select(sql)
        if len(both_search_result) > 0:
            """循环设备"""
            result["cycle_device"] = "Y"
            """总货架寿命"""
            total_shelf_life_array = []
            """存储等级"""
            storage_level_array = []
            """包装方式"""
            packaging_method_array = []
            for value in both_search_result:
                total_shelf_life_array.append(value[4])
                storage_level_array.append(value[7])
                packaging_method_array.append(value[9])
            result["life_num"] = Counter(total_shelf_life_array).most_common(1)[0][0]
            result["store_level"] = Counter(storage_level_array).most_common(1)[0][0]
            result["pack_style"] = Counter(packaging_method_array).most_common(1)[0][0]
    """仅匹配材料"""
    if material != "":
        sql = "SELECT * FROM RPA_PLATFORM_DEV.AUX_GUANGHE_GROUP_SPARE_PARTS_LIST WHERE " + \
              "MANUFACTURING_MATERIAL LIKE  " + \
              "'" + material + "%';"
        logger.info("仅匹配材料SQL:" + sql)
        material_search_result = connect_dm_select(sql)
        if len(material_search_result) > 0 and result["life_num"] == "":
            """总货架寿命"""
            total_shelf_life_array = []
            for vaule in material_search_result:
                total_shelf_life_array.append(vaule[4])
            result["life_num"] = Counter(total_shelf_life_array).most_common(1)[0][0]

    """库存单位映射 RPA_PLATFORM_DEV.AUX_INVENTORY_FIELD_MAPPING"""
    sql_select = "SELECT VALUE FROM RPA_PLATFORM_DEV.AUX_INVENTORY_FIELD_MAPPING  WHERE SAP_BASIC_UNIT=" \
                 "'" + result["store_unit"] + "';"
    logger.info("根据库存单位映射查询SQL:" + sql_select)
    kcdw_search_result = connect_dm_select(sql_select)
    if len(kcdw_search_result) == 0:
        result["store_unit"] = ''
    else:
        result["store_unit"] = kcdw_search_result[0][0]

    """寿期默认值"""
    if result["life_num"] == "":
        result["life_num"] = "0"
    if "橡胶" in chineseName:
        result["pack_style"] = "2"
        result["store_level"] = "A"
    logger.info(result)
    return result


"""get_history_data("酚醛压层塑料棒","PAXOLIN")"""
"""
    #获取寿期
    #备件寿期检索字段.xlsx  --->RPA_PLATFORM_DEV.AUX_SPARE_PART_LIFESPAN_SEARCH
    #:param   
            chineseName --->描述 str
            material--->材料  str 
    #:return result --->寿期结果 str
"""
def get_life(chinese_name, material):
    result = ""
    sqlone_sq_select = "SELECT DAYS FROM RPA_PLATFORM_DEV.AUX_SPARE_PART_LIFESPAN_SEARCH WHERE  " \
                       "SPARE_PART_DESCRIPTION LIKE '%" \
                       + chinese_name + "%';"
    logger.info("备件寿期检索字段+nameSQL：" + sqlone_sq_select)
    sqlone_search_result = connect_dm_select(sqlone_sq_select)
    if len(sqlone_search_result) > 0:
        result = sqlone_search_result[0][0]
    else:
        if material != "":
            sqltwo_sq_select = "SELECT DAYS FROM RPA_PLATFORM_DEV.AUX_SPARE_PART_LIFESPAN_SEARCH WHERE  " \
                               "SPARE_PART_DESCRIPTION LIKE '%" \
                               + material + "%';"
            sqltwo_search_result = connect_dm_select(sqltwo_sq_select)
            logger.info("备件寿期检索字段+materialSQL：" + sqltwo_sq_select)
            if len(sqltwo_search_result) > 0:
                result = sqltwo_search_result[0][0]
            else:
                sqlthree_sq_select = "SELECT DAYS FROM RPA_PLATFORM_DEV.AUX_SPARE_PART_LIFESPAN_SEARCH WHERE  " \
                                     "MATERIAL_CODE LIKE '%" \
                                     + material + "%';"
                logger.info("备件寿期检索字段材质代码+materialSQL：" + sqlthree_sq_select)
                sqlthree_search_result = connect_dm_select(sqlthree_sq_select)
                if len(sqlthree_search_result) > 0:
                    result = sqlthree_search_result[0][0]
    logger.info("寿期结果:" + result)
    return result
# get_life("oo","IR")


"""
    处理手册采购包编号
"""


def str_replace(tar_str: str):
    tar_str = tar_str.replace(b'\xc2\xa0'.decode(), "")
    return tar_str


def get_name(tarStr):
    tarStr = Regex.Replace(tarStr, " ", "", 0)
    return Regex.FindStr(tarStr, "[\\u4e00-\\u9fa5]+", 0)


""" 主备件信息处理
    #main_part_info 主备件信息
    #sheetData OCR表的著备件信息和手册信息
    #sheetName sheet名
    #flag 标识，双OCR表是否有主备件表存在
    #:param   main_part_info--->主备件信息 {}
    #:return  main_part_info--->主备件信息 {}
"""


def main_part_info_deal(main_part_info, sheet_data, sheetName, flag):
    """双OCR表中的手册信息和主备件信息进行比对,若是双OCR表有值，则是取值OCR的值"""
    if not flag:
        """手册路径"""
        main_part_info["SCLJ"] = sheet_data[1][1]
        """手册编号"""
        if len(sheetData[2][6]) != 0:
            main_part_info["SCBH"] = Replace(sheet_data[2][6], "\n", "", False)
        else:
            main_part_info["SCBH"] = Replace(sheet_data[2][1], "\n", "", False)
        """手册名称"""
        if len(sheetData[3][6]) != 0:
            main_part_info["SCMC"] = Replace(sheet_data[3][6], "\n", "", False)
        else:
            maimain_part_infonBJInfo["SCMC"] = Replace(sheet_data[3][1], "\n", "", False)

        """手册采购包编号"""
        if len(sheetData[4][6]) != 0:
            main_part_info["SCCGBBH"] = Replace(sheetData[4][6], "\n", "", False)
        else:
            main_part_info["SCCGBBH"] = Replace(sheetData[4][1], "\n", "", False)
        """调用函数"""
        main_part_info["SCCGBBH"] = str_replace(Regex.Replace(main_part_info["SCCGBBH"], "[ ]", "", 0))

        """手册版本"""
        if len(sheetData[5][6]) != 0:
            main_part_info["SCBB"] = Replace(sheetData[5][6], "\n", "", False)
        else:
            main_part_info["SCBB"] = Replace(sheetData[5][1], "\n", "", False)

        """手册主审专业"""
        if len(sheetData[6][6]) != 0:
            main_part_info["SCZSZY"] = Replace(sheetData[6][6], "\n", "", False)
        else:
            main_part_info["SCZSZY"] = Replace(sheetData[6][1], "\n", "", False)

        """手册主专业审查人"""
        if len(sheetData[7][6]) != 0:
            main_part_info["SCZZYSCR"] = Replace(sheetData[7][6], "\n", "", False)
        else:
            main_part_info["SCZZYSCR"] = Replace(sheetData[7][1], "\n", "", False)

        main_part_info["SCZZYSCRQX"] = get_name(mainBJInfo["SCZZYSCR"])
        """备件编码人"""
        if len(sheetData[8][6]) != 0:
            main_part_info['BJBMR'] = Replace(sheetData[8][6], "\n", "", False)
        else:
            main_part_info['BJBMR'] = Replace(sheetData[8][1], "\n", "", False)
        if len(main_part_info['BJBMR']) == 0:
            main_part_info['BJBMR'] = main_part_info["SCZZYSCR"]
        """手册协审专业"""
        if len(sheetData[9][6]) != 0:
            main_part_info["SCXSZY"] = Replace(sheetData[9][6], "\n", "", False)
        else:
            main_part_info["SCXSZY"] = Replace(sheetData[9][1], "\n", "", False)
        """手册协审专业审查人"""
        if len(sheetData[10][6]) != 0:
            main_part_info['SCXSZYSCR'] = Replace(sheetData[10][6], "\n", "", False)
        else:
            main_part_info['SCXSZYSCR'] = Replace(sheetData[10][1], "\n", "", False)
        """装配图供货商"""
        if len(sheetData[11][6]) != 0:
            main_part_info["ZPTGHS"] = Replace(sheetData[11][6], "\n", "", False)
        else:
            main_part_info["ZPTGHS"] = Replace(sheetData[11][1], "\n", "", False)
        main_part_info["ZPTGHS"] = strip(main_part_info["ZPTGHS"])

    """装配图文件全名称"""
    main_part_info["ZPTWJQMC"] = Split(sheetData[13][1], "\\")[Len(Split(sheetData[13][1], "\\")) - 1]
    """型号"""
    if len(sheetData[14][6]) != 0:
        main_part_info["XH"] = Replace(sheetData[14][6], "\n", "", False)
    else:
        main_part_info["XH"] = Replace(sheetData[14][1], "\n", "", False)
    """电站图号"""
    if len(sheetData[15][6]) != 0:
        main_part_info["DZTH"] = Replace(sheetData[15][6], "\n", "", False)
    else:
        main_part_info["DZTH"] = Replace(sheetData[15][1], "\n", "", False)
    """装配图版本"""
    if len(sheetData[16][6]) != 0:
        main_part_info["ZPTBB"] = Replace(sheetData[16][6], "\n", "", False)
    else:
        main_part_info["ZPTBB"] = Replace(sheetData[16][1], "\n", "", False)
    """功能位置"""
    if len(sheetData[17][6]) != 0:
        main_part_info["GNWZ"] = Replace(sheetData[17][6], "\n", "", False)
    else:
        main_part_info["GNWZ"] = Replace(sheetData[17][1], "\n", "", False)
    """工程质保等级"""
    if len(sheetData[18][6]) != 0:
        main_part_info["GCZBDJ"] = Replace(sheetData[18][6], "\n", "", False)
    else:
        main_part_info["GCZBDJ"] = Replace(sheetData[18][1], "\n", "", False)
    """质保等级"""
    if len(sheetData[19][6]) != 0:
        main_part_info["ZBDJ"] = Replace(sheetData[19][6], "\n", "", False)
    else:
        main_part_info["ZBDJ"] = Replace(sheetData[19][1], "\n", "", False)
    """装配图标题"""
    if len(sheetData[20][6]) != 0:
        main_part_info["ZPTBT"] = Replace(sheetData[20][6], "\n", "", False)
    else:
        main_part_info["ZPTBT"] = Replace(sheetData[20][1], "\n", "", False)
    """装配图制造商"""
    if len(sheetData[21][6]) != 0:
        main_part_info["ZPTZZS"] = Replace(sheetData[21][6], "\n", "", False)
    else:
        main_part_info["ZPTZZS"] = Replace(sheetData[21][1], "\n", "", False)
    main_part_info["ZPTZZS"] = strip(main_part_info["ZPTZZS"])
    """内部编码"""
    if len(sheetData[22][6]) != 0:
        main_part_info["NBBM"] = Replace(sheetData[22][6], "\n", "", False)
    else:
        main_part_info["NBBM"] = Replace(sheetData[22][1], "\n", "", False)
    """备注"""
    if len(sheetData[23][6]) != 0:
        main_part_info["BZ"] = Replace(sheetData[23][6], "\n", "", False)
    else:
        main_part_info["BZ"] = Replace(sheetData[23][1], "\n", "", False)
    """预留配置参数"""
    main_part_info["YLPZCS"] = "RPA"
    return main_part_info


"""
    主备件规则
    :param--->main_part_info 主备件信息 {}字典
    :return--->main_part_dic 主备件 {}字典
"""


def main_part_info_rule(main_part_info):
    """
    :param main_part_info:
           main_part_info["功能位置"]=main_part_info["function_position"]
           main_part_info["装配图标题"]=main_part_info["drawing_title"]
           main_part_Info["型号"]=main_part_Info["type"]
           main_part_info["质保等级"]=main_part_info["owner_quality_level"]
           main_part_info["工程质保等级"]=main_part_info["project_quality_level"]
           main_part_info["装配图文件全名称"]=main_part_info["drawing_file_name"]
           main_part_info["装配图版本"]=main_part_info["drawing_bak_no"]
           main_part_info["手册编号"]=main_part_info["handbook_no"]
           main_part_info["手册版本号"]=main_part_info["handbook_bak_no"]
           main_part_info["内部编码"]=main_part_info["in_code"]
           main_part_info["装配图供货商"]=main_part_info["drawing_supplier"]
           main_part_info["装配图制造商"]=main_part_info["drawing_maker"]
           main_part_info["手册采购包编号"]=main_part_info["handbook_lot_no"]
           main_part_info["手册主专业审查人清洗"]=main_part_info["handbook_major_employee_id"]
           main_part_info["备注"]=main_part_info["remarks"]
           main_part_info["手册主审专业"]=main_part_info["handbook_review_major"]
           main_part_info["预留配置参数"]=main_part_info["params"]

           "特殊情况，"
           main_part_info[“装配图编号”]=main_part_info["drawing_no"]

    :return main_part_dic:
            main_part_dic["功能位置"]=main_part_dic["function_position"]
            main_part_dic["物资类别"]=main_part_dic["material_class"]
            main_part_dic["备件类别"]=main_part_dic["part_class"]
            main_part_dic["子备件数"]=main_part_dic["sub_part_sum"]
            main_part_dic["物项中文描述"]=main_part_dic["chinese_name"]
            main_part_dic["物项英文描述"]=main_part_dic["english_name"]
            main_part_dic["备件型号"]=main_part_dic["part_type"]
            main_part_dic["库存单位"]=main_part_dic["store_unit"]
            main_part_dic["材料"]=main_part_dic["material"]
            main_part_dic["系统"]=main_part_dic["system"]
            main_part_dic["业主质保等级"]=main_part_dic["owner_quality_level"]
            main_part_dic["工程质保等级"]=main_part_dic["project_quality_level"]
            main_part_dic["电站图号"]=main_part_dic["station_no"]
            main_part_dic["电站图版本号"]=main_part_dic["station_bak_no"]
            main_part_dic["EOMM手册号"]=main_part_dic["EOMM_handbook_no"]
            main_part_dic["EOMM手册版本号"]=main_part_dic["EOMM_handbook_bak_no"]
            main_part_dic["制造厂图号"]=main_part_dic["factory_draw_no"]
            main_part_dic["制造厂图项号"]=main_part_dic["factory_draw_item_no"]
            main_part_dic["制造厂参考号"] =main_part_dic["factory_draw_reference_no"]
            main_part_dic["供货商代码"]=main_part_dic["supplier_code"]
            main_part_dic["制造商代码"]=main_part_dic["maker_code"]
            main_part_dic["采购包号"]=main_part_dic["lot_no"]
            main_part_dic["工厂"]=main_part_dic["factory_code"]
            main_part_dic["归口专业"] = main_part_dic["belong_to_major"]
            main_part_dic["物料组"] = main_part_dic["material_group"]
            main_part_dic["主工作中心"] = main_part_dic["work_center"]
            main_part_dic["采购组"] =main_part_dic["buy_group"]
            main_part_dic["循环设备"]=main_part_dic["loop_equipment"]
            main_part_dic["["备件A_B分类"]"] = main_part_dic["part_AB_class"]
            main_part_dic["验收方式"]=main_part_dic["check_style"]
            main_part_dic["包装方式"]=main_part_dic["pack_style"]
            main_part_dic["存储放置方式"]=main_part_dic["deposit_style"]
            main_part_dic["存储等级"]=main_part_dic["store_level"]
            main_part_dic["是否带放射性"]=main_part_dic["if_risk"]
            main_part_dic["是否核级"]=main_part_dic["if_nucleus"]
            main_part_dic["是否核监管"]=main_part_dic["if_nucleus_regulation"]
            main_part_dic["是否CCM"]=main_part_dic["if_CCM"]
            main_part_dic["是否受控"]=main_part_dic["if_control"]
            main_part_dic["寿期"]=main_part_dic["life_num"]
            main_part_dic["备注"]=main_part_dic["remarks"]
            main_part_dic["重要性等级"]=main_part_dic["importance_level"]
            main_part_dic["MRP类型"]=main_part_dic["MRP_type"]
            main_part_dic["批量大小"]=main_part_dic["batch_size"]
            main_part_dic["重订购点(最小库存)"]=main_part_dic["min_stock"]
            main_part_dic["备件代码"]=main_part_dic["part_code"]
    """
    main_part_dic = {}
    """
        专业代码
    """
    major_code = {"M": "机械", "I": "仪控", "E": "电气", "G": "服务"}
    """
        备件代码第一位
    """
    part_code_first_dic = {"0": "X", "1": "X", "2": "X", "9": "X", "X": "X", "3": "Y", "4": "Y", "8": "Y", "Y": "Y",
                           "5": "Z", "6": "Z", "7": "Z", "Z": "Z"}
    """
        物项中文描述过滤配置
    """
    chinese_name_array = ["装配图", "剖面图", "外形图", "爆炸图"]
    """
        业主质保等级匹配配置
    """
    warranty_level_array = ["RCCM", "RCCE", "RCCP", "安全1级", "安全2级", "安全3级", "LS", "1E", "K1", "K3"]
    """
        核级匹配配置
    """
    nucleus_level_array = ["安全1级", "安全2级", "安全3级", "LS级", "1E级", "K1级", "K3级"]
    """优先获取物项中文描述，为其它字段规则提供输入参数"""
    """获取装配图标题第一行"""
    chinese_name = main_part_info["drawing_title"].split("\n")[0]
    for element in chinese_name_array:
        chinese_name = chinese_name.replace(element, "")
    logger.info("主备件规则处理 物质项中文描述：" + chinese_name)

    query_result = {}
    query_result = get_history_data(chinese_name, "")
    """** ** ** ** ** ** ** ** ** ** 1、获取功能位置 ** ** ** ** ** ** ** ** ** **"""

    function_position = main_part_info["function_position"].split(",")
    main_part_dic["function_position"] = function_position

    """** ** ** ** ** ** ** ** ** ** 2、获取物资类别 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["material_class"] = query_result["material_class"]

    """** ** ** ** ** ** ** ** ** ** 3、获取备件类别 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["part_class"] = "0"

    """** ** ** ** ** ** ** ** ** **4、获取子备件数 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["sub_part_sum"] = "0"

    """* ** ** ** ** ** ** ** ** ** 5、获取物项中文描述 ** ** ** ** ** ** ** ** **"""

    main_part_dic["chinese_name"] = chinese_name

    """** ** ** ** ** ** ** ** ** **6、获取物项英文描述 ** ** ** ** ** ** ** ** **"""

    english_name = query_result["english_name"]
    if english_name == "" and main_part_dic["chinese_name"] != "":
        english_name = JSON.Parse(fun_fan_yi(main_part_dic["chinese_name"]))["data"]["trans_result"][0]["dst"]
    main_part_dic["english_name"] = english_name

    """** ** ** ** ** ** ** ** ** ** 7、获取备件型号 ** ** ** ** ** ** ** ** ** **"""

    part_type = "TYPE:" & main_part_Info["type"]
    main_part_dic["part_type"] = part_type

    """** ** ** ** ** ** ** ** ** ** 8、获取库存单位 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["store_unit"] = query_result["store_unit"]

    """** ** ** ** ** ** ** ** ** ** 9、获取材料 ** ** ** ** ** ** ** ** ** **"""

    material = ""
    main_part_dic["material"] = material

    """** ** ** ** ** ** ** ** ** ** 10、获取系统 ** ** ** ** ** ** ** ** ** **"""

    if len(main_part_dic["function_position"]) < 0:
        main_part_dic["system"] = ""
    else:
        """主备件的系统字段  获取主备件信息【功能位置】3到6位的字符串"""
        main_part_dic["system"] = main_part_dic["function_position"][0][3:6]

    """** ** ** ** ** ** ** ** ** ** 11、获取业主质保等级 ** ** ** ** ** ** ** **"""

    main_part_dic["owner_quality_level"] = main_part_info["owner_quality_level"]

    """** ** ** ** ** ** ** ** ** ** 获取工程质保等级 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["project_quality_level"] = main_part_info["project_quality_level"]

    """** ** ** ** ** ** ** ** ** ** 12、获取电站图号 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["station_no"] = main_part_info["drawing_file_name"][0:19]

    """** ** ** ** ** ** ** ** ** ** 13、获取电站图版本号 ** ** ** ** ** ** ** ** ***"""

    main_part_dic["station_bak_no"] = main_part_info["drawing_bak_no"]

    """** ** ** ** ** ** ** ** ** ** 14、获取EOMM手册号 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["EOMM_handbook_no"] = main_part_info["handbook_no"]

    """** ** ** ** ** ** ** ** ** ** 15、获取EOMM手册版本号 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["EOMM_handbook_bak_no"] = main_part_info["handbook_bak_no"]

    """** ** ** ** ** ** ** ** ** ** 16、获取制造厂图号 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["factory_draw_no"] = main_part_info["in_code"].replace("\"","")

    """** ** ** ** ** ** ** ** ** ** 17、获取制造厂图项号 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["factory_draw_item_no"] = ""

    """** ** ** ** ** ** ** ** ** ** 18、获取制造厂参考号 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["factory_draw_reference_no"] = ""

    """** ** ** ** ** ** ** ** ** ** 19、获取制造商代码 ** ** ** ** ** ** ** ** ** **"""
    """
        供应商基本信息报表.xls--->RPA_PLATFORM_DEV.AUX_SUPPLIER_INFO
    """
    supplier_sql_select = "select SUPPLIER_CODE from RPA_PLATFORM_DEV.AUX_SUPPLIER_INFO  WHERE SUPPLIER_NAME='" + \
                          main_part_info["drawing_maker"] + "';"
    logger.info("供应商查询匹配SQL:" + supplier_sql_select)
    supplier_result = connect_dm_select(supplier_sql_select)
    if len(supplier_result) > 0:
        main_part_dic["maker_code"] = supplier_result[0][0]
    else:
        main_part_dic["maker_code"] = ""
    """** ** ** ** ** ** ** ** ** ** 20、获取供货商代码 ** ** ** ** ** ** ** ** ** **"""
    """
            供货商基本信息报表.xls--->RPA_PLATFORM_DEV.AUX_SUPPLIER_INFO
    """
    one_supplier_sql_select = "select SUPPLIER_CODE from RPA_PLATFORM_DEV.AUX_SUPPLIER_INFO  WHERE SUPPLIER_NAME='" + \
                              main_part_info["drawing_supplier"] + "';"
    logger.info("供货商查询匹配SQL:" + one_supplier_sql_select)
    one_supplier_result = connect_dm_select(one_supplier_sql_select)
    if len(one_supplier_result) > 0:
        main_part_dic["supplier_code"] = one_supplier_result[0][0]
    else:
        main_part_dic["supplier_code"] = ""

    """** ** ** ** ** ** ** ** ** ** 21、获取采购包号 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["lot_no"] = main_part_info["handbook_lot_no"]

    """** ** ** ** ** ** ** ** ** ** 22、获取工厂 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["factory_code"] = "5110"

    """** ** ** ** ** ** ** ** ** ** 23、获取归口专业 ** ** ** ** ** ** ** ** ** **"""

    """
        //todo  EMPLOYEE_ID进行检索--主备件信息["手册主专业审查人清洗"]
        维修部人员归口专业对应清单路径 RPA_PLATFORM_DEV.AUX_MAINTENANCE_DEPARTMENT_SPECIALTY_MAPPING
    """
    main_major_sql_select = "SELECT SPECIALTY,MATERIAL_GROUP,MAIN_WORK_CENTER" \
                            " FROM RPA_PLATFORM_DEV.AUX_MAINTENANCE_DEPARTMENT_SPECIALTY_MAPPING WHERE EMPLOYEE_ID =" \
                            "'" + main_part_info["handbook_major_employee_id"] + "';"
    logger.info("维修部人员归口专业匹配SQL:" + main_major_sql_select)
    main_major_result = connect_dm_select(main_major_sql_select)
    if len(main_major_result) > 0:
        main_part_dic["belong_to_major"] = (re.compile(r'[\u4e00-\u9fa5]+').sub('', main_major_result[0][0])).strip()
        main_part_dic["material_group"] = main_major_result[0][1]
        main_part_dic["work_center"] = main_major_result[0][2]
    else:
        main_part_dic["belong_to_major"] = ""
        main_part_dic["material_group"] = ""
        main_part_dic["work_center"] = ""

    """** ** ** ** ** ** ** ** ** ** 24、获取采购组 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["buy_group"] = main_part_dic["belong_to_major"][0:5]

    """** ** ** ** ** ** ** ** ** ** 25、获取循环设备 ** ** ** ** ** ** ** ** ** **"""
    """通过物项中文描述匹配描述[物料型号] + 材料查询《SAP群场历史数据》，能匹配填Y，否则填N"""

    main_part_dic["loop_equipment"] = "N"

    """** ** ** ** ** ** ** ** ** ** 26、获取备件A_B分类 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["part_AB_class"] = "B2"

    """** ** ** ** ** ** ** ** ** ** 28、获取验收方式 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["check_style"] = query_result["check_style"]

    """** ** ** ** ** ** ** ** ** ** 29、获取包装方式 ** ** ** ** ** ** ** ** ** **"""

    if query_result["pack_style"] == null or query_result["pack_style"] == "":
        main_part_dic["pack_style"] = "6"
    else:
        main_part_dic["pack_style"] = ""

    """** ** ** ** ** ** ** ** ** ** 30、获取存储放置方式 ** ** ** ** ** ** ** ** ** **"""

    if query_result["deposit_style"] == "" or query_result["deposit_style"] == null :
        main_part_dic["deposit_style"] = "0"
    else:
        main_part_dic["deposit_style"] = query_result["deposit_style"]

    """** ** ** ** ** ** ** ** ** ** 31、获取存储等级 ** ** ** ** ** ** ** ** ** **"""
    """
            存储等级	原【字段映射表】sheet【存储等级】-- RPA_PLATFORM_DEV.AUX_STORAGE_LEVEL
    """
    storage_level_sql_select = "SELECT LEVEL FROM RPA_PLATFORM_DEV.AUX_STORAGE_LEVEL WHERE KEYWORDLIST LIKE" \
                               "'%" + chinese_name + "%';"
    logger.info("存储等级SQL:" + storage_level_sql_select)
    storage_level_result = connect_dm_select(storage_level_sql_select)
    if len(storage_level_result) > 0:
        main_part_dic["store_level"] = query_result["store_level"]
    else:
        main_part_dic["store_level"] = ""

    """** ** ** ** ** ** ** ** ** ** 32、获取是否带放射性 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["if_risk"] = "N"

    """** ** ** ** ** ** ** ** ** ** 33、获取是否核级 ** ** ** ** ** ** ** ** ** **"""
    if_nucleus = "N"
    for value in nucleus_level_array:
        if value in chinese_name:
            one_contains = true
        if value in material:
            two_contains = true
        if value in part_type:
            three_contains = true
        if one_contains or two_contains or three_contains:
            if_nucleus = "Y"
            break

    main_part_dic["if_nucleus"] = if_nucleus
    """** ** ** ** ** ** ** ** ** ** 34、获取是否核监管 ** ** ** ** ** ** ** ** ** **"""
    """用户需要事先提供并持续维护一份《核监管设备清单》清单Excel表。通过功能位置，匹配后获取。匹配不到，填N"""
    if_nucleus_regulation = "N"
    for value in main_part_dic["function_position"]:
        if value == "":
            continue
        """
            核监管设备清单.xlsx RPA_PLATFORM_DEV.AUX_NUCLEAR_SUPERVISED_EQUIPMENT_LIST
        """
        nuclear_sql_select = "SELECT DESCRIPTION  FROM RPA_PLATFORM_DEV.AUX_NUCLEAR_SUPERVISED_EQUIPMENT_LIST " \
                             "WHERE FUNCTIONAL_LOCATION_CODE <>'' AND LEN(FUNCTIONAL_LOCATION_CODE)>2 AND " \
                             "SUBSTR(FUNCTIONAL_LOCATION_CODE,3)=" \
                             "'" + value + "';"
        logger.info("核监管设备清单SQL:" + nuclear_sql_select)
        nuclear_level_result = connect_dm_select(nuclear_sql_select)
        if len(nuclear_level_result) > 0:
            if_nucleus_regulation = "Y"
            break
    main_part_dic["if_nucleus_regulation"] = if_nucleus_regulation

    """** ** ** ** ** ** ** ** ** ** 35、获取是否CCM ** ** ** ** ** ** ** ** ** **"""
    """用户需要事先提供并持续维护一份《CCM设备清单》清单Excel表。通过功能位置，匹配后获取。匹配不到，填N"""

    if_CCM = "N"
    for value in main_part_dic["function_position"]:
        if value == "":
            continue
        """
            CCM设备映射表  RPA_PLATFORM_DEV.AUX_CCM_EQUIPMENT_SUMMARY
        """
        ccm_sql_select = "SELECT SYSTEM FROM RPA_PLATFORM_DEV.AUX_CCM_EQUIPMENT_SUMMARY WHERE REPLACE(" \
                         "FUNCTIONAL_LOCATION,'-','') LIKE" \
                         "'" + value + "%';"
        logger.info("CCM设备清单SQL:" + ccm_sql_select)
        ccm_sql_result = connect_dm_select(ccm_sql_select)
        if len(ccm_sql_result) > 0:
            if_CCM = "Y"
            break
    main_part_dic["if_CCM"] = if_CCM

    """** ** ** ** ** ** ** ** ** ** 36、获取是否受控 ** ** ** ** ** ** ** ** ** **"""

    main_part_dic["if_control"] = "N"

    """** ** ** ** ** ** ** ** ** ** 37、获取寿期 ** ** ** ** ** ** ** ** ** **"""
    """优先通过《寿期表格》匹配后获得寿期。无法匹配的情况下通过物项中文描述匹配描述（不包含中括号）+材料查询《SAP群场历史数据》"""
    """填写查询结果中的“总货架寿命”，无法匹配的情况下通过 + 材料查询《SAP群场历史数据》，填写查询结果中的“总货架寿命”，主备件无法匹配时填0"""

    life_num = get_life(chinese_name, material)
    if life_num == "" or life_num == null:
        main_part_dic["life_num"] = query_result["life_num"]
    else:
        main_part_dic["life_num"] = life_num

    """** ** ** ** ** ** ** ** ** ** 38、获取备注 ** ** ** ** ** ** ** ** ** **"""
    remarks = ""
    if if_nucleus_regulation == "Y":
        remarks = "核监管设备"
    else:
        if if_CCM == "Y":
            remarks = "CCM设备"
        for value in warranty_level_array:
            if value in chinese_name:
                four_contains = true
            if value in material:
                five_contains = true
            if value in part_type:
                six_contains = true
            if four_contains or five_contains or six_contains:
                remarks = "有RCC规范要求"
                break
    if main_part_info["remarks"] != "":
        remarks = main_part_info["remarks"]
    main_part_dic["remarks"] = remarks

    """** ** ** ** ** ** ** ** ** ** 39、获取重要性等级 ** ** ** ** ** ** ** ** ** **"""
    """质保等级C1或者在《CCM设备清单》中：3（高），其他都为2，今后根据设备分级清单选择"""

    if main_part_dic["owner_quality_level"] == "C1" or main_part_dic["if_CCM"] == "Y":
        main_part_dic["importance_level"] = "3"
    else:
        main_part_dic["importance_level"] = "2"

    """// ** ** ** ** ** ** ** ** ** ** 40、MRP类型、批量大小、重订购点(最小库存) ** ** ** ** ** ** ** ** ** ** """
    """// 对于A类备件备件，MRP类型设置为ZB，批量大小设置为EX，重订购点(最小库存)为"1"。固定批量大小、最大库存空；"""
    """// 对于B1类备件，MRP类型设置为ZB，批量大小设置为EX，重订购点(最小库存)为1。固定批量大小、最大库存空；"""
    """// 对于B2类和C类备件，MRP类型设置为PD，批量大小设置为EX，重订购点(最小库存)、固定批量大小、最大库存为空。"""
    MRP_type = ""
    batch_size = ""
    min_stock = ""
    if main_part_dic["part_AB_class"] == "A":
        MRP_type = "ZB"
        batch_size = "EX"
        min_stock = "1"
    elif main_part_dic["part_AB_class"] == "B1":
        MRP_type = "ZB"
        batch_size = "EX"
        min_stock = "1"
    elif main_part_dic["part_AB_class"] == "B2":
        MRP_type = "PD"
        batch_size = "EX"
        min_stock = ""
    else:
        MRP_type = "PD"
        batch_size = "EX"
        min_stock = ""

    main_part_dic["MRP_type"] = MRP_type
    main_part_dic["batch_size"] = batch_size
    main_part_dic["min_stock"] = min_stock

    """** ** ** ** ** ** ** ** ** ** 1、获取备件代码 ** ** ** ** ** ** ** ** ** **"""
    """
        MEIP导出备件信息.xlsx  RPA_PLATFORM_DEV.AUX_MEIP_EXPORT_SPARE_PARTS
    """
    meip_export_sql_select = "SELECT SPARE_PART_CODE FROM RPA_PLATFORM_DEV.AUX_MEIP_EXPORT_SPARE_PARTS WHERE" \
                             "CHINESE_DESCRIPTION LIKE " + "'%" + chinese_name + "%'" \
                             "AND \"MODEL\"  LIKE " + "'%" + main_part_dic["part_type"] + "%'" \
                             "AND MATERIAL LIKE " + "'%" + main_part_dic["material"] + "%'" \
                             "AND MANUFACTURER_REFERENCE_NUMBER LIKE " + "'%" + \
                             main_part_dic["factory_draw_reference_no"] + "%'" \
                             "AND POWER_STATION_DRAWING_NUMBER LIKE " + "'%" + \
                             main_part_dic["station_no"] + "%'" \
                             "AND MANUFACTURER_DRAWING_NUMBER = " + "'" \
                             + main_part_dic["factory_draw_no"] + "';"
    logger.info(" MEIP导出备件信息SQL:" + meip_export_sql_select)
    meip_export_sql_result = connect_dm_select(meip_export_sql_select)
    if len(meip_export_sql_result) != 1:
        """
            RPA流水号台账  RPA_PLATFORM_DEV.AUX_RPA_SERIAL_NUMBER_LEDGER
        """
        rpa_serial_sql_select = "SELECT MAXNUM FROM RPA_PLATFORM_DEV.AUX_RPA_SERIAL_NUMBER_LEDGER WHERE LOTPACKAGENUM=" \
                                "'" + main_part_info["handbook_lot_no"] + "';"
        logger.info("RPA流水号台账SQL:" + rpa_serial_sql_select)
        rpa_serial_sql_result = connect_dm_select(rpa_serial_sql_select)
        if len(rpa_serial_sql_result) == 1:
            """
                update RPA_PLATFORM_DEV.AUX_RPA_SERIAL_NUMBER_LEDGER
            """
            lot_number = int(rpa_serial_sql_result[0][0])
            rpa_serial_sql_update = "UPDATE RPA_PLATFORM_DEV.AUX_RPA_SERIAL_NUMBER_LEDGER SET MAXNUM = " \
                                    "'" + str(lot_number + 1) + "' WHERE LOTPACKAGENUM=" \
                                                                "'" + main_part_info["handbook_lot_no"] + "';"
            logger.info("RPA流水号台账更新SQL:" + rpa_serial_sql_update)
            connect_dm_dml(rpa_serial_sql_update)
            main_part_info["drawing_no"] = lot_number + 1
        if len(rpa_serial_sql_result) == 0:
            """
                insert  RPA_PLATFORM_DEV.AUX_RPA_SERIAL_NUMBER_LEDGER
            """
            rpa_serial_sql_insert = "INSERT INTO RPA_PLATFORM_DEV.AUX_RPA_SERIAL_NUMBER_LEDGER( \"LOTPACKAGENUM\", " \
                                    "\"MAXNUM\") VALUES(" \
                                    "'" + main_part_info["handbook_lot_no"] + "','1')"
            logger.info("RPA流水号台账插入SQL:" + rpa_serial_sql_insert)
            connect_dm_dml(rpa_serial_sql_insert)
            main_part_info["drawing_no"] = 1
        """
            补零 左边  3位
        """
        main_part_info["drawing_no"] = str(main_part_info["drawing_no"]).zfill(3)
        """
            1.截取手册编号第二位字符 [0,1,2]
            2.根据part_code_first_dic  备件代码第一位字典表进行匹配
            
        """
        part_code_one = part_code_first_dic[main_part_info["handbook_no"][2:3]]
        """
            遍历专业代码字典，判断其value值是否在main_part_info["手册主审专业"]中
            在  令part_code_two等于其key值 并推出循环
        """
        part_code_two = ""
        for key, value in major_code:
            if value in main_part_info["handbook_review_major"]:
                part_code_two = key
                break
        """
            提取手册采购包编号中的数字
        """
        lot_num = ''.join(re.findall(r'\d+', main_part_info["handbook_lot_no"]))
        """
            替换手册采购包编号中的”LOT“为”“
        """
        part_code_three = main_part_info["handbook_lot_no"].replace("LOT", "")
        """
            判断lot_num的长度，为1，拼接‘00’ 为2拼接‘0’
        """
        if len(lot_num) == 1:
            part_code_three = "00" + part_code_three
        if len(lot_num) == 2:
            part_code_three = "0" + part_code_three
        part_code_four = main_part_info["params"]
        part_code_five = main_part_info["drawing_no"]
        spare_part_code = part_code_one + part_code_two + part_code_three + part_code_four + part_code_five + "E"
    else:
        spare_part_code = meip_export_sql_result[0][0]
    main_part_dic["part_code"] = spare_part_code.replace(" ", "")
    return main_part_dic



"""
    根据主备件字典，生成主备件数组
"""
def get_main_part_array(main_part_dic):
    main_part_array = []
    # 主备件["备件代码"]
    main_part_array.append(main_part_dic["part_code"])
    # 主备件["物资类别"]
    main_part_array.append(main_part_dic["material_class"])
    # 主备件["备件类别"]
    main_part_array.append(main_part_dic["part_class"])
    # 主备件["子备件数"]
    main_part_array.append(main_part_dic["sub_part_sum"])
    # 主备件["物项中文描述"]
    main_part_array.append(main_part_dic["chinese_name"])
    # 主备件["物项英文描述"]
    main_part_array.append(main_part_dic["english_name"])
    # 主备件["备件型号"]
    main_part_array.append(main_part_dic["part_type"])
    # 主备件["库存单位"]
    main_part_array.append(main_part_dic["store_unit"])
    # 主备件["材料"]
    main_part_array.append(main_part_dic["material"])
    # 主备件["系统"]
    main_part_array.append(main_part_dic["system"])
    # 主备件["业主质保等级"]
    main_part_array.append(main_part_dic["owner_quality_level"])
    # 主备件["电站图号"]
    main_part_array.append(main_part_dic["station_no"])
    # 主备件["电站图版本号"]
    main_part_array.append(main_part_dic["station_bak_no"])
    # 主备件["EOMM手册号"]
    main_part_array.append(main_part_dic["EOMM_handbook_no"])
    # 主备件["EOMM手册版本号"]
    main_part_array.append(main_part_dic["EOMM_handbook_bak_no"])
    # 主备件["制造厂图号"]
    main_part_array.append(main_part_dic["factory_draw_no"])
    # 主备件["制造厂图项号"]
    main_part_array.append(main_part_dic["factory_draw_item_no"])
    # 主备件["制造厂参考号"]
    main_part_array.append(main_part_dic["factory_draw_reference_no"])
    # 主备件["制造商代码"]
    main_part_array.append(main_part_dic["maker_code"])
    # 主备件["供货商代码"]
    main_part_array.append(main_part_dic["supplier_code"])
    # 主备件["采购包号"]
    main_part_array.append(main_part_dic["lot_no"])
    # 主备件["工厂"]
    main_part_array.append(main_part_dic["factory_code"])
    # 主备件["归口专业"]
    main_part_array.append(main_part_dic["belong_to_major"])
    # 主备件["采购组"]
    main_part_array.append(main_part_dic["buy_group"])
    # 主备件["循环设备"]
    main_part_array.append(main_part_dic["loop_equipment"])
    # 主备件["备件A_B分类"]
    main_part_array.append(main_part_dic["part_AB_class"])
    # 主备件["物料组"]
    main_part_array.append(main_part_dic["material_group"])
    # 主备件["验收方式"]
    main_part_array.append(main_part_dic["check_style"])
    # 主备件["包装方式"]
    main_part_array.append(main_part_dic["pack_style"])
    # 主备件["存储放置方式"]
    main_part_array.append(main_part_dic["deposit_style"])
    # 主备件["存储等级"]
    main_part_array.append(main_part_dic["store_level"])
    # 主备件["是否带放射性"]
    main_part_array.append(main_part_dic["if_risk"])
    # 主备件["是否核级"]
    main_part_array.append(main_part_dic["if_nucleus"])
    # 主备件["是否核监管"]
    main_part_array.append(main_part_dic["if_nucleus_regulation"])
    # 主备件["是否CCM"]
    main_part_array.append(main_part_dic["if_CCM"])
    # 主备件["是否受控"]
    main_part_array.append(main_part_dic["if_control"])
    # 主备件["寿期"]
    main_part_array.append(main_part_dic["life_num"])
    # 主备件["备注"]
    main_part_array.append(main_part_dic["remarks"])
    # 主备件["重要性等级"]
    main_part_array.append(main_part_dic["importance_level"])
    # 主备件["MRP类型"]
    main_part_array.append(main_part_dic["MRP_type"])
    # 主备件["批量大小"]
    main_part_array.append(main_part_dic["batch_size"])
    # 主备件["重订购点"]
    main_part_array.append(main_part_dic["min_stock"])
    # 八个空字段
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    # 主备件["工程质保等级"]
    main_part_array.append(main_part_dic["project_quality_level"])
    # 29个孔子段
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    #
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    #
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    #
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    #
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    #
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    main_part_array.append("")
    # 销售组织
    main_part_array.append("手册审核："+主备件信息["手册主专业审查人"])
    # 分销渠道
    main_part_array.append(主备件信息["备件编码人"])
    return main_part_array



"""
    子备件信息处理
    #:param  
        main_part_info_array 主备件信息数组
        sub_part_data  子备件数据[] 数据库查询获得
    #:return
        sub_part_info {}  处理后的子备件信息字典
"""


def sub_part_info_deal(sub_part_data, main_part_array):
    """序号"""
    if subSheetData[6] != "":
        sub_info["序号"] = Replace(subSheetData[6], "\n", "", False)
    else:
        sub_info["序号"] = Replace(subSheetData[0], "\n", "", False)
    """代号"""
    if subSheetData[7] != "":
        sub_info["代号"] = Replace(subSheetData[7], "\n", "", False)
    else:
        sub_info["代号"] = Replace(subSheetData[1], "\n", "", False)
    """名称"""
    if subSheetData[8] != "":
        sub_info["名称"] = Replace(subSheetData[8], " ", "", False)
    else:
        sub_info["名称"] = Replace(subSheetData[2], " ", "", False)
    """数量"""
    if subSheetData[9] != "":
        sub_info["数量"] = Replace(subSheetData[9], " ", "", False)
    else:
        sub_info["数量"] = Replace(subSheetData[3], " ", "", False)
    """材料"""
    if subSheetData[10] != "":
        sub_info["材料"] = Replace(subSheetData[10], "\n", "", False)
    else:
        sub_info["材料"] = Replace(subSheetData[4], "\n", "", False)
    """规格"""
    if subSheetData[11] != "":
        sub_info['规格'] = Replace(subSheetData[11], "\n", "", False)
    else:
        sub_info['规格'] = Replace(subSheetData[5], "\n", "", False)
    """归口专业"""
    if Len(subSheetData[12]) == 0:
        sub_info['归口专业'] = Replace(main_info_array[22], "\n", "", False)
    else:
        sub_info['归口专业'] = Replace(subSheetData[12], "\n", "", False)
    """采购组"""
    if Len(subSheetData[13]) == 0:
        sub_info['采购组'] = Replace(main_info_array[23], "\n", "", False)
    else:
        sub_info['采购组'] = Replace(subSheetData[13], "\n", "", False)
    """物料组"""
    if Len(subSheetData[14]) == 0:
        sub_info["物料组"] = Replace(main_info_array[26], "\n", "", False)
    else:
        sub_info["物料组"] = Replace(subSheetData[14], "\n", "", False)

    sub_info['业主质保等级'] = Replace(subSheetData[15], "\n", "", False)
    sub_info['电站图号'] = Replace(subSheetData[16], "\n", "", False)
    sub_info['电站图版本号'] = Replace(subSheetData[17], "\n", "", False)
    sub_info['制造厂图号'] = Replace(subSheetData[18], "\n", "", False)
    sub_info['制造商'] = Replace(subSheetData[19], "\n", "", False)
    sub_info['制造商'] = strip(sub_info['制造商'])
    sub_info['供货商'] = Replace(subSheetData[20], "\n", "", False)
    sub_info['供货商'] = strip(sub_info['供货商'])
    sub_info["备注"] = Replace(subSheetData[21], "\n", "", False)
    return sub_info




"""
    子备件规则中获取备件A_B分类
    param:
        子备件["物质的中文描述"]
    return:
        str A/B1/B2/C
"""


def get_AB_class(sub_chinese_name):
    part_AB_class = ""
    for i in range(100):
        AB_sql_select = "SELECT COUNT(CLASS_A_SPARE_PARTS) AS SUM_OUT FROM " \
                        "RPA_PLATFORM_DEV.AUX_SPARE_PART_CLASSIFICATION WHERE CLASS_A_SPARE_PARTS LIKE " \
                        "'%" + sub_chinese_name + "%'" \
                        "UNION  ALL " \
                        "SELECT COUNT(CLASS_B1_SPARE_PARTS) AS SUM_OUT FROM " \
                        "RPA_PLATFORM_DEV.AUX_SPARE_PART_CLASSIFICATION  WHERE  CLASS_B1_SPARE_PARTS LIKE" \
                        "'%" + sub_chinese_name + "%'" \
                        "UNION  ALL " \
                        "SELECT  COUNT(CLASS_B2_SPARE_PARTS) AS SUM_OUT FROM " \
                        "RPA_PLATFORM_DEV.AUX_SPARE_PART_CLASSIFICATION WHERE CLASS_B2_SPARE_PARTS LIKE "\
                        "'%" + sub_chinese_name + "%'" \
                        "UNION  ALL " \
                        "SELECT COUNT(CLASS_C_SPARE_PARTS)  AS SUM_OUT FROM " \
                        "RPA_PLATFORM_DEV.AUX_SPARE_PART_CLASSIFICATION WHERE CLASS_C_SPARE_PARTS LIKE "\
                        "'%" + sub_chinese_name + "%';"
        logger.info("获取AB类SQL:"+AB_sql_select)
        AB_sql_result = connect_dm_select(AB_sql_select)

        if AB_sql_result[0][0] != 0:
            part_AB_class = "A"
            break
        if AB_sql_result[1][0] != 0:
            part_AB_class = "B1"
            break
        if AB_sql_result[2][0] != 0:
            part_AB_class = "B2"
            break
        if AB_sql_result[3][0] != 0:
            part_AB_class = "C"
            break
        if len(sub_chinese_name) == 1:
            break
        else:
            if sub_chinese_name[0:1] == "\\":
                sub_chinese_name = sub_chinese_name[-(len(sub_chinese_name)-2):]
            else:
                sub_chinese_name = sub_chinese_name[-(len(sub_chinese_name)-1):]
        logger.info("物质中文描述"+sub_chinese_name)
    logger.info(part_AB_class)
    return part_AB_class

# get_AB_class("这机械密封")

"""
    #子备件规则
    #param: 
        main_part_info_dic 主备件  参数由”主备件规则“处理返回  字典{}
        sub_part_info      子备件信息 参数由”子备件信息处理“处理返回  字典{}
    #return  sub_part_dic 子备件 字典{}
"""


def sub_part_info_rule(main_part_info_dic, sub_part_info):
    """
    子备件信息：sub_part_info
            sub_part_info["名称"]=sub_part_info["name"]
            sub_part_info["材料"]=sub_part_info["material"]
            sub_part_info["流水号"]=sub_part_info["seq"]
            sub_part_info["序号"]=sub_part_info["serialNo"]
            sub_part_info["数量"]=sub_part_info["number"]
            sub_part_info['规格']=sub_part_info["specs"]
            sub_part_info['代号']=sub_part_info["codename"]
            sub_part_info["业主质保等级"]=sub_part_info["quality_level"]
            sub_part_info["电站图号"]=sub_part_info["power_station_number"]
            sub_part_info["电站图版本号"]=sub_part_info["power_station_version_number"]
            sub_part_info['制造商']=sub_part_info["maker"]
            sub_part_info['供货商']=sub_part_info['supplier']
            sub_part_info["归口专业"]=sub_part_info["belong_to_major"]
            sub_part_info["采购组"]=sub_part_info["purchase_group"]
            sub_part_info["物料组"]=sub_part_info["material_group"]
            sub_part_info["备注"]=sub_part_info["comment"]
    主备件：main_part_info_dic
            main_part_info_dic["备件代码"]=main_part_info_dic["part_code"]
            main_part_info_dic["备件型号"]=main_part_info_dic["part_type"]
            main_part_info_dic["系统"]=main_part_info_dic["system"]
            main_part_info_dic["业主质保等级"]=main_part_info_dic["owner_quality_level"]
            main_part_info_dic["电站图号"]=main_part_info_dic["station_no"]
            main_part_info_dic["电站图版本号"]=main_part_info_dic["station_bak_no"]
            main_part_info_dic["EOMM手册号"]=main_part_info_dic["EOMM_handbook_no"]
            main_part_info_dic["EOMM手册版本号"]=main_part_info_dic["EOMM_handbook_bak_no"]
            main_part_info_dic["制造厂图号"]=main_part_info_dic["factory_draw_no"]
            main_part_info_dic["制造商代码"]=main_part_info_dic["maker_code"]
            main_part_info_dic["供货商代码"]=main_part_info_dic["supplier_code"]
            main_part_info_dic["采购包号"]=main_part_info_dic["lot_no"]
            main_part_info_dic["工厂"]=main_part_info_dic["factory_code"]
            main_part_info_dic["归口专业"]=main_part_info_dic["belong_to_major"]
            main_part_info_dic["采购组"]=main_part_info_dic["buy_group"]
            main_part_info_dic["物料组"]=main_part_info_dic["material_group"]
            main_part_info_dic["验收方式"]=main_part_info_dic["check_style"]
            main_part_info_dic["是否CCM"]=main_part_info_dic["if_CCM"]
            main_part_info_dic["是否受控"]=main_part_info_dic["if_control"]
    :return
            sub_part_dic["备件代码"] =sub_part_dic["part_code"]
            sub_part_dic["物资类别"]=sub_part_dic["material_class"]
            sub_part_dic["备件类别"]=sub_part_dic["part_class"]
            sub_part_dic["子备件数"]=sub_part_dic["sub_part_sum"]
            sub_part_dic["物项中文描述"]=sub_part_dic["chinese_name"]
            sub_part_dic["物项英文描述"]=sub_part_dic["english_name"]
            sub_part_dic["备件型号"] = sub_part_dic["part_type"]
            sub_part_dic["库存单位"]=sub_part_dic["store_unit"]
            sub_part_dic["材料"]=sub_part_dic["material"]
            sub_part_dic["系统"] =sub_part_dic["system"]
            sub_part_dic["业主质保等级"]=sub_part_dic["owner_quality_level"]
            sub_part_dic["电站图号"]=sub_part_dic["station_no"]
            sub_part_dic["电站图版本号"]=sub_part_dic["station_bak_no"]
            sub_part_dic["EOMM手册号"]=sub_part_dic["EOMM_handbook_no"]
            sub_part_dic["EOMM手册版本号"]=sub_part_dic["EOMM_handbook_bak_no"]
            sub_part_dic["制造厂图号"]=sub_part_dic["factory_draw_no"]
            sub_part_dic["制造厂图项号"]=sub_part_dic["factory_draw_item_no"]
            sub_part_dic["制造厂参考号"]=sub_part_dic["factory_draw_reference_no"]
            sub_part_dic["制造商代码"]=sub_part_dic["maker_code"]
            sub_part_dic["供货商代码"]=sub_part_dic["supplier_code"]
            sub_part_dic["采购包号"]=sub_part_dic["lot_no"]
            sub_part_dic["工厂"] =sub_part_dic["factory_code"]
            sub_part_dic["归口专业"] =sub_part_dic["belong_to_major"]
            sub_part_dic["采购组"] =sub_part_dic["buy_group"]
            sub_part_dic["循环设备"]=sub_part_dic["cycle_device"]
            sub_part_dic["物料组"]=sub_part_dic["material_group"]
            sub_part_dic["验收方式"]=sub_part_dic["check_style"]
            sub_part_dic["包装方式"] =sub_part_dic["pack_style"]
            sub_part_dic["存储放置方式"] =sub_part_dic["deposit_style"]
            sub_part_dic["存储等级"] =sub_part_dic["store_level"]
            sub_part_dic["是否带放射性"]=sub_part_dic["if_risk"]
            sub_part_dic["是否核级"]=sub_part_dic["if_nucleus"]
            sub_part_dic["是否核监管"]=sub_part_dic["if_nucleus_regulation"]
            sub_part_dic["是否CCM"]=sub_part_dic["if_CCM"]
            sub_part_dic["是否受控"]=sub_part_dic["if_control"]
            sub_part_dic["寿期"]=sub_part_dic["life_num"]
            sub_part_dic["备件A_B分类"]=sub_part_dic["part_AB_class"]
            sub_part_dic["备注"]=sub_part_dic["comment"]
            sub_part_dic["业主质保等级"]=sub_part_dic["owner_quality_level"]
            sub_part_dic["重要性等级"]=sub_part_dic["importance_level"]
            sub_part_dic["MRP类型"]=sub_part_dic["MRP_type"]
            sub_part_dic["批量大小"]=sub_part_dic["batch_size"]
            sub_part_dic["重订购点"]=sub_part_dic["min_stock"]
    """

    sub_part_dic = {}
    warranty_level_array = ["RCCM", "RCCE", "RCCP", "安全1级", "安全2级", "安全3级", "LS", "1E", "K1", "K3"]
    nucleus_level_array = ["安全1级", "安全2级", "安全3级", "LS级", "1E级", "K1级", "K3级"]
    """优先获取物项中文描述，为其它字段规则提供输入参数"""
    """
        截取 sub_part_info["name"] 起始 到 最右侧中文字符
    """
    sub_chinese_name = ""
    length = len(sub_part_info["name"])
    if length > 0:
        s = sub_part_info["name"]
        for i in reversed(s):
            if '\u4e00' <= i <= '\u9fff':
                sub_chinese_name = s[0:(s.index(i) + 1)]
                break
    else:
        sub_chinese_name = ""
    logger.info(sub_chinese_name)
    """
    从《SAP群场历史数据》一次性获取所有相关数据
    输出排序为：物资类别、总货架寿命、基本单位、存放方式、包装方式、验收方式
    根据函数get_history_data获取字典  后续会根据字典进行处理
    """
    sub_query_result = get_history_data(sub_chinese_name, str(sub_part_info["material"]))

    """** ** ** ** ** ** ** ** ** ** 1、获取备件代码 ** ** ** ** ** ** ** ** ** **"""
    sub_spare_part_one = main_part_info_dic["part_code"].rstrip("E")
    sub_lot_num = str(sub_part_info["seq"])
    if len(sub_lot_num) == 1:
        sub_spare_part_two = "00" + sub_lot_num
    if len(sub_lot_num) == 2:
        sub_spare_part_two = "00" + sub_lot_num
    if len(sub_lot_num) == 3:
        sub_spare_part_two = sub_lot_num
    """
        子备件代码 = 去掉备件备件代码末尾的E + 子备件的流水号 + “S”
    """
    sub_spare_part_code = sub_spare_part_one + sub_spare_part_two + "S"
    sub_part_dic["part_code"] = sub_spare_part_code
    """** ** ** ** ** ** ** ** ** ** 2、获取物资类别 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["material_class"] = sub_query_result["material_class"]
    """** ** ** ** ** ** ** ** ** ** 3、获取备件类别 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["part_class"] = "1"
    """** ** ** ** ** ** ** ** ** ** 4、获取子备件数 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["sub_part_sum"] = sub_part_info["number"]
    """** ** ** ** ** ** ** ** ** ** 5、获取物项中文描述 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["chinese_name"] = sub_chinese_name
    """** ** ** ** ** ** ** ** ** ** 6、获取物项英文描述 ** ** ** ** ** ** ** ** ** **"""
    sub_english_profile = sub_query_result["english_name"]
    if sub_english_profile == "" and sub_chinese_name != "":
        sub_english_profile = JSON.Parse(fun_fan_yi(sub_part_dic["chinese_name"]))["data"]["trans_result"][0]["dst"]
    sub_part_dic["english_name"] = sub_english_profile
    """** ** ** ** ** ** ** ** ** ** 7、获取备件型号 ** ** ** ** ** ** ** ** ** **"""
    """
        特殊字符
    """
    special_char = ["×", "X", "x"]
    """备件型号"""
    part_type = ""
    un_complete_type = sub_part_info["name"].replace(sub_chinese_name, "")
    if un_complete_type == "" and sub_part_info["specs"] == "":
        part_type = "TYPE:FOR " + main_part_info_dic["part_type"].replace("TYPE:", "")
    else:
        part_type = "TYPE:" + un_complete_type + str(sub_part_info["specs"])
    if un_complete_type != "" and sub_part_info["specs"] != "":
        part_type = "TYPE:" + un_complete_type + " " + str(sub_part_info["specs"])
    for value in special_char:
        """
             只有数字之间的X号变成* 例如：223X123 123 X129X 999 X　92这种会被替换为*，waX13X12,只有后面的X会被替换为*，jiX12XAB不会被替换
        """
        part_type = re.sub("(\\d+)[　 ]*?" + value + "[　 ]*?(?=\\d+)","\\1*",part_type)
    if sub_part_info["codename"] != "" and sub_part_info["codename"] != null:
        part_type = part_type + " " + str(sub_part_info["codename"])
    logger.info("备件型号：" + part_type)
    sub_part_dic["part_type"] = part_type
    """** ** ** ** ** ** ** ** ** ** 8、获取库存单位 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["store_unit"] = sub_query_result["store_unit"]
    """** ** ** ** ** ** ** ** ** ** 9、获取材料 ** ** ** ** ** ** ** ** ** **"""
    material = sub_part_info["material"]
    sub_part_dic["material"] = material
    """** ** ** ** ** ** ** ** ** ** 10、获取系统 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["system"] = main_part_info_dic["system"]
    """** ** ** ** ** ** ** ** ** ** 11、获取业主质保等级 ** ** ** ** ** ** ** ** ** **"""
    """优先使用用户补录，再通过主备件确定"""
    quality_level = ""
    if sub_part_info["quality_level"] == "":
        if main_part_info_dic["owner_quality_level"] == "C3":
            quality_level = "C3"
        if main_part_info_dic["owner_quality_level"] == "C2":
            """
            字段映射表 sheet->常见标准代码表   RPA_PLATFORM_DEV.AUX_COMMON_STANDARD_CODES
            """
            quality_level_sql_select = "SELECT * FROM RPA_PLATFORM_DEV.AUX_COMMON_STANDARD_CODES WHERE " \
                                       "STANDARD_HEADER LIKE "\
                                       "'%" + part_type + "%';"
            logger.info("字段映射表-常见标准代码表SQL:"+quality_level_sql_select)
            quality_level_sql_result = connect_dm_select(quality_level_sql_select)
            if len(quality_level_sql_result) > 0:
                quality_level = "C3"
            else:
                quality_level = "C2/C3"
        if main_part_info_dic["owner_quality_level"] == "C1":
            """
                        字段映射表 sheet->常见标准代码表   RPA_PLATFORM_DEV.AUX_COMMON_STANDARD_CODES
                        """
            quality_level_sql_select = "SELECT * FROM RPA_PLATFORM_DEV.AUX_COMMON_STANDARD_CODES WHERE " \
                                       "STANDARD_HEADER LIKE " \
                                       "'%" + part_type + "%';"
            logger.info("字段映射表-常见标准代码表SQL:" + quality_level_sql_select)
            quality_level_sql_result = connect_dm_select(quality_level_sql_select)
            if len(quality_level_sql_result) > 0:
                quality_level = "C3"
            else:
                quality_level = "C2"
            for value in warranty_level_array:
                if value in sub_chinese_name:
                    one_boo = true
                if value in material:
                    two_boo = true
                if value in part_type:
                    three_boo = true
                if one_boo or two_boo or three_boo:
                    quality_level = "C1"
                    break
    else:
        quality_level = sub_part_info["quality_level"]
    sub_part_dic["owner_quality_level"] = quality_level
    """** ** ** ** ** ** ** ** ** ** 12、获取电站图号 ** ** ** ** ** ** ** ** ** **"""
    power_station_number = main_part_info_dic["station_no"]
    if sub_part_info["power_station_number"] != "":
        power_station_number = sub_part_info["power_station_number"]
    sub_part_dic["station_no"] = power_station_number
    """** ** ** ** ** ** ** ** ** ** 13、获取电站图版本号 ** ** ** ** ** ** ** ** ** **"""
    power_station_version_number = main_part_info_dic["station_bak_no"]
    if sub_part_info["power_station_version_number"] != "":
        power_station_version_number = sub_part_info["power_station_version_number"]
    sub_part_dic["station_bak_no"] = power_station_version_number
    """** ** ** ** ** ** ** ** ** 14、获取EOMM手册号 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["EOMM_handbook_no"] = main_part_info_dic["EOMM_handbook_no"]
    """** ** ** ** ** ** ** ** ** ** 15、获取EOMM手册版本号 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["EOMM_handbook_bak_no"] = main_part_info_dic["EOMM_handbook_bak_no"]
    """** ** ** ** ** ** ** ** ** ** 16、获取制造厂图号 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["factory_draw_no"] = main_part_info_dic["factory_draw_no"]
    """** ** ** ** ** ** ** ** ** ** 17、获取制造厂图项号 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["factory_draw_item_no"] = sub_part_info["serialNo"]
    """** ** ** ** ** ** ** ** ** ** 18、获取制造厂参考号 ** ** ** ** ** ** ** ** ** **"""
    factory_number = sub_part_info["codename"]
    """
       字段映射表 sheet->常见标准代码表   RPA_PLATFORM_DEV.AUX_COMMON_STANDARD_CODES
    """
    quality_level_sql_select = "SELECT * FROM RPA_PLATFORM_DEV.AUX_COMMON_STANDARD_CODES WHERE " \
                               "STANDARD_HEADER LIKE " \
                               "'%" + factory_number + "%';"
    logger.info("字段映射表-常见标准代码表SQL:" + quality_level_sql_select)
    quality_level_sql_result = connect_dm_select(quality_level_sql_select)
    if len(quality_level_sql_result) > 0:
        factory_number = ""
    sub_part_dic["factory_draw_reference_no"] = factory_number
    """** ** ** ** ** ** ** ** ** ** 19、获取制造商代码 ** ** ** ** ** ** ** ** ** **"""
    factory_code = ""
    if sub_part_info["maker"] == "" or sub_part_info["maker"] == null:
        factory_code = main_part_info_dic["maker_code"]
    else:
        """
            供应商基本信息报表  AUX_SUPPLIER_INFO
        """
        supplier_sql_select = "SELECT SUPPLIER_CODE FROM RPA_PLATFORM_DEV.AUX_SUPPLIER_INFO WHERE SUPPLIER_NAME = " \
                              "'" + sub_part_info['maker'] + "';"
        logger.info("供应商查询SQL:"+supplier_sql_select)
        supplier_sql_result = connect_dm_select(supplier_sql_select)
        if len(supplier_sql_result) == 0:
            factory_code = sub_part_info['maker']
        if len(supplier_sql_result) > 0:
            factory_code = supplier_sql_result[0][0]
    sub_part_dic["maker_code"] = factory_code
    """** ** ** ** ** ** ** ** ** ** 20、获取供货商代码 ** ** ** ** ** ** ** ** ** **"""
    supplier_code = ""
    if sub_part_info['supplier'] == "" or sub_part_info['supplier'] == null:
        supplier_code = main_part_info_dic["supplier_code"]
    else:
        """
             供应商基本信息报表  AUX_SUPPLIER_INFO
        """
        supplier_sql_select = "SELECT SUPPLIER_CODE FROM RPA_PLATFORM_DEV.AUX_SUPPLIER_INFO WHERE SUPPLIER_NAME = " \
                              "'" + sub_part_info['supplier'] + "';"
        logger.info("供应商查询SQL:" + supplier_sql_select)
        supplier_sql_result = connect_dm_select(supplier_sql_select)
        if len(supplier_sql_result) == 0:
            supplier_code = sub_part_info['supplier']
        if len(supplier_sql_result) > 0:
            supplier_code = supplier_sql_result[0][0]
    sub_part_dic["supplier_code"] = supplier_code
    """** ** ** ** ** ** ** ** ** ** 21、获取采购包号 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["lot_no"] = main_part_info_dic["lot_no"]
    """** ** ** ** ** ** ** ** ** ** 22、获取工厂 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["factory_code"] = main_part_info_dic["factory_code"]
    """** ** ** ** ** ** ** ** ** ** 23、获取归口专业 ** ** ** ** ** ** ** ** ** **"""
    if sub_part_info['belong_to_major'] != null and strip(sub_part_info['belong_to_major']) != "":
        sub_part_dic["belong_to_major"] = sub_part_info["belong_to_major"]
    else:
        sub_part_dic["belong_to_major"] = main_part_info_dic["belong_to_major"]
    """** ** ** ** ** ** ** ** ** ** 24、获取采购组 ** ** ** ** ** ** ** ** ** **"""
    if sub_part_info["purchase_group"] != null and strip(sub_part_info["purchase_group"]) != "":
        sub_part_dic["buy_group"] = sub_part_info["purchase_group"]
    else:
        sub_part_dic["buy_group"] = main_part_info_dic["buy_group"]
    """** ** ** ** ** ** ** ** ** ** 25、获取循环设备 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["cycle_device"] = "N"
    """** ** ** ** ** ** ** ** ** ** 27、获取物料组 ** ** ** ** ** ** ** ** ** **"""
    if sub_part_info["material_group"] != "":
        sub_part_dic["material_group"] = sub_part_info["material_group"]
    else:
        sub_part_dic["material_group"] = main_part_info_dic["material_group"]
    """** ** ** ** ** ** ** ** ** ** 28、获取验收方式 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["check_style"] = sub_query_result["check_style"]
    """** ** ** ** ** ** ** ** ** ** 29;、获取包装方式 ** ** ** ** ** ** ** ** ** **"""
    if sub_query_result["pack_style"] == "":
        sub_part_dic["pack_style"] = "6"
    else:
        sub_part_dic["pack_style"] = sub_query_result["pack_style"]
    """** ** ** ** ** ** ** ** ** ** 30、获取存储放置方式 ** ** ** ** ** ** ** ** ** **"""
    if sub_query_result["deposit_style"] == "":
        sub_part_dic["deposit_style"] = "0"
    else:
        sub_part_dic["deposit_style"] = sub_query_result["deposit_style"]
    """** ** ** ** ** ** ** ** ** ** 31、获取存储等级 ** ** ** ** ** ** ** ** ** **"""
    """
        字段映射表  sheet->存储等级  RPA_PLATFORM_DEV.AUX_STORAGE_LEVEL
    """
    store_level_sql_select = "SELECT KEYWORDLIST FROM RPA_PLATFORM_DEV.AUX_STORAGE_LEVEL WHERE KEYWORDLIST LIKE " \
                             "'%" + sub_chinese_name + "%';"
    logger.info("字段映射表->存储等级SQL:"+store_level_sql_select)
    store_level_sql_result = connect_dm_select(store_level_sql_select)
    if len(store_level_sql_result) == 0:
        sub_part_dic["store_level"] = sub_query_result["store_level"]
    else:
        sub_part_dic["store_level"] = store_level_sql_result[0][0]
    """** ** ** ** ** ** ** ** ** ** 32、获取是否带放射性 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["if_risk"] = "N"
    """** ** ** ** ** ** ** ** ** ** 33、获取是否核级 ** ** ** ** ** ** ** ** ** **"""
    if_nucleus = "N"
    for value in nucleus_level_array:
        if value in sub_chinese_name:
            four_boo = true
        if value in material:
            five_boo = true
        if value in part_type:
            six_boo = true
        if four_boo or five_boo or six_boo:
            if_nucleus = "Y"
            break
    sub_part_dic["if_nucleus"] = if_nucleus
    """** ** ** ** ** ** ** ** ** ** 34、获取是否核监管 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["if_nucleus_regulation"] = "N"
    """** ** ** ** ** ** ** ** ** ** 35、获取是否CCM ** ** ** ** ** ** ** ** ** **"""
    """和主备件一致"""
    sub_part_dic["if_CCM"] = main_part_info_dic["if_CCM"]
    """** ** ** ** ** ** ** ** ** ** 36、获取是否受控 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["if_control"] = main_part_info_dic["if_control"]
    """** ** ** ** ** ** ** ** ** ** 37、获取寿期 ** ** ** ** ** ** ** ** ** **"""
    sub_result = get_life(sub_chinese_name, material)
    if sub_result == "" or sub_result == null:
        sub_part_dic["life_num"] = sub_query_result["life_num"]
    else:
        sub_part_dic["life_num"] = sub_result
    """ ** ** ** ** ** ** ** ** ** ** 获取备件A_B分类 ** ** ** ** ** ** ** ** ** **"""
    """用户需要事先提供并持续维护一份《备件A / B分类关键字表格》清单Excel表，通过中文描述 + 材料，匹配后获取，无法匹配填B"""
    part_AB_class = ""
    if sub_part_dic["life_num"] != "":
        if sub_part_dic["life_num"] == "0":
            part_AB_class = "B2"
        else:
            part_AB_class = "A"
    else:
        """字段映射表 sheet-> 备件AB类  RPA_PLATFORM_DEV.AUX_SPARE_PART_CLASSIFICATION"""
        part_AB_class = get_AB_class(sub_chinese_name)
    if part_AB_class == "":
        part_AB_class = "B2"
    sub_part_dic["part_AB_class"] = part_AB_class
    """** ** ** ** ** ** ** ** ** ** 38、获取备注 ** ** ** ** ** ** ** ** ** **"""
    sub_part_dic["comment"] = sub_part_info["comment"]
    """** ** ** ** ** ** ** ** ** ** 39、获取重要性等级 ** ** ** ** ** ** ** ** ** **"""
    """质保等级C1或者在《CCM设备清单》中：3（高），其他都为2，今后根据设备分级清单选择"""
    if sub_part_dic["owner_quality_level"] == "C1" or sub_part_dic["if_CCM"] == "Y":
        sub_part_dic["importance_level"] = "3"
    else:
        sub_part_dic["importance_level"] = "2"
    """** ** ** ** ** ** ** ** ** ** 40、MRP类型、批量大小、重订购点(最小库存) ** ** ** ** ** ** ** ** ** **"""
    """对于A类备件备件，MRP类型设置为ZB，批量大小设置为EX，重订购点(最小库存)为"1"。固定批量大小、最大库存空；"""
    """对于B1类备件，MRP类型设置为ZB，批量大小设置为EX，重订购点(最小库存)为1。固定批量大小、最大库存空；"""
    """对于B2类和C类备件，MRP类型设置为PD，批量大小设置为EX，重订购点(最小库存)、固定批量大小、最大库存为空。"""
    MRP_type = ""
    batch_size = ""
    min_stock = ""
    if part_AB_class == "A":
        MRP_type = "ZB"
        batch_size = "EX"
        min_stock = sub_part_info["number"]
    elif part_AB_class == "B1":
        MRP_type = "ZB"
        batch_size = "EX"
        min_stock = "1"
    elif part_AB_class == "B2":
        MRP_type = "PD"
        batch_size = "EX"
        min_stock = ""
    elif part_AB_class == "C":
        MRP_type = "PD"
        batch_size = "EX"
        min_stock = ""
    sub_part_dic["MRP_type"] = MRP_type

    sub_part_dic["batch_size"] = batch_size

    sub_part_dic["min_stock"] = min_stock
    return sub_part_dic



"""
获取子备件数组
params: sub_part_dic
return: sub_part_array
"""
def get_sub_part_array(sub_part_dic):
    sub_part_array = []
    # 子备件["备件代码"]
    sub_part_array.append(sub_part_dic["part_code"])
    # 子备件["物资类别"]
    sub_part_array.append(sub_part_dic["material_class"])
    # 子备件["备件类别"]
    sub_part_array.append(sub_part_dic["part_class"])
    # 子备件["子备件数"]
    sub_part_array.append(sub_part_dic["sub_part_sum"])
    # 子备件["物项中文描述"]
    sub_part_array.append(sub_part_dic["chinese_name"])
    # 子备件["物项英文描述"]
    sub_part_array.append(sub_part_dic["english_name"])
    # 子备件["备件型号"]
    sub_part_array.append(sub_part_dic["part_type"])
    # 子备件["库存单位"]
    sub_part_array.append(sub_part_dic["store_unit"])
    # 子备件["材料"]
    sub_part_array.append(sub_part_dic["material"])
    # 子备件["系统"]
    sub_part_array.append(sub_part_dic["system"])
    # 子备件["业主质保等级"]
    sub_part_array.append(sub_part_dic["owner_quality_level"])
    # 子备件["电站图号"]
    sub_part_array.append(sub_part_dic["station_no"])
    # 子备件["电站图版本号"]
    sub_part_array.append(sub_part_dic["station_bak_no"])
    # 子备件["EOMM手册号"]
    sub_part_array.append(sub_part_dic["EOMM_handbook_no"])
    # 子备件["EOMM手册版本号"]
    sub_part_array.append(sub_part_dic["EOMM_handbook_bak_no"])
    # 子备件["制造厂图号"]
    sub_part_array.append(sub_part_dic["factory_draw_no"])
    # 子备件["制造厂图项号"]
    sub_part_array.append(sub_part_dic["factory_draw_item_no"])
    # 子备件["制造厂参考号"]
    sub_part_array.append(sub_part_dic["factory_draw_reference_no"])
    # 子备件["制造商代码"]
    sub_part_array.append(sub_part_dic["maker_code"])
    # 子备件["供货商代码"]
    sub_part_array.append(sub_part_dic["supplier_code"])
    # 子备件["采购包号"]
    sub_part_array.append(sub_part_dic["lot_no"])
    # 子备件["工厂"]
    sub_part_array.append(sub_part_dic["factory_code"])
    # 子备件["归口专业"]
    sub_part_array.append(sub_part_dic["belong_to_major"])
    # 子备件["采购组"]
    sub_part_array.append(sub_part_dic["buy_group"])
    # 子备件["循环设备"]
    sub_part_array.append(sub_part_dic["cycle_device"])
    # 子备件["备件A_B分类"]
    sub_part_array.append(sub_part_dic["part_AB_class"])
    # 子备件["物料组"]
    sub_part_array.append(sub_part_dic["material_group"])
    # 子备件["验收方式"]
    sub_part_array.append(sub_part_dic["check_style"])
    # 子备件["包装方式"]
    sub_part_array.append(sub_part_dic["pack_style"])
    # 子备件["存储放置方式"]
    sub_part_array.append(sub_part_dic["deposit_style"])
    # 子备件["存储等级"]
    sub_part_array.append(sub_part_dic["store_level"])
    # 子备件["是否带放射性"]
    sub_part_array.append(sub_part_dic["if_risk"])
    # 子备件["是否核级"]
    sub_part_array.append(sub_part_dic["if_nucleus"])
    # 子备件["是否核监管"]
    sub_part_array.append(sub_part_dic["if_nucleus_regulation"])
    # 子备件["是否CCM"]
    sub_part_array.append(sub_part_dic["if_CCM"])
    # 子备件["是否受控"]
    sub_part_array.append(sub_part_dic["if_control"])
    # 子备件["寿期"]
    sub_part_array.append(sub_part_dic["life_num"])
    # 子备件["备注"]
    sub_part_array.append(sub_part_dic["comment"])
    # 子备件["重要性等级"]
    sub_part_array.append(sub_part_dic["importance_level"])
    # 子备件["MRP类型"]
    sub_part_array.append(sub_part_dic["MRP_type"])
    # 子备件["批量大小"]
    sub_part_array.append(sub_part_dic["batch_size"])
    # 子备件["重订购点"]
    sub_part_array.append(sub_part_dic["min_stock"])
    # 八个空字段
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    # 子备件["工程质保等级"]
    sub_part_array.append("")
    # 29个孔子段
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    #
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    #
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    #
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    #
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    #
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    sub_part_array.append("")
    # 销售组织
    sub_part_array.append("手册审核：" + 主备件信息["手册主专业审查人"])
    # 分销渠道
    sub_part_array.append(主备件信息["备件编码人"])
    return sub_part_array



"""
    功能位置导入
    parameter：
             main_info_dic 字典 主备件 由 “主备件规则”返回
    return:
            function_position_dic  {}
    
"""


def function_position_import(main_info_dic):
    function_position_dic = {}
    """功能位置导入"""
    function_position_import = [[]]
    """功能位置备件关系导入"""
    function_position_part_import = [[]]
    array_data = []
    for value in main_info_dic["position"]:
        str_position = value.upper()
        """功能位置"""
        if str_position.startswith("HZ"):
            array_data.append(str_position)
        else:
            array_data.append("HZ" + str_position)
        """备件主码"""
        array_data.append("")
        """功能位置描述"""
        array_data.append(main_info_dic[chinese_name])
        """房间号"""
        array_data.append("NA")
        """维护工厂"""
        if array_data[0][3:4] == "1":
            array_data.append("5111")
        elif array_data[0][3:4] == "2":
            array_data.append("5112")
        elif array_data[0][3:4] == "0":
            array_data.append("5111")
        elif array_data[0][3:4] == "9":
            array_data.append("5111")
        """主工作中心"""
        array_data.append(main_info_dic["主工作中心"])
        """上级功能位置"""
        array_data.append(array_data[0][1:1 + 6])

        function_position_import = function_position_import.append(array_data)
        function_position_part_import = function_position_part_import.append([array_data[0],main_info_dic["备件代码"]])
    """
        function_position_import  功能位置导入
        function_position_part_import 功能位置备件关系导入
    """
    function_position_dic["function_position_import"] = function_position_import
    function_position_dic["function_position_part_import"] = function_position_part_import
    return function_position_dic




"""
API接口，平台传入ID参数，生成四个excel文件
"""
@app.get("/getExcel/{id}")
async def get_excel_by_id(id : str):
    logger.info("这是执行没有参数路径测试")
    sql = "select * from  RPA_PLATFORM_DEV.AUX_MAINTENANCE_DEPARTMENT_SPECIALTY_MAPPING where EMPLOYEE_ID =" \
          "'" + id + "';"
    sql_result = connect_dm_select(sql)
    logger.info(sql_result)
    temp_array = []
    temp_array.append(list(sql_result[0]))
    df = pd.DataFrame(temp_array, columns=['EMPLOYEE_ID', 'NAME', 'MOBILE', 'SPECIALTY', 'MATERIAL_GROUP', 'MAIN_WORK_CENTER', 'ID'])
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)  # 移动到流的开始位置，以便读取内容

    # 使用Response返回文件内容，并设置正确的媒体类型和文件名
    return Response(output.getvalue(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    headers={"Content-Disposition": "attachment; filename=example.xlsx"})


@app.get("/get/")
async def test_one():
    logger.info("这是执行没有参数路径测试")
    return {"message": "My first fastapi project"}


@app.post("/post/")
async def test_two():
    logger.info("这是执行没有参数路径测试")
    return {"message": "My first fastapi project"}

"""
启动容器
"""
if __name__ == '__main__':
    multiprocessing.freeze_support()
    host_api = config['api']['host']
    port_api = config['api']['port']

    # uvicorn.run("main:app", host="0.0.0.0", port=8080)
    uvicorn.run(app, host=host_api, port=int(port_api))






