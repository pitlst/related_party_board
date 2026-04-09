from pathlib import Path
import mimetypes
from litestar import Litestar, get
from litestar.static_files.config import StaticFilesConfig
from litestar.response import Response
import clickhouse_connect

mimetypes.add_type("application/javascript", ".js")

client = clickhouse_connect.get_client(
    host="10.24.5.59",
    port=8123,
    username="cheakf",
    password="Swq8855830.",
    database="default"
)


@get("/")
async def index_html() -> Response:
    html_path = Path("static/index.html")
    html_content = html_path.read_text(encoding="utf-8")
    return Response(content=html_content, media_type="text/html")


# 测试的虚假数据
template_data = {
    "metrics": {
        "today_total": 17,
        "today_assembly": 13,
        "today_delivery": 4,
        "today_outside": 0,
        "month_total": 97,
        "month_assembly": 71,
        "month_delivery": 26,
        "month_outside": 0
    },
    "charts": {
        "overall_status": [
            {"value": 2, "name": "临时外出"},
            {"value": 10, "name": "作业完成"},
            {"value": 5, "name": "作业完成"}
        ],
        "assembly_status": [
            {"value": 1, "name": "正在作业"},
            {"value": 8, "name": "已进入"},
            {"value": 4, "name": "作业完成"}
        ],
        "delivery_status": [
            {"value": 1, "name": "正在作业"},
            {"value": 2, "name": "已进入"},
            {"value": 1, "name": "作业完成"}
        ],
        "approval_dept": [
            {"value": 17, "name": "质量技术部"},
            {"value": 3, "name": "总成组装板块"},
            {"value": 3, "name": "调试交付板块"}
        ],
        "approval_contrast": {
            "categories": ["申请量", "已审批"],
            "values": [20, 15]
        },
        "hazards": {
            "categories": ["动火", "高处", "受限"],
            "values": [14, 2, 1]
        }
    },
    "table_data": [
        {"applicant": "廖成旺", "company": "广州联诚物联设备科技有限公司", "status": "正在作业", "start": "2026-04-07 下午", "end": "2026-04-08 下午", "location": "总成组装板块", "detail": "Q30不合格预警作业"},
        {"applicant": "蓝萍", "company": "株洲精工制造股份有限公司", "status": "正在作业", "start": "2026-04-07 上午", "end": "2026-04-10 上午", "location": "调试交付板块", "detail": "配合静调作业"},
        {"applicant": "刘水涛", "company": "株洲九方装备股份有限公司", "status": "已进入", "start": "2026-04-07 上午", "end": "2026-04-07 下午", "location": "总成组装板块", "detail": "Q30不合格预警作业"},
        {"applicant": "刘永吉", "company": "武汉研奥电气有限公司", "status": "已进入", "start": "2026-04-07 上午", "end": "2026-04-10 上午", "location": "调试交付板块", "detail": "配合静调作业"},
        {"applicant": "徐强强", "company": "兰普电器股份有限公司", "status": "已进入", "start": "2026-04-07 上午", "end": "2026-04-10 上午", "location": "总成组装板块", "detail": "部件装配作业"},
        {"applicant": "王建平", "company": "深圳伟德远航电器有限公司", "status": "作业完成", "start": "2026-04-06 上午", "end": "2026-04-06 下午", "location": "总成组装板块", "detail": "部件装配作业"},
        {"applicant": "苏有鹏", "company": "石家庄国祥运输设备有限公司", "status": "未进入", "start": "2026-04-07 下午", "end": "2026-04-10 下午", "location": "调试交付板块", "detail": "配合静调作业"},
        {"applicant": "廖成旺", "company": "广州联诚物联设备科技有限公司", "status": "正在作业", "start": "2026-04-07 下午", "end": "2026-04-08 下午", "location": "总成组装板块", "detail": "Q30不合格预警作业"},
        {"applicant": "蓝萍", "company": "株洲精工制造股份有限公司", "status": "正在作业", "start": "2026-04-07 上午", "end": "2026-04-10 上午", "location": "调试交付板块", "detail": "配合静调作业"},
        {"applicant": "刘水涛", "company": "株洲九方装备股份有限公司", "status": "已进入", "start": "2026-04-07 上午", "end": "2026-04-07 下午", "location": "总成组装板块", "detail": "Q30不合格预警作业"},
        {"applicant": "刘永吉", "company": "武汉研奥电气有限公司", "status": "已进入", "start": "2026-04-07 上午", "end": "2026-04-10 上午", "location": "调试交付板块", "detail": "配合静调作业"},
        {"applicant": "徐强强", "company": "兰普电器股份有限公司", "status": "已进入", "start": "2026-04-07 上午", "end": "2026-04-10 上午", "location": "总成组装板块", "detail": "部件装配作业"},
        {"applicant": "王建平", "company": "深圳伟德远航电器有限公司", "status": "作业完成", "start": "2026-04-06 上午", "end": "2026-04-06 下午", "location": "总成组装板块", "detail": "部件装配作业"},
        {"applicant": "苏有鹏", "company": "石家庄国祥运输设备有限公司", "status": "未进入", "start": "2026-04-07 下午", "end": "2026-04-10 下午", "location": "调试交付板块", "detail": "配合静调作业"}
    ]
}


@get("/api/dashboard-data")
async def get_dashboard_data(time: str = "每日") -> Response:
    print(f"Received request for time filter: {time}")
    # 在这里可以根据 time 参数 (每日, 本日, 每周, 本周, 等) 从数据库中查询不同的数据
    try:
        if time in ["每日", "本日"]:
            data = await process_today()
            time_filter_sql = "toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())"
        elif time in ["每周", "本周"]:
            data = await process_week()
            time_filter_sql = "bill.`计划开工日期` >= toStartOfWeek(today()) AND bill.`计划开工日期` < toStartOfWeek(today()) + 7"
        elif time in ["每月", "本月"]:
            data = await process_month()
            time_filter_sql = "toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())"
        elif time in ["每季", "本季"]:
            data = await process_quarter()
            time_filter_sql = "bill.`计划开工日期` >= toStartOfQuarter(now()) AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))"
        elif time in ["每年", "本年"]:
            data = await process_year()
            time_filter_sql = "toYear(bill.`计划开工日期`) = toYear(now())"
        else:
            data = await process_today()
            time_filter_sql = "toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())"

        total_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND {time_filter_sql}
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
        """)
        data["metrics"]["today_total"] = int(total_count_res.iloc[0]["total_count"])
        assembly_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND {time_filter_sql}
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域')
        """)
        data["metrics"]["today_assembly"] = int(assembly_count_res.iloc[0]["total_count"])
        delivery_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND {time_filter_sql}
    AND bill.`作业地点` IN ('新调试', '老调试', '动车组调试基地', '交车车间落车调车区域')
        """)
        data["metrics"]["today_delivery"] = int(delivery_count_res.iloc[0]["total_count"])
        outside_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND {time_filter_sql}
    AND bill.`作业地点` IN ('库外')
        """)
        data["metrics"]["today_outside"] = int(outside_count_res.iloc[0]["total_count"])

        month_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
        """)
        data["metrics"]["month_total"] = int(month_count_res.iloc[0]["total_count"])
        month_assembly_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域')
        """)
        data["metrics"]["month_assembly"] = int(month_assembly_count_res.iloc[0]["total_count"])
        month_delivery_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(now())
    AND bill.`作业地点` IN ('新调试', '老调试', '动车组调试基地', '交车车间落车调车区域')
        """)
        data["metrics"]["month_delivery"] = int(month_delivery_count_res.iloc[0]["total_count"])
        month_outside_count_res = client.query_df(
            f"""
SELECT 
    count() AS total_count
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(now())
    AND bill.`作业地点` IN ('库外')
        """)
        data["metrics"]["month_outside"] = int(month_outside_count_res.iloc[0]["total_count"])

    except Exception as e:
        print(e)
    return Response(content=data, media_type="application/json")


async def process_today() -> dict:
    data = template_data.copy()
    overall_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["overall_status"] = overall_res.to_dict(orient="records")
    overall_assembly_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["assembly_status"] = overall_assembly_res.to_dict(orient="records")
    overall_delivery_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
    AND bill.`作业地点` IN ('新调试', '老调试', '动车组调试基地', '交车车间落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["delivery_status"] = overall_delivery_res.to_dict(orient="records")
    approval_dept_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`事业部对接人部门` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`事业部对接人部门`
    """)
    data["charts"]["approval_dept"] = approval_dept_res.to_dict(orient="records")
    approval_contrast_list = []
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    AND bill.`单据状态` = '已审核'
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    data["charts"]["approval_contrast"]['values'] = approval_contrast_list
    hazards_res = client.query_df(
        f"""
SELECT 
    trim(category) as clean_category,
    count() as values
FROM (
    SELECT
        arrayJoin(
            arrayDistinct(
                arrayMap(x -> trim(x), 
                    splitByChar(',', trim(bill.`作业危险性`))
                )
            )
        ) as category
    FROM ods.interested_party_review AS bill FINAL
    WHERE bill.Deleted = 0
        AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
        AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
) AS _bill
GROUP BY clean_category
    """)
    data["charts"]["hazards"]['categories'] = hazards_res["clean_category"].tolist()
    data["charts"]["hazards"]['values'] = hazards_res["values"].tolist()
    table_data_res = client.query_df(
        f"""
SELECT 
    DISTINCT
    bill.`申请人姓名` AS `applicant`,
    bill.`公司名称` AS `company`,
    bill.`作业状态` AS `status`,
    concat(formatDateTime(bill.`计划开工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划开工日期上午/下午`) AS `start`,
    concat(formatDateTime(bill.`计划完工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划完工日期上午/下午`) AS `end`,
    bill.`作业地点` AS `location`,
    bill.`具体作业内容` AS `detail`
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfDay(bill.`计划开工日期`) = toStartOfDay(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    data["table_data"] = table_data_res.to_dict(orient="records")
    return data


async def process_week() -> dict:
    data = template_data.copy()
    overall_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfWeek(today())
    AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["overall_status"] = overall_res.to_dict(orient="records")
    overall_assembly_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfWeek(today())
    AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["assembly_status"] = overall_assembly_res.to_dict(orient="records")
    overall_delivery_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfWeek(today())
    AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
    AND bill.`作业地点` IN ('新调试', '老调试', '动车组调试基地', '交车车间落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["delivery_status"] = overall_delivery_res.to_dict(orient="records")
    approval_dept_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`事业部对接人部门` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfWeek(today())
    AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`事业部对接人部门`
    """)
    data["charts"]["approval_dept"] = approval_dept_res.to_dict(orient="records")
    approval_contrast_list = []
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfWeek(today())
    AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfWeek(today())
    AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    AND bill.`单据状态` = '已审核'
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    data["charts"]["approval_contrast"]['values'] = approval_contrast_list
    hazards_res = client.query_df(
        f"""
SELECT 
    trim(category) as clean_category,
    count() as values
FROM (
    SELECT
        arrayJoin(
            arrayDistinct(
                arrayMap(x -> trim(x), 
                    splitByChar(',', trim(bill.`作业危险性`))
                )
            )
        ) as category
    FROM ods.interested_party_review AS bill FINAL
    WHERE bill.Deleted = 0
        AND bill.`计划开工日期` >= toStartOfWeek(today())
        AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
        AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
) AS _bill
GROUP BY clean_category
    """)
    data["charts"]["hazards"]['categories'] = hazards_res["clean_category"].tolist()
    data["charts"]["hazards"]['values'] = hazards_res["values"].tolist()
    table_data_res = client.query_df(
        f"""
SELECT 
    DISTINCT
    bill.`申请人姓名` AS `applicant`,
    bill.`公司名称` AS `company`,
    bill.`作业状态` AS `status`,
    concat(formatDateTime(bill.`计划开工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划开工日期上午/下午`) AS `start`,
    concat(formatDateTime(bill.`计划完工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划完工日期上午/下午`) AS `end`,
    bill.`作业地点` AS `location`,
    bill.`具体作业内容` AS `detail`
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfWeek(today())
    AND bill.`计划开工日期` < toStartOfWeek(today()) + 7
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    data["table_data"] = table_data_res.to_dict(orient="records")
    return data


async def process_month() -> dict:
    data = template_data.copy()
    overall_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["overall_status"] = overall_res.to_dict(orient="records")
    overall_assembly_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["assembly_status"] = overall_assembly_res.to_dict(orient="records")
    overall_delivery_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
    AND bill.`作业地点` IN ('新调试', '老调试', '动车组调试基地', '交车车间落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["delivery_status"] = overall_delivery_res.to_dict(orient="records")
    approval_dept_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`事业部对接人部门` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`事业部对接人部门`
    """)
    data["charts"]["approval_dept"] = approval_dept_res.to_dict(orient="records")
    approval_contrast_list = []
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    AND bill.`单据状态` = '已审核'
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    data["charts"]["approval_contrast"]['values'] = approval_contrast_list
    hazards_res = client.query_df(
        f"""
SELECT 
    trim(category) as clean_category,
    count() as values
FROM (
    SELECT
        arrayJoin(
            arrayDistinct(
                arrayMap(x -> trim(x), 
                    splitByChar(',', trim(bill.`作业危险性`))
                )
            )
        ) as category
    FROM ods.interested_party_review AS bill FINAL
    WHERE bill.Deleted = 0
        AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
        AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
) AS _bill
GROUP BY clean_category
    """)
    data["charts"]["hazards"]['categories'] = hazards_res["clean_category"].tolist()
    data["charts"]["hazards"]['values'] = hazards_res["values"].tolist()
    table_data_res = client.query_df(
        f"""
SELECT 
    DISTINCT
    bill.`申请人姓名` AS `applicant`,
    bill.`公司名称` AS `company`,
    bill.`作业状态` AS `status`,
    concat(formatDateTime(bill.`计划开工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划开工日期上午/下午`) AS `start`,
    concat(formatDateTime(bill.`计划完工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划完工日期上午/下午`) AS `end`,
    bill.`作业地点` AS `location`,
    bill.`具体作业内容` AS `detail`
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toStartOfMonth(bill.`计划开工日期`) = toStartOfMonth(today())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    data["table_data"] = table_data_res.to_dict(orient="records")
    return data


async def process_quarter() -> dict:
    data = template_data.copy()
    overall_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfQuarter(now())
    AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["overall_status"] = overall_res.to_dict(orient="records")
    overall_assembly_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfQuarter(now())
    AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["assembly_status"] = overall_assembly_res.to_dict(orient="records")
    overall_delivery_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfQuarter(now())
    AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
    AND bill.`作业地点` IN ('新调试', '老调试', '动车组调试基地', '交车车间落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["delivery_status"] = overall_delivery_res.to_dict(orient="records")
    approval_dept_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`事业部对接人部门` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfQuarter(now())
    AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`事业部对接人部门`
    """)
    data["charts"]["approval_dept"] = approval_dept_res.to_dict(orient="records")
    approval_contrast_list = []
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfQuarter(now())
    AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfQuarter(now())
    AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    AND bill.`单据状态` = '已审核'
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    data["charts"]["approval_contrast"]['values'] = approval_contrast_list
    hazards_res = client.query_df(
        f"""
SELECT 
    trim(category) as clean_category,
    count() as values
FROM (
    SELECT
        arrayJoin(
            arrayDistinct(
                arrayMap(x -> trim(x), 
                    splitByChar(',', trim(bill.`作业危险性`))
                )
            )
        ) as category
    FROM ods.interested_party_review AS bill FINAL
    WHERE bill.Deleted = 0
        AND bill.`计划开工日期` >= toStartOfQuarter(now())
        AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
        AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
) AS _bill
GROUP BY clean_category
    """)
    data["charts"]["hazards"]['categories'] = hazards_res["clean_category"].tolist()
    data["charts"]["hazards"]['values'] = hazards_res["values"].tolist()
    table_data_res = client.query_df(
        f"""
SELECT 
    DISTINCT
    bill.`申请人姓名` AS `applicant`,
    bill.`公司名称` AS `company`,
    bill.`作业状态` AS `status`,
    concat(formatDateTime(bill.`计划开工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划开工日期上午/下午`) AS `start`,
    concat(formatDateTime(bill.`计划完工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划完工日期上午/下午`) AS `end`,
    bill.`作业地点` AS `location`,
    bill.`具体作业内容` AS `detail`
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND bill.`计划开工日期` >= toStartOfQuarter(now())
    AND bill.`计划开工日期` < toStartOfQuarter(now() + toIntervalQuarter(1))
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    data["table_data"] = table_data_res.to_dict(orient="records")
    return data


async def process_year() -> dict:
    data = template_data.copy()
    overall_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toYear(bill.`计划开工日期`) = toYear(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["overall_status"] = overall_res.to_dict(orient="records")
    overall_assembly_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toYear(bill.`计划开工日期`) = toYear(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["assembly_status"] = overall_assembly_res.to_dict(orient="records")
    overall_delivery_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`作业状态` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toYear(bill.`计划开工日期`) = toYear(now())
    AND bill.`作业地点` IN ('新调试', '老调试', '动车组调试基地', '交车车间落车调车区域')
GROUP BY 
    bill.`作业状态`
    """)
    data["charts"]["delivery_status"] = overall_delivery_res.to_dict(orient="records")
    approval_dept_res = client.query_df(
        f"""
SELECT 
    DISTINCT 
    bill.`事业部对接人部门` AS name,
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toYear(bill.`计划开工日期`) = toYear(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
GROUP BY 
    bill.`事业部对接人部门`
    """)
    data["charts"]["approval_dept"] = approval_dept_res.to_dict(orient="records")
    approval_contrast_list = []
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toYear(bill.`计划开工日期`) = toYear(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    approval_contrast_res = client.query_df(
        f"""
SELECT 
    count() AS value
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toYear(bill.`计划开工日期`) = toYear(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    AND bill.`单据状态` = '已审核'
    """)
    approval_contrast_list.append(int(approval_contrast_res.iloc[0]["value"]))
    data["charts"]["approval_contrast"]['values'] = approval_contrast_list
    hazards_res = client.query_df(
        f"""
SELECT 
    trim(category) as clean_category,
    count() as values
FROM (
    SELECT
        arrayJoin(
            arrayDistinct(
                arrayMap(x -> trim(x), 
                    splitByChar(',', trim(bill.`作业危险性`))
                )
            )
        ) as category
    FROM ods.interested_party_review AS bill FINAL
    WHERE bill.Deleted = 0
        AND toYear(bill.`计划开工日期`) = toYear(now())
        AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
) AS _bill
GROUP BY clean_category
    """)
    data["charts"]["hazards"]['categories'] = hazards_res["clean_category"].tolist()
    data["charts"]["hazards"]['values'] = hazards_res["values"].tolist()
    table_data_res = client.query_df(
        f"""
SELECT 
    DISTINCT
    bill.`申请人姓名` AS `applicant`,
    bill.`公司名称` AS `company`,
    bill.`作业状态` AS `status`,
    concat(formatDateTime(bill.`计划开工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划开工日期上午/下午`) AS `start`,
    concat(formatDateTime(bill.`计划完工日期`, '%Y-%m-%d %H:%i:%s'), ' ', bill.`计划完工日期上午/下午`) AS `end`,
    bill.`作业地点` AS `location`,
    bill.`具体作业内容` AS `detail`
FROM ods.interested_party_review AS bill FINAL
WHERE bill.Deleted = 0
    AND toYear(bill.`计划开工日期`) = toYear(now())
    AND bill.`作业地点` IN ('总成车间', '总成车间其他区域', '总成所属交车落车调车区域', '新调试', '老调试', '动车组调试基地', '交车车间落车调车区域', '库外')
    """)
    data["table_data"] = table_data_res.to_dict(orient="records")
    return data


app = Litestar(
    route_handlers=[index_html, get_dashboard_data],
    static_files_config=[
        StaticFilesConfig(path="/static", directories=["static"], name="static")
    ],
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=12385)
