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
            {"value": 2, "name": "正在作业"},
            {"value": 10, "name": "已进入"},
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
    # 在这里可以根据 time 参数 (每日, 每周, 每月, 每季, 每年) 从数据库中查询不同的数据
    data = template_data.copy()
    try:
        if time == "每日":
            data = await process_today(data)
        elif time == "每周":
            data = await process_week(data)
        elif time == "每月":
            data = await process_month(data)
        elif time == "每季":
            data = await process_quarter(data)
        elif time == "每年":
            data = await process_year(data)
        else:
            data = await process_today(data)
    except Exception as e:
        print(e)
    return Response(content=data, media_type="application/json")


async def process_today(data: dict) -> dict:
    ...


async def process_week(data: dict) -> dict:
    ...


async def process_month(data: dict) -> dict:
    ...


async def process_quarter(data: dict) -> dict:
    ...


async def process_year(data: dict) -> dict:
    ...


app = Litestar(
    route_handlers=[index_html, get_dashboard_data],
    static_files_config=[
        StaticFilesConfig(path="/static", directories=["static"], name="static")
    ],
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=12385)
