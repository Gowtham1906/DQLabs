from selenium.webdriver import chrome
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
import base64
import pandas as pd
from decimal import Decimal
import logging
from datetime import timedelta, datetime
import time
from xhtml2pdf import pisa
from itertools import groupby
from operator import itemgetter
import os
import uuid
import re
import json
import tempfile
import shutil

from dqlabs.utils.notifications import get_dqlabs_default_images, get_theme
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.enums.approval_status import ApprovalStatus

logger = logging.getLogger(__name__)

user_data_dir = tempfile.mkdtemp(prefix=f"chrome_{str(uuid.uuid4())}_")
def initalize_driver() -> WebDriver:
    driver_class = chrome.webdriver.WebDriver
    options = chrome.options.Options()
    # Create a unique temporary user data directory
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1600,950')
    options.add_argument("--force-device-scale-factor=1")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    temp_user_data_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={temp_user_data_dir}")
    if os.environ.get("SELENIUM_MANAGER_DISABLE", "false").lower() == "true":
        service = Service("/opt/chromedriver/chromedriver")
        kwargs = {
            "service": service,
            "options": options
        }
    else:
        kwargs = {
            "options": options
        }
    driver = driver_class(**kwargs)
    return driver


def generate_metadata_report(url: str, file_name: str, report_format: str, class_name:str = "loaderContainer", wait_time:int = None, is_image:bool = False, type:str="bar"):
    try:
        driver = initalize_driver()
        driver.get(url)
        _screenshot_load_wait = int(timedelta(seconds=100).total_seconds())
        WebDriverWait(driver, _screenshot_load_wait).until_not(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, class_name))
                    )
        waiting_time = wait_time if wait_time else 60
        time.sleep(waiting_time)
        page_width = None
        page_height = None
        if type == "hierachy":
            element = driver.find_element(By.CLASS_NAME, "org-chart")
            driver.execute_script("arguments[0].scrollIntoView();", element)
            dimensions = driver.execute_script("""
                return {
                    width: arguments[0].scrollWidth,
                    height: arguments[0].scrollHeight
                };
            """, element)
            page_width = dimensions.get("width")
            page_height =  dimensions.get("height")
            content_height = page_height+240 if page_height>700 else 800
            container_element = driver.find_element(By.CLASS_NAME, "widgetContainer")
            driver.execute_script("arguments[0].style.height = arguments[1] + 'px';", container_element, content_height)
            driver.set_window_size(page_width, content_height+10)

        if is_image:
            page_width = driver.execute_script("return document.body.scrollWidth")
            page_height = driver.execute_script("return document.body.scrollHeight")
            page_height += 150
            driver.set_window_size(page_width, page_height)
        if report_format == "pdf":
            if type == "hierachy":
                print_options = {
                    'paperWidth': page_width/96,
                    'paperHeight': 700,
                    'marginTop': 0,
                    'marginBottom': 0,
                    'marginLeft': 0,
                    'marginRight': 0,
                    'printBackground': True,
                    'landscape': False
                }
            else:
                if not page_width:
                    page_width = driver.execute_script("return document.body.scrollWidth")
                if not page_height:
                    page_height = driver.execute_script("""
                        return Math.max(
                            document.body.scrollHeight,
                            document.documentElement.scrollHeight,
                        );
                    """)
                print_options = {
                    'paperWidth': page_width/96,
                    'paperHeight': page_height/96,
                    'marginTop': 0,
                    'marginBottom': 0,
                    'marginLeft': 0,
                    'marginRight': 0,
                    'printBackground': True,
                    'landscape': False
                }
                
            base64_string = driver.execute_cdp_cmd("Page.printToPDF", print_options)
            decoded_bytes = base64.b64decode(base64_string.get("data"))

            # Write the decoded bytes to a PDF file
            with open(file_name, 'wb') as pdf_file:
                pdf_file.write(decoded_bytes)
        else:
            driver.get_screenshot_as_file(file_name)
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)
    except Exception as e:
        raise e


def prepare_csv_report(data: list, file_name: str):
    """ 
        Prepare CSV File
    """
    for row in data:
        is_score = False
        if 'dimension' in row :
            if isinstance(row['dimension'], list):
                if all(isinstance(d, dict) for d in row['dimension']):
                    # Process each dictionary in the list
                    for dimension in row['dimension']:
                        row[dimension['name']] = dimension['score']
                    is_score = True
            del row['dimension']
        for column in row:
            column_value = row[column]
            is_score = (column == "score")
            if type(column_value) == datetime:
                column_value = column_value.strftime("%m-%d-%Y %I:%M:%S")
            elif type(column_value) == Decimal and not is_score:
                column_value = str(column_value).rstrip('0').rstrip(
                    '.') if '.' in str(column_value) else str(column_value)
    df = pd.DataFrame(data)
    column_list = []
    for column in df.columns:
        column_list.append(column.upper())
    df.columns = column_list
    df.to_csv(file_name, index=False, header=True)

def prepare_pdf_report(config:dict, data: list, report: dict, file_name:str):
    """
    Prepare PDF Report
    """
    if data:
        theme = get_theme(config)
        table_string = ""
        name = report.get("name")
        properties = report.get("properties", {}).get("default", {})
        properties = properties.get("properties")
        hierarchy_view = properties.get("is_hierarchy")
        logo = theme.get("logo")
        theme = theme.get("reporting")
        table_theme = theme.get("greyshades")
        border_color = table_theme.get("cell_border")
        header_background = table_theme.get("header_bg")
        fonts = theme.get("headers")
        page_header_style = fonts.get("pdf_header_text")
        header_style= fonts.get("table_header_text")
        row_style = fonts.get("table_cell_text")
        footer_style = fonts.get("pdf_footer_text")
        column_widths = calculate_column_widths_dicts(data)
        page_header_style = f'font-family:{__get_pdf_font(page_header_style.get("fontFamily"))};font-size:{page_header_style.get("size")}px;font-weight:{page_header_style.get("weight")};color:{page_header_style.get("color")};font-style:{page_header_style.get("style")};text-align:left;padding-left:5px;min-height:30px;'
        header_row_style = f'font-family:{__get_pdf_font(header_style.get("fontFamily"))};font-size:{header_style.get("size")}px;font-weight:{header_style.get("weight")};color:{header_style.get("color")};font-style:{header_style.get("style")};text-align:left;padding-left:5px;min-height:30px;'
        row_style = f'border-left-color: {border_color};border-right-color: {border_color};font-family:{__get_pdf_font(row_style.get("fontFamily"))};font-size:12px;font-weight:{row_style.get("weight")};color:{row_style.get("color")};font-style:{row_style.get("style")};'
        footer_style = f'font-family:{__get_pdf_font(footer_style.get("fontFamily"))};font-size:{footer_style.get("size")}px;font-weight:{footer_style.get("weight")};color:{footer_style.get("color")};font-style:{footer_style.get("style")};'
        if not logo:
            default_icons = get_dqlabs_default_images()
            logo = default_icons.get("logo")
        if not hierarchy_view:
            rows = []
            header_columns = []
            for row in data:
                columns = []
                for column in row:
                    column_value = row[column]
                    column_max_width = f"{column_widths[column]}px" if column in column_widths else "200px"
                    column_style = f'{row_style}max-width:{column_max_width};'
                    value_length = len(str(column_value)) if column_value else 0
                    if column_value is None:
                        column_value = "NULL"
                    elif column_value == "":
                        column_value = "NA"
                    elif type(column_value) == datetime:
                        column_value = column_value.strftime("%m-%d-%Y %I:%M:%S")
                    elif type(column_value) == Decimal:
                        column_value = (
                            str(column_value).rstrip("0").rstrip(".")
                            if "." in str(column_value)
                            else str(column_value)
                        )
                    check_decimal_pipe_bool,check_decimal_data = check_decimal_pipe_concat(column_value)
                    if check_decimal_pipe_bool:
                        processed_values = []
                        for indx, value in enumerate(check_decimal_data):
                            float_value = float(value)
                            if float_value.is_integer():
                                processed_values.append(str(int(float_value)))  # Convert to integer and then to string
                            else:
                                processed_values.append(str(round(float_value, 9)))
                            if (indx + 1) % 6 == 0:
                                processed_values.append('<br>')

                        column_value = '|'.join(processed_values)
                    

                    elif value_length > 30:
                        part_length = value_length // 5
                        last_part_length = value_length - (4 * part_length)

                        # Split the string into five parts
                        first_part = column_value[0:part_length]
                        second_part = column_value[part_length:2*part_length]
                        third_part = column_value[2*part_length:3*part_length]
                        fourth_part = column_value[3*part_length:4*part_length]
                        fifth_part = column_value[4*part_length:4*part_length + last_part_length]

                        column_value = first_part + "\n" + second_part + "\n" + third_part + "\n" + fourth_part + "\n" + fifth_part

                    columns.append(f'<td class="tableRowCell" style="{column_style}"><p style = "text-align:left;padding-top:5px;padding-left:5px;word-break:break-all;white-space:pre-line;">{column_value}</p></td>')

                row_content = f"""<tr class="tableRowContainer" style="border-bottom-color: {border_color}">{''.join(columns)}</tr>"""
                rows.append(row_content)
                header_columns = [
                        f"""<th class="tableHeaderCell" style="max-width:{column_widths[column]}px;border-right-color: {border_color};{header_row_style}">{column.upper()}</th>"""
                        for column in data[0].keys()
                    ]
            table_string = f"""<table class="tableContainer" style="border-color:{border_color}"><thead><tr class="tableHeaderContainer" style="border-bottom-color: {border_color};background-color: {header_background}">{''.join(header_columns)}</tr></thead><tbody>{''.join(rows)}</tbody></table>"""
        else:
            detail = {
                "columns": properties.get("remediation_attributes", []),
                "group_by": properties.get("group_by", []),
                "theme" : theme.get("headers")
            }
            hierarchy_rows = []
            __prepare_hierarchy(data, 0, detail, hierarchy_rows)
            table_string = " ".join(hierarchy_rows)

        css = """
            .headerText {
                text-align: center;
            }
            .headerDescription {
                padding-top: 30px;
                font-size: 15px;
                color: #ED7A61;
                text-align: left;
                font-family:Poppins;
            }
            .image {
                width: 50px;
                height: 50px;
            }
            .tableContainer {
                margin-top: 5px;
                border-width: 1px;
                border-color: #bff0fd;
            }
            .tableHeaderContainer {
                border-bottom-color: #bff0fd;
                background-color: #bff0fd;
                border-bottom-width: 1px;
                min-height: 30px;
                text-align: center;
            }
            .tableHeaderCell {
                border-right-width: 1px;
                border-right-style: solid;
                text-transform: uppercase;
            }
            .tableRowContainer {
                border-bottom-width: 1px;
                border-bottom-style:solid;
                min-height: 36px;
                font-style: bold;
            }
            
            .tableRowCell {
                text-align: left;
                border-right-style:solid;
                border-right-width: 1px;
                border-left-width: 1px;
                border-left-style:solid;
                padding-left: 8px;
                width: 100%;
            }

            .hText {
                padding-top: 7px;
                padding-bottom: 7px;
            }
            .footer-center {
                    text-align:center;
                }
                p {
    page-break-inside: avoid;
    }
        """
        size = "a4 portrait"
        #content_width = "512pt" if orientation == "portrait" else "745pt"
        #content_height = "632pt" if orientation == "portrait" else "402pt"
        
        content_footer_top = "772pt"
        column_length = len(dict(data[0])) if data else 0
        if not hierarchy_view:
                aggregate_has_group = any(item.get("aggregate") == "group" for item in report.get("group_by", []))
                if aggregate_has_group:

                    width = int(sum(column_widths.values())/1.2)
                else:
                    width = int(sum(column_widths.values())/2)
                height = int(width* 1.1)
        else:
            dpi_size = 25
            width = int((column_length * dpi_size) * 2.2)
            height = int(width * 1.1)
            
        size = f"{width}mm {height}mm"
        content_width = f"{int(width-34)}mm"
        content_height = f"{height-80}mm"
        content_footer_top = f"{height-20}mm"
        page_style = f"""
        @page {{
            size: {size};
            @frame header_frame {{ 
                -pdf-frame-content: header_content;
                left: 17mm; width: {content_width}; top: 50pt; height: 40pt;
            }}
            @frame content_frame {{
                left: 17mm; width: {content_width}; top: 90pt; height: {content_height};
            }}
            @frame footer_frame {{
                -pdf-frame-content: footer_content;
                left: 17mm; width: {content_width}; top: {content_footer_top}; height: 20pt;
            }}
        }}"""
        footer_text = datetime.now().strftime("%m/%d/%y %I:%M:%S %p")
        font_faces = __load_font_face(theme)
        header_style = f'font-family:{__get_pdf_font(header_style.get("fontFamily"))};font-size:{header_style.get("size")}px;font-weight:{header_style.get("weight")};color:{header_style.get("color")};font-style:{header_style.get("style")};'
        html_string = f"""
            <html>
            <head>
                <style>
                {font_faces}
                {page_style}
                {css}
                </style>
            </head>
            <body>
                <div id="header_content">
                <table>
                        <tr>
                            <td style="width:90%;"> <p class="headerText" style="{page_header_style}">{name}</p></td>
                            <td style="text-align: right;"><img style="height: 50px;" class="image" src="{logo}"/></td>
                        </tr>
                    </table>
                
                </div>
                <div id="footer_content">
                <table>
                    <tr>
                        <td class="footer-center" style="{footer_style}">
                            {footer_text}
                        </td>
                    </tr>
                </table>
                </div>
                {table_string}
            </body>
            </html>
        """
    else:
        html_string = "<html><body></body></html>"

    result_file = open(file_name, "w+b")
    pisa.CreatePDF(html_string, dest=result_file)

    # # close output file
    result_file.close()
    return file_name


def prepare_predefined_report(config:dict, parent_data: list, report: dict, file_name:str):
    """
    Prepare PDF Report
    """

    theme = get_theme(config)
    table_string = ""
    name = report.get("name")
    properties = report.get("properties", {}).get("default", {})
    properties = properties.get("properties")
    predefined_template = properties.get("predefined_template", "")
    logo = theme.get("logo")
    theme = theme.get("reporting")
    table_theme = theme.get("greyshades")
    border_color = table_theme.get("cell_border")
    header_background = table_theme.get("header_bg")
    fonts = theme.get("headers")
    page_header_style = fonts.get("pdf_header_text")
    header_style= fonts.get("table_header_text")
    row_style = fonts.get("table_cell_text")
    footer_style = fonts.get("pdf_footer_text")
    page_header_style = f'font-family:{__get_pdf_font(page_header_style.get("fontFamily"))};font-size:{page_header_style.get("size")}px;font-weight:{page_header_style.get("weight")};color:{page_header_style.get("color")};font-style:{page_header_style.get("style")};text-align:left;padding-left:5px;min-height:30px;'
    header_row_style = f'font-family:{__get_pdf_font(header_style.get("fontFamily"))};font-size:{header_style.get("size")}px;font-weight:{header_style.get("weight")};color:{header_style.get("color")};font-style:{header_style.get("style")};text-align:left;padding-left:5px;min-height:30px;'
    row_style = f'border-left-color: {border_color};border-right-color: {border_color};font-family:{__get_pdf_font(row_style.get("fontFamily"))};font-size:12px;font-weight:{row_style.get("weight")};color:{row_style.get("color")};font-style:{row_style.get("style")};'
    footer_style = f'font-family:{__get_pdf_font(footer_style.get("fontFamily"))};font-size:{footer_style.get("size")}px;font-weight:{footer_style.get("weight")};color:{footer_style.get("color")};font-style:{footer_style.get("style")};'
    if not logo:
        default_icons = get_dqlabs_default_images()
        logo = default_icons.get("logo")
    index = 0
    for child_data in parent_data:
        data = child_data.get("tableContent", [])
        table_heading = child_data.get("heading", "")
        column_widths = calculate_column_widths_dicts(data)
        table_html = ""
        rows = []
        header_columns = []
        for row in data:
            columns = []
            for column in row:
                if column == "threshold":
                    continue
                column_value = row[column]
                column_value = process_predefined_column_value(column_value, column)
                column_max_width = f"{column_widths[column]}px" if column in column_widths else "200px"
                column_style = f'{row_style}max-width:{column_max_width};'
                value_length = len(str(column_value)) if column_value else 0
                if column_value is None:
                    column_value = "NULL"
                elif column_value == "":
                    column_value = "NA"
                elif type(column_value) == datetime:
                    column_value = column_value.strftime("%m-%d-%Y %I:%M:%S")
                elif type(column_value) == Decimal:
                    column_value = (
                        str(column_value).rstrip("0").rstrip(".")
                        if "." in str(column_value)
                        else str(column_value)
                    )
                check_decimal_pipe_bool,check_decimal_data = check_decimal_pipe_concat(column_value)

                if check_decimal_pipe_bool:
                    processed_values = []
                    for indx, value in enumerate(check_decimal_data):
                        float_value = float(value)
                        if float_value.is_integer():
                            processed_values.append(str(int(float_value)))  # Convert to integer and then to string
                        else:
                            processed_values.append(str(round(float_value, 9)))
                        if (indx + 1) % 6 == 0:
                            processed_values.append('<br>')

                    column_value = '|'.join(processed_values)

                elif value_length > 30:
                    part_length = value_length // 5
                    last_part_length = value_length - (4 * part_length)

                    # Split the string into five parts
                    first_part = column_value[0:part_length]
                    second_part = column_value[part_length:2*part_length]
                    third_part = column_value[2*part_length:3*part_length]
                    fourth_part = column_value[3*part_length:4*part_length]
                    fifth_part = column_value[4*part_length:4*part_length + last_part_length]

                    column_value = first_part + "\n" + second_part + "\n" + third_part + "\n" + fourth_part + "\n" + fifth_part
                

                columns.append(f'<td class="tableRowCell" style="word-break:break-all; {column_style}"><p style = "text-align:left;padding-top:5px;padding-left:5px;word-break:break-all;white-space:pre-line;">{column_value}</p></td>')
            row_content = f"""<tr class="tableRowContainer" style="border-bottom-color: {border_color}">{''.join(columns)}</tr>"""
            rows.append(row_content)
            header_columns = [
                    f"""<th class="tableHeaderCell" style="word-break:break-all; max-width:{column_widths[column]}px;border-right-color: {border_color};{header_row_style}">{column.upper()}</th>"""
                    for column in data[0].keys()
                ]
        table_html = f"""<div><h1>{table_heading}</h1><table class="tableContainer" style="border-color:{border_color}"><thead><tr class="tableHeaderContainer" style="border-bottom-color: {border_color};background-color: {header_background}">{''.join(header_columns)}</tr></thead><tbody>{''.join(rows)}</tbody></table><div>"""
        if index:
            table_html = f"<div class='newPage'></div>{table_html}"
        table_string += table_html
        index += 1

    table_string = f"""<div>{table_string}<div>"""

    css = """
        .newPage {
            page-break-before: always;
        }
        .headerText {
            text-align: center;
        }
        .headerDescription {
            padding-top: 30px;
            font-size: 15px;
            color: #ED7A61;
            text-align: left;
            font-family:Poppins;
        }
        .image {
            width: 50px;
            height: 50px;
        }
        .tableContainer {
            margin-top: 5px;
            border-width: 1px;
            border-color: #bff0fd;
        }
        .tableHeaderContainer {
            border-bottom-color: #bff0fd;
            background-color: #bff0fd;
            border-bottom-width: 1px;
            min-height: 30px;
            text-align: center;
        }
        .tableHeaderCell {
            border-right-width: 1px;
            border-right-style: solid;
            text-transform: uppercase;
        }
        .tableRowContainer {
            border-bottom-width: 1px;
            border-bottom-style:solid;
            min-height: 36px;
            font-style: bold;
        }
        
        .tableRowCell {
            text-align: left;
            border-right-style:solid;
            border-right-width: 1px;
            border-left-width: 1px;
            border-left-style:solid;
            padding-left: 8px;
            width: 100%;
        }

        .hText {
            padding-top: 7px;
            padding-bottom: 7px;
        }
        .footer-center {
                text-align:center;
            }
            p {
        page-break-inside: avoid;
    }
    """
    size = "a4 portrait"
    #content_width = "512pt" if orientation == "portrait" else "745pt"
    #content_height = "632pt" if orientation == "portrait" else "402pt"
    
    content_footer_top = "772pt"
    width = 670
    height = 225.425
    if predefined_template == "Alerts":
        width = 370

    elif predefined_template == "Issues":
        width = 240
        
    size = f"{width}mm {height}mm"
    content_width = f"{int(width-34)}mm"
    content_height = f"{height-80}mm"
    content_footer_top = f"{height-20}mm"
    page_style = f"""
    @page {{
        size: {size};
        @frame header_frame {{ 
            -pdf-frame-content: header_content;
            left: 17mm; width: {content_width}; top: 50pt; height: 40pt;
        }}
        @frame content_frame {{
            left: 17mm; width: {content_width}; top: 90pt; height: {content_height};
        }}
        @frame footer_frame {{
            -pdf-frame-content: footer_content;
            left: 17mm; width: {content_width}; top: {content_footer_top}; height: 20pt;
        }}
    }}"""
    footer_text = datetime.now().strftime("%m/%d/%y %I:%M:%S %p")
    font_faces = __load_font_face(theme)
    header_style = f'font-family:{__get_pdf_font(header_style.get("fontFamily"))};font-size:{header_style.get("size")}px;font-weight:{header_style.get("weight")};color:{header_style.get("color")};font-style:{header_style.get("style")};'
    html_string = f"""
        <html>
        <head>
            <style>
            {font_faces}
            {page_style}
            {css}
            </style>
        </head>
        <body>
            <div id="header_content">
            <table>
                    <tr>
                        <td style="width:90%;"> <p class="headerText" style="{page_header_style}">{name}</p></td>
                        <td style="text-align: right;"><img style="height: 50px;" class="image" src="{logo}"/></td>
                    </tr>
                </table>
            
            </div>
            <div id="footer_content">
            <table>
                <tr>
                    <td class="footer-center" style="{footer_style}">
                        {footer_text}
                    </td>
                </tr>
            </table>
            </div>
            {table_string}
        </body>
        </html>
    """
    result_file = open(file_name, "w+b")
    pisa.CreatePDF(html_string, dest=result_file)

    # # close output file
    result_file.close()
    return file_name

def __heirarchy_group_by(data, group_by_column):
    group_by_data = []
    for key, value in groupby(data, key=itemgetter(group_by_column)):
        group_by_data.append({"data": list(value), group_by_column: key})
    return group_by_data


def __group_by_values(data, level, detail, hierachy_rows):
    columns = detail.get("group_by", [])
    theme = detail.get("theme")
    style  = __get_hierachy_style(level, theme)
    style = f"margin:0px;padding-left:{level*20}px;{style}"
    for column in columns:
        g_column = column.get("column").lower()
        g_agg = column.get("aggregate").lower()
        g_col_agg = f"{g_column}_{g_agg}"
        for item in data:
            f_data = str(item[g_col_agg]).split("|") if item[g_col_agg] else []
            for i in f_data:
                if is_decimal(i):
                    i = round(float(i), 9)
                row_content = f'<div class="hText" style="{style}"><p>{g_column.upper()} : {i} </p> </div>'
                hierachy_rows.append(row_content)

def __prepare_hierarchy(rows, level, detail, hierachy_rows):
    columns = detail.get("columns", [])
    theme = detail.get('theme')
    columns = [column.get("name") if "name" in column else column  for column in columns]
    h_column = columns[level]
    row = rows[0]
    row_columns = list(row.keys())
    if row_columns[0].islower():
        h_column = h_column.lower()
    h_data = __heirarchy_group_by(rows, h_column)
    style  = __get_hierachy_style(level, theme)
    style = f"margin:0px;{style}"
    for row in h_data:
        column_value = row[h_column]
        if column_value is None:
            column_value = "NULL"
        elif column_value == "":
            column_value = "NA"
        elif type(column_value) == datetime:
            column_value = column_value.strftime("%m-%d-%Y %I:%M:%S")
        elif type(column_value) == Decimal:
            column_value = (
                str(column_value).rstrip("0").rstrip(".")
                if "." in str(column_value)
                else str(column_value)
            )
        if is_decimal(column_value):
            column_value = round(float(column_value), 9)
        else:
            pass

        row_content = f'<div class="hText" style="padding-left:{level*20}px"><p style="{style}">{h_column.upper()} : {column_value}</p> </div>'
        hierachy_rows.append(row_content)
        if level < len(columns) - 1:
            __prepare_hierarchy(row.get("data"), level + 1, detail, hierachy_rows)
        else:
            __group_by_values(row.get("data"), level + 1, detail, hierachy_rows)


def __get_pdf_font(font):
    if (font and "," in font):
        font = font.split(",")[0].strip()
    return font

def __load_font_face(theme):
    headers_fonts = theme.get("headers")
    fonts = set([
        headers_fonts.get("h1").get("fontFamily"),
        headers_fonts.get("h2").get("fontFamily"),
        headers_fonts.get("h3").get("fontFamily"),
        headers_fonts.get("h4").get("fontFamily"),
        headers_fonts.get("h5").get("fontFamily"),
        headers_fonts.get("h6").get("fontFamily"),
        headers_fonts.get("pdf_header_text").get("fontFamily"),
        headers_fonts.get("pdf_footer_text").get("fontFamily"),
        headers_fonts.get("table_header_text").get("fontFamily"),
        headers_fonts.get("table_cell_text").get("fontFamily")
    ])
    font_faces = ""
    font_list = __font_faces()
    for font in fonts:
        font  = __get_pdf_font(font)
        font = font_list[font]
        if font:
            font_faces = f"{font_faces} {font}"
    return font_faces


def __font_faces():
    return {
        "Plus Jakarta Sans":"""
            @font-face {
                font-family: Plus Jakarta Sans;
                src: url('https://fonts.gstatic.com/s/plusjakartasans/v8/LDIuaomQNQcsA88c7O9yZ4KMCoOg4Koz4y6qhNnZR-A.woff2');
            }
        """,
        "Inter":"""
            @font-face {
                font-family: Inter;
                src: url('https://fonts.gstatic.com/s/inter/v18/UcC73FwrK3iLTeHuS_fjbvMwCp504jAa2JL7W0Q5n-wU.woff2');
            }
            @font-face {
                font-family: Inter;
                src: url('https://fonts.gstatic.com/s/inter/v18/UcC73FwrK3iLTeHuS_fjbvMwCp500DAa2JL7W0Q5n-wU.woff2');
                font-weight: 500;
            }
            @font-face {
                font-family: Inter;
                src: url('https://fonts.gstatic.com/s/inter/v18/UcC73FwrK3iLTeHuS_fjbvMwCp50BTca2JL7W0Q5n-wU.woff2');
                font-weight: 700;
            }
        """,
        "Lato": """
            @font-face {
                font-family: Lato;
                src: url('https://fonts.gstatic.com/s/lato/v24/S6uyw4BMUTPHvxk.ttf');
            }
            @font-face {
                font-family: Lato;
                src: url('https://fonts.gstatic.com/s/lato/v24/S6u9w4BMUTPHh6UVew8.ttf');
                font-weight: 700;
            }
        """,
        "Roboto": """
            @font-face {
                font-family: Roboto;
                src: url('https://fonts.gstatic.com/s/roboto/v30/KFOmCnqEu92Fr1Me5Q.ttf');
            }
            @font-face {
                font-family: Roboto;
                src: url('https://fonts.gstatic.com/s/roboto/v30/KFOlCnqEu92Fr1MmEU9vAw.ttf');
                font-weight: 500;
            }
            @font-face {
                font-family: Roboto;
                src: url('https://fonts.gstatic.com/s/roboto/v30/KFOlCnqEu92Fr1MmWUlvAw.ttf');
                font-weight: 700;
            }
        """,
        "Lora": """
            @font-face {
                font-family: Lora;
                src: url('https://fonts.gstatic.com/s/lora/v32/0QI6MX1D_JOuGQbT0gvTJPa787weuyJG.ttf');
            }
            @font-face {
                font-family: Lora;
                src: url('https://fonts.gstatic.com/s/lora/v32/0QI6MX1D_JOuGQbT0gvTJPa787wsuyJG.ttf');
                font-weight: 500;
            }
            @font-face {
                font-family: Lora;
                src: url('https://fonts.gstatic.com/s/lora/v32/0QI6MX1D_JOuGQbT0gvTJPa787z5vCJG.ttf');
                font-weight: 700;
            }
        """,
         "Montserrat": """
            @font-face {
                font-family: Montserrat;
                src: url('https://fonts.gstatic.com/s/montserrat/v26/JTUHjIg1_i6t8kCHKm4532VJOt5-QNFgpCtr6Ew-.ttf');
            }
            @font-face {
                font-family: Montserrat;
                src: url('https://fonts.gstatic.com/s/montserrat/v26/JTUHjIg1_i6t8kCHKm4532VJOt5-QNFgpCtZ6Ew-.ttf');
                font-weight: 500;
            }
            @font-face {
                font-family: Montserrat;
                src: url('https://fonts.gstatic.com/s/montserrat/v26/JTUHjIg1_i6t8kCHKm4532VJOt5-QNFgpCuM70w-.ttf');
                font-weight: 700;
            }
        """,
        "Noto Sans": """
            @font-face {
                font-family: Noto Sans;
                src: url('https://fonts.gstatic.com/s/notosans/v35/o-0mIpQlx3QUlC5A4PNB6Ryti20_6n1iPHjcz6L1SoM-jCpoiyD9A99d.ttf');
            }
            @font-face {
                font-family: Noto Sans;
                src: url('https://fonts.gstatic.com/s/notosans/v35/o-0mIpQlx3QUlC5A4PNB6Ryti20_6n1iPHjcz6L1SoM-jCpoiyDPA99d.ttf');
                font-weight: 500;
            }
            @font-face {
                font-family: Noto Sans;
                src: url('https://fonts.gstatic.com/s/notosans/v35/o-0mIpQlx3QUlC5A4PNB6Ryti20_6n1iPHjcz6L1SoM-jCpoiyAaBN9d.ttf');
                font-weight: 700;
            }
        """,
        "Nunito": """
            @font-face {
                font-family: Nunito;
                src: url('https://fonts.gstatic.com/s/nunito/v26/XRXI3I6Li01BKofiOc5wtlZ2di8HDLshRTM.ttf');
            }
            @font-face {
                font-family: Nunito;
                src: url('https://fonts.gstatic.com/s/nunito/v26/XRXI3I6Li01BKofiOc5wtlZ2di8HDIkhRTM.ttf');
                font-weight: 500;
            }
            @font-face {
                font-family: Nunito;
                src: url('https://fonts.gstatic.com/s/nunito/v26/XRXI3I6Li01BKofiOc5wtlZ2di8HDFwmRTM.ttf');
                font-weight: 700;
            }
        """,
        "Nunito Sans": """
            @font-face {
                font-family: Nunito Sans;
                src: url('https://fonts.gstatic.com/s/nunitosans/v15/pe1mMImSLYBIv1o4X1M8ce2xCx3yop4tQpF_MeTm0lfGWVpNn64CL7U8upHZIbMV51Q42ptCp5F5bxqqtQ1yiU4G1ilntA.ttf');
            }
            @font-face {
                font-family: Nunito Sans;
                src: url('https://fonts.gstatic.com/s/nunitosans/v15/pe1mMImSLYBIv1o4X1M8ce2xCx3yop4tQpF_MeTm0lfGWVpNn64CL7U8upHZIbMV51Q42ptCp5F5bxqqtQ1yiU4GCC5ntA.ttf');
                font-weight: 600;
            }
            @font-face {
                font-family: Nunito Sans;
                src: url('https://fonts.gstatic.com/s/nunitosans/v15/pe1mMImSLYBIv1o4X1M8ce2xCx3yop4tQpF_MeTm0lfGWVpNn64CL7U8upHZIbMV51Q42ptCp5F5bxqqtQ1yiU4GMS5ntA.ttf');
                font-weight: 700;
            }
        """,
        "Open Sans": """
            @font-face {
                font-family: Open Sans;
                src: url('https://fonts.gstatic.com/s/opensans/v36/memSYaGs126MiZpBA-UvWbX2vVnXBbObj2OVZyOOSr4dVJWUgsjZ0C4n.ttf');
            }
            @font-face {
                font-family: Open Sans;
                src: url('https://fonts.gstatic.com/s/opensans/v36/memSYaGs126MiZpBA-UvWbX2vVnXBbObj2OVZyOOSr4dVJWUgsgH1y4n.ttf');
                font-weight: 600;
            }
            @font-face {
                font-family: Open Sans;
                src: url('https://fonts.gstatic.com/s/opensans/v36/memSYaGs126MiZpBA-UvWbX2vVnXBbObj2OVZyOOSr4dVJWUgsg-1y4n.ttf');
                font-weight: 700;
            }
        """,
        "Oswald": """
            @font-face {
                font-family: Oswald;
                src: url('https://fonts.gstatic.com/s/oswald/v53/TK3_WkUHHAIjg75cFRf3bXL8LICs1_FvgUE.ttf');
            }
            @font-face {
                font-family: Oswald;
                src: url('https://fonts.gstatic.com/s/oswald/v53/TK3_WkUHHAIjg75cFRf3bXL8LICs1y9ogUE.ttf');
                font-weight: 600;
            }
            @font-face {
                font-family: Oswald;
                src: url('https://fonts.gstatic.com/s/oswald/v53/TK3_WkUHHAIjg75cFRf3bXL8LICs1xZogUE.ttf');
                font-weight: 700;
            }
        """,
        "Poppins": """
            @font-face {
                font-family: Poppins;
                src: url('https://fonts.gstatic.com/s/poppins/v20/pxiEyp8kv8JHgFVrFJA.ttf');
            }
            @font-face {
                font-family: Poppins;
                src: url('https://fonts.gstatic.com/s/poppins/v20/pxiByp8kv8JHgFVrLEj6V1s.ttf');
                font-weight: 600;
            }
            @font-face {
                font-family: Poppins;
                src: url('https://fonts.gstatic.com/s/poppins/v20/pxiByp8kv8JHgFVrLCz7V1s.ttf');
                font-weight: 700;
            }
        """,
        "Raleway": """
            @font-face {
                font-family: Raleway;
                src: url('https://fonts.gstatic.com/s/raleway/v29/1Ptxg8zYS_SKggPN4iEgvnHyvveLxVvaooCP.ttf');
            }
            @font-face {
                font-family: Raleway;
                src: url('https://fonts.gstatic.com/s/raleway/v29/1Ptxg8zYS_SKggPN4iEgvnHyvveLxVsEpYCP.ttf');
                font-weight: 600;
            }
            @font-face {
                font-family: Raleway;
                src: url('https://fonts.gstatic.com/s/raleway/v29/1Ptxg8zYS_SKggPN4iEgvnHyvveLxVs9pYCP.ttf');
                font-weight: 700;
            }
        """,
        "Slabo": """
            @font-face {
                font-family: Slabo;
                src: url('https://fonts.gstatic.com/s/raleway/v29/1Ptxg8zYS_SKggPN4iEgvnHyvveLxVvaooCP.ttf');
            }
        """,
        "PT Sans":"""
             @font-face {
                font-family: PT San;
                src: url(https://fonts.gstatic.com/s/ptsans/v17/jizYRExUiTo99u79D0eEwA.ttf');
            }
             @font-face {
                font-family: PT San;
                src: url('https://fonts.gstatic.com/s/ptsans/v17/jizfRExUiTo99u79B_mh4Ok.ttf');
                font-weight: 700;
            }
        """,
        "Source Sans Pro": """
            @font-face {
                font-family: Source Sans Pro;
                src: url('https://fonts.gstatic.com/s/sourcesanspro/v22/6xK3dSBYKcSV-LCoeQqfX1RYOo3aPw.ttf');
            }
            @font-face {
                font-family: Source Sans Pro;
                src: url('https://fonts.gstatic.com/s/sourcesanspro/v22/6xKydSBYKcSV-LCoeQqfX1RYOo3i54rAkA.ttf');
                font-weight: 600;
            }
            @font-face {
                font-family: Source Sans Pro;
                src: url('https://fonts.gstatic.com/s/sourcesanspro/v22/6xKydSBYKcSV-LCoeQqfX1RYOo3ig4vAkA.ttf');
                font-weight: 700;
            }
        """,
        "Work Sans": """
            @font-face {
                font-family: Work Sans;
                src: url('https://fonts.gstatic.com/s/worksans/v19/QGY_z_wNahGAdqQ43RhVcIgYT2Xz5u32K0nXNig.ttf');
            }
            @font-face {
                font-family: Work Sans;
                src: url('https://fonts.gstatic.com/s/worksans/v19/QGY_z_wNahGAdqQ43RhVcIgYT2Xz5u32K5fQNig.ttf');
                font-weight: 600;
            }
            @font-face {
                font-family: Work Sans;
                src: url('https://fonts.gstatic.com/s/worksans/v19/QGY_z_wNahGAdqQ43RhVcIgYT2Xz5u32K67QNig.ttf');
                font-weight: 700;
            }
        """
    }

def __get_hierachy_style(level, theme):
    font = theme.get("h6")
    if level == 0:
        font = theme.get("h1")
    elif level == 1:
        font = theme.get("h2")
    elif level == 2:
        font = theme.get("h3")
    elif level == 3:
        font = theme.get("h4")
    elif level == 4:
        font = theme.get("h5")
    return f'font-family:{__get_pdf_font(font.get("fontFamily"))};font-size:{font.get("size")}px;color:{font.get("color")};font-weight:{font.get("weight")};font-style:{font.get("style")};'

def is_decimal(data : str):
    try:
        float(data)
        return "." in data
    except:
        return False
    
def is_valid_json(value):
    try:
        json.loads(value)
        return True
    except (ValueError, TypeError):
        return False

def process_column_value(column_value, column):
    if isinstance(column_value, str):
        return column_value
    
    elif isinstance(column_value, list):
        name_list = [item['name'] for item in column_value if isinstance(item, dict) and 'name' in item]
        return ", ".join(name_list) if name_list else ""
    
    return column_value


def process_predefined_column_value(column_value, column):
        if column_value is None:
            return "NA"
        if column.lower() in ["last_runs", "recent"]:
            if is_valid_json(column_value):
                run_ui = json.loads(column_value)
                if isinstance(run_ui, str) and is_valid_json(run_ui):
                    run_ui = json.loads(run_ui)

                if column.lower() == "recent":
                    status_texts = [f"alert_count: {item.get('alert_count')}" 
                                    for item in run_ui if item.get('status') == "alert"]
                else:
                    status_texts = [item.get('status', '').lower() 
                                    for item in run_ui if item.get('status')]
                
                return ", ".join(status_texts) if status_texts else "NA"
            else:
                return "NA"
            
        if column.lower() == "score":
            try:
                float_value = float(column_value)
                if float_value.is_integer():
                    return str(int(float_value))
                else:
                    return f"{float_value:.2f}"
            except (ValueError, TypeError):
                return "NA"

        if isinstance(column_value, str):
            return column_value
        
        elif isinstance(column_value, list):
            name_list = [item['name'] for item in column_value if isinstance(item, dict) and 'name' in item]
            return ", ".join(name_list) if name_list else ""
        
        return column_value
    
    
def check_decimal_pipe_concat(data: str):
    try:
        values = data.split('|')                      
        try:
            if all(is_decimal(value.strip()) for value in values):
                return True,values
            else:
                return False,[]
        except ValueError:
            return False,[]
    except:
        return False,[]
    
def calculate_column_widths_dicts(rows):
    if not rows:
        return {}
    
    # Initialize max_widths considering the column names as well
    max_widths = {column: max(len(column) * 14, 75) for column in rows[0].keys()}
    
    for row in rows:
        for column, value in row.items():
            # Check if value is None or an empty string
            if value is None or value == "":
                value_width = 75
            else:
                value_str = str(value)
                value_width = max(len(value_str) * 14, 75)
            max_widths[column] = max(max_widths[column], value_width)
    
    # Limit the max width to 200 pixels
    for column in max_widths:
        max_widths[column] = min(max_widths[column], 200)
    return max_widths

def __get_query_to_filter_active_measures(measure_id_columnn) -> str:
    query = f"""
        select distinct mes.id from core.measure as mes
        left join core.attribute as attr on attr.id = mes.attribute_id 
        left join core.asset as ast on ast.id = mes.asset_id 
        left join core.connection as conn on conn.id = mes.connection_id
        where 
            mes.id = {measure_id_columnn} and mes.is_active = true and mes.is_delete = false and mes.status != '{ApprovalStatus.Deprecated.value}'
            and (conn.is_active = true and conn.is_delete = false)
            and ((ast.is_delete = false and ast.is_active = true and ast.status != '{ApprovalStatus.Deprecated.value}' ) or ast.is_delete is null)
            and ((attr.is_selected = true and attr.is_delete = false and attr.status != '{ApprovalStatus.Deprecated.value}' ) or attr.is_delete is null)
    """
    return query

def __get_date_filter_query(properties: dict, query_string: str, utc_convert: bool = False) -> str:
    try:
        tab = (properties.get("selectedTab") if properties.get("selectedTab") else "default")
        date_filter_object = properties.get("date_filter_column", None)
        date_field = date_filter_object.get(tab) if date_filter_object else None
        selectedFilter = properties.get("selectedFilter", {})
        date_selected = selectedFilter.get("selected", None)
        days = selectedFilter.get("days", 0)
        utc_convert_query = " at time zone 'UTC' " if utc_convert else ""

        if date_field and date_selected:
            usage_date_field = "run_date"
            usage_asset_date_filter = ""
            if date_selected == "Last Run":
                filter_condition = f" and metrics.run_id = measure.last_run_id"
            elif date_selected == "All":
                filter_condition = ""
                usage_asset_date_filter = ""
            else:
                if days == 1:
                    filter_condition = f"and date({date_field}) {utc_convert_query} = date(current_date) {utc_convert_query} "
                    usage_asset_date_filter = f"and date({usage_date_field}) {utc_convert_query} = date(current_date) {utc_convert_query} "
                elif days == 2:
                    filter_condition = f"and date({date_field}) {utc_convert_query} = date(current_date - interval '1 day') {utc_convert_query} "
                    usage_asset_date_filter = f"and date({usage_date_field}) {utc_convert_query} = date(current_date - interval '1 day') {utc_convert_query} "
                else:
                    filter_condition = f"and date({date_field}) {utc_convert_query} > date(current_date - interval '{days} days') {utc_convert_query} "
                    usage_asset_date_filter = f"and date({usage_date_field}) {utc_convert_query} > date(current_date - interval '{days} days') {utc_convert_query} "

            query_string = query_string.replace(
                "<date_filter_query>", filter_condition
            )
            query_string = query_string.replace(
                "<usage_date_filter_query>", usage_asset_date_filter
            )
        else:
            query_string = query_string.replace("<date_filter_query>", "")
    except Exception as error:
        query_string = query_string.replace("<date_filter_query>", "")
    finally:
        return query_string
    
def __get_measure_active_filter_query(properties: dict, query_string: str) -> str:
    try:
        updated_query = ''
        tab = properties.get('selectedTab') if properties.get(
            'selectedTab') else "default"
        query_properties = properties.get(tab)

        if query_properties:
            tables = query_properties.get(
                'tables', []) if query_properties else []
            active_measure_filter_query = ""
            if tables:
                if "metrics" in tables:
                    active_measure_filter_query = __get_query_to_filter_active_measures(
                        'metrics.measure_id')
                    active_measure_filter_query = f" and metrics.measure_id in ({active_measure_filter_query})" if active_measure_filter_query else ""
                elif "measure" in tables:
                    active_measure_filter_query = __get_query_to_filter_active_measures(
                        'measure.id')
                    active_measure_filter_query = f" and measure.id in ({active_measure_filter_query})" if active_measure_filter_query else ""
                elif "issues" in tables:
                    active_measure_filter_query = __get_query_to_filter_active_measures(
                        'issues.measure_id')
                    active_measure_filter_query = f" and issues.measure_id in ({active_measure_filter_query})" if active_measure_filter_query else ""
            if query_string:
                updated_query = query_string.replace(
                    '<active_measure_filter>', active_measure_filter_query)

    except Exception as error:
        updated_query = query_string.replace(
                    '<active_measure_filter>', "")
    finally:
        return updated_query

def __get_semantics_restrictions_query(config: dict, properties: dict, query_string: str, user:dict
    ) -> str:
        try:
            semantics_filter_query = ""
            tab = (
                properties.get("selectedTab")
                if properties.get("selectedTab")
                else "default"
            )
            query_properties = properties.get(tab)

            if query_properties:
                tables = query_properties.get("tables", []) if query_properties else []
                permission_query = ""
                if tables:
                    if "metrics" in tables:
                        permission_query = get_accesss_by_role(config, type="measure", user_permission=user)
                        permission_query = (
                            f" and metrics.measure_id in ({permission_query})"
                            if permission_query
                            else ""
                        )
                    elif "measure" in tables:
                        permission_query = get_accesss_by_role(config, type="measure", user_permission=user)
                        permission_query = (
                            f" and measure.id in ({permission_query})"
                            if permission_query
                            else ""
                        )
                    elif "attribute" in tables:
                        permission_query = get_accesss_by_role(
                            config, type="attribute", user_permission=user
                        )
                        permission_query = (
                            f" and attribute.id in ({permission_query})"
                            if permission_query
                            else ""
                        )
                    elif "asset" in tables:
                        permission_query = get_accesss_by_role(config, type="asset", user_permission=user)
                        permission_query = (
                            f" and asset.id in ({permission_query})"
                            if permission_query
                            else ""
                        )
                    elif "domain" in tables:
                        permission_query = get_accesss_by_role(config, query=False, user_permission=user)
                        associated_type = permission_query.get("associated_type", "full")
                        domains = permission_query.get("domain")

                        if domains:
                            domains = f"""({','.join(f"'{w}'" for w in domains)})"""
                            if associated_type == "full":
                                permission_query = f" and domain.id in {domains}"
                            else:
                                permission_query = f" and domain.id not in {domains}"
                        else:
                            permission_query = ""

                    elif "application" in tables:
                        permission_query = get_accesss_by_role(config, query=False, user_permission=user)
                        associated_type = permission_query.get("associated_type", "full")
                        application = permission_query.get("application")

                        if application:
                            application = (
                                f"""({','.join(f"'{w}'" for w in application)})"""
                            )
                            if associated_type == "full":
                                permission_query = (
                                    f" and application.id in {application}"
                                )
                            else:
                                permission_query = (
                                    f" and application.id not in {application}"
                                )
                        else:
                            permission_query = ""

                    elif "tags" in tables:
                        permission_query = get_accesss_by_role(config, query=False, user_permission=user)
                        associated_type = permission_query.get("associated_type", "full")
                        tags = permission_query.get("tags")

                        if tags:
                            tags = f"""({','.join(f"'{w}'" for w in tags)})"""
                            if associated_type == "full":
                                permission_query = f" and tags.id in {tags}"
                            else:
                                permission_query = f" and tags.id not in {tags}"
                        else:
                            permission_query = ""

                    elif "issues" in tables:
                        permission_query = get_accesss_by_role(config, type="measure", user_permission=user)
                        permission_query = (
                            f" and issues.measure_id in ({permission_query})"
                            if permission_query
                            else ""
                        )
                    elif "product" in tables:
                        permission_query = get_accesss_by_role(
                            config, query=False, user_permission=user)
                        associated_type = permission_query.get(
                            "associated_type", "full")
                        products = permission_query.get("product")

                        if products:
                            products = f"""({','.join(f"'{w}'" for w in products)})"""
                            if associated_type == "full":
                                permission_query = f" and product.id in {products}"
                            else:
                                permission_query = f" and product.id not in {products}"
                        else:
                            permission_query = ""
                if query_string:
                    semantics_filter_query = query_string.replace(
                        "<sementics_filter_query>", permission_query
                    )
                asset_permission_query = get_accesss_by_role(
                    config, type="asset", user_permission=user)
                asset_permission_query = (
                    f" and asset.id in ({asset_permission_query})"
                    if asset_permission_query
                    else ""
                )

                attribute_permission_query = get_accesss_by_role(
                    config, type="attribute", user_permission=user)
                attribute_permission_query = (
                    f" and attribute.id in ({attribute_permission_query})"
                    if attribute_permission_query
                    else ""
                )

                measure_permission_query = get_accesss_by_role(
                    config, type="measure", user_permission=user)
                measure_permission_query = (
                    f" and measure.id in ({measure_permission_query})"
                    if measure_permission_query
                    else ""
                )

                semantics_filter_query = semantics_filter_query.replace(
                    "<sementics_asset_filter_query>", asset_permission_query
                ).replace(
                    "<sementics_attribute_filter_query>", attribute_permission_query
                ).replace(
                    "<sementics_measure_filter_query>", measure_permission_query
                ).replace(
                    "<domain_hierachy_filters>", get_assocaited_domains_query(
                        "domain.id")
                ).replace(
                    "<product_hierachy_filters>", get_assocaited_products_query(
                        'product.id')
                )

        except:
            semantics_filter_query = query_string.replace(
                        "<sementics_filter_query>", ""
                    )
        finally:
            return semantics_filter_query
        
def get_assocaited_domains_query(field: str) -> str:
    query = f"""
            WITH RECURSIVE domain_list AS (
                SELECT p.id, p.parent_id
                FROM core.domain p
  				where p.id = {field}
                UNION ALL
                SELECT t.id, t.parent_id
                FROM core.domain t
                JOIN domain_list ON t.parent_id = domain_list.id
            ) 
            select id from domain_list
        """
    return query


def get_assocaited_products_query(field: str) -> str:
    query = f"""
           WITH RECURSIVE product_list AS (
                SELECT p.id, p.parent_id
                FROM core.product p
  				where p.id = {field}
                UNION ALL
                SELECT t.id, t.parent_id
                FROM core.product t
                JOIN product_list ON t.parent_id = product_list.id
            ) 
            select id from product_list
        """
    return query

def get_accesss_by_role(config: dict, query: bool = True, type: str = "asset", user_permission:dict = {}) -> dict:

    # Check Discover Settings
    discover = config.get("dag_info", {}).get("settings", {}).get("discover", {})
    role_association = user_permission.get("association", [])
    associated_type = user_permission.get("associated_type", "full")
    domains = []
    applications = []
    tags = []
    products = []
    domains = [id.get("id")
                for id in role_association if id.get("type") == "Domain"] if discover.get("domain").get("is_active") else []
    tags = [id.get("id") for id in role_association if id.get("type") == "Tag"] if discover.get("tag").get("is_active") else []
    applications = [id.get("id") for id in role_association if id.get(
        "type") == "Application"] if discover.get("app").get("is_active") else []
    products = [id.get("id")
                for id in role_association if id.get("type") == "Product"] if discover.get("product").get("is_active") else []

    # Get Assocaited Values
    if domains:
        domains = get_assocaited_domains(config, domains)
    if tags:
        tags = get_assocaited_tags(config, tags)
    if products:
        products = get_assocaited_products(products)

    if query:
        if query:
            query_condition = "asset.id"
            if type == "attribute":
                query_condition = "attribute.id"
            elif type == "measure":
                query_condition = "measure.id"
            conditions = []
            if associated_type == "full":
                if domains:
                    conditions.append(
                        f""" domain_mapping.domain_id in({','.join(f"'{i}'" for i in domains)})""")
                if applications:
                    conditions.append(
                        f""" application_mapping.application_id in ({','.join(f"'{i}'" for i in applications)})""")
                if tags:
                    conditions.append(
                        f""" tags_mapping.tags_id in ({','.join(f"'{i}'" for i in tags)})""")
                if products:
                    conditions.append(
                        f""" product_mapping.product_id in({','.join(f"'{i}'" for i in products)})"""
                    )
            else:
                if domains:
                    condition = "domain_mapping.asset_id = asset.id"
                    if type == "measure":
                        condition = "domain_mapping.asset_id = asset.id or measure.id=domain_mapping.measure_id"
                    conditions.append(f"""
                        (not exists (select 1
                        from core.domain_mapping
                        where ({condition})
                        and domain_mapping.domain_id in ({','.join(f"'{i}'" for i in domains)})))
                    """)
                if products:
                    condition = "product_mapping.asset_id = asset.id"
                    if type == "measure":
                        condition = "product_mapping.asset_id = asset.id or measure.id=product_mapping.measure_id"
                    conditions.append(
                        f"""
                        (not exists (select 1
                        from core.product_mapping
                        where ({condition})
                        and product_mapping.product_id in ({','.join(f"'{i}'" for i in products)})))
                    """
                    )
                if applications:
                    condition = "application_mapping.asset_id = asset.id"
                    if type == "measure":
                        condition = "application_mapping.asset_id = asset.id or measure.id=application_mapping.measure_id"
                    conditions.append(f"""
                        (not exists (select 1
                        from core.application_mapping
                        where ({condition})
                        and application_mapping.application_id in ({','.join(f"'{i}'" for i in applications)})))
                    """)
                if tags:
                    if type != "asset":
                        condition = "tags_mapping.attribute_id=attribute.id"
                        conditions.append(f"""
                            (not exists (select 1
                            from core.tags_mapping
                            where {condition}
                            and tags_mapping.tags_id in ({','.join(f"'{i}'" for i in tags)})))
                        """)

            base_query = ""
            if conditions:
                conditions = f' and ({" or ".join(conditions)})' if associated_type == "full" else f' and ({" and ".join(conditions)})'
                if type == "measure":
                    base_query = f"""
                    select distinct {query_condition}
                    from core.measure
                    join core.connection on connection.id=measure.connection_id
                    left join core.asset on asset.id = measure.asset_id
                    left join core.attribute on attribute.asset_id = asset.id and attribute.id = measure.attribute_id
                    left join core.application_mapping on application_mapping.asset_id=asset.id or application_mapping.measure_id=measure.id
                    left join core.domain_mapping on domain_mapping.asset_id=asset.id or domain_mapping.measure_id=measure.id
                    left join core.tags_mapping on tags_mapping.attribute_id=attribute.id
                    where (attribute.id is null or attribute.is_selected=true) and (asset.id is null or asset.is_active=true)
                    and measure.is_delete=false
                    {conditions}
                """
                else:
                    base_query = f"""
                    select distinct {query_condition}
                    from core.asset
                    left join core.attribute on attribute.asset_id=asset.id
                    left join core.application_mapping on application_mapping.asset_id=asset.id
                    left join core.domain_mapping on domain_mapping.asset_id=asset.id
                    left join core.tags_mapping on tags_mapping.attribute_id=attribute.id
                    where (attribute.id is null or attribute.is_selected=true) and asset.is_active=true
                    {conditions}
                """

            return base_query
    else:
        return {
            "domain": domains,
            "application": applications,
            "tags": tags,
            "associated_type": associated_type,
            "product": products
        }

def get_assocaited_domains(config, domains: list) -> list:
    domains = [w for w in domains if w != "_show_blank_select_"]
    if not domains:
        return []
    domains = (f"""({','.join(f"'{w}'" for w in domains)})""") if domains else ""
    query = f"""
            with RECURSIVE domain_list AS
                (
                    SELECT d.id, d.parent_id
                    FROM core.domain d
                    where d.id in {domains}
                    union all
                    SELECT t.id, t.parent_id
                    FROM core.domain t
                    JOIN domain_list ON t.parent_id = domain_list.id
                )
               select distinct id from domain_list
            """
    connection = get_postgres_connection(config)
    domains = []
    with connection.cursor() as cursor:
        cursor.execute(query)
        domains = fetchall(cursor)
        domains = [str(domain.get("id")) for domain in domains]
    return domains


def get_assocaited_tags(config, tags: list) -> list:
    tags = f"""({','.join(f"'{w}'" for w in tags)})"""
    query = f"""select distinct id from core.tags where id in {tags} or parent_id in {tags}"""
    connection = get_postgres_connection(config)
    tags = []
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query)
        tags = fetchall(cursor)
        tags = [tag.get("id") for tag in tags]
    return tags

def get_assocaited_products(config, products: list) -> list:
    products = [w for w in products if w != "_show_blank_select_"]
    if not products:
        return []
    products = f"""({','.join(f"'{w}'" for w in products)})"""
    query = f"""
            with RECURSIVE product_list AS
                (
                    SELECT p.id, p.parent_id
                    FROM core.product p
                    where p.id in {products}
                    union all
                    SELECT t.id, t.parent_id
                    FROM core.product t
                    JOIN product_list ON t.parent_id = product_list.id
                )
                 select distinct id from product_list
        """

    connection = get_postgres_connection(config)
    products = []
    with connection.cursor() as cursor:
        cursor.execute(query)
        products = fetchall(cursor)
        products = [str(product.get("id")) for product in products]
    return products

def validate_report_attachment(file_name:str, config:dict):
    on_premise = os.environ.get("ON_PREMISE", False)
    if on_premise:
        file_name = f"./{file_name}"
    file_size = os.path.getsize(file_name)
    file_size_kb = file_size / 1024
    file_size = file_size_kb / 1024
    reporting = config.get("dag_info", {}).get("settings", {}).get("reporting", {})
    attachement_limit = reporting.get("attachement_limit", 5)
    attachement_limit = int(attachement_limit) if (attachement_limit) == str else attachement_limit
    return attachement_limit>=file_size


def parse_numeric_value(value):
    """
    Parsing numeric values by removing scientific notation,
    trailing zero's after decimal etc.,
    """
    if not value:
        return value
    try:
        source_type = type(value)
        if source_type == str:
            value = float(value) if value else value
        elif source_type == int:
            value = int(value) if value else 0
            return value
        elif source_type == float:
            value = float(value) if value else 0

        if re.match("^[+-]?[0-9]+.[0]+$", str(value)):
            value = int(value) if value else 0

        if "e" in str(value).lower():
            value = Decimal(format(value, ".9f"))
            if re.match("^[+-]?[0-9]+.[0]+$", str(value)):
                value = int(value)

        if source_type == str:
            value = str(value)
        return value
    except:
        return value


def format_masking_data(is_exception:bool, value:str):
    if is_exception:
        if type(value) == datetime:
            value = value.strftime("%m-%d-%Y %I:%M:%S")
        elif type(value) == Decimal:
            value = (
                str(value).rstrip("0").rstrip(".")
                if "." in str(value)
                else str(value)
            )
        value = parse_numeric_value(value)
    return value


def prepare_report_masking_data(config:dict, data:list, assets:list, role_id:str, is_exception=True):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            mask_query = f"select * from core.roles_permission where role_id='{role_id}' and feature_id='311b3bff-b914-4252-9d8c-4f5619e0cfda' and is_on=false"
            cursor.execute(mask_query)
            mask_permission = fetchone(cursor)
            if mask_permission:
                assets = [asset.get("id") for asset in assets if is_valid_uuid(asset.get("id"))] if assets else []
                assets = f"""({','.join(f"'{w}'" for w in assets)})""" if assets else ""
                asset_filter = f" and attribute.asset_id in {assets}" if assets else ""
                attribute_query = f"""
                    select attribute.name from core.attribute
                    join core.tags_mapping on tags_mapping.attribute_id=attribute.id
                    join core.tags on tags.id=tags_mapping.tags_id
                    where tags.is_active=true and tags.is_mask_data=true {asset_filter}
                """
                cursor.execute(attribute_query)
                attributes = fetchall(cursor)
                attributes = [attribute.get("name").lower() for attribute in attributes]
                data = [
                    {
                        key: ("X" * len(str(format_masking_data(is_exception, value))) if key.lower() in attributes else value)
                        for key, value in item.items()
                    }
                    for item in data
                ]
        return data
    except Exception as e:
        raise ValueError(e)

def is_valid_uuid(value):
    try:
        uuid.UUID(value)
        return True
    except (ValueError, TypeError):
        return False
