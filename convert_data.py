#!/usr/bin/env python3
"""
达人看板数据转换脚本
将双轨合并 Excel 文件转换为 JSON 数据文件，供 HTML 看板使用

输入: 双轨合并本地版.xlsx（3个sheet）
输出:
  - data_history.json   历史在投数据
  - data_camp.json      创造营数据  
  - data_extra.json     销售补充新增数据
"""

import pandas as pd
import json
import os
from datetime import datetime

# === 配置 ===
INPUT_FILE = '/Users/leahchen/Desktop/达人看板搭建/双轨合并本地版.xlsx'
OUTPUT_DIR = '/Users/leahchen/WorkBuddy/20260420173826/data'

# 共同字段（15个）- 用于浅层展示和筛选
SHARED_FIELDS = [
    '达人来源',
    '等级(A/B/C/D)',
    '流转策略',
    '对接销售',           # 历史在投用这个字段名
    '最终销售归属',       # 创造营用这个字段名
    '销售归属',           # 统一后的字段名
    '达人IP',
    '视频号账号/直播间',
    '达人标签',
    '粉丝画像',
    '合作类型',
    '带货类型',
    '图书类目倾向',
    '课程类目倾向',
    '历史/意向带货单价',
    '撮合状态',
    '撮合客户',
]

# 历史在投独有字段
HISTORY_ONLY_FIELDS = [
    '达人类型',
    '客户简称',
    '课程阶段',
    'ADQ投放状态',
    '课程细分方向',
    '近30天日耗',
    'Q1日耗',
    '半年日耗',
    '25年至今日耗',
]

# 创造营独有字段
CAMP_ONLY_FIELDS = [
    '地区',
    '微信昵称',
    '是否有MCN',
    'MCN名称',
    '视频号带货品类赛道',
    '现视频号品类销售额（月）-短视频',
    '现视频号品类销售额（月）-直播',
    '视频号粉丝数量',
    '最近3个月日常短视频更新频率',
    '最近3个月日常直播频率',
]


def clean_value(val):
    """清理数据值：处理 NaN、空值等"""
    if pd.isna(val):
        return ''
    if isinstance(val, (int, float)):
        if val == int(val):
            return int(val)
        return val
    return str(val).strip()


def process_row(row, sheet_type):
    """处理单行数据，返回标准化字典"""
    result = {}
    
    # 处理共同字段
    # 销售归属需要根据 sheet 类型取不同字段名
    if sheet_type == 'history':
        sales_field = '对接销售'
    else:
        sales_field = '最终销售归属'
    
    shared_mapping = {
        '达人来源': '达人来源',
        '等级': '等级(A/B/C/D)',
        '流转策略': '流转策略',
        '销售归属': sales_field,
        '达人IP': '达人IP',
        '视频号账号': '视频号账号/直播间',
        '达人标签': '达人标签',
        '粉丝画像': '粉丝画像',
        '合作类型': '合作类型',
        '带货类型': '带货类型',
        '图书类目倾向': '图书类目倾向',
        '课程类目倾向': '课程类目倾向',
        '带货单价': '历史/意向带货单价',
        '撮合状态': '撮合状态',
        '撮合客户': '撮合客户',
    }
    
    for key, orig_key in shared_mapping.items():
        if orig_key in row.index:
            result[key] = clean_value(row[orig_key])
        else:
            result[key] = ''
    
    return result


def process_sheet(df, sheet_type, source_name):
    """处理整个 sheet 数据"""
    records = []
    
    for idx, row in df.iterrows():
        record = {
            '_id': idx + 1,  # 行号作为唯一标识
            '_source': source_name,  # 来源标记
            '_sheet_type': sheet_type,  # history/camp/extra
        }
        
        # 添加共同字段（使用统一的中文字段名）
        if sheet_type in ('history', 'extra'):
            sales_col = '对接销售'
        else:
            sales_col = '最终销售归属'
        
        # 标准化共同字段名
        record['达人来源'] = clean_value(row.get('达人来源', ''))
        record['等级'] = clean_value(row.get('等级(A/B/C/D)', ''))
        record['流转策略'] = clean_value(row.get('流转策略', ''))
        record['销售归属'] = clean_value(row.get(sales_col, ''))
        record['达人IP'] = clean_value(row.get('达人IP', ''))
        record['视频号账号'] = clean_value(row.get('视频号账号/直播间', ''))
        record['达人标签'] = clean_value(row.get('达人标签', ''))
        record['粉丝画像'] = clean_value(row.get('粉丝画像', ''))
        record['合作类型'] = clean_value(row.get('合作类型', ''))
        record['带货类型'] = clean_value(row.get('带货类型', ''))
        record['图书类目倾向'] = clean_value(row.get('图书类目倾向', ''))
        record['课程类目倾向'] = clean_value(row.get('课程类目倾向', ''))
        record['带货单价'] = clean_value(row.get('历史/意向带货单价', ''))
        record['撮合状态'] = clean_value(row.get('撮合状态', ''))
        record['撮合客户'] = clean_value(row.get('撮合客户', ''))
        
        # 全部原始字段（用于详情展开）
        all_fields = {}
        for col in df.columns:
            all_fields[col] = clean_value(row[col])
        record['_all_fields'] = all_fields
        
        records.append(record)
    
    return records


def extract_filter_options(records):
    """从记录中提取筛选选项值"""
    options = {
        '等级': set(),
        '流转策略': set(),
        '达人标签': set(),
        '合作类型': set(),
        '带货类型': set(),
        '撮合状态': set(),
    }
    
    for r in records:
        for key in options:
            val = r.get(key, '')
            if val:
                # 支持多值分割（如逗号、顿号分隔）
                separators = [',', '、', '/', '|', '；', ';']
                multi_values = [val]
                for sep in separators:
                    if sep in val:
                        multi_values = [v.strip() for v in val.split(sep)]
                        break
                for v in multi_values:
                    if v:
                        options[key].add(v)
    
    # 转为排序列表
    return {k: sorted(list(v)) for k, v in options.items()}


def main():
    print("=" * 50)
    print("达人看板 - 数据转换脚本")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 读取 Excel 所有 sheet
    print(f"\n📂 读取文件: {INPUT_FILE}")
    all_sheets = pd.read_excel(INPUT_FILE, sheet_name=None)
    print(f"   发现 {len(all_sheets)} 个 sheet: {list(all_sheets.keys())}")
    
    # ===== Sheet 1: 历史在投299个 =====
    if '历史在投299个' in all_sheets:
        df_history = all_sheets['历史在投299个']
        print(f"\n📊 处理「历史在投299个」: {len(df_history)} 条记录")
        history_records = process_sheet(df_history, 'history', '历史沉淀')
        history_options = extract_filter_options(history_records)
        
        output_path = os.path.join(OUTPUT_DIR, 'data_history.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({
                'meta': {
                    'source': '历史在投299个',
                    'count': len(history_records),
                    'updated_at': datetime.now().isoformat()
                },
                'options': history_options,
                'data': history_records
            }, f, ensure_ascii=False, indent=2)
        print(f"   ✅ 输出: {output_path} ({len(history_records)} 条)")
    
    # ===== Sheet 2: 创造营662个 =====
    if '创造营662个' in all_sheets:
        df_camp = all_sheets['创造营662个']
        print(f"\n📊 处理「创造营662个」: {len(df_camp)} 条记录")
        camp_records = process_sheet(df_camp, 'camp', '创造营')
        camp_options = extract_filter_options(camp_records)
        
        output_path = os.path.join(OUTPUT_DIR, 'data_camp.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({
                'meta': {
                    'source': '创造营662个',
                    'count': len(camp_records),
                    'updated_at': datetime.now().isoformat()
                },
                'options': camp_options,
                'data': camp_records
            }, f, ensure_ascii=False, indent=2)
        print(f"   ✅ 输出: {output_path} ({len(camp_records)} 条)")
    
    # ===== Sheet 3: 销售补充新增 =====
    if '销售补充新增' in all_sheets:
        df_extra = all_sheets['销售补充新增']
        print(f"\n📊 处理「销售补充新增」: {len(df_extra)} 条记录")
        extra_records = process_sheet(df_extra, 'extra', '销售新增')
        extra_options = extract_filter_options(extra_records)
        
        output_path = os.path.join(OUTPUT_DIR, 'data_extra.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({
                'meta': {
                    'source': '销售补充新增',
                    'count': len(extra_records),
                    'updated_at': datetime.now().isoformat()
                },
                'options': extra_options,
                'data': extra_records
            }, f, ensure_ascii=False, indent=2)
        print(f"   ✅ 输出: {output_path} ({len(extra_records)} 条)")
    
    print("\n" + "=" * 50)
    print("✅ 数据转换完成！")
    print(f"输出目录: {OUTPUT_DIR}")
    print("=" * 50)


if __name__ == '__main__':
    main()
