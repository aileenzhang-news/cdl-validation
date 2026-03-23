#!/usr/bin/env python3
"""
创建测试用的CDL子集

从完整的CDL Excel中提取特定时间范围的数据，用于快速测试
"""

import pandas as pd
import sys
from pathlib import Path

def create_test_cdl(
    input_file: str = "../subscription_transaction_fct.xlsx",
    output_file: str = "../subscription_transaction_fct_2026_feb_mar.xlsx",
    start_date: str = "2026-02-01",
    end_date: str = "2026-03-31"
):
    """
    从完整CDL中提取时间范围子集

    Args:
        input_file: 输入的完整CDL文件
        output_file: 输出的测试CDL文件
        start_date: 开始日期
        end_date: 结束日期
    """

    print("=" * 80)
    print("创建测试CDL子集")
    print("=" * 80)

    # 读取完整CDL
    print(f"\n1. 读取完整CDL: {input_file}")
    cdl = pd.read_excel(input_file)
    cdl['report_date'] = pd.to_datetime(cdl['report_date'])

    print(f"   原始数据: {len(cdl):,} 行")
    print(f"   日期范围: {cdl['report_date'].min().date()} 到 {cdl['report_date'].max().date()}")

    # 过滤时间范围
    print(f"\n2. 过滤时间范围: {start_date} 到 {end_date}")
    mask = (
        (cdl['report_date'] >= start_date) &
        (cdl['report_date'] <= end_date)
    )

    test_cdl = cdl[mask].copy()

    print(f"   过滤后: {len(test_cdl):,} 行")
    print(f"   唯一日期: {test_cdl['report_date'].nunique()}")

    if len(test_cdl) == 0:
        print("\n⚠️  警告: 过滤后没有数据!")
        print("   请检查时间范围是否正确")
        return 1

    # 显示统计
    print(f"\n3. 数据统计:")
    if 'masthead' in test_cdl.columns:
        print(f"   mastheads: {test_cdl['masthead'].nunique()}")
        top_mastheads = test_cdl['masthead'].value_counts().head(5)
        for mh, count in top_mastheads.items():
            print(f"     - {mh}: {count} 行")

    # 关键指标汇总
    print(f"\n4. 关键指标汇总:")
    metrics = [
        'subscription_count',
        'acquisition_count',
        'cancellation_count',
        'TotalAcquisition',
        'NetAcquisition'
    ]

    for metric in metrics:
        if metric in test_cdl.columns:
            total = test_cdl[metric].sum()
            print(f"   {metric}: {total:,.0f}")

    # 保存
    print(f"\n5. 保存测试CDL: {output_file}")
    test_cdl.to_excel(output_file, index=False)

    print(f"   ✓ 已保存: {output_file}")
    print(f"   ✓ 行数: {len(test_cdl):,}")
    print(f"   ✓ 列数: {len(test_cdl.columns)}")

    print("\n" + "=" * 80)
    print("✅ 完成!")
    print("=" * 80)

    print(f"\n下一步:")
    print(f"1. 从BigQuery导出source数据 (2026-02-01 到 2026-03-31)")
    print(f"   使用: EXPORT_QUERIES_2026_FEB_MAR.sql")
    print(f"2. 保存到: source_data_test/")
    print(f"3. 运行验证:")
    print(f"   python3 validate_from_source.py \\")
    print(f"     --cdl {output_file} \\")
    print(f"     --source-data source_data_test \\")
    print(f"     --validate-all")

    return 0


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='从完整CDL创建测试子集'
    )

    parser.add_argument(
        '--input',
        default='../subscription_transaction_fct.xlsx',
        help='输入CDL文件'
    )

    parser.add_argument(
        '--output',
        default='../subscription_transaction_fct_2026_feb_mar.xlsx',
        help='输出测试CDL文件'
    )

    parser.add_argument(
        '--start-date',
        default='2026-02-01',
        help='开始日期 (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        default='2026-03-31',
        help='结束日期 (YYYY-MM-DD)'
    )

    args = parser.parse_args()

    sys.exit(create_test_cdl(
        input_file=args.input,
        output_file=args.output,
        start_date=args.start_date,
        end_date=args.end_date
    ))
