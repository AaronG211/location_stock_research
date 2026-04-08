import pandas as pd
import numpy as np
import os

def build_msa_panel(zip_panel_path, crosswalk_path, output_path):
    print(f"正在加载纯净的 ZIP 宽表面板: {zip_panel_path}")
    zip_df = pd.read_csv(zip_panel_path, index_col=0, dtype=str)
    
    print(f"读取 HUD ZIP-to-CBSA 映射表: {crosswalk_path}")
    # 读取 HUD 官方数据
    mapping_df = pd.read_csv(crosswalk_path, dtype=str)
    
    # ------------------
    # 清洗和处理一对多的映射
    # ------------------
    # 由于一个邮编可能跨越多个都会区（CBSA），HUD文件给出了一对多。
    # 既然我们面对的是公司总部(Business)，我们要挑出在这个邮编内覆盖公司比例 (BUS_RATIO) 最大的那个唯一的 CBSA。
    print("正在处理邮编的跨区比例，提取权重最大的主要 CBSA (基于 BUS_RATIO)...")
    
    # 转换 BUS_RATIO 为浮点数用来排序
    mapping_df['BUS_RATIO'] = pd.to_numeric(mapping_df['BUS_RATIO'], errors='coerce').fillna(0)
    
    # 按照 ZIP 聚类，保留 BUS_RATIO 最大的那个 CBSA
    mapping_df = mapping_df.sort_values(by=['ZIP', 'BUS_RATIO'], ascending=[True, False])
    mapping_unique = mapping_df.drop_duplicates(subset=['ZIP'], keep='first')
    
    # 生成终极字典: { '10001': '31080', ... }
    zip_to_cbsa_dict = dict(zip(mapping_unique['ZIP'].str.strip(), mapping_unique['CBSA'].str.strip()))
    
    # ------------------
    # 大规模数据转换
    # ------------------
    print("生成超速映射字典并执行全表覆盖...")
    # 利用 Pandas 的 applymap 批量转换
    # 逻辑：如果 ZIP 能在字典里找到，就替换为对应的 CBSA 代码；否则如果是 99999(HUD对非都会区的标记) 或者找不到，就视情况保留为 NaN
    def get_msa(zip_code):
        if pd.isna(zip_code):
            return pd.NA
        zip_str = str(zip_code).strip()
        cbsa = zip_to_cbsa_dict.get(zip_str, pd.NA)
        
        # safely handle pd.NA
        if pd.isna(cbsa):
            return pd.NA
            
        # HUD 通常把非 CBSA 的乡村地区标记为 99999，在金融截面数据里最好将其转换为 NaN，代表不在大都会区
        if str(cbsa) == '99999':
            return pd.NA
        return cbsa
        
    # Pandas >= 2.1 推荐用 map
    msa_df = zip_df.map(get_msa)
    
    # 可选: 删除没有映射出任何都会区的列（如果有些公司一直在荒山野岭，CBSA全空）
    # print("清理全空的无效公司...")
    # msa_df = msa_df.dropna(axis=1, how='all')
    
    # ------------------
    # 保存结果
    # ------------------
    print(f"开始保存拥有同样时间切片维度 ({msa_df.shape[0]} 个月 x {msa_df.shape[1]} 家公司) 的全新 MSA 面板...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    msa_df.to_csv(output_path)
    
    print(f"✅ 转换完毕！完全等时间线的 MSA(CBSA) 宽表文件安全储存在：{output_path}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # 输入文件
    INPUT_ZIP_PANEL = os.path.join(BASE_DIR, '../../cleaned_data/monthly_zip_panel_144mo_cleaned.csv')
    
    # HUD 的官方数据集
    CROSSWALK_FILE = os.path.join(BASE_DIR, '../../raw_data/ZIP_MSA_122023.csv')
    
    # 输出文件
    OUTPUT_MSA_PANEL = os.path.join(BASE_DIR, '../../cleaned_data/monthly_msa_panel_144mo.csv')
    
    build_msa_panel(INPUT_ZIP_PANEL, CROSSWALK_FILE, OUTPUT_MSA_PANEL)
