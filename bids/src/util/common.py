import os
import json
import re


def camel2snake(name: str) -> str:
    """camelCase를 snake_case로 변환"""
    s1 = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


def bdsp_walk(target_folder: str, output_filename: str = "bdsp_file_list.json"):
    """폴더를 스캔하여 파일 경로를 JSON으로 저장"""
    file_paths = []
    
    for root, dirs, files in os.walk(target_folder):
        for file in files:
            file_paths.append(os.path.join(root, file))
    
    result = {"path": file_paths}
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    return result


def zero_fill(num: int) -> str:
    """정수를 두자리 zerofilling한 후 string으로 변경"""
    return f"{num:02d}"