#/BDSP/bids_app/src/utils/common.py
import os
import json
import re
from pathlib import Path


def camel2snake(name: str) -> str:
    """camelCase를 snake_case로 변환"""
    s1 = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


import os
import json

def bdsp_walk(target_folder: str, output_filename: str = "bdsp_file_list.json"):
    """폴더를 스캔하여 파일 경로를 JSON으로 저장
       단, 'bdsp'로 시작하고 '.json' 확장자인 파일은 제외"""
    file_paths = []
    index = 1
    
    for root, dirs, files in os.walk(target_folder):
        for file in files:
            # 제외 조건: 파일명이 'bdsp'로 시작하고 .json으로 끝나는 경우
            if file.lower().startswith("bdsp") and file.lower().endswith(".json"):
                continue

            file_path = os.path.join(root, file)
            file_paths.append({
                "index": index,
                "file_path": file_path
            })
            index += 1
    
    result = {"path": file_paths}
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    return result



def zero_fill(num) -> str:
    """정수나 문자열을 두자리 zerofilling한 후 string으로 변경"""
    try:
        return f"{int(num):02d}"
    except ValueError:
        return str(num).zfill(2)  # 숫자가 아닌 문자열은 그냥 zfill 사용


def bdsp_path_maker(path: str) -> bool:
    """경로가 존재하는지 확인하고, 존재하지 않으면 폴더 생성"""
    if os.path.exists(path):
        print(f"경로가 이미 존재합니다: {path}")
        return False  # 이미 존재하므로 생성하지 않음
    else:
        try:
            os.makedirs(path, exist_ok=True)
            print(f"폴더를 생성했습니다: {path}")
            return True  # 새로 생성함
        except OSError as e:
            print(f"폴더 생성 중 오류 발생: {e}")
            return False
        
def remove_all_whitespace(text):
    return re.sub(r'\s+', '', text)

def separated_walk(separated_path, json_output_path):
    """분리된 파일들을 스캔하여 파일명의 index 기준으로 JSON 생성
    
    파일명 패턴: item_{set_id}_{index}{ext} (index는 가변 자리수)
    """
    separated_path = Path(separated_path)
    entries = []
    
    # item_{set_id}_{숫자}{ext} 패턴 매칭 (숫자 자리수 제한 없음)
    pattern = re.compile(r'item_.*?_(\d+)\.')
    
    for file_path in separated_path.iterdir():
        if file_path.is_file() and not file_path.name.endswith('.json'):
            match = pattern.search(file_path.name)
            if match:
                index = int(match.group(1))  # 01 -> 1, 0001 -> 1, 123 -> 123
                entries.append({
                    "index": index,
                    "file_path": str(file_path)  # 전체 경로 저장
                })
    
    # index 순서로 정렬
    entries.sort(key=lambda x: x["index"])
    
    # JSON 생성
    data = {"path": entries}
    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)