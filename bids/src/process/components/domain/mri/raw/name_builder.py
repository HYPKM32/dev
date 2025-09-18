import json
import os
import glob
import logging
import pydicom
from utils.common import zero_fill

logger = logging.getLogger(__name__)

def create_bids_mapping(path_mapping, structured_config, global_vars, raw_path):
    """
    소스 데이터 경로를 BIDS 형식 경로로 매핑하는 함수
    
    Args:
        path_mapping (dict): 소스 폴더 경로와 모달리티 매핑
        structured_config (dict): 프로젝트 설정 정보
        global_vars (dict): 전역 변수 (suffix_map 경로 포함)
        raw_path (str): BIDS rawdata 기본 경로
    
    Returns:
        dict: 소스 폴더 경로와 BIDS 형식 파일 경로 매핑
    """
    
    # 1. suffix_map JSON 파일 로드
    try:
        with open(global_vars['suffix_map'], 'r') as f:
            suffix_rules = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load suffix_map: {e}")
        raise
    
    # 2. 기본 정보 추출
    request_info = structured_config['request']
    task_info = structured_config['task']
    
    subject_id = request_info['subjectId']
    trial_index = zero_fill(request_info['trialIndex'])
    
    # 3. 결과 매핑 딕셔너리
    bids_mapping = {}
    
    # 4. 각 소스 폴더에 대해 BIDS 파일명 생성
    for source_folder, modality in path_mapping.items():
        # 모달리티에 해당하는 데이터 타입 찾기
        data_type = find_data_type(modality, suffix_rules)
        if not data_type:
            logger.error(f"Unknown modality: {modality}")
            raise ValueError(f"Unknown modality: {modality}")
        
        # BIDS 엔티티 규칙 가져오기
        entity_rules = suffix_rules[data_type][modality]
        
        # BIDS 파일명 생성
        bids_filename = build_bids_filename(
            source_folder, 
            modality, 
            entity_rules, 
            subject_id, 
            trial_index, 
            task_info, 
            raw_path,
            suffix_rules
        )
        
        # 최종 BIDS 경로 생성
        bids_path = os.path.join(
            raw_path, 
            f"sub-{subject_id}", 
            f"ses-{trial_index}", 
            data_type, 
            bids_filename
        )
        
        bids_mapping[source_folder] = bids_path
    
    return bids_mapping


def find_data_type(modality, suffix_rules):
    """모달리티에 해당하는 데이터 타입 찾기"""
    for data_type, modalities in suffix_rules.items():
        if modality in modalities:
            return data_type
    return None


def build_bids_filename(source_folder, modality, entity_rules, subject_id, trial_index, task_info, raw_path, suffix_rules):
    """BIDS 파일명 생성"""
    
    # 기본 엔티티들
    entities = []
    
    # 1. subject (무조건 포함)
    entities.append(f"sub-{subject_id}")
    
    # 2. session (무조건 포함)  
    entities.append(f"ses-{trial_index}")
    
    # 3. task (func 모달리티인 경우만)
    if 'task' in entity_rules:
        if not task_info['isFunc']:
            raise ValueError(f"Task entity required for {modality} but isFunc=False")
        task_value = task_info['option']
        if not task_value:
            raise ValueError(f"Task option is empty for {modality}")
        entities.append(f"task-{task_value}")
    
    # 4. acq (해당하는 경우만)
    if 'acq' in entity_rules:
        entities.append("acq-%u")
    
    # 5. dir (PhaseEncodingDirection이 있는 경우만)
    if 'dir' in entity_rules:
        phase_encoding_dir = get_phase_encoding_direction(source_folder)
        if phase_encoding_dir:
            entities.append(f"dir-{phase_encoding_dir}")
    
    # 6. run (무조건 포함)
    run_number = get_next_run_number(source_folder, modality, subject_id, trial_index, raw_path, suffix_rules)
    entities.append(f"run-{run_number}")
    
    # 7. 모달리티 suffix
    entities.append(modality)
    
    # 최종 파일명 생성 (확장자는 .nii.gz로 가정)
    filename = "_".join(entities) + ".nii.gz"
    
    return filename


def get_phase_encoding_direction(source_folder):
    """PhaseEncodingDirection 헤더 값 추출"""
    try:
        # DICOM 파일 확인
        dicom_pattern = os.path.join(source_folder, "*0001.dcm")
        dicom_files = glob.glob(dicom_pattern)
        
        if dicom_files:
            # DICOM에서 PhaseEncodingDirection 추출
            ds = pydicom.dcmread(dicom_files[0])
            if hasattr(ds, 'PhaseEncodingDirection'):
                return ds.PhaseEncodingDirection
        
        # PARREC 파일 확인
        par_file = os.path.join(source_folder, "0001.par")
        if os.path.exists(par_file):
            # PAR 파일에서 PhaseEncodingDirection 추출 (구현 필요)
            # TODO: PAR 파일 파싱 로직 추가
            pass
            
    except Exception as e:
        logger.warning(f"Failed to get PhaseEncodingDirection from {source_folder}: {e}")
    
    return None


def get_next_run_number(source_folder, modality, subject_id, trial_index, raw_path, suffix_rules):
    """다음 run 번호 계산"""
    
    # suffix_rules에서 데이터 타입 찾기
    data_type = find_data_type(modality, suffix_rules)
    if not data_type:
        logger.warning(f"Unknown modality for run calculation: {modality}")
        return "01"
    
    # 대상 폴더 경로
    target_dir = os.path.join(
        raw_path,
        f"sub-{subject_id}",
        f"ses-{trial_index}",
        data_type
    )
    
    # 폴더가 존재하지 않으면 run-01
    if not os.path.exists(target_dir):
        return "01"
    
    # 기존 run 번호들 찾기
    pattern = os.path.join(target_dir, f"*run-*.nii.gz")
    existing_files = glob.glob(pattern)
    
    if not existing_files:
        return "01"
    
    # run 번호 추출
    run_numbers = []
    for file_path in existing_files:
        filename = os.path.basename(file_path)
        # run-XX 패턴 찾기
        parts = filename.split('_')
        for part in parts:
            if part.startswith('run-'):
                try:
                    run_num = int(part.split('-')[1])
                    run_numbers.append(run_num)
                except ValueError:
                    continue
    
    # 다음 run 번호 계산
    if run_numbers:
        next_run = max(run_numbers) + 1
    else:
        next_run = 1
    
    return zero_fill(next_run)