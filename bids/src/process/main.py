#/BDSP/bids_app/src/process/main.py
import json
import logging
import os
from pathlib import Path
from process.components import mss, origin, export
from utils import common

logger = logging.getLogger(__name__)

# 전역 변수를 저장할 딕셔너리
global_vars = {}

def __init__():
    """초기화 함수"""
    logger.info("Process module initialized")

def load_json_config(json_file_path):
    """JSON 설정 파일 로드"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"JSON 설정 파일 로드 성공: {json_file_path}")
        return config
    except Exception as e:
        logger.error(f"JSON 설정 파일 로드 실패: {e}")
        raise

def remove_whitespace_from_dict(data):
    """딕셔너리의 모든 문자열 값에서 공백 제거 (재귀적으로 처리)"""
    if isinstance(data, dict):
        return {key: remove_whitespace_from_dict(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [remove_whitespace_from_dict(item) for item in data]
    elif isinstance(data, str):
        return common.remove_all_whitespace(data)
    else:
        return data

def validate_and_initialize_config(config):
    """Step 0: JSON 변수 정리 및 초기화"""
    logger.info("Step 0: 설정 검증 및 초기화")
    
    try:
        # 필수 필드 확인 (uploadTime 추가)
        required_fields = ['user', 'systemId', 'projectCode', 'projectSeq', 'orgId', 'trialIndex', 'uploadTime','subjectId']
        missing_fields = []
        
        for field in required_fields:
            if field not in config:
                missing_fields.append(field)
        
        if missing_fields:
            logger.error(f"누락된 필수 필드: {missing_fields}")
            raise Exception(f"누락된 필수 필드: {missing_fields}")
                
        # 선택적 필드 초기화
        if 'domain' not in config or not config['domain']:
            config['domain'] = "DATA"
            print("Domain 필드를 기본값으로 초기화")
        
        # bodyPart 필드 초기화 (기본값: BRAIN)
        if 'bodyPart' not in config or not config['bodyPart']:
            config['bodyPart'] = "BRAIN"
            print("BodyPart 필드를 기본값으로 초기화: BRAIN")
        else:
            print(f"Body Part: {config['bodyPart']}")
        
        # category 필드 초기화 (기본값: IMAGE)
        if 'category' not in config or not config['category']:
            config['category'] = "IMAGE"
            print("Category 필드를 기본값으로 초기화: IMAGE")
        else:
            print(f"Category: {config['category']}")
        
        # public 필드 초기화 (기본값: False)
        if 'public' not in config or config['public'] is None:
            config['public'] = False
            print("Public 필드를 기본값으로 초기화: False")
        else:
            print(f"Public: {config['public']}")
        
        # task 필드 초기화 (기본값: isFunc: False, option: "")
        if 'task' not in config:
            config['task'] = {
                "isFunc": False,
                "option": ""
            }
            print("Task 설정을 기본값으로 초기화")
        else:
            # task 필드 검증
            task = config['task']
            if 'isFunc' not in task:
                task['isFunc'] = False
            if 'option' not in task:
                task['option'] = ""
            print(f"Task 설정 검증 완료 - isFunc: {task['isFunc']}")
        
        if 'flag' not in config:
            config['flag'] = []
            print("Flag 배열을 빈 배열로 초기화")
        else:
            # flag 배열 검증 및 초기화
            print(f"{len(config['flag'])}개의 flag 항목 발견")
            for i, flag_item in enumerate(config['flag']):
                if 'process' not in flag_item:
                    flag_item['process'] = ""
                if 'enabled' not in flag_item:
                    flag_item['enabled'] = False
                if 'options' not in flag_item:
                    flag_item['options'] = ""
                print(f"Flag {i+1}: {flag_item['process']} (활성화: {flag_item['enabled']})")
        
        # 프로젝트 시퀀스를 문자열로 변환 (경로 생성시 일관성 위해)
        config['projectSeq'] = str(config['projectSeq'])
        
        # structured_config 생성 - request 딕셔너리에 bodyPart, category, public 포함
        structured_config = {
            'request': {
                'user': config['user'],
                'systemId': config['systemId'],
                'projectCode': config['projectCode'],
                'projectSeq': config['projectSeq'],
                'orgId': config['orgId'],
                'trialIndex': config['trialIndex'],
                'uploadTime': config['uploadTime'],
                'domain': config['domain'],
                'bodyPart': config['bodyPart'],  
                'category': config['category'],
                'public': config['public'],
                'subjectId': config['subjectId']
            },
            'task': config['task'],
            'flag': {
                'processes': config['flag'],
                'enabled_count': sum(1 for flag in config['flag'] if flag.get('enabled', False)),
                'enabled_processes': [flag['process'] for flag in config['flag'] if flag.get('enabled', False)]
            }
        }
        
        # 모든 딕셔너리 문자열 값에서 공백 제거
        structured_config = remove_whitespace_from_dict(structured_config)
        
        logger.info("설정 검증 및 초기화가 성공적으로 완료됨")
        print("모든 설정 변수가 검증 및 초기화됨 (공백 제거 완료)")
        
        return structured_config
        
    except Exception as e:
        logger.error(f"설정 검증 및 초기화 실패: {e}")
        raise

def update_paths_after_step(paths, step_name, **step_paths):
    """각 스텝 완료 후 경로 정보를 업데이트"""
    if step_name not in paths:
        paths[step_name] = {}
    
    paths[step_name].update(step_paths)
    
    logger.info(f"{step_name} 경로 정보 업데이트 완료")
    for key, value in step_paths.items():
        print(f"  {key}: {value}")
    
    return paths

def process_flags(structured_config, paths):
    """Step 7: process flag 처리 (조건부 실행)"""
    logger.info("Step 7: Flag 처리")
    
    flag = structured_config['flag']
    
    if flag['enabled_count'] == 0:
        logger.info("처리할 활성화된 flag가 없음")
        return
    
    try:
        print(f"{flag['enabled_count']}개의 활성화된 프로세스 처리: {', '.join(flag['enabled_processes'])}")
        
        for flag_item in flag['processes']:
            if flag_item.get('enabled', False):
                process_name = flag_item.get('process', '')
                options = flag_item.get('options', '')
                
                logger.info(f"Flag 처리: {process_name}, 옵션: {options}")
                print(f"{process_name} 프로세스 실행 중, 옵션: {options}")
                
                # 각 프로세스별 처리 로직
                if process_name == 'defacing':
                    print("defacing 알고리즘 실행 중...")
                elif process_name == 'canonical':
                    print("canonical orientation 프로세스 실행 중...")
                elif process_name == 'normalization':
                    print("normalization 프로세스 실행 중...")
                else:
                    print(f"알 수 없는 프로세스: {process_name}")
        
        logger.info("Flag 처리 완료")
    except Exception as e:
        logger.error(f"Step 7 - Flag 처리 실패: {e}")
        raise

def main(json_file_path, upload_dir=None, backup_dir=None, error_dir=None, working_dir=None,
         dicom_modality=None, nifti_modality=None, parrec_modality=None, suffix_map=None,
         flag_dir=None):
    """JSON 파일을 처리하는 메인 함수"""
    global global_vars
    
    __init__()
    
    # app.py에서 전달받은 모든 변수들을 global_vars 딕셔너리에 저장
    global_vars = {
        'json_file_path': json_file_path,
        'upload_dir': upload_dir,
        'backup_dir': backup_dir,
        'error_dir': error_dir,
        'working_dir': working_dir,
        'dicom_modality': dicom_modality,
        'nifti_modality': nifti_modality,
        'parrec_modality': parrec_modality,
        'suffix_map': suffix_map,
        'flag_dir': flag_dir
    }
    
    logger.info(f"JSON 파일 처리 시작: {json_file_path}")
    logger.info(f"전역 변수 저장 완료: {len(global_vars)}개 변수")
    print(f"전역 변수들이 global_vars 딕셔너리에 저장됨:")
    for key, value in global_vars.items():
        if value is not None:
            print(f"  {key}: {value}")
    
    try:
        # 모든 경로 정보를 저장할 딕셔너리 초기화
        paths = {}
        
        # JSON 설정 로드
        config = load_json_config(json_file_path)
        
        # Step 0: 변수 정리 및 초기화
        structured_config = validate_and_initialize_config(config)
        
        # Step 1: MSS 구조 생성
        mss_path = mss.create_mss_structure(structured_config, global_vars)
        mss_state_path = os.path.join(mss_path, "state")
        paths = update_paths_after_step(paths, "step1_mss", 
                                      mss_path=mss_path,
                                      mss_state_path=mss_state_path)
        
        
        # Step 2: origin 경로 생성 (origin.py에서 처리)
        origin_path = origin.create_origin_path(structured_config, global_vars, mss_path)
        origin_zip_path = os.path.join(origin_path,"zip")
        origin_unzip_path = os.path.join(origin_path,"unzip")
        paths = update_paths_after_step(paths, "step2_origin",
                                      origin_path=origin_path,
                                      origin_zip_path=origin_zip_path,
                                      origin_unzip_path=origin_unzip_path)
        
        # Step 3,4,5: domain에 따른 source/raw/thumbnail 생성 (domain별 모듈에서 처리)
        domain = structured_config['request']['domain'].upper()
        logger.info(f"Step 3: Domain '{domain}'에 따른 source 처리")
        
        try:
            if domain == "MRI" or domain == "DATA" or domain == "CT":
                # MRI 또는 DATA 도메인인 경우 MRI 모듈 사용
                from process.components.domain.mri.source import source as mri_source
                from process.components.domain.mri.raw import raw as mri_raw
                from process.components.domain.mri.post import thumbnail as mri_thumbnail
                # source_path로 받아서 개별 변수로 저장
                source_path = mri_source.create_source_path(structured_config, mss_path, origin_unzip_path)
                paths = update_paths_after_step(paths, "step3_source",
                            source_path=source_path)
                
                logger.info(f"Step 4: Domain '{domain}'에 따른 raw 처리")
                raw_path = mri_raw.create_raw_path(structured_config,source_path,global_vars)
                paths = update_paths_after_step(paths,"step4_raw",
                            raw_path=raw_path)
                
                logger.info(f"Step 5-1: Domain '{domain}' 후처리: Thumbnail 생성")
                thumbnail_path = mri_thumbnail.thumbnail(raw_path)
                paths = update_paths_after_step(paths, "step5_thumbnail",
                            thumbnail_path=thumbnail_path)
                print(f"MRI 도메인 source 처리 완료 (Domain: {domain})")
                
            elif domain == "PET":
                # CT 도메인인 경우 CT 모듈 사용
                from process.components.domain.pet import source as pet_source
                from process.components.domain.pet import raw as pet_raw
                from process.components.domain.pet import thumbnail as pet_thumbnail
                print(f"PET 도메인 source 처리 완료")
                
            else:
                # 지원되지 않는 도메인인 경우 에러 처리
                logger.error(f"지원되지 않는 도메인: {domain}")
                raise Exception(f"지원되지 않는 도메인: {domain}")
            
        except ImportError as e:
            logger.error(f"도메인 '{domain}' 모듈에서 에러: {e}")
            raise Exception(f"도메인 '{domain}' 모듈에서 에러: {e}")
            
        except Exception as e:
            logger.error(f"Domain '{domain}'처리 실패: {e}")
            raise
        
       
        # Step 6: Export JSON 생성 (export.py에서 처리)
        #export_path = export.create_export(structured_config, paths)
        
        # Step 6 완료 후 경로 정리
        #paths = update_paths_after_step(paths, "step6_export",
        #                              export_path=export_path)
        
        # Step 7: Process flags (조건부)
        #process_flags(structured_config, paths)
        
        # 최종 경로 정보 출력
        print("\n=== 최종 경로 정보 요약 ===")
        for step, step_paths in paths.items():
            print(f"{step}:")
            for path_name, path_value in step_paths.items():
                print(f"  {path_name}: {path_value}")
        
        logger.info("모든 처리 단계가 성공적으로 완료됨")
        return paths
        
    except Exception as e:
        logger.error(f"처리 실패: {e}")
        raise